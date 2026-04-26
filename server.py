#!/usr/bin/env python3
import json
import base64
import hashlib
import hmac
import mimetypes
import os
import secrets
import ssl
import smtplib
import sqlite3
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from email.message import EmailMessage
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # Optional dependency for PostgreSQL/Supabase.
    psycopg = None
    dict_row = None

try:
    import certifi
except ImportError:  # Optional dependency for robust TLS trust store.
    certifi = None


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "crm.sqlite3"
DIST = ROOT / "dist"
ACTIVE_DB_ENGINE = None
REFRESH_SESSIONS = {}
AI_CACHE = {}
AI_INFLIGHT = set()
AI_RATE_LIMIT = {}
AI_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()
REQUEST_RATE_LIMIT = {}
REQUEST_RATE_LOCK = threading.Lock()


def preferred_db_engine():
    if os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL"):
        return "postgres"
    return "sqlite"


def db_engine():
    global ACTIVE_DB_ENGINE
    if ACTIVE_DB_ENGINE is None:
        ACTIVE_DB_ENGINE = preferred_db_engine()
    return ACTIVE_DB_ENGINE


def auto_fallback_enabled():
    return str(os.environ.get("DB_AUTO_FALLBACK", "true")).strip().lower() not in ("0", "false", "no", "off")


def database_label():
    if db_engine() == "postgres":
        raw = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL", "postgres")
        if "@" in raw and "://" in raw:
            prefix, rest = raw.split("://", 1)
            auth, host_part = rest.split("@", 1)
            if ":" in auth:
                user, _ = auth.split(":", 1)
                return f"{prefix}://{user}:***@{host_part}"
        return raw
    return str(DB_PATH)


def db_available():
    return db_engine() != "postgres" or psycopg is not None


def q(sql):
    if db_engine() == "postgres":
        return sql.replace("?", "%s")
    return sql


def load_env():
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def supabase_url():
    return str(os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or "").strip().rstrip("/")


def supabase_anon_key():
    key = str(os.environ.get("SUPABASE_ANON_KEY") or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY") or "").strip()
    if not key or key.upper().startswith("YOUR_"):
        return ""
    return key


def supabase_auth_ready():
    return bool(supabase_url() and supabase_anon_key())


def build_account_profile(state, email, auth_user=None):
    users = state.get("users", []) if isinstance(state, dict) else []
    local_account = next((item for item in users if item.get("email", "").lower() == email.lower()), {})
    auth_user = auth_user or {}
    user_metadata = auth_user.get("user_metadata") or {}
    app_metadata = auth_user.get("app_metadata") or {}
    role = str(user_metadata.get("role") or app_metadata.get("role") or local_account.get("role") or "MANAGER").upper()
    if role not in ("ADMIN", "MANAGER", "SALES", "VIEWER"):
        role = "MANAGER"
    name = user_metadata.get("name") or user_metadata.get("full_name") or local_account.get("name") or email.split("@", 1)[0].replace(".", " ").title()
    return {
        "id": auth_user.get("id") or local_account.get("id") or email,
        "name": name,
        "email": email,
        "role": role,
        "phone": user_metadata.get("phone") or local_account.get("phone", ""),
        "active": True,
    }


def supabase_password_login(email, password):
    body = json.dumps({"email": email, "password": password}).encode("utf-8")
    request = urllib.request.Request(
        f"{supabase_url()}/auth/v1/token?grant_type=password",
        data=body,
        headers={
            "apikey": supabase_anon_key(),
            "authorization": f"Bearer {supabase_anon_key()}",
            "content-type": "application/json",
        },
        method="POST",
    )
    ssl_context = None
    if certifi:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with urllib.request.urlopen(request, timeout=8, context=ssl_context) as response:
            return json.loads(response.read().decode("utf-8")), None, 200
    except urllib.error.HTTPError as exc:
        detail = "Invalid email or password"
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            detail = payload.get("msg") or payload.get("error_description") or payload.get("error") or detail
        except Exception:
            pass
        return None, detail, exc.code
    except Exception as exc:
        app_log("Supabase auth failed", error=str(exc))
        return None, "Supabase login is unavailable right now.", 503


def auth_secret():
    return os.environ.get("JWT_ACCESS_SECRET", "jkcrm-dev-secret-change-me")


def refresh_secret():
    return os.environ.get("JWT_REFRESH_SECRET", auth_secret() + "-refresh")


def b64url_encode(value):
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def b64url_decode(value):
    padded = value + "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def sign_token(payload, secret):
    body = b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = b64url_encode(hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).digest())
    return f"{body}.{signature}"


def verify_token(token, secret):
    if not token or "." not in token:
        return None
    body, signature = token.split(".", 1)
    expected = b64url_encode(hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).digest())
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        payload = json.loads(b64url_decode(body).decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return None
    if int(payload.get("exp", 0)) < int(time.time()):
        return None
    return payload


def issue_access_token(user):
    now_ts = int(time.time())
    payload = {
        "sub": user.get("id"),
        "email": user.get("email"),
        "role": user.get("role"),
        "name": user.get("name"),
        "iat": now_ts,
        "exp": now_ts + int(os.environ.get("ACCESS_TOKEN_TTL_SECONDS", "900")),
    }
    return sign_token(payload, auth_secret())


def issue_refresh_token(user):
    now_ts = int(time.time())
    jti = secrets.token_hex(16)
    payload = {
        "sub": user.get("id"),
        "email": user.get("email"),
        "role": user.get("role"),
        "name": user.get("name"),
        "jti": jti,
        "iat": now_ts,
        "exp": now_ts + int(os.environ.get("REFRESH_TOKEN_TTL_SECONDS", "604800")),
    }
    token = sign_token(payload, refresh_secret())
    REFRESH_SESSIONS[token] = {"email": user.get("email"), "jti": jti, "exp": payload["exp"]}
    return token


def parse_bearer(headers):
    auth = str(headers.get("Authorization", "")).strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def db():
    if db_engine() == "postgres":
        if psycopg is None:
            raise RuntimeError("PostgreSQL selected but psycopg is not installed. Run: pip install psycopg[binary]")
        try:
            conn = psycopg.connect(os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL"), row_factory=dict_row)
            conn.autocommit = False
            return conn
        except Exception as exc:
            if auto_fallback_enabled():
                global ACTIVE_DB_ENGINE
                ACTIVE_DB_ENGINE = "sqlite"
                app_log("PostgreSQL unavailable, falling back to SQLite", error=str(exc))
                return db()
            raise
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    with db() as connection:
        if db_engine() == "postgres":
            connection.execute(
                """
            CREATE TABLE IF NOT EXISTS crm_state (
              id TEXT PRIMARY KEY,
              payload TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ai_logs (
              id BIGSERIAL PRIMARY KEY,
              kind TEXT NOT NULL,
              prompt TEXT NOT NULL,
              response TEXT NOT NULL,
              provider TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_events (
              id BIGSERIAL PRIMARY KEY,
              email TEXT NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS communication_logs (
              id BIGSERIAL PRIMARY KEY,
              channel TEXT NOT NULL,
              direction TEXT NOT NULL,
              recipient TEXT NOT NULL,
              subject TEXT,
              content TEXT NOT NULL,
              status TEXT NOT NULL,
              provider TEXT NOT NULL,
              linked TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_logs (
              id BIGSERIAL PRIMARY KEY,
              method TEXT NOT NULL,
              path TEXT NOT NULL,
              status INTEGER NOT NULL,
              message TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lead_followups (
              lead_id TEXT PRIMARY KEY,
              last_contacted TEXT,
              follow_up_due TEXT,
              follow_up_sent INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activities (
              id BIGSERIAL PRIMARY KEY,
              lead_id TEXT,
              type TEXT NOT NULL,
              status TEXT NOT NULL,
              detail TEXT NOT NULL,
              meta TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activity_logs (
              id BIGSERIAL PRIMARY KEY,
              user_id TEXT NOT NULL,
              action TEXT NOT NULL,
              entity_type TEXT NOT NULL,
              entity_id TEXT NOT NULL,
              details TEXT,
              created_at TEXT NOT NULL
            );
            """
            )
        else:
            connection.executescript(
                """
            CREATE TABLE IF NOT EXISTS crm_state (
              id TEXT PRIMARY KEY,
              payload TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ai_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              kind TEXT NOT NULL,
              prompt TEXT NOT NULL,
              response TEXT NOT NULL,
              provider TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL,
              status TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS communication_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              channel TEXT NOT NULL,
              direction TEXT NOT NULL,
              recipient TEXT NOT NULL,
              subject TEXT,
              content TEXT NOT NULL,
              status TEXT NOT NULL,
              provider TEXT NOT NULL,
              linked TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS api_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              method TEXT NOT NULL,
              path TEXT NOT NULL,
              status INTEGER NOT NULL,
              message TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lead_followups (
              lead_id TEXT PRIMARY KEY,
              last_contacted TEXT,
              follow_up_due TEXT,
              follow_up_sent INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activities (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              lead_id TEXT,
              type TEXT NOT NULL,
              status TEXT NOT NULL,
              detail TEXT NOT NULL,
              meta TEXT,
              created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activity_logs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id TEXT NOT NULL,
              action TEXT NOT NULL,
              entity_type TEXT NOT NULL,
              entity_id TEXT NOT NULL,
              details TEXT,
              created_at TEXT NOT NULL
            );
            """
            )
        connection.commit()


def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def today_iso():
    return time.strftime("%Y-%m-%d")


def static_root():
    return DIST if (DIST / "index.html").exists() else ROOT


def app_log(message, **fields):
    detail = " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
    sys.stderr.write(f"[{now()}] {message}{(' ' + detail) if detail else ''}\n")


def parse_datetime(value):
    if not value:
        return None
    text = str(value).strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y, %H:%M:%S",
        "%m/%d/%Y, %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def elapsed_hours_since(value):
    timestamp = parse_datetime(value)
    if not timestamp:
        return 10**6
    return (datetime.now() - timestamp).total_seconds() / 3600


def load_state(state_id="default"):
    with STATE_LOCK:
        with db() as connection:
            row = connection.execute(q("SELECT payload FROM crm_state WHERE id = ?"), (state_id,)).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["payload"])
    except json.JSONDecodeError:
        return None


def save_state(payload, state_id="default"):
    with STATE_LOCK:
        with db() as connection:
            connection.execute(
                q(
                    """
                INSERT INTO crm_state (id, payload, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
                """
                ),
                (state_id, json.dumps(payload, separators=(",", ":")), now()),
            )
            connection.commit()


def log_ai(kind, prompt, response, provider):
    with db() as connection:
        connection.execute(
            q("INSERT INTO ai_logs (kind, prompt, response, provider, created_at) VALUES (?, ?, ?, ?, ?)"),
            (kind, prompt[:4000], response[:8000], provider, now()),
        )
        connection.commit()


def log_auth(email, status):
    with db() as connection:
        connection.execute(
            q("INSERT INTO auth_events (email, status, created_at) VALUES (?, ?, ?)"),
            (email, status, now()),
        )
        connection.commit()


def log_activity_event(user_id, action, entity_type, entity_id, details=None):
    try:
        with db() as connection:
            connection.execute(
                q(
                    """
                INSERT INTO activity_logs (user_id, action, entity_type, entity_id, details, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                ),
                (
                    text_value(user_id, "anonymous", 180),
                    text_value(action, max_len=80),
                    text_value(entity_type, max_len=80),
                    text_value(entity_id, max_len=180),
                    json.dumps(details or {}, separators=(",", ":"))[:4000],
                    now(),
                ),
            )
            connection.commit()
    except Exception as exc:
        app_log("Failed to write activity log", error=str(exc), action=action, entity_type=entity_type, entity_id=entity_id)


def log_communication(channel, direction, recipient, subject, content, status, provider, linked=""):
    with db() as connection:
        connection.execute(
            q(
                """
            INSERT INTO communication_logs
              (channel, direction, recipient, subject, content, status, provider, linked, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            ),
            (channel, direction, recipient, subject, content[:8000], status, provider, linked, now()),
        )
        connection.commit()


def communication_logs(limit=50):
    with db() as connection:
        rows = connection.execute(
            q(
                """
            SELECT channel, direction, recipient, subject, content, status, provider, linked, created_at
            FROM communication_logs
            ORDER BY id DESC
            LIMIT ?
            """
            ),
            (limit,),
        ).fetchall()
    return [dict(row) if not isinstance(row, dict) else row for row in rows]


def api_logs(limit=100):
    with db() as connection:
        rows = connection.execute(
            q(
                """
            SELECT method, path, status, message, created_at
            FROM api_logs
            ORDER BY id DESC
            LIMIT ?
            """
            ),
            (limit,),
        ).fetchall()
    return [dict(row) if not isinstance(row, dict) else row for row in rows]


def log_api(method, path, status, message=""):
    if not path.startswith("/api/"):
        return
    try:
        with db() as connection:
            connection.execute(
                q("INSERT INTO api_logs (method, path, status, message, created_at) VALUES (?, ?, ?, ?, ?)"),
                (method, path, status, str(message)[:1000], now()),
            )
            connection.commit()
    except sqlite3.Error:
        pass


def crm_summary(state):
    if not state:
        return "No CRM data is available yet."
    companies = state.get("companies", [])
    inquiries = state.get("inquiries", [])
    pipeline = state.get("pipeline", [])
    quotations = state.get("quotations", [])
    orders = state.get("orders", [])
    activities = state.get("activities", [])
    stages = {stage.get("id"): stage.get("name") for stage in state.get("stages", [])}
    pipeline_value = sum(float(deal.get("value") or 0) for deal in pipeline)
    due = [item for item in activities if not item.get("done")]
    by_stage = {}
    for deal in pipeline:
        name = stages.get(deal.get("stageId"), "Unknown")
        by_stage[name] = by_stage.get(name, 0) + float(deal.get("value") or 0)
    stage_lines = ", ".join(f"{name}: INR {value:,.0f}" for name, value in by_stage.items())
    return (
        f"Companies: {len(companies)}. Inquiries: {len(inquiries)}. "
        f"Quotations: {len(quotations)}. Orders: {len(orders)}. "
        f"Pipeline value: INR {pipeline_value:,.0f}. Open activities: {len(due)}. "
        f"Pipeline by stage: {stage_lines}."
    )


def detect_intent(text):
    prompt = (text or "").lower()
    if any(word in prompt for word in ("price", "quote", "quotation", "rate", "discount")):
        return "QUOTE_REQUEST"
    if any(word in prompt for word in ("delivery", "dispatch", "tracking", "courier", "status")):
        return "ORDER_STATUS"
    if any(word in prompt for word in ("spec", "datasheet", "drawing", "material", "pressure", "size", "technical")):
        return "TECHNICAL_QUERY"
    if any(word in prompt for word in ("payment", "advance", "invoice", "bank")):
        return "PAYMENT_QUERY"
    if any(word in prompt for word in ("human", "call", "manager", "urgent")):
        return "HUMAN_ESCALATION"
    return "GENERAL"


def contact_context(state, contact_id):
    contact = find_contact(state, contact_id)
    company = find_company(state, contact.get("companyId")) if contact else {}
    inquiries = [item for item in state.get("inquiries", []) if item.get("contactId") == contact_id or item.get("companyId") == company.get("id")]
    orders = [item for item in state.get("orders", []) if item.get("companyId") == company.get("id")]
    quotations = [item for item in state.get("quotations", []) if item.get("companyId") == company.get("id")]
    latest_quote = quotations[0] if quotations else {}
    latest_order = orders[0] if orders else {}
    return (
        f"Contact: {contact.get('first', '')} {contact.get('last', '')}, "
        f"company: {company.get('name', 'unknown')}, "
        f"open inquiries: {len(inquiries)}, latest quote: {latest_quote.get('no', 'none')} "
        f"({latest_quote.get('status', 'n/a')}), latest order: {latest_order.get('no', 'none')} "
        f"({latest_order.get('status', 'n/a')})."
    )


def fallback_ai(kind, prompt, state):
    prompt_text = (prompt or "").lower()
    summary = crm_summary(state)
    activities = state.get("activities", []) if state else []
    pipeline = state.get("pipeline", []) if state else []
    intent = detect_intent(prompt)
    if kind == "email":
        return (
            "Subject: Follow-up on quotation and technical clarification\n\n"
            "Dear Customer,\n\n"
            "Thank you for reviewing our proposal. Based on your process requirement, "
            "we recommend proceeding with the quoted JK Fluid Controls valve package. "
            "Please let us know if you would like a revised commercial offer, compliance "
            "documents, or a quick technical call with our team.\n\n"
            "Regards,\nJK Fluid Controls"
        )
    if kind == "whatsapp":
        if intent == "ORDER_STATUS":
            return "Thanks for your message. I am checking the linked quotation/order status and our sales team will confirm the exact dispatch or delivery update shortly."
        if intent == "QUOTE_REQUEST":
            return "Thanks. Please share size, pressure rating, material, quantity, and required delivery date so we can prepare the correct valve quotation."
        if intent == "TECHNICAL_QUERY":
            return "Noted. Please share the process media, pressure, temperature, valve size, and any standard like IBR/API/ATEX. Our team will confirm the technical details."
        if intent == "HUMAN_ESCALATION":
            return "Understood. I have marked this for human follow-up and a JK Fluid Controls team member will contact you shortly."
        return "Thanks for contacting JK Fluid Controls. We have received your message and will respond with the relevant CRM details shortly."
    if "overdue" in prompt_text or "follow" in prompt_text:
        pending = [item for item in activities if not item.get("done")]
        first = pending[0].get("title") if pending else "none"
        return f"{len(pending)} follow-ups are open. Highest priority item: {first}. {summary}"
    if "pipeline" in prompt_text:
        value = sum(float(deal.get("value") or 0) for deal in pipeline)
        return f"Current pipeline is INR {value:,.0f}. {summary}"
    if "quote" in prompt_text:
        return "Suggested follow-up: restate the quotation number, confirm validity, mention GST-inclusive total, and offer a short technical call. " + summary
    return summary


def fallback_followup_message(lead_id):
    return (
        f"Dear Client ({lead_id}), we are following up regarding your requirement. "
        "Please share if you need a revised quotation, technical clarification, or delivery update."
    )


def sanitize_phone(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit() or ch == "+")
    if not digits:
        return ""
    if digits.startswith("+"):
        raw = "+" + "".join(ch for ch in digits[1:] if ch.isdigit())
    else:
        raw = "".join(ch for ch in digits if ch.isdigit())
        if len(raw) == 10:
            raw = "+91" + raw
        elif raw and not raw.startswith("+"):
            raw = "+" + raw
    return raw


def valid_phone(phone):
    cleaned = sanitize_phone(phone)
    return cleaned.startswith("+") and 10 <= len(cleaned.replace("+", "")) <= 15


def create_activity(lead_id, activity_type, status, detail, meta=None):
    with db() as connection:
        connection.execute(
            q("INSERT INTO activities (lead_id, type, status, detail, meta, created_at) VALUES (?, ?, ?, ?, ?, ?)"),
            (lead_id, activity_type, status, detail[:2000], json.dumps(meta or {}), now()),
        )
        connection.commit()


def get_activities(limit=200):
    with db() as connection:
        rows = connection.execute(
            q("SELECT lead_id, type, status, detail, meta, created_at FROM activities ORDER BY id DESC LIMIT ?"),
            (limit,),
        ).fetchall()
    result = []
    for row in rows:
        entry = dict(row) if not isinstance(row, dict) else row
        try:
            entry["meta"] = json.loads(entry.get("meta") or "{}")
        except json.JSONDecodeError:
            entry["meta"] = {}
        result.append(entry)
    return result


def mark_lead_contacted(lead_id, follow_up_sent=False):
    due = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
    with db() as connection:
        connection.execute(
            q(
                """
            INSERT INTO lead_followups (lead_id, last_contacted, follow_up_due, follow_up_sent, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
              last_contacted = excluded.last_contacted,
              follow_up_due = excluded.follow_up_due,
              follow_up_sent = excluded.follow_up_sent,
              updated_at = excluded.updated_at
            """
            ),
            (lead_id, now(), due, 1 if follow_up_sent else 0, now()),
        )
        connection.commit()


def mark_followup_sent(lead_id):
    with db() as connection:
        connection.execute(
            q("UPDATE lead_followups SET follow_up_sent = 1, updated_at = ? WHERE lead_id = ?"),
            (now(), lead_id),
        )
        connection.commit()


def pending_followups(limit=100):
    with db() as connection:
        rows = connection.execute(
            q(
                """
            SELECT lead_id, last_contacted, follow_up_due, follow_up_sent
            FROM lead_followups
            WHERE follow_up_sent = 0 AND follow_up_due <= ?
            ORDER BY follow_up_due ASC
            LIMIT ?
            """
            ),
            (now(), limit),
        ).fetchall()
    return [dict(row) if not isinstance(row, dict) else row for row in rows]


def ai_rate_allowed(user_id):
    user_key = user_id or "anonymous"
    limit = int(os.environ.get("AI_SESSION_LIMIT", "40"))
    window = int(os.environ.get("AI_SESSION_WINDOW_SECONDS", "3600"))
    now_ts = int(time.time())
    with AI_LOCK:
        timestamps = [ts for ts in AI_RATE_LIMIT.get(user_key, []) if now_ts - ts < window]
        if len(timestamps) >= limit:
            AI_RATE_LIMIT[user_key] = timestamps
            return False
        timestamps.append(now_ts)
        AI_RATE_LIMIT[user_key] = timestamps
    return True


def generate_message_safe(lead_id, prompt, state, user_id="", kind="assistant"):
    lead_key = str(lead_id or "general")
    prompt_key = str(prompt or "").strip()
    cache_key = f"{kind}:{lead_key}:{hashlib.sha256(prompt_key.encode('utf-8')).hexdigest()}"
    with AI_LOCK:
        cached = AI_CACHE.get(cache_key)
        if cached:
            return cached["answer"], cached["provider"], True, None
        if cache_key in AI_INFLIGHT:
            fallback = fallback_followup_message(lead_key)
            return fallback, "fallback", False, "duplicate_request"
        AI_INFLIGHT.add(cache_key)
    try:
        if not ai_rate_allowed(user_id):
            fallback = fallback_followup_message(lead_key)
            return fallback, "fallback", False, "rate_limited"
        attempts = 3
        delay = 1.0
        last_error = ""
        timeout = int(os.environ.get("OPENAI_TIMEOUT_SECONDS", "25"))
        for _ in range(attempts):
            try:
                # call_openai already contains provider fallback logic and timeout handling.
                answer, provider = call_openai(kind, prompt_key, state, "")
                if answer:
                    with AI_LOCK:
                        AI_CACHE[cache_key] = {"answer": answer, "provider": provider, "at": now()}
                    return answer, provider, False, None
            except Exception as exc:
                last_error = str(exc)
            time.sleep(delay)
            delay *= 2
            _ = timeout  # reserved for future per-attempt overrides
        fallback = fallback_followup_message(lead_key)
        return fallback, "fallback", False, last_error or "ai_unavailable"
    finally:
        with AI_LOCK:
            AI_INFLIGHT.discard(cache_key)

def next_id(prefix):
    return f"{prefix}-{int(time.time() * 1000)}"


STATE_COLLECTION_KEYS = (
    "users",
    "companies",
    "contacts",
    "stages",
    "inquiries",
    "products",
    "pipeline",
    "quotations",
    "quoteItems",
    "orders",
    "activities",
    "messages",
    "emails",
    "automations",
    "automationLog",
    "audit",
)
PAGINATED_COLLECTIONS = {
    "companies",
    "contacts",
    "inquiries",
    "pipeline",
    "quotations",
    "orders",
    "activities",
    "messages",
    "emails",
    "automationLog",
    "audit",
}
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


class ValidationError(ValueError):
    pass


class ConflictError(ValueError):
    pass


def json_clone(value):
    return json.loads(json.dumps(value if value is not None else {}))


def state_scope_for_user(user):
    if user.get("workspace_id"):
        return f"workspace:{user['workspace_id']}"
    identifier = str(user.get("sub") or user.get("id") or user.get("email") or "anonymous").strip()
    return f"user:{identifier}"


def merge_state_payload(existing, incoming):
    merged = json_clone(existing if isinstance(existing, dict) else {})
    payload = incoming if isinstance(incoming, dict) else {}
    loaded = payload.get("loadedCollections")
    loaded = loaded if isinstance(loaded, dict) else {}
    for key, value in payload.items():
        if key in ("session", "activePage"):
            continue
        if key in STATE_COLLECTION_KEYS:
            if loaded.get(key) or (key in payload and not loaded):
                merged[key] = json_clone(value if isinstance(value, list) else [])
            continue
        merged[key] = json_clone(value)
    merged["loadedCollections"] = {
        **(merged.get("loadedCollections") if isinstance(merged.get("loadedCollections"), dict) else {}),
        **loaded,
    }
    return merged


def text_value(value, default="", max_len=400):
    text = str(value or default).strip()
    return text[:max_len]


def numeric_value(value, default=0.0):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float(default)
    if numeric != numeric:  # NaN guard
        return float(default)
    return float(numeric)


def integer_value(value, default=0):
    return int(round(numeric_value(value, default)))


def boolean_value(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def dedupe_by_id(items):
    seen = set()
    result = []
    for item in items:
        item_id = str(item.get("id") or "").strip()
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        result.append(item)
    return result


def normalize_product_line(item, default_prefix="prd"):
    record = item if isinstance(item, dict) else {}
    category = text_value(record.get("category") or record.get("product"), max_len=160) or "Item"
    qty = max(1, integer_value(record.get("qty"), 1))
    unit_price = max(0.0, numeric_value(record.get("unitPrice") if record.get("unitPrice") is not None else record.get("unit"), 0))
    return {
        **record,
        "id": text_value(record.get("id"), next_id(default_prefix), 120),
        "category": category,
        "size": text_value(record.get("size"), max_len=80),
        "material": text_value(record.get("material"), max_len=80),
        "pressure": text_value(record.get("pressure"), max_len=80),
        "media": text_value(record.get("media"), max_len=80),
        "actuation": text_value(record.get("actuation"), max_len=80),
        "qty": qty,
        "unitPrice": round(unit_price, 2),
    }


def sanitize_products(products, source, fallback_products=None):
    raw_products = products if isinstance(products, list) else fallback_products if isinstance(fallback_products, list) else []
    normalized = [normalize_product_line(item, f"{source}-item") for item in raw_products]
    total = round(sum(item["qty"] * item["unitPrice"] for item in normalized), 2)
    return normalized, total


def sanitize_status(value, allowed, default):
    status = text_value(value, default, 40).upper()
    return status if status in allowed else default


def sanitize_discount(value):
    return max(0.0, min(100.0, round(numeric_value(value, 0), 2)))


def iso_now():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def canonical_record_timestamp(record):
    for key in ("updatedAt", "updated_at", "createdAt", "created_at", "at", "time"):
        parsed = parse_datetime(record.get(key) if isinstance(record, dict) else None)
        if parsed:
            return parsed
    return datetime.min


def record_sort_key(record):
    timestamp = canonical_record_timestamp(record)
    identifier = text_value((record or {}).get("id"), max_len=180)
    return (timestamp, identifier)


def sort_collection_items(items):
    return sorted(items or [], key=record_sort_key, reverse=True)


def collections_with_versions():
    return (
        "companies",
        "contacts",
        "inquiries",
        "pipeline",
        "quotations",
        "orders",
        "activities",
        "messages",
        "emails",
        "automations",
        "automationLog",
        "audit",
    )


def rate_limit_allowed(identity, bucket, limit, window_seconds):
    now_ts = time.time()
    key = f"{bucket}:{identity}"
    with REQUEST_RATE_LOCK:
        history = [ts for ts in REQUEST_RATE_LIMIT.get(key, []) if now_ts - ts < window_seconds]
        if len(history) >= limit:
            REQUEST_RATE_LIMIT[key] = history
            return False
        history.append(now_ts)
        REQUEST_RATE_LIMIT[key] = history
    return True


def default_state_payload():
    return json_clone(load_state("default") or {})


def load_user_state(user):
    state_id = state_scope_for_user(user)
    scoped = load_state(state_id)
    if scoped is not None:
        return scoped
    return default_state_payload()


def record_without_mutation_fields(record):
    if not isinstance(record, dict):
        return {}
    ignored = {"updatedAt", "updated_at", "createdAt", "created_at", "version"}
    return {key: value for key, value in record.items() if key not in ignored}


def detect_locked_inquiry_changes(current, incoming):
    current_inquiries = {
        text_value(item.get("id"), max_len=120): item
        for item in (current.get("inquiries") if isinstance(current.get("inquiries"), list) else [])
        if isinstance(item, dict)
    }
    for item in incoming.get("inquiries") if isinstance(incoming.get("inquiries"), list) else []:
        if not isinstance(item, dict):
            continue
        inquiry_id = text_value(item.get("id"), max_len=120)
        current_item = current_inquiries.get(inquiry_id)
        if not current_item or not boolean_value(current_item.get("isLocked")):
            continue
        if record_without_mutation_fields(item) != record_without_mutation_fields(current_item):
            raise ConflictError(f"Inquiry {current_item.get('no') or inquiry_id} is locked after quotation conversion.")


def detect_stale_updates(current, incoming):
    loaded = incoming.get("loadedCollections") if isinstance(incoming.get("loadedCollections"), dict) else {}
    for collection in collections_with_versions():
        if not loaded.get(collection):
            continue
        current_map = {
            text_value(item.get("id"), max_len=120): item
            for item in (current.get(collection) if isinstance(current.get(collection), list) else [])
            if isinstance(item, dict)
        }
        for item in incoming.get(collection) if isinstance(incoming.get(collection), list) else []:
            if not isinstance(item, dict):
                continue
            record_id = text_value(item.get("id"), max_len=120)
            current_item = current_map.get(record_id)
            if not current_item:
                continue
            incoming_version = integer_value(item.get("version"), 0)
            current_version = integer_value(current_item.get("version"), 0)
            incoming_updated = canonical_record_timestamp(item)
            current_updated = canonical_record_timestamp(current_item)
            if incoming_version and current_version and incoming_version < current_version:
                raise ConflictError(f"{collection[:-1].title()} {record_id} was updated in another tab.")
            if incoming_updated and current_updated and incoming_updated < current_updated:
                raise ConflictError(f"{collection[:-1].title()} {record_id} is stale. Refresh and try again.")


def enforce_idempotent_relationships(current, merged):
    current_quotes = {
        text_value(item.get("inquiryId"), max_len=120): item
        for item in (current.get("quotations") if isinstance(current.get("quotations"), list) else [])
        if isinstance(item, dict) and text_value(item.get("inquiryId"), max_len=120)
    }
    current_orders = {
        text_value(item.get("quotationId"), max_len=120): item
        for item in (current.get("orders") if isinstance(current.get("orders"), list) else [])
        if isinstance(item, dict) and text_value(item.get("quotationId"), max_len=120)
    }

    merged_quotes = []
    seen_inquiry_ids = set()
    for item in merged.get("quotations") if isinstance(merged.get("quotations"), list) else []:
        if not isinstance(item, dict):
            continue
        inquiry_id = text_value(item.get("inquiryId"), max_len=120)
        if inquiry_id and inquiry_id in current_quotes and text_value(item.get("id"), max_len=120) != text_value(current_quotes[inquiry_id].get("id"), max_len=120):
            if inquiry_id not in seen_inquiry_ids:
                merged_quotes.append(json_clone(current_quotes[inquiry_id]))
                seen_inquiry_ids.add(inquiry_id)
            continue
        if inquiry_id:
            if inquiry_id in seen_inquiry_ids:
                continue
            seen_inquiry_ids.add(inquiry_id)
        merged_quotes.append(item)
    for inquiry_id, item in current_quotes.items():
        if inquiry_id and inquiry_id not in seen_inquiry_ids:
            merged_quotes.append(json_clone(item))
    merged["quotations"] = merged_quotes

    merged_orders = []
    seen_quotation_ids = set()
    for item in merged.get("orders") if isinstance(merged.get("orders"), list) else []:
        if not isinstance(item, dict):
            continue
        quotation_id = text_value(item.get("quotationId"), max_len=120)
        if quotation_id and quotation_id in current_orders and text_value(item.get("id"), max_len=120) != text_value(current_orders[quotation_id].get("id"), max_len=120):
            if quotation_id not in seen_quotation_ids:
                merged_orders.append(json_clone(current_orders[quotation_id]))
                seen_quotation_ids.add(quotation_id)
            continue
        if quotation_id:
            if quotation_id in seen_quotation_ids:
                continue
            seen_quotation_ids.add(quotation_id)
        merged_orders.append(item)
    for quotation_id, item in current_orders.items():
        if quotation_id and quotation_id not in seen_quotation_ids:
            merged_orders.append(json_clone(item))
    merged["orders"] = merged_orders
    return merged


def apply_versions(current, sanitized):
    current = current if isinstance(current, dict) else {}
    for collection in collections_with_versions():
        previous = {
            text_value(item.get("id"), max_len=120): item
            for item in (current.get(collection) if isinstance(current.get(collection), list) else [])
            if isinstance(item, dict)
        }
        updated = []
        for item in sanitized.get(collection) if isinstance(sanitized.get(collection), list) else []:
            if not isinstance(item, dict):
                continue
            record_id = text_value(item.get("id"), max_len=120)
            existing = previous.get(record_id)
            base = dict(item)
            if existing:
                if record_without_mutation_fields(base) == record_without_mutation_fields(existing):
                    base["version"] = integer_value(existing.get("version"), 1)
                    base["createdAt"] = text_value(existing.get("createdAt"), text_value(base.get("createdAt"), now(), 40), 40)
                    base["updatedAt"] = text_value(existing.get("updatedAt"), text_value(base.get("updatedAt"), now(), 40), 40)
                else:
                    base["version"] = integer_value(existing.get("version"), 1) + 1
                    base["createdAt"] = text_value(existing.get("createdAt"), text_value(base.get("createdAt"), now(), 40), 40)
                    base["updatedAt"] = iso_now()
            else:
                base["version"] = max(1, integer_value(base.get("version"), 1))
                base["createdAt"] = text_value(base.get("createdAt"), iso_now(), 40)
                base["updatedAt"] = text_value(base.get("updatedAt"), base["createdAt"], 40)
            updated.append(base)
        sanitized[collection] = updated
    return sanitized


def audit_state_changes(user, current, updated):
    user_id = text_value(user.get("sub") or user.get("email"), "anonymous", 180)
    action_map = {
        "companies": "company",
        "contacts": "contact",
        "inquiries": "inquiry",
        "quotations": "quotation",
        "orders": "order",
    }
    for collection, entity_type in action_map.items():
        previous = {
            text_value(item.get("id"), max_len=120): item
            for item in (current.get(collection) if isinstance(current.get(collection), list) else [])
            if isinstance(item, dict)
        }
        latest = {
            text_value(item.get("id"), max_len=120): item
            for item in (updated.get(collection) if isinstance(updated.get(collection), list) else [])
            if isinstance(item, dict)
        }
        for entity_id, record in latest.items():
            before = previous.get(entity_id)
            if not before:
                action = "create"
                if collection == "quotations" and text_value(record.get("inquiryId"), max_len=120):
                    action = "convert"
                if collection == "orders":
                    action = "order_create"
                log_activity_event(user_id, action, entity_type, entity_id, {"number": record.get("no")})
                continue
            if record_without_mutation_fields(before) != record_without_mutation_fields(record):
                log_activity_event(user_id, "update", entity_type, entity_id, {"number": record.get("no")})
        for entity_id, record in previous.items():
            if entity_id not in latest:
                log_activity_event(user_id, "delete", entity_type, entity_id, {"number": record.get("no")})


def structured_summary(state):
    companies = state.get("companies", []) if isinstance(state, dict) else []
    inquiries = state.get("inquiries", []) if isinstance(state, dict) else []
    quotations = state.get("quotations", []) if isinstance(state, dict) else []
    orders = state.get("orders", []) if isinstance(state, dict) else []
    activities = state.get("activities", []) if isinstance(state, dict) else []
    pipeline = state.get("pipeline", []) if isinstance(state, dict) else []
    stages = {text_value(stage.get("id"), max_len=80): stage for stage in state.get("stages", []) if isinstance(stage, dict)} if isinstance(state, dict) else {}
    pipeline_value = round(sum(numeric_value(item.get("value"), 0) for item in pipeline), 2)
    quote_value = round(sum(numeric_value(item.get("totalAmount") or item.get("value"), 0) for item in quotations), 2)
    overdue = [item for item in activities if not boolean_value(item.get("done")) and text_value(item.get("due"), max_len=20) <= today_iso()]
    funnel = []
    for stage_id, stage in stages.items():
        deals = [item for item in pipeline if text_value(item.get("stageId"), max_len=80) == stage_id]
        funnel.append(
            {
                "id": stage_id,
                "name": text_value(stage.get("name"), max_len=120),
                "count": len(deals),
                "value": round(sum(numeric_value(item.get("value"), 0) for item in deals), 2),
            }
        )
    return {
        "counts": {
            "companies": len(companies),
            "contacts": len(state.get("contacts", []) if isinstance(state, dict) else []),
            "inquiries": len(inquiries),
            "openInquiries": len([item for item in inquiries if text_value(item.get("status"), max_len=20).upper() not in ("WON", "LOST")]),
            "quotations": len(quotations),
            "orders": len(orders),
            "activities": len(activities),
            "overdueActivities": len(overdue),
        },
        "pipelineValue": pipeline_value,
        "quoteValue": quote_value,
        "overdueActivities": len(overdue),
        "funnel": funnel,
    }


def paginate_items(items, limit, offset):
    ordered = sort_collection_items(items)
    page = ordered[offset:offset + limit]
    return {
        "items": page,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "total": len(ordered),
            "hasMore": offset + limit < len(ordered),
        },
    }


def sanitize_crm_state(payload, user=None):
    state = payload if isinstance(payload, dict) else {}
    sanitized = {}
    default_loaded = {key: False for key in STATE_COLLECTION_KEYS}
    if isinstance(state.get("loadedCollections"), dict):
        default_loaded.update({key: boolean_value(value) for key, value in state.get("loadedCollections", {}).items()})
    else:
        default_loaded.update({key: True for key in STATE_COLLECTION_KEYS if key in state})
    sanitized["loadedCollections"] = default_loaded
    sanitized["theme"] = "dark" if text_value(state.get("theme"), "light", 10) == "dark" else "light"
    sanitized["selectedContactId"] = text_value(state.get("selectedContactId"), max_len=120)
    sanitized["summary"] = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    sanitized["pagination"] = state.get("pagination") if isinstance(state.get("pagination"), dict) else {}

    users = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("u"), 120),
            "name": text_value((item or {}).get("name"), "User", 120),
            "email": text_value((item or {}).get("email"), max_len=180).lower(),
            "role": sanitize_status((item or {}).get("role"), {"ADMIN", "MANAGER", "SALES", "VIEWER"}, "MANAGER"),
            "phone": sanitize_phone((item or {}).get("phone")),
            "active": boolean_value((item or {}).get("active", True)),
        }
        for item in (state.get("users") if isinstance(state.get("users"), list) else [])
        if isinstance(item, dict) and text_value(item.get("email"), max_len=180)
    ])
    if user and not any(text_value(item.get("email"), max_len=180).lower() == text_value(user.get("email"), max_len=180).lower() for item in users):
        users.insert(
            0,
            {
                "id": text_value(user.get("sub") or user.get("id"), next_id("u"), 120),
                "name": text_value(user.get("name"), "CRM User", 120),
                "email": text_value(user.get("email"), max_len=180).lower(),
                "role": sanitize_status(user.get("role"), {"ADMIN", "MANAGER", "SALES", "VIEWER"}, "MANAGER"),
                "phone": sanitize_phone(user.get("phone")),
                "active": True,
            },
        )
    sanitized["users"] = users

    stages = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("stage"), 120),
            "name": text_value((item or {}).get("name"), "Stage", 120),
            "color": text_value((item or {}).get("color"), "#2563eb", 24),
            "probability": max(0, min(100, integer_value((item or {}).get("probability"), 0))),
        }
        for item in (state.get("stages") if isinstance(state.get("stages"), list) else [])
        if isinstance(item, dict) and text_value(item.get("name"), max_len=120)
    ])
    sanitized["stages"] = stages
    valid_stage_ids = {item["id"] for item in stages}

    companies = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("c"), 120),
            "name": text_value((item or {}).get("name"), max_len=180),
            "industry": text_value((item or {}).get("industry"), max_len=120),
            "city": text_value((item or {}).get("city"), max_len=120),
            "state": text_value((item or {}).get("state"), max_len=120),
            "country": text_value((item or {}).get("country"), max_len=120),
            "phone": sanitize_phone((item or {}).get("phone")),
            "email": text_value((item or {}).get("email"), max_len=180).lower(),
            "website": text_value((item or {}).get("website"), max_len=240),
            "location": text_value((item or {}).get("location") or f"{text_value((item or {}).get('city'), max_len=120)}, {text_value((item or {}).get('state'), max_len=120)}".strip(", "), max_len=240),
            "gst": text_value((item or {}).get("gst"), max_len=64),
            "status": sanitize_status((item or {}).get("status"), {"LEAD", "PROSPECT", "QUALIFIED", "CONVERTED", "CUSTOMER", "INACTIVE"}, "LEAD"),
            "size": text_value((item or {}).get("size"), max_len=40),
            "assignedTo": text_value((item or {}).get("assignedTo"), max_len=120),
            "tags": [text_value(tag, max_len=40) for tag in ((item or {}).get("tags") if isinstance((item or {}).get("tags"), list) else []) if text_value(tag, max_len=40)],
        }
        for item in (state.get("companies") if isinstance(state.get("companies"), list) else [])
        if isinstance(item, dict) and text_value(item.get("name"), max_len=180)
    ])
    sanitized["companies"] = companies
    company_ids = {item["id"] for item in companies}

    contacts = []
    contact_emails = set()
    for item in state.get("contacts") if isinstance(state.get("contacts"), list) else []:
        if not isinstance(item, dict):
            continue
        company_id = text_value(item.get("companyId"), max_len=120)
        if company_id not in company_ids:
            continue
        email = text_value(item.get("email"), max_len=180).lower()
        if email and email in contact_emails:
            continue
        if email:
            contact_emails.add(email)
        contacts.append(
            {
                **item,
                "id": text_value(item.get("id"), next_id("p"), 120),
                "companyId": company_id,
                "first": text_value(item.get("first"), max_len=80),
                "last": text_value(item.get("last"), max_len=80),
                "name": text_value(item.get("name") or f"{text_value(item.get('first'), max_len=80)} {text_value(item.get('last'), max_len=80)}".strip(), max_len=180),
                "designation": text_value(item.get("designation"), max_len=120),
                "email": email,
                "phone": sanitize_phone(item.get("phone")),
                "whatsapp": sanitize_phone(item.get("whatsapp")),
                "primary": boolean_value(item.get("primary")),
                "waOptIn": boolean_value(item.get("waOptIn", True)),
            }
        )
    contacts = dedupe_by_id(contacts)
    sanitized["contacts"] = contacts
    contact_ids = {item["id"] for item in contacts}
    contact_company = {item["id"]: item["companyId"] for item in contacts}

    inquiries = []
    inquiry_totals = {}
    for item in state.get("inquiries") if isinstance(state.get("inquiries"), list) else []:
        if not isinstance(item, dict):
            continue
        company_id = text_value(item.get("companyId"), max_len=120)
        if company_id not in company_ids:
            continue
        contact_id = text_value(item.get("contactId"), max_len=120)
        if contact_id and (contact_id not in contact_ids or contact_company.get(contact_id) != company_id):
            contact_id = ""
        products, total = sanitize_products(item.get("products"), f"Inquiry {text_value(item.get('no'), 'draft', 120)}")
        inquiry_id = text_value(item.get("id"), next_id("i"), 120)
        inquiry_totals[inquiry_id] = total
        inquiries.append(
            {
                **item,
                "id": inquiry_id,
                "no": text_value(item.get("no"), f"JK-{inquiry_id}", 120),
                "companyId": company_id,
                "contactId": contact_id,
                "assignedTo": text_value(item.get("assignedTo"), max_len=120),
                "status": sanitize_status(item.get("status"), {"LEAD", "NEW", "IN_REVIEW", "QUOTED", "NEGOTIATION", "WON", "LOST"}, "NEW"),
                "priority": sanitize_status(item.get("priority"), {"LOW", "MEDIUM", "HIGH", "URGENT"}, "MEDIUM"),
                "source": text_value(item.get("source"), max_len=80),
                "projectType": text_value(item.get("projectType"), max_len=80),
                "budgetMin": max(0.0, round(numeric_value(item.get("budgetMin"), 0), 2)),
                "budgetMax": max(total, round(numeric_value(item.get("budgetMax"), total), 2)),
                "requiredDate": text_value(item.get("requiredDate"), max_len=20),
                "requirements": [text_value(req, max_len=80) for req in (item.get("requirements") if isinstance(item.get("requirements"), list) else []) if text_value(req, max_len=80)],
                "notes": text_value(item.get("notes"), max_len=2000),
                "createdAt": text_value(item.get("createdAt"), today_iso(), 40),
                "updatedAt": text_value(item.get("updatedAt"), text_value(item.get("createdAt"), now(), 40), 40),
                "version": max(1, integer_value(item.get("version"), 1)),
                "isLocked": boolean_value(item.get("isLocked")),
                "products": products,
            }
        )
    inquiries = dedupe_by_id(inquiries)
    sanitized["inquiries"] = inquiries
    inquiry_ids = {item["id"] for item in inquiries}
    inquiry_company = {item["id"]: item["companyId"] for item in inquiries}
    inquiry_map = {item["id"]: item for item in inquiries}

    pipeline = []
    for item in state.get("pipeline") if isinstance(state.get("pipeline"), list) else []:
        if not isinstance(item, dict):
            continue
        inquiry_id = text_value(item.get("inquiryId"), max_len=120)
        if inquiry_id not in inquiry_ids:
            continue
        pipeline.append(
            {
                **item,
                "id": text_value(item.get("id"), next_id("deal"), 120),
                "inquiryId": inquiry_id,
                "stageId": text_value(item.get("stageId"), max_len=120) if text_value(item.get("stageId"), max_len=120) in valid_stage_ids else (stages[0]["id"] if stages else ""),
                "value": max(inquiry_totals.get(inquiry_id, 0), round(numeric_value(item.get("value"), inquiry_totals.get(inquiry_id, 0)), 2)),
                "expectedClose": text_value(item.get("expectedClose"), max_len=20),
                "movedAt": text_value(item.get("movedAt"), today_iso(), 40),
            }
        )
    sanitized["pipeline"] = dedupe_by_id(pipeline)

    quotations = []
    quote_lookup = {}
    for item in state.get("quotations") if isinstance(state.get("quotations"), list) else []:
        if not isinstance(item, dict):
            continue
        company_id = text_value(item.get("companyId"), max_len=120)
        inquiry_id = text_value(item.get("inquiryId"), max_len=120)
        if inquiry_id and inquiry_id not in inquiry_ids:
            inquiry_id = ""
        if inquiry_id and inquiry_company.get(inquiry_id) != company_id:
            continue
        if company_id not in company_ids:
            continue
        discount = sanitize_discount(item.get("discount"))
        fallback_products = inquiry_map.get(inquiry_id, {}).get("products", []) if inquiry_id else []
        products, subtotal = sanitize_products(item.get("products"), f"Quotation {text_value(item.get('no'), 'draft', 120)}", fallback_products=fallback_products)
        taxable = max(0.0, subtotal - (subtotal * discount / 100.0))
        total_amount = round(taxable + (taxable * 0.18), 2)
        quote_id = text_value(item.get("id"), next_id("q"), 120)
        record = {
            **item,
            "id": quote_id,
            "no": text_value(item.get("no"), f"QT-{quote_id}", 120),
            "inquiryId": inquiry_id or None,
            "companyId": company_id,
            "status": sanitize_status(item.get("status"), {"DRAFT", "SENT", "REVISED", "ACCEPTED", "EXPIRED"}, "DRAFT"),
            "validUntil": text_value(item.get("validUntil"), max_len=20),
            "discount": discount,
            "paymentTerms": text_value(item.get("paymentTerms"), max_len=240),
            "sentAt": text_value(item.get("sentAt"), max_len=40),
            "createdAt": text_value(item.get("createdAt"), text_value(item.get("sentAt"), today_iso(), 40), 40),
            "updatedAt": text_value(item.get("updatedAt"), text_value(item.get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value(item.get("version"), 1)),
            "products": [{**product, "quoteItemId": text_value(product.get("quoteItemId"), next_id("qi"), 120)} for product in products],
            "totalAmount": total_amount,
        }
        quotations.append(record)
        quote_lookup[quote_id] = record
    quotations = dedupe_by_id(quotations)
    sanitized["quotations"] = quotations

    seen_quotation_orders = set()
    orders = []
    for item in state.get("orders") if isinstance(state.get("orders"), list) else []:
        if not isinstance(item, dict):
            continue
        company_id = text_value(item.get("companyId"), max_len=120)
        quotation_id = text_value(item.get("quotationId"), max_len=120)
        if company_id not in company_ids or quotation_id not in quote_lookup:
            continue
        if quotation_id in seen_quotation_orders:
            continue
        seen_quotation_orders.add(quotation_id)
        quote = quote_lookup[quotation_id]
        if quote["companyId"] != company_id:
            continue
        products, subtotal = sanitize_products(item.get("products"), f"Order {text_value(item.get('no'), 'draft', 120)}", fallback_products=quote.get("products"))
        taxable = max(0.0, subtotal - (subtotal * sanitize_discount(quote.get("discount")) / 100.0))
        total_amount = round(taxable + (taxable * 0.18), 2)
        orders.append(
            {
                **item,
                "id": text_value(item.get("id"), next_id("o"), 120),
                "no": text_value(item.get("no"), f"ORD-{quotation_id}", 120),
                "quotationId": quotation_id,
                "companyId": company_id,
                "po": text_value(item.get("po"), max_len=120),
                "status": sanitize_status(item.get("status"), {"CONFIRMED", "PROCESSING", "DISPATCHED", "DELIVERED", "CANCELLED"}, "CONFIRMED"),
                "payment": sanitize_status(item.get("payment"), {"PENDING", "PARTIAL", "PAID"}, "PENDING"),
                "courier": text_value(item.get("courier"), max_len=120),
                "tracking": text_value(item.get("tracking"), max_len=160),
                "dispatchDate": text_value(item.get("dispatchDate"), max_len=20),
                "expectedDelivery": text_value(item.get("expectedDelivery"), max_len=20),
                "createdAt": text_value(item.get("createdAt"), text_value(item.get("dispatchDate"), today_iso(), 40), 40),
                "updatedAt": text_value(item.get("updatedAt"), text_value(item.get("createdAt"), now(), 40), 40),
                "version": max(1, integer_value(item.get("version"), 1)),
                "products": products,
                "value": total_amount,
                "amount": total_amount,
            }
        )
    sanitized["orders"] = dedupe_by_id(orders)

    locked_inquiries = {text_value(item.get("inquiryId"), max_len=120) for item in quotations if text_value(item.get("inquiryId"), max_len=120)}
    sanitized["inquiries"] = [
        {
            **item,
            "isLocked": True if item["id"] in locked_inquiries else boolean_value(item.get("isLocked")),
            "status": "QUOTED" if item["id"] in locked_inquiries and item.get("status") == "NEW" else item.get("status"),
        }
        for item in sanitized["inquiries"]
    ]

    sanitized["products"] = [
        {**product, "id": text_value(product.get("id"), f"ip-{inquiry['id']}-{index + 1}", 120), "inquiryId": inquiry["id"]}
        for inquiry in inquiries
        for index, product in enumerate(inquiry.get("products", []))
    ]
    sanitized["quoteItems"] = [
        {
            "id": text_value(product.get("quoteItemId"), f"qi-{quote['id']}-{index + 1}", 120),
            "quotationId": quote["id"],
            "product": text_value(product.get("category"), "Product", 160),
            "qty": integer_value(product.get("qty"), 1),
            "unit": round(numeric_value(product.get("unitPrice"), 0), 2),
            "size": text_value(product.get("size"), max_len=80),
            "material": text_value(product.get("material"), max_len=80),
            "pressure": text_value(product.get("pressure"), max_len=80),
            "media": text_value(product.get("media"), max_len=80),
            "actuation": text_value(product.get("actuation"), max_len=80),
            "lead": integer_value(product.get("lead"), 14),
            "brand": text_value(product.get("brand"), "JK Fluid Controls", 120),
            "hsn": text_value(product.get("hsn"), "84818030", 32),
        }
        for quote in quotations
        for index, product in enumerate(quote.get("products", []))
    ]

    sanitized["activities"] = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("a"), 120),
            "type": text_value((item or {}).get("type"), max_len=40),
            "title": text_value((item or {}).get("title"), max_len=180),
            "companyId": text_value((item or {}).get("companyId"), max_len=120) if text_value((item or {}).get("companyId"), max_len=120) in company_ids else "",
            "contactId": text_value((item or {}).get("contactId"), max_len=120) if text_value((item or {}).get("contactId"), max_len=120) in contact_ids else "",
            "inquiryId": text_value((item or {}).get("inquiryId"), max_len=120) if text_value((item or {}).get("inquiryId"), max_len=120) in inquiry_ids else "",
            "owner": text_value((item or {}).get("owner"), max_len=120),
            "due": text_value((item or {}).get("due"), max_len=20),
            "outcome": text_value((item or {}).get("outcome"), max_len=2000),
            "done": boolean_value((item or {}).get("done")),
            "createdAt": text_value((item or {}).get("createdAt"), now(), 40),
            "updatedAt": text_value((item or {}).get("updatedAt"), text_value((item or {}).get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value((item or {}).get("version"), 1)),
        }
        for item in (state.get("activities") if isinstance(state.get("activities"), list) else [])
        if isinstance(item, dict)
    ])
    sanitized["messages"] = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("w"), 120),
            "contactId": text_value((item or {}).get("contactId"), max_len=120) if text_value((item or {}).get("contactId"), max_len=120) in contact_ids else "",
            "direction": sanitize_status((item or {}).get("direction"), {"IN", "OUT"}, "OUT"),
            "content": text_value((item or {}).get("content"), max_len=2000),
            "time": text_value((item or {}).get("time"), max_len=80),
            "bot": boolean_value((item or {}).get("bot")),
            "createdAt": text_value((item or {}).get("createdAt"), now(), 40),
            "updatedAt": text_value((item or {}).get("updatedAt"), text_value((item or {}).get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value((item or {}).get("version"), 1)),
        }
        for item in (state.get("messages") if isinstance(state.get("messages"), list) else [])
        if isinstance(item, dict) and text_value(item.get("content"), max_len=2000)
    ])
    sanitized["emails"] = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("e"), 120),
            "from": text_value((item or {}).get("from"), max_len=180).lower(),
            "to": text_value((item or {}).get("to"), max_len=180).lower(),
            "subject": text_value((item or {}).get("subject"), max_len=240),
            "status": text_value((item or {}).get("status"), max_len=40).upper(),
            "linked": text_value((item or {}).get("linked"), max_len=120),
            "provider": text_value((item or {}).get("provider"), max_len=40),
            "time": text_value((item or {}).get("time"), max_len=80),
            "body": text_value((item or {}).get("body"), max_len=4000),
            "createdAt": text_value((item or {}).get("createdAt"), now(), 40),
            "updatedAt": text_value((item or {}).get("updatedAt"), text_value((item or {}).get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value((item or {}).get("version"), 1)),
        }
        for item in (state.get("emails") if isinstance(state.get("emails"), list) else [])
        if isinstance(item, dict)
    ])
    sanitized["automations"] = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("seq"), 120),
            "name": text_value((item or {}).get("name"), max_len=180),
            "trigger": text_value((item or {}).get("trigger"), max_len=80).upper(),
            "active": boolean_value((item or {}).get("active", True)),
            "delayHours": max(0, integer_value((item or {}).get("delayHours"), 0)),
            "condition": text_value((item or {}).get("condition"), "ALWAYS", 80).upper(),
            "steps": text_value((item or {}).get("steps"), max_len=400),
            "createdAt": text_value((item or {}).get("createdAt"), now(), 40),
            "updatedAt": text_value((item or {}).get("updatedAt"), text_value((item or {}).get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value((item or {}).get("version"), 1)),
        }
        for item in (state.get("automations") if isinstance(state.get("automations"), list) else [])
        if isinstance(item, dict) and text_value(item.get("trigger"), max_len=80)
    ])
    sanitized["automationLog"] = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("auto"), 120),
            "key": text_value((item or {}).get("key"), max_len=180),
            "title": text_value((item or {}).get("title"), max_len=180),
            "channel": text_value((item or {}).get("channel"), max_len=80),
            "status": text_value((item or {}).get("status"), max_len=40).upper(),
            "detail": text_value((item or {}).get("detail"), max_len=400),
            "at": text_value((item or {}).get("at"), now(), 80),
            "createdAt": text_value((item or {}).get("createdAt"), text_value((item or {}).get("at"), now(), 40), 40),
            "updatedAt": text_value((item or {}).get("updatedAt"), text_value((item or {}).get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value((item or {}).get("version"), 1)),
        }
        for item in (state.get("automationLog") if isinstance(state.get("automationLog"), list) else [])
        if isinstance(item, dict)
    ])
    sanitized["audit"] = dedupe_by_id([
        {
            **(item if isinstance(item, dict) else {}),
            "id": text_value((item or {}).get("id"), next_id("log"), 120),
            "user": text_value((item or {}).get("user"), max_len=120),
            "action": text_value((item or {}).get("action"), max_len=180),
            "entity": text_value((item or {}).get("entity"), max_len=120),
            "at": text_value((item or {}).get("at"), now(), 80),
            "createdAt": text_value((item or {}).get("createdAt"), text_value((item or {}).get("at"), now(), 40), 40),
            "updatedAt": text_value((item or {}).get("updatedAt"), text_value((item or {}).get("createdAt"), now(), 40), 40),
            "version": max(1, integer_value((item or {}).get("version"), 1)),
        }
        for item in (state.get("audit") if isinstance(state.get("audit"), list) else [])
        if isinstance(item, dict)
    ])
    sanitized["summary"] = structured_summary(sanitized)
    for collection in collections_with_versions():
        sanitized[collection] = sort_collection_items(sanitized.get(collection, []))
    return sanitized


def resolve_request_state(user, request_state=None, persist=False):
    current = load_user_state(user)
    merged = merge_state_payload(current, request_state) if isinstance(request_state, dict) else current
    sanitized = sanitize_crm_state(merged, user)
    if persist:
        save_state(sanitized, state_scope_for_user(user))
    return sanitized


def state_lists(state):
    for key in ("emails", "messages", "activities", "audit", "automationLog"):
        if not isinstance(state.get(key), list):
            state[key] = []
    if not isinstance(state.get("automations"), list):
        state["automations"] = []
    defaults = [
        {
            "id": "seq1",
            "name": "Quotation Follow-up",
            "trigger": "QUOTE_SENT",
            "active": True,
            "delayHours": 72,
            "condition": "NO_REPLY",
            "steps": "Day 3 email approval, Day 3 WhatsApp, Day 7 email, Day 14 manager alert",
        },
        {
            "id": "seq2",
            "name": "Post Delivery Check-in",
            "trigger": "ORDER_DELIVERED",
            "active": True,
            "delayHours": 72,
            "condition": "ALWAYS",
            "steps": "Day 3 feedback, Day 30 check-in, Day 180 reorder suggestion",
        },
        {
            "id": "seq3",
            "name": "New Inquiry Acknowledgement",
            "trigger": "INQUIRY_CREATED",
            "active": True,
            "delayHours": 0,
            "condition": "ALWAYS",
            "steps": "Instant WhatsApp acknowledgement, task creation for assigned sales rep",
        },
    ]
    existing_triggers = {item.get("trigger") for item in state["automations"]}
    for sequence in defaults:
        if sequence["trigger"] not in existing_triggers:
            state["automations"].append(sequence)
    for sequence in state["automations"]:
        default = next((item for item in defaults if item["trigger"] == sequence.get("trigger")), {})
        sequence.setdefault("delayHours", default.get("delayHours", 0))
        sequence.setdefault("condition", default.get("condition", "ALWAYS"))


def find_company(state, company_id):
    return next((item for item in state.get("companies", []) if item.get("id") == company_id), {})


def find_contact(state, contact_id):
    return next((item for item in state.get("contacts", []) if item.get("id") == contact_id), {})


def primary_contact_for_company(state, company_id):
    contacts = [item for item in state.get("contacts", []) if item.get("companyId") == company_id]
    return next((item for item in contacts if item.get("primary")), contacts[0] if contacts else {})


def inquiry_for_quote(state, quote):
    return next((item for item in state.get("inquiries", []) if item.get("id") == quote.get("inquiryId")), {})


def automation_sequence(state, trigger):
    state_lists(state)
    return next((item for item in state.get("automations", []) if item.get("trigger") == trigger and item.get("active", True)), None)


def delayed_enough(sequence, timestamp):
    delay = float(sequence.get("delayHours") or 0)
    return elapsed_hours_since(timestamp) >= delay


def has_customer_reply_after(state, contact_id, timestamp):
    since = parse_datetime(timestamp)
    if not since:
        return False
    for message in state.get("messages", []):
        if message.get("contactId") != contact_id or message.get("direction") != "IN":
            continue
        message_time = parse_datetime(message.get("createdAt") or message.get("at") or message.get("time"))
        if message_time and message_time >= since:
            return True
    return False


def append_audit(state, action, entity, user="Automation"):
    state_lists(state)
    state["audit"].append({"id": next_id("log"), "user": user, "action": action, "entity": entity, "at": now()})


def send_email_provider(to_email, subject, content):
    smtp_host = os.environ.get("SMTP_HOST") or os.environ.get("GMAIL_SMTP_HOST")
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")
    smtp_from = os.environ.get("SMTP_FROM") or smtp_user or "sales@jkfluidcontrols.com"
    if not smtp_host or not smtp_user or not smtp_pass:
        return "SIMULATED", "simulated"

    message = EmailMessage()
    message["From"] = smtp_from
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(content)
    port = int(os.environ.get("SMTP_PORT", "587"))
    try:
        if port == 465:
            with smtplib.SMTP_SSL(smtp_host, port, timeout=20) as smtp:
                smtp.login(smtp_user, smtp_pass)
                smtp.send_message(message)
        else:
            with smtplib.SMTP(smtp_host, port, timeout=20) as smtp:
                smtp.starttls()
                smtp.login(smtp_user, smtp_pass)
                smtp.send_message(message)
        return "SENT", "smtp"
    except Exception as exc:
        app_log("SMTP send failed", error=str(exc))
        return "FAILED", "smtp"


def send_whatsapp_provider(to_phone, content):
    cleaned = sanitize_phone(to_phone)
    if not valid_phone(cleaned):
        return "FAILED", "validation"

    twilio_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    twilio_token = os.environ.get("TWILIO_AUTH_TOKEN")
    twilio_from = os.environ.get("TWILIO_WHATSAPP_FROM")
    if twilio_sid and twilio_token and twilio_from:
        endpoint = f"https://api.twilio.com/2010-04-01/Accounts/{twilio_sid}/Messages.json"
        payload = urllib.parse.urlencode(
            {
                "From": f"whatsapp:{twilio_from}",
                "To": f"whatsapp:{cleaned}",
                "Body": content[:1500],
            }
        ).encode("utf-8")
        auth_token = base64.b64encode(f"{twilio_sid}:{twilio_token}".encode("utf-8")).decode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=payload,
            headers={"Authorization": f"Basic {auth_token}", "Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        twilio_context = None
        if str(os.environ.get("TWILIO_INSECURE_TLS", "false")).strip().lower() in ("1", "true", "yes", "on"):
            twilio_context = ssl._create_unverified_context()
        elif certifi:
            twilio_context = ssl.create_default_context(cafile=certifi.where())
        for attempt in range(2):
            try:
                with urllib.request.urlopen(request, timeout=25, context=twilio_context):
                    return "SENT", "twilio"
            except Exception as exc:
                app_log("Twilio WhatsApp send failed", attempt=attempt + 1, error=str(exc))
                if attempt == 0:
                    time.sleep(1)
        return "FAILED", "twilio"

    token = os.environ.get("META_WHATSAPP_TOKEN")
    phone_number_id = os.environ.get("META_PHONE_NUMBER_ID")
    if not token or not phone_number_id:
        return "DELIVERED", "simulated"

    payload = json.dumps(
        {
            "messaging_product": "whatsapp",
            "to": cleaned.replace("+", ""),
            "type": "text",
            "text": {"preview_url": False, "body": content},
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"https://graph.facebook.com/v19.0/{phone_number_id}/messages",
        data=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=25):
            return "SENT", "meta"
    except Exception as exc:
        app_log("WhatsApp send failed", error=str(exc))
        return "FAILED", "meta"


def add_email_to_state(state, to_email, subject, content, linked="", status="SENT", provider="simulated"):
    state_lists(state)
    email = {
        "id": next_id("e"),
        "from": os.environ.get("SMTP_FROM", "sales@jkfluidcontrols.com"),
        "to": to_email,
        "subject": subject,
        "body": content,
        "status": status,
        "provider": provider,
        "linked": linked or "CRM",
        "time": now(),
        "createdAt": now(),
    }
    state["emails"].insert(0, email)
    state["activities"].insert(
        0,
        {
            "id": next_id("a"),
            "type": "EMAIL",
            "title": subject,
            "companyId": "",
            "contactId": "",
            "inquiryId": "",
            "owner": "",
            "due": time.strftime("%Y-%m-%d"),
            "outcome": f"{status} via {provider} to {to_email}",
            "done": True,
        },
    )
    return email


def add_whatsapp_to_state(state, contact_id, to_phone, content, direction="OUT", bot=False, status="DELIVERED", provider="simulated"):
    state_lists(state)
    message = {
        "id": next_id("w"),
        "contactId": contact_id,
        "direction": direction,
        "content": content,
        "time": time.strftime("%H:%M"),
        "createdAt": now(),
        "bot": bot,
        "status": status,
        "provider": provider,
    }
    state["messages"].append(message)
    return message


def automation_key_exists(state, key):
    return any(item.get("key") == key for item in state.get("automationLog", []))


def record_automation(state, key, title, channel, status, detail):
    state_lists(state)
    entry = {
        "id": next_id("auto"),
        "key": key,
        "title": title,
        "channel": channel,
        "status": status,
        "detail": detail,
        "at": now(),
    }
    state["automationLog"].insert(0, entry)
    return entry


def run_automation(state, state_id=None):
    state = state or {}
    state_lists(state)
    results = []
    today_key = today_iso()
    quote_sequence = automation_sequence(state, "QUOTE_SENT")
    inquiry_sequence = automation_sequence(state, "INQUIRY_CREATED")
    delivery_sequence = automation_sequence(state, "ORDER_DELIVERED")

    if quote_sequence:
        for quote in state.get("quotations", []):
            if quote.get("status") not in ("SENT", "REVISED", "VIEWED"):
                continue
            if not delayed_enough(quote_sequence, quote.get("sentAt") or quote.get("createdAt")):
                continue
            key = f"QUOTE_SENT:{quote.get('no')}:{today_key}"
            if automation_key_exists(state, key):
                continue
            try:
                company = find_company(state, quote.get("companyId"))
                inquiry = inquiry_for_quote(state, quote)
                contact = find_contact(state, inquiry.get("contactId")) or primary_contact_for_company(state, quote.get("companyId"))
                if quote_sequence.get("condition") == "NO_REPLY" and has_customer_reply_after(state, contact.get("id", ""), quote.get("sentAt")):
                    results.append(record_automation(state, key, f"Skipped {quote.get('no')}", "EMAIL+WHATSAPP", "SKIPPED", "Customer already replied"))
                    continue
                subject = f"Follow-up on quotation {quote.get('no')}"
                body = (
                    f"Dear {contact.get('first', 'Customer')},\n\n"
                    f"This is a quick follow-up on quotation {quote.get('no')} for {company.get('name', 'your requirement')}. "
                    "Please let us know if you need any technical documents, revised pricing, or a short call with our team.\n\n"
                    "Regards,\nJK Fluid Controls"
                )
                email_status, email_provider = send_email_provider(company.get("email", ""), subject, body)
                add_email_to_state(state, company.get("email", ""), subject, body, quote.get("no"), email_status, email_provider)
                log_communication("EMAIL", "OUT", company.get("email", ""), subject, body, email_status, email_provider, quote.get("no"))
                wa_text = f"Hello {contact.get('first', '')}, following up on quotation {quote.get('no')}. Reply here if you need documents, revised pricing, or a technical call."
                wa_status, wa_provider = send_whatsapp_provider(contact.get("whatsapp") or contact.get("phone", ""), wa_text)
                add_whatsapp_to_state(state, contact.get("id", ""), contact.get("whatsapp") or contact.get("phone", ""), wa_text, "OUT", True, wa_status, wa_provider)
                log_communication("WHATSAPP", "OUT", contact.get("whatsapp") or contact.get("phone", ""), "", wa_text, wa_status, wa_provider, quote.get("no"))
                results.append(record_automation(state, key, subject, "EMAIL+WHATSAPP", "DONE", company.get("name", "")))
            except Exception as exc:
                app_log("Automation quote follow-up failed", quote=quote.get("no"), error=str(exc))
                results.append(record_automation(state, key, f"Failed {quote.get('no')}", "EMAIL+WHATSAPP", "FAILED", str(exc)))

    if inquiry_sequence:
        for inquiry in state.get("inquiries", []):
            if inquiry.get("status") != "NEW":
                continue
            if not delayed_enough(inquiry_sequence, inquiry.get("createdAt")):
                continue
            key = f"INQUIRY_CREATED:{inquiry.get('no')}:{today_key}"
            if automation_key_exists(state, key):
                continue
            try:
                contact = find_contact(state, inquiry.get("contactId"))
                text = f"Thank you for inquiry {inquiry.get('no')}. JK Fluid Controls has received your requirement and our sales team will review it shortly."
                wa_status, wa_provider = send_whatsapp_provider(contact.get("whatsapp") or contact.get("phone", ""), text)
                add_whatsapp_to_state(state, contact.get("id", ""), contact.get("whatsapp") or contact.get("phone", ""), text, "OUT", True, wa_status, wa_provider)
                log_communication("WHATSAPP", "OUT", contact.get("whatsapp") or contact.get("phone", ""), "", text, wa_status, wa_provider, inquiry.get("no"))
                results.append(record_automation(state, key, f"Acknowledge {inquiry.get('no')}", "WHATSAPP", "DONE", contact.get("first", "")))
            except Exception as exc:
                app_log("Automation inquiry ack failed", inquiry=inquiry.get("no"), error=str(exc))
                results.append(record_automation(state, key, f"Failed {inquiry.get('no')}", "WHATSAPP", "FAILED", str(exc)))

    if delivery_sequence:
        for order in state.get("orders", []):
            if order.get("status") != "DELIVERED":
                continue
            if not delayed_enough(delivery_sequence, order.get("deliveredAt") or order.get("expectedDelivery") or order.get("dispatchDate")):
                continue
            key = f"ORDER_DELIVERED:{order.get('no')}:{today_key}"
            if automation_key_exists(state, key):
                continue
            try:
                contact = primary_contact_for_company(state, order.get("companyId"))
                text = f"Hope order {order.get('no')} has been received in good condition. Please share feedback or any support requirement."
                wa_status, wa_provider = send_whatsapp_provider(contact.get("whatsapp") or contact.get("phone", ""), text)
                add_whatsapp_to_state(state, contact.get("id", ""), contact.get("whatsapp") or contact.get("phone", ""), text, "OUT", True, wa_status, wa_provider)
                log_communication("WHATSAPP", "OUT", contact.get("whatsapp") or contact.get("phone", ""), "", text, wa_status, wa_provider, order.get("no"))
                results.append(record_automation(state, key, f"Feedback request {order.get('no')}", "WHATSAPP", "DONE", contact.get("first", "")))
            except Exception as exc:
                app_log("Automation delivery follow-up failed", order=order.get("no"), error=str(exc))
                results.append(record_automation(state, key, f"Failed {order.get('no')}", "WHATSAPP", "FAILED", str(exc)))

    append_audit(state, "Ran communication automation", f"{len(results)} actions")
    if state_id:
        try:
            save_state(state, state_id)
        except sqlite3.Error as exc:
            app_log("Failed to save state after automation", error=str(exc))
    return state, results


def extract_openai_text(payload):
    if payload.get("output_text"):
        return str(payload["output_text"]).strip()
    chunks = []
    for item in payload.get("output", []):
        for part in item.get("content", []):
            if part.get("type") in ("output_text", "text") and part.get("text"):
                chunks.append(part["text"])
    if chunks:
        return "\n".join(chunks).strip()
    choices = payload.get("choices") or []
    if choices:
        return choices[0].get("message", {}).get("content", "").strip()
    return ""


def call_openai(kind, prompt, state, contact_id=""):
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return fallback_ai(kind, prompt, state), "fallback"

    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    intent = detect_intent(prompt)
    system_msg = (
        "You are the JK Fluid Controls CRM assistant for industrial valves, actuators, and process equipment. "
        "Use only the CRM context provided. Be concise, accurate, and sales-operations focused. "
        "For WhatsApp replies, keep the response under 80 words, professional, and action oriented. "
        "If the customer asks for price or quote, request size, pressure rating, body material, quantity, media, and delivery date when missing. "
        "If the customer asks for order or delivery status, refer to available order/dispatch context and say a sales executive will confirm if details are incomplete. "
        "Do not invent prices, commitments, dispatch dates, certifications, or stock."
    )
    user_prompt = (
        f"CRM summary:\n{crm_summary(state)}\n\n"
        f"Contact context:\n{contact_context(state, contact_id) if contact_id else 'No specific contact selected.'}\n\n"
        f"Detected intent: {intent}\n\n"
        f"User request:\n{prompt}"
    )
    body = json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": 700,
            "temperature": 0.3,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    ssl_context = None
    if str(os.environ.get("OPENAI_INSECURE_TLS", "false")).strip().lower() in ("1", "true", "yes", "on"):
        ssl_context = ssl._create_unverified_context()
    elif certifi:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with urllib.request.urlopen(request, timeout=25, context=ssl_context) as response:
            payload = json.loads(response.read().decode("utf-8"))
        text = extract_openai_text(payload)
        if text:
            return text, "openai"
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        app_log("OpenAI request failed", error=str(exc))
        return fallback_ai(kind, f"{prompt}\n\nProvider error: {exc}", state), "fallback"
    return fallback_ai(kind, prompt, state), "fallback"


def resolve_lead_targets(state, lead_id):
    inquiries = state.get("inquiries", []) if state else []
    inquiry = next((item for item in inquiries if item.get("id") == lead_id or item.get("no") == lead_id), None)
    if not inquiry:
        return {"email": "", "phone": "", "name": "Client"}
    company = find_company(state, inquiry.get("companyId"))
    contact = find_contact(state, inquiry.get("contactId")) or primary_contact_for_company(state, inquiry.get("companyId"))
    return {
        "email": company.get("email", ""),
        "phone": contact.get("whatsapp") or contact.get("phone", ""),
        "name": contact.get("first") or company.get("name") or "Client",
    }


def run_due_followups():
    state = load_state() or {}
    due = pending_followups()
    for item in due:
        lead_id = str(item.get("lead_id") or item.get("leadId") or "")
        if not lead_id:
            continue
        target = resolve_lead_targets(state, lead_id)
        prompt = f"Draft a concise follow-up message for lead {lead_id}."
        message, provider, _, reason = generate_message_safe(lead_id, prompt, state, "scheduler", "assistant")
        sent = False
        channel = "NONE"
        status = "SKIPPED"
        if target.get("email"):
            email_status, email_provider = send_email_provider(target["email"], f"Follow-up for {lead_id}", message)
            channel = "EMAIL"
            sent = email_status in ("SENT", "SIMULATED")
            status = email_status
            create_activity(lead_id, "FOLLOW_UP_TRIGGERED", status, f"email={target['email']}", {"provider": email_provider, "ai_provider": provider, "fallback_reason": reason})
        elif valid_phone(target.get("phone")):
            wa_status, wa_provider = send_whatsapp_provider(target["phone"], message)
            channel = "WHATSAPP"
            sent = wa_status in ("SENT", "DELIVERED")
            status = wa_status
            create_activity(lead_id, "FOLLOW_UP_TRIGGERED", status, f"phone={target['phone']}", {"provider": wa_provider, "ai_provider": provider, "fallback_reason": reason})
        else:
            create_activity(lead_id, "FOLLOW_UP_TRIGGERED", "FAILED", "No email/phone target found", {"ai_provider": provider})
        if sent:
            mark_followup_sent(lead_id)
            app_log("Follow-up sent", lead_id=lead_id, channel=channel, status=status)


def followup_scheduler_loop():
    interval_seconds = int(os.environ.get("FOLLOWUP_POLL_SECONDS", "3600"))
    app_log("Follow-up scheduler started", interval_seconds=interval_seconds)
    while True:
        try:
            run_due_followups()
        except Exception as exc:
            app_log("Follow-up scheduler error", error=str(exc))
        time.sleep(max(60, interval_seconds))


class CRMHandler(BaseHTTPRequestHandler):
    server_version = "JKCRM/1.0"

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        path = self.route_path()
        if path == "/api/health":
            self.send_json(
                {
                    "ok": True,
                    "database": database_label(),
                    "database_engine": db_engine(),
                    "database_driver": "psycopg" if db_engine() == "postgres" else "sqlite3",
                    "database_ready": db_available(),
                    "ai": "openai" if os.environ.get("OPENAI_API_KEY") else "fallback",
                    "email": "smtp" if os.environ.get("SMTP_HOST") else "simulated",
                    "whatsapp": "meta" if os.environ.get("META_WHATSAPP_TOKEN") and os.environ.get("META_PHONE_NUMBER_ID") else "simulated",
                }
            )
            return
        if path == "/api/summary":
            user = self.require_auth()
            if not user:
                return
            self.send_json({"summary": structured_summary(resolve_request_state(user))})
            return
        if path == "/api/state":
            user = self.require_auth()
            if not user:
                return
            self.send_json({"state": resolve_request_state(user)})
            return
        if path.startswith("/api/data/"):
            user = self.require_auth()
            if not user:
                return
            self.handle_collection_get(path.split("/api/data/", 1)[1], user)
            return
        if path == "/api/automation/logs":
            user = self.require_auth()
            if not user:
                return
            self.send_json({"logs": communication_logs()})
            return
        if path == "/api/logs":
            user = self.require_auth()
            if not user:
                return
            self.send_json({"logs": api_logs()})
            return
        if path == "/api/auth/me":
            user = self.require_auth()
            if not user:
                return
            self.send_json(
                {
                    "user": {
                        "id": user.get("sub"),
                        "name": user.get("name") or user.get("email"),
                        "email": user.get("email"),
                        "role": user.get("role"),
                        "active": True,
                    }
                }
            )
            return
        if path in ("/activities", "/api/activities"):
            user = self.require_auth()
            if not user:
                return
            self.send_json({"activities": get_activities()})
            return
        self.serve_static()

    def do_POST(self):
        try:
            from middleware import check_idempotency
            is_dup, cached = check_idempotency(self, None)
            if is_dup:
                self.send_json(cached, status=200)
                return
        except ImportError:
            pass
        path = self.route_path()
        if path == "/api/auth/login":
            body = self.read_json()
            email = str(body.get("email", "")).strip().lower()
            password = str(body.get("password", "") or "")
            if not email or not password:
                self.send_json({"error": "Email and password are required"}, status=400)
                return
            account = None
            access_token = ""
            refresh_token = ""
            provider_access_token = ""
            provider_refresh_token = ""
            expires_in = int(os.environ.get("ACCESS_TOKEN_TTL_SECONDS", "900"))
            if supabase_auth_ready():
                auth_payload, error_message, status_code = supabase_password_login(email, password)
                if not auth_payload:
                    log_auth(email, "failed")
                    self.send_json({"error": error_message}, status=status_code if status_code in (400, 401, 403, 422) else 503)
                    return
                account = build_account_profile({}, email, auth_payload.get("user") or {})
                provider_access_token = str(auth_payload.get("access_token") or "")
                provider_refresh_token = str(auth_payload.get("refresh_token") or "")
                expires_in = int(auth_payload.get("expires_in") or expires_in)
            else:
                existing = load_state() or {}
                users = existing.get("users", []) if existing else []
                account = next((item for item in users if item.get("email", "").lower() == email and item.get("active", True)), None)
                demo_password = str(os.environ.get("DEMO_LOGIN_PASSWORD", "demo123"))
                if not account or password != demo_password:
                    log_auth(email, "failed")
                    self.send_json({"error": "Invalid email or password"}, status=401)
                    return
            access_token = issue_access_token(account)
            refresh_token = issue_refresh_token(account)
            log_auth(email, "success")
            self.send_json(
                {
                    "user": account,
                    "token": access_token,
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "expires_in": expires_in,
                    "auth_provider": "supabase" if supabase_auth_ready() else "local",
                    "provider_access_token": provider_access_token,
                    "provider_refresh_token": provider_refresh_token,
                }
            )
            return
        if path == "/api/auth/refresh":
            body = self.read_json()
            refresh_token = str(body.get("refresh_token") or "")
            payload = verify_token(refresh_token, refresh_secret())
            session = REFRESH_SESSIONS.get(refresh_token)
            if not payload or not session or session.get("jti") != payload.get("jti"):
                self.send_json({"error": "Invalid refresh token"}, status=401)
                return
            account = {
                "id": text_value(payload.get("sub"), max_len=120),
                "name": text_value(payload.get("name"), max_len=120) or text_value(payload.get("email"), max_len=180),
                "email": text_value(payload.get("email"), max_len=180).lower(),
                "role": sanitize_status(payload.get("role"), {"ADMIN", "MANAGER", "SALES", "VIEWER"}, "MANAGER"),
                "active": True,
            }
            access_token = issue_access_token(account)
            self.send_json({"access_token": access_token, "expires_in": int(os.environ.get("ACCESS_TOKEN_TTL_SECONDS", "900"))})
            return
        if path == "/api/auth/logout":
            body = self.read_json()
            refresh_token = str(body.get("refresh_token") or "")
            if refresh_token in REFRESH_SESSIONS:
                del REFRESH_SESSIONS[refresh_token]
            self.send_json({"ok": True})
            return
        if path == "/api/ai/assistant":
            self.handle_ai("assistant")
            return
        if path == "/api/ai/email-draft":
            self.handle_ai("email")
            return
        if path == "/api/email/send":
            self.handle_email_send()
            return
        if path == "/api/whatsapp/send":
            self.handle_whatsapp_send()
            return
        if path == "/api/whatsapp/inbound":
            self.handle_whatsapp_inbound()
            return
        if path == "/api/automation/run":
            self.handle_automation_run()
            return
        if path in ("/generate-message", "/api/generate-message"):
            self.handle_generate_message()
            return
        if path in ("/send-email", "/api/send-email"):
            self.handle_send_email_v2()
            return
        if path in ("/send-whatsapp", "/api/send-whatsapp"):
            self.handle_send_whatsapp_v2()
            return
        self.send_json({"error": "Not found"}, status=404)

    def do_PUT(self):
        try:
            from middleware import check_idempotency
            is_dup, cached = check_idempotency(self, None)
            if is_dup:
                self.send_json(cached, status=200)
                return
        except ImportError:
            pass
        path = self.route_path()
        if path == "/api/state":
            user = self.require_auth()
            if not user:
                return
            body = self.read_json()
            payload = body.get("state", body)
            if not isinstance(payload, dict):
                self.send_json({"error": "State payload must be an object"}, status=400)
                return
            try:
                state = resolve_request_state(user, payload, persist=True)
            except ValidationError as exc:
                self.send_json({"error": str(exc)}, status=400)
                return
            self.send_json({"ok": True, "updatedAt": now(), "state": state})
            return
        self.send_json({"error": "Not found"}, status=404)

    def do_PATCH(self):
        path = self.route_path()
        if path.startswith("/lead/") and path.endswith("/contacted"):
            self.handle_lead_contacted(path)
            return
        if path.startswith("/api/lead/") and path.endswith("/contacted"):
            self.handle_lead_contacted(path.replace("/api", "", 1))
            return
        self.send_json({"error": "Not found"}, status=404)

    def route_path(self):
        return urlparse(self.path).path

    def query_params(self):
        return urllib.parse.parse_qs(urlparse(self.path).query)

    def authenticated_user(self):
        payload = verify_token(parse_bearer(self.headers), auth_secret())
        if not payload:
            return None
        return {
            "sub": text_value(payload.get("sub"), max_len=120),
            "email": text_value(payload.get("email"), max_len=180).lower(),
            "role": sanitize_status(payload.get("role"), {"ADMIN", "MANAGER", "SALES", "VIEWER"}, "MANAGER"),
            "name": text_value(payload.get("name"), max_len=120),
            "workspace_id": text_value(self.headers.get("X-Workspace-Id"), max_len=120) or None,
        }

    def require_auth(self):
        user = self.authenticated_user()
        if not user:
            self.send_json({"error": "Unauthorized"}, status=401)
            return None
        return user

    def handle_collection_get(self, collection, user):
        if collection not in STATE_COLLECTION_KEYS:
            self.send_json({"error": "Unknown collection"}, status=404)
            return
        state = resolve_request_state(user)
        items = list(state.get(collection, []))
        query = self.query_params()
        company_id = text_value((query.get("companyId") or [""])[0], max_len=120)
        inquiry_id = text_value((query.get("inquiryId") or [""])[0], max_len=120)
        quotation_id = text_value((query.get("quotationId") or [""])[0], max_len=120)
        status = text_value((query.get("status") or [""])[0], max_len=40).upper()
        if company_id:
            items = [item for item in items if text_value(item.get("companyId"), max_len=120) == company_id]
        if inquiry_id:
            items = [item for item in items if text_value(item.get("inquiryId"), max_len=120) == inquiry_id]
        if quotation_id:
            items = [item for item in items if text_value(item.get("quotationId"), max_len=120) == quotation_id]
        if status:
            items = [item for item in items if text_value(item.get("status"), max_len=40).upper() == status]
        requested_limit = integer_value((query.get("limit") or [DEFAULT_PAGE_SIZE])[0], DEFAULT_PAGE_SIZE)
        requested_offset = max(0, integer_value((query.get("offset") or [0])[0], 0))
        limit = min(MAX_PAGE_SIZE, max(1, requested_limit))
        if collection not in PAGINATED_COLLECTIONS:
            requested_offset = 0
            limit = max(limit, len(items) or 1)
        self.send_json(paginate_items(items, limit, requested_offset))

    def handle_ai(self, kind):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        prompt = body.get("prompt", "")
        request_state = body.get("state")
        try:
            state = resolve_request_state(user, request_state)
        except ValidationError as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        contact_id = str(body.get("contactId") or "")
        answer, provider = call_openai(kind, prompt, state, contact_id)
        log_ai(kind, prompt, answer, provider)
        self.send_json({"answer": answer, "provider": provider, "intent": detect_intent(prompt)})

    def handle_email_send(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        try:
            state = resolve_request_state(user, body.get("state"))
        except ValidationError as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        to_email = str(body.get("to") or "").strip()
        subject = str(body.get("subject") or "CRM email").strip()
        content = str(body.get("body") or "").strip()
        linked = str(body.get("linked") or "CRM").strip()
        if not to_email or not content:
            self.send_json({"error": "Email recipient and body are required"}, status=400)
            return
        try:
            status, provider = send_email_provider(to_email, subject, content)
        except Exception as exc:
            status, provider = "FAILED", "smtp"
            content = f"{content}\n\nDelivery error: {exc}"
        email = add_email_to_state(state, to_email, subject, content, linked, status, provider)
        append_audit(state, "Sent email", linked, "CRM")
        state = sanitize_crm_state(state, user)
        save_state(state, state_scope_for_user(user))
        log_communication("EMAIL", "OUT", to_email, subject, content, status, provider, linked)
        self.send_json({"state": state, "email": email, "status": status, "provider": provider})

    def handle_whatsapp_send(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        try:
            state = resolve_request_state(user, body.get("state"))
        except ValidationError as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        contact_id = str(body.get("contactId") or "").strip()
        to_phone = str(body.get("to") or "").strip()
        content = str(body.get("content") or "").strip()
        linked = str(body.get("linked") or "CRM").strip()
        if not to_phone and contact_id:
            contact = find_contact(state, contact_id)
            to_phone = contact.get("whatsapp") or contact.get("phone", "")
        if not to_phone or not content:
            self.send_json({"error": "WhatsApp recipient and content are required"}, status=400)
            return
        try:
            status, provider = send_whatsapp_provider(to_phone, content)
        except Exception as exc:
            status, provider = "FAILED", "meta"
            status, provider = "FAILED", "meta"
            content = f"{content}\n\nDelivery error: {exc}"
        message = add_whatsapp_to_state(state, contact_id, to_phone, content, "OUT", bool(body.get("bot")), status, provider)
        append_audit(state, "Sent WhatsApp message", linked, "CRM")
        state = sanitize_crm_state(state, user)
        save_state(state, state_scope_for_user(user))
        log_communication("WHATSAPP", "OUT", to_phone, "", content, status, provider, linked)
        self.send_json({"state": state, "message": message, "status": status, "provider": provider})

    def handle_whatsapp_inbound(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        try:
            state = resolve_request_state(user, body.get("state"))
        except ValidationError as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        contact_id = str(body.get("contactId") or "").strip()
        content = str(body.get("content") or "").strip()
        if not contact_id or not content:
            self.send_json({"error": "Contact and content are required"}, status=400)
            return
        contact = find_contact(state, contact_id)
        phone = contact.get("whatsapp") or contact.get("phone", "")
        inbound = add_whatsapp_to_state(state, contact_id, phone, content, "IN", False, "RECEIVED", "webhook")
        log_communication("WHATSAPP", "IN", phone, "", content, "RECEIVED", "webhook", contact_id)
        reply = None
        if body.get("autoReply", True):
            answer, provider = call_openai("whatsapp", f"Write a concise WhatsApp reply to: {content}", state, contact_id)
            reply_text = answer[:900]
            status, wa_provider = send_whatsapp_provider(phone, reply_text)
            reply = add_whatsapp_to_state(state, contact_id, phone, reply_text, "OUT", True, status, wa_provider if wa_provider != "simulated" else provider)
            log_communication("WHATSAPP", "OUT", phone, "", reply_text, status, wa_provider, contact_id)
        append_audit(state, "Received WhatsApp message", contact_id, "Webhook")
        state = sanitize_crm_state(state, user)
        save_state(state, state_scope_for_user(user))
        self.send_json({"state": state, "message": inbound, "reply": reply})

    def handle_automation_run(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        try:
            state = resolve_request_state(user, body.get("state"))
        except ValidationError as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        state, results = run_automation(state, state_scope_for_user(user))
        state = sanitize_crm_state(state, user)
        save_state(state, state_scope_for_user(user))
        self.send_json({"state": state, "results": results, "count": len(results), "logs": communication_logs()})

    def handle_generate_message(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        lead_id = str(body.get("leadId") or body.get("lead_id") or "LEAD")
        prompt = str(body.get("prompt") or f"Draft follow-up message for lead {lead_id}")
        request_state = body.get("state")
        try:
            state = resolve_request_state(user, request_state)
        except ValidationError as exc:
            self.send_json({"error": str(exc)}, status=400)
            return
        answer, provider, cached, reason = generate_message_safe(lead_id, prompt, state, user.get("sub"), "assistant")
        log_ai("generate-message", prompt, answer, provider)
        create_activity(lead_id, "AI_MESSAGE_GENERATED", "SUCCESS" if provider != "fallback" else "FALLBACK", f"Provider={provider}", {"cached": cached, "reason": reason})
        self.send_json({"message": answer, "provider": provider, "cached": cached, "fallback_reason": reason})

    def handle_send_email_v2(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        lead_id = str(body.get("leadId") or body.get("lead_id") or "LEAD")
        to_email = str(body.get("to") or "").strip()
        subject = str(body.get("subject") or "Follow-up from JK Fluid Controls").strip()
        message = str(body.get("message") or body.get("body") or "").strip()
        if not to_email or not message:
            self.send_json({"error": "Email recipient and message are required"}, status=400)
            return
        status, provider = send_email_provider(to_email, subject, message)
        create_activity(lead_id, "EMAIL_SENT", status, f"to={to_email}", {"provider": provider, "subject": subject})
        mark_lead_contacted(lead_id, follow_up_sent=False)
        self.send_json({"ok": status in ("SENT", "SIMULATED"), "status": status, "provider": provider})

    def handle_send_whatsapp_v2(self):
        user = self.require_auth()
        if not user:
            return
        body = self.read_json()
        lead_id = str(body.get("leadId") or body.get("lead_id") or "LEAD")
        to_phone = str(body.get("to") or "").strip()
        message = str(body.get("message") or body.get("content") or "").strip()
        if not to_phone or not message:
            self.send_json({"error": "WhatsApp recipient and message are required"}, status=400)
            return
        status, provider = send_whatsapp_provider(to_phone, message)
        create_activity(lead_id, "WHATSAPP_SENT", status, f"to={to_phone}", {"provider": provider})
        mark_lead_contacted(lead_id, follow_up_sent=False)
        self.send_json({"ok": status in ("SENT", "DELIVERED"), "status": status, "provider": provider})

    def handle_lead_contacted(self, path):
        lead_id = path.split("/")[2] if len(path.split("/")) > 2 else ""
        if not lead_id:
            self.send_json({"error": "Lead id is required"}, status=400)
            return
        body = self.read_json()
        follow_up_sent = bool(body.get("follow_up_sent", False))
        mark_lead_contacted(lead_id, follow_up_sent=follow_up_sent)
        create_activity(lead_id, "LEAD_CONTACT_UPDATED", "SUCCESS", "Lead contact fields updated", {"follow_up_sent": follow_up_sent})
        self.send_json({"ok": True, "lead_id": lead_id, "follow_up_sent": follow_up_sent})

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def send_json(self, payload, status=200):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        try:
            from middleware import apply_security_headers, cache_idempotency
            apply_security_headers(self)
            cache_idempotency(self, payload)
        except ImportError:
            pass
        self.end_headers()
        self.wfile.write(data)
        log_api(self.command, self.route_path(), status, payload.get("error", "") if isinstance(payload, dict) else "")

    def serve_static(self):
        root = static_root()
        request_path = self.route_path().lstrip("/") or "index.html"
        target = (root / request_path).resolve()
        if root not in target.parents and target != root:
            self.send_error(403)
            return
        if not target.exists() or target.is_dir():
            target = root / "index.html"
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        try:
            from middleware import log_structured
            log_structured("http_request", "anonymous", 0, 200, {"message": fmt % args, "path": self.path})
        except ImportError:
            sys.stderr.write("[%s] %s\n" % (now(), fmt % args))


def main():
    load_env()
    try:
        init_db()
    except Exception as exc:
        app_log("Database init failed", engine=db_engine(), error=str(exc))
        raise
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8765"))
    server = ThreadingHTTPServer((host, port), CRMHandler)
    scheduler_thread = threading.Thread(target=followup_scheduler_loop, daemon=True)
    scheduler_thread.start()
    print(f"JK Fluid Controls CRM running at http://{host}:{port}")
    print(f"Database engine: {db_engine()}")
    print(f"Database target: {database_label()}")
    print("Static files:", static_root())
    print("AI provider:", "OpenAI" if os.environ.get("OPENAI_API_KEY") else "fallback")
    server.serve_forever()


if __name__ == "__main__":
    main()
