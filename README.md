# JK Fluid Controls CRM

This is a complete local CRM prototype built from the provided PRD. It now includes a local backend, database persistence (SQLite or PostgreSQL/Supabase), and AI endpoints.

## Quick Start

Install/build the frontend and start the backend:

```bash
npm install
npm run build
python3 server.py
```

The server will serve `dist/` when it exists, and falls back to source files during development.

Open:

```text
http://127.0.0.1:8765
```

The app still falls back to browser-only mode if `index.html` is opened directly.

## Demo Login

Use any of these accounts. Password is accepted for demo mode.

- `admin@jkfluidcontrols.com` - Admin
- `manager@jkfluidcontrols.com` - Sales Manager
- `sales@jkfluidcontrols.com` - Sales Executive
- `viewer@jkfluidcontrols.com` - Viewer

## Included Modules

- Authentication with role-based navigation
- Dashboard KPIs, charts, and activity feed
- Companies, contacts, inquiries, pipeline board
- Quotation builder with GST calculations and printable quotation
- Orders, dispatch tracking, activities, and calendar
- WhatsApp inbox, email inbox, automation sequences
- Backend email send endpoint with SMTP support and simulated delivery fallback
- Backend WhatsApp send/inbound endpoints with Meta Cloud API support and simulated delivery fallback
- One-click automation runner for quote follow-ups, inquiry acknowledgements, and delivery feedback
- Reports with CSV export
- AI assistant for CRM insights, using OpenAI when configured
- Settings, users, templates, audit log, dark mode

## Database

By default, the server stores CRM state in SQLite:

```text
/Users/sagarmali/Downloads/CODEX CRM/crm.sqlite3
```

You can switch to PostgreSQL (including Supabase Postgres) via `.env`.

1) Install PostgreSQL driver:

```bash
pip install "psycopg[binary]"
```

2) Set one database URL:

```text
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require
```

or for Supabase:

```text
SUPABASE_DB_URL=postgresql://postgres:PASSWORD@db.<project-ref>.supabase.co:5432/postgres?sslmode=require
```

All create/update actions in the app are synced to the database through `/api/state`.

## Supabase Backend

The production-ready Supabase schema lives in [supabase/README.md](</Users/sagarmali/Downloads/CODEX CRM/supabase/README.md>) and [20260425213000_crm_backend.sql](</Users/sagarmali/Downloads/CODEX CRM/supabase/migrations/20260425213000_crm_backend.sql>).

It includes:

- Supabase Auth for email/password login
- `companies`, `contacts`, and `orders` tables with UUID primary keys
- `user_id` defaulted from `auth.uid()`
- RLS policies on every table with `USING (auth.uid() = user_id)` and `WITH CHECK (auth.uid() = user_id)`
- Tenant-safe relationships so contacts and orders can only point at companies owned by the same user
- Case-insensitive unique email enforcement on contacts
- Indexed `user_id`, `company_id`, and status columns for common CRM queries

The example `supabase-js` calls for sign-up, sign-in, insert, fetch, and update live in [crm-client.mjs](</Users/sagarmali/Downloads/CODEX CRM/supabase/examples/crm-client.mjs>).

## AI

Copy `.env.example` to `.env` and add your key:

```bash
cp .env.example .env
```

Set:

```text
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4o-mini
```

AI uses OpenAI's Chat Completions API. Without a key, the AI assistant, intent detection, and email draft tool use a deterministic local fallback so the CRM remains usable.

### Production-safe communication endpoints

The backend now exposes quota-safe communication APIs:

- `POST /generate-message` (retry + exponential backoff + timeout + fallback template)
- `POST /send-email` (SMTP send + DB activity log)
- `POST /send-whatsapp` (Twilio WhatsApp with number validation and retry, then Meta fallback if Twilio is not configured)
- `PATCH /lead/:id/contacted` (updates `last_contacted`, `follow_up_due`, `follow_up_sent`)
- `GET /activities` (returns communication activity log)

The scheduler runs every hour (`FOLLOWUP_POLL_SECONDS`, default `3600`) and sends due follow-ups using AI with fallback messaging.

## Email Automation

The app sends and logs email through:

```text
POST /api/email/send
```

For Gmail SMTP, set:

```text
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your.gmail@gmail.com
SMTP_PASS=your_google_app_password
SMTP_FROM=your.gmail@gmail.com
```

If SMTP credentials are missing, the CRM records messages as `SIMULATED`, which is useful for testing workflows without sending real mail.

## WhatsApp Automation

The app sends, receives, and auto-replies through:

```text
POST /api/whatsapp/send
POST /api/whatsapp/inbound
```

If Meta WhatsApp credentials are set in `.env`, outbound WhatsApp uses the Cloud API. If not, messages are logged as simulated deliveries.

## Run Automations

Use the Automation page's `Run Now` button, or call:

```text
POST /api/automation/run
```

Current triggers:

- `QUOTE_SENT` - sends email + WhatsApp follow-up for sent/revised quotations after the configured delay
- `INQUIRY_CREATED` - sends WhatsApp acknowledgement for new inquiries
- `ORDER_DELIVERED` - sends WhatsApp feedback request for delivered orders after the configured delay

Each sequence supports:

- `delayHours` - wait this many hours after the trigger timestamp
- `condition` - `ALWAYS` or `NO_REPLY`; `NO_REPLY` skips follow-up if the contact has already replied

## Notes

The PRD specifies a production React/Express/PostgreSQL stack. This build uses a zero-install Python backend and supports SQLite by default, with optional PostgreSQL/Supabase via environment configuration.
# jk-crm
