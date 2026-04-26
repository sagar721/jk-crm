import json
import time
import logging

# Configure structured JSON logging globally
logging.basicConfig(level=logging.INFO, format='%(message)s')

def log_structured(action, user_id, duration_ms, status_code, extra=None):
    """
    Replaces print() with structured JSON logging for monitoring tools.
    """
    log_data = {
        "timestamp": time.time(),
        "action": action,
        "user_id": user_id,
        "duration_ms": duration_ms,
        "status_code": status_code,
        **(extra or {})
    }
    logging.info(json.dumps(log_data))

# Simple in-memory cache for Idempotency (in production, use Redis or Postgres)
_idempotency_cache = {}

def check_idempotency(handler, payload):
    """
    Checks if a request with an Idempotency-Key has already been processed.
    Returns (True, cached_response) if duplicate.
    Returns (False, None) if new request.
    """
    key = handler.headers.get("Idempotency-Key")
    if key and key in _idempotency_cache:
        log_structured("idempotent_hit", "system", 0, 200, {"key": key})
        return True, _idempotency_cache[key]
    return False, None

def cache_idempotency(handler, response_dict):
    """
    Saves the successful response to the idempotency cache.
    """
    key = handler.headers.get("Idempotency-Key")
    if key:
        _idempotency_cache[key] = response_dict

def apply_security_headers(handler):
    """
    Applies strict security headers to all outgoing API responses.
    """
    handler.send_header("X-Frame-Options", "DENY")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
    handler.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
