"""
wsgi.py — Gunicorn-compatible entry point for the CRM SaaS backend.

This starts the existing ThreadingHTTPServer in a background thread
and then exposes a minimal WSGI app that proxies all requests to it
via a local socket, making it fully compatible with Gunicorn.

Render Start Command:
    gunicorn wsgi:application --workers 1 --threads 4 --bind 0.0.0.0:$PORT --timeout 60
"""
import os
import sys
import threading
import urllib.request
import urllib.error

from server import CRMHandler, load_env, init_db
from http.server import ThreadingHTTPServer

# ── Startup ───────────────────────────────────────────────────────────────────
load_env()
try:
    init_db()
except Exception as e:
    sys.stderr.write(f"[wsgi] DB init failed: {e}\n")

# Internal port the real CRMHandler listens on (never exposed externally)
_INTERNAL_PORT = int(os.environ.get("INTERNAL_PORT", "8766"))

def _start_internal_server():
    """Start the real CRMHandler on an internal port in a background thread."""
    server = ThreadingHTTPServer(("127.0.0.1", _INTERNAL_PORT), CRMHandler)
    server.serve_forever()

_thread = threading.Thread(target=_start_internal_server, daemon=True)
_thread.start()

# ── WSGI Proxy ────────────────────────────────────────────────────────────────
def application(environ, start_response):
    """
    WSGI callable. Gunicorn calls this for every request.
    Forwards the request to the internal CRMHandler and returns its response.
    """
    method = environ.get("REQUEST_METHOD", "GET")
    path = environ.get("PATH_INFO", "/")
    query = environ.get("QUERY_STRING", "")
    full_path = f"{path}?{query}" if query else path
    content_length = int(environ.get("CONTENT_LENGTH") or 0)
    body = environ["wsgi.input"].read(content_length) if content_length > 0 else None

    # Build target URL on the internal server
    target_url = f"http://127.0.0.1:{_INTERNAL_PORT}{full_path}"

    # Forward request headers
    headers = {}
    for key, val in environ.items():
        if key.startswith("HTTP_"):
            header_name = key[5:].replace("_", "-").title()
            headers[header_name] = val
    content_type = environ.get("CONTENT_TYPE", "")
    if content_type:
        headers["Content-Type"] = content_type

    req = urllib.request.Request(target_url, data=body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as res:
            status = f"{res.status} {res.reason}"
            response_headers = list(res.headers.items())
            response_body = res.read()
    except urllib.error.HTTPError as e:
        status = f"{e.code} {e.reason}"
        response_headers = list(e.headers.items())
        response_body = e.read()
    except Exception as e:
        start_response("502 Bad Gateway", [("Content-Type", "text/plain")])
        return [f"Gateway error: {e}".encode()]

    start_response(status, response_headers)
    return [response_body]
