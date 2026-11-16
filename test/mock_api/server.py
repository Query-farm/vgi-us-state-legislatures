#!/usr/bin/env python3
"""Offline stand-in for the Open States API v3 and the dataset downloads, serving test/fixtures.

Endpoints
---------
``GET /files/<name>``
    A file from ``test/fixtures`` (people.tar.gz, klarner.tab, shor_mccarty_*.tab), with an
    ``ETag``; ``If-None-Match`` answers 304.
``GET /v3/jurisdictions``, ``/v3/jurisdictions/<id>``, ``/v3/people``, ``/v3/bills``,
``/v3/committees``, ``/v3/events``
    Recorded/synthetic Open States responses for Nebraska. Requests must send
    ``X-API-KEY: <MOCK_API_KEY>`` (default ``test-key``) or get 401. ``/v3/bills`` applies the
    ``session`` and ``updated_since`` filters itself and answers 400 without a jurisdiction or q.
``GET /_requests``
    The query strings received so far, per path (for pushdown assertions in unit tests).
``GET /health``
    ``{"status": "ok"}``.

``MOCK_API_FLAKY=1``: the first request for each path answers 429 with ``Retry-After: 0``.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
API = FIXTURES / "openstates"
NE = "ocd-jurisdiction/country:us/state:ne/government"

_lock = threading.Lock()
_seen: set[str] = set()
_requests: dict[str, list[str]] = {}


def _load(name: str) -> Any:
    return json.loads((API / name).read_text())


def _empty() -> dict[str, Any]:
    return {"results": [], "pagination": {"per_page": 20, "page": 1, "max_page": 1, "total_items": 0}}


def _jurisdiction(q: dict[str, list[str]]) -> str:
    value = q.get("jurisdiction", [""])[0].lower()
    return NE if value in (NE, "ne", "nebraska") else value


def _bills(q: dict[str, list[str]]) -> tuple[int, Any]:
    if not q.get("jurisdiction") and not q.get("q"):
        return 400, {"detail": "must provide either jurisdiction or query string"}
    if _jurisdiction(q) != NE:
        return 200, _empty()
    page = int(q.get("page", ["1"])[0])
    if page not in (1, 2):
        return 200, _empty()
    body = _load(f"bills_ne_p{page}.json")
    session = q.get("session", [None])[0]
    since = q.get("updated_since", [None])[0]
    body["results"] = [
        b
        for b in body["results"]
        if (session is None or b["session"] == session) and (since is None or b["updated_at"] >= since)
    ]
    return 200, body


def route(path: str, q: dict[str, list[str]]) -> tuple[int, Any]:
    """Answer one API request."""
    if path == "/v3/jurisdictions":
        return 200, _load("jurisdictions.json")
    if path.startswith("/v3/jurisdictions/"):
        jid = unquote(path[len("/v3/jurisdictions/") :])
        return (200, _load("jurisdiction_ne.json")) if jid == NE else (404, {"detail": "not found"})
    if path == "/v3/people":
        if _jurisdiction(q) != NE:
            return 200, _empty()
        page = int(q.get("page", ["1"])[0])
        if page not in (1, 2, 3):
            return 200, _empty()
        body = _load(f"people_ne_p{page}.json")
        district = q.get("district", [None])[0]
        if district is not None:
            body["results"] = [p for p in body["results"] if str(p["current_role"]["district"]) == district]
        return 200, body
    if path == "/v3/bills":
        return _bills(q)
    if path == "/v3/committees":
        return 200, _load("committees_ne.json") if _jurisdiction(q) == NE else _empty()
    if path == "/v3/events":
        return 200, _load("events_ne.json") if _jurisdiction(q) == NE else _empty()
    return 404, {"detail": f"no route {path}"}


class Handler(BaseHTTPRequestHandler):
    """Serve fixture files and API responses."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (base-class signature)
        """Log to stderr only when MOCK_API_VERBOSE is set."""
        if os.environ.get("MOCK_API_VERBOSE"):
            sys.stderr.write(format % args + "\n")

    def _send(self, status: int, body: bytes, content_type: str, headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj: Any) -> None:
        self._send(status, json.dumps(obj).encode(), "application/json")

    def do_GET(self) -> None:
        """Route a GET."""
        url = urlparse(self.path)
        q = parse_qs(url.query)
        if url.path == "/health":
            self._json(200, {"status": "ok"})
            return
        if url.path == "/_requests":
            with _lock:
                self._json(200, _requests)
            return
        flaky = os.environ.get("MOCK_API_FLAKY") == "1"
        with _lock:
            _requests.setdefault(url.path, []).append(url.query)
            first = url.path not in _seen
            _seen.add(url.path)
        if flaky and first:
            self._send(429, b'{"detail": "slow down"}', "application/json", {"Retry-After": "0"})
            return
        if url.path.startswith("/files/"):
            target = FIXTURES / url.path[len("/files/") :]
            if not target.is_file() or target.parent != FIXTURES:
                self._json(404, {"detail": "no such file"})
                return
            data = target.read_bytes()
            etag = '"' + hashlib.sha256(data).hexdigest()[:16] + '"'
            if self.headers.get("If-None-Match") == etag:
                self._send(304, b"", "application/octet-stream", {"ETag": etag})
                return
            self._send(200, data, "application/octet-stream", {"ETag": etag})
            return
        if url.path.startswith("/v3/"):
            if self.headers.get("X-API-KEY") != os.environ.get("MOCK_API_KEY", "test-key"):
                self._json(401, {"detail": "invalid API key"})
                return
            status, body = route(url.path, q)
            self._json(status, body)
            return
        self._json(404, {"detail": "not found"})


def main() -> None:
    """Serve until killed, printing ``PORT:<n>`` once listening."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
