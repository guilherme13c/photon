#!/usr/bin/env python3
"""Deterministic crawl origin used only by Photon functional tests."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import os
import ssl
import threading
import time
from urllib.parse import urlparse

state = {"requests": [], "active": 0, "max_active": 0}
# `/__requests` records its own request and then serializes state. `_send`
# records completion as well, so this must be re-entrant rather than deadlock
# while holding the state lock.
lock = threading.RLock()

class Handler(BaseHTTPRequestHandler):
    server_version = "PhotonFixtureOrigin/1"

    def log_message(self, *_args):
        pass

    def _record(self):
        started_at_ms = round(time.time() * 1000)
        record = {
            "path": self.path, "host": self.headers.get("Host", ""),
            "user_agent": self.headers.get("User-Agent", ""),
            "trace_id": self.headers.get("X-Photon-Trace-ID", urlparse(self.path).query),
            "started_at_ms": started_at_ms,
        }
        self.request_record = record
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            record["active_at_start"] = state["active"]
            state["requests"].append(record)

    def _complete_request(self, status, payload_size):
        with lock:
            if hasattr(self, "request_record"):
                self.request_record.update({
                    "completed_at_ms": round(time.time() * 1000),
                    "status": status, "payload_size_bytes": payload_size,
                    "active_at_completion": state["active"],
                })

    def _done(self):
        with lock:
            state["active"] -= 1

    def _send(self, status, body, content_type="text/html; charset=utf-8"):
        encoded = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)
        self._complete_request(status, len(encoded))

    def do_GET(self):
        self._record()
        try:
            path = urlparse(self.path).path
            if path == "/__requests":
                with lock:
                    self._send(200, json.dumps(state), "application/json")
            elif path == "/__reset":
                with lock:
                    # This request is itself in flight. Resetting `active`
                    # here would make its final `_done()` drive the counter
                    # negative and corrupt subsequent concurrency evidence.
                    state.update(requests=[], max_active=state["active"])
                self._send(200, "reset", "text/plain")
            elif path == "/robots.txt":
                delay = os.getenv("PHOTON_ORIGIN_CRAWL_DELAY", "1")
                self._send(200, f"User-agent: *\nAllow: /\nDisallow: /blocked\nCrawl-delay: {delay}\n", "text/plain")
            elif path == "/blocked":
                self._send(200, "<html>blocked fixture</html>")
            elif path == "/redirect":
                self.send_response(302); self.send_header("Location", "/static"); self.end_headers(); self._complete_request(302, 0)
            elif path == "/dynamic":
                self._send(200, '<html><body><div id="root"></div><script>window.__INITIAL_STATE__={};</script></body></html>')
            elif path.startswith("/slow/"):
                time.sleep(min(float(path.rsplit("/", 1)[-1]), 5.0))
                self._send(200, "<html><title>Slow</title><body>slow page</body></html>")
            elif path.startswith("/payload/"):
                size = {"small": 1024, "medium": 64 * 1024, "large": 512 * 1024}.get(path.rsplit("/", 1)[-1], 1024)
                self._send(200, "<html><title>Payload</title><body>" + ("x" * size) + "</body></html>")
            else:
                self._send(200, '<html><title>Static</title><body><a href="/linked">linked</a>static page</body></html>')
        finally:
            self._done()

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=8088)
parser.add_argument("--cert")
parser.add_argument("--key")
args = parser.parse_args()
server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
if args.cert and args.key:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.cert, args.key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
