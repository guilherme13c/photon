#!/usr/bin/env python3
"""Deterministic crawl origin used only by Photon functional tests."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import ssl
import threading
import time
from urllib.parse import urlparse

state = {"requests": [], "active": 0, "max_active": 0}
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    server_version = "PhotonFixtureOrigin/1"

    def log_message(self, *_args):
        pass

    def _record(self):
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            state["requests"].append({
                "path": self.path, "host": self.headers.get("Host", ""),
                "user_agent": self.headers.get("User-Agent", ""),
                "started_at_ms": round(time.time() * 1000),
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

    def do_GET(self):
        self._record()
        try:
            path = urlparse(self.path).path
            if path == "/__requests":
                with lock:
                    self._send(200, json.dumps(state), "application/json")
            elif path == "/__reset":
                with lock:
                    state.update(requests=[], active=0, max_active=0)
                self._send(200, "reset", "text/plain")
            elif path == "/robots.txt":
                self._send(200, "User-agent: *\nAllow: /\nDisallow: /blocked\nCrawl-delay: 1\n", "text/plain")
            elif path == "/blocked":
                self._send(200, "<html>blocked fixture</html>")
            elif path == "/redirect":
                self.send_response(302); self.send_header("Location", "/static"); self.end_headers()
            elif path == "/dynamic":
                self._send(200, '<html><body><div id="root"></div><script>window.__INITIAL_STATE__={};</script></body></html>')
            elif path.startswith("/slow/"):
                time.sleep(min(float(path.rsplit("/", 1)[-1]), 5.0))
                self._send(200, "<html><title>Slow</title><body>slow page</body></html>")
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
