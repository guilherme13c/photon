import json
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .service import SearchService


def make_handler(embedder, repository, metrics=None):
    service = SearchService(embedder, repository)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                self._write(200, {"status": "ok"})
                return
            if self.path == "/readyz":
                try:
                    repository.ready()
                    self._write(200, {"status": "ready"})
                except Exception:
                    self._write(503, {"error": "dependencies unavailable"})
                return
            if self.path == "/metrics":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.end_headers()
                self.wfile.write((metrics.render() if metrics else "").encode())
                return
            if urlparse(self.path).path != "/v1/search":
                self._write(404, {"error": "not found"})
                return
            query = parse_qs(urlparse(self.path).query)
            try:
                started = time.perf_counter()
                limit = int(query.get("limit", ["0"])[0])
                response = service.search(query.get("q", [""])[0], limit, query.get("cursor", [""])[0])
                if metrics:
                    metrics.observe(200, len(response["results"]), time.perf_counter() - started)
                self._write(200, response)
            except ValueError as exc:
                if metrics:
                    metrics.observe(400, 0)
                self._write(400, {"error": str(exc)})
            except Exception:
                if metrics:
                    metrics.observe(502, 0)
                self._write(502, {"error": "search dependency unavailable"})

        def log_message(self, *_args):
            return

        def _write(self, status, payload):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler
