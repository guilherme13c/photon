import json
import mimetypes
import time
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .service import SearchService

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


def make_handler(embedder, repository, metrics=None, static_dir=STATIC_DIR, sparse_encoder=None):
    service = SearchService(embedder, repository, sparse_encoder=sparse_encoder)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/" or path.startswith("/assets/") or path in ("/app.js", "/styles.css"):
                self._serve_static(path)
                return
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
            if path != "/v1/search":
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

        def _serve_static(self, path):
            relative = "index.html" if path == "/" else path.lstrip("/")
            candidate = (Path(static_dir) / relative).resolve()
            root = Path(static_dir).resolve()
            if root not in candidate.parents and candidate != root:
                self._write(404, {"error": "not found"})
                return
            try:
                body = candidate.read_bytes()
            except OSError:
                self._write(404, {"error": "not found"})
                return
            content_type = mimetypes.guess_type(candidate.name)[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler
