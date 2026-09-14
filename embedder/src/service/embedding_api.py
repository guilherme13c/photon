import json
from http.server import BaseHTTPRequestHandler


def make_handler(model):
    class EmbeddingHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/v1/embed":
                self._write(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length))
                texts = body.get("texts")
                if not isinstance(texts, list) or not texts or not all(isinstance(text, str) for text in texts):
                    raise ValueError("texts must be a non-empty list of strings")
                vectors = model.encode(texts, show_progress_bar=False)
                embeddings = [vector.tolist() if hasattr(vector, "tolist") else list(vector) for vector in vectors]
                dimensions = len(embeddings[0]) if embeddings else 0
                if not embeddings or any(len(vector) != dimensions for vector in embeddings):
                    raise ValueError("model returned inconsistent vectors")
                self._write(200, {"embeddings": embeddings, "dimensions": dimensions})
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self._write(400, {"error": str(exc)})
            except Exception:
                self._write(500, {"error": "embedding failed"})

        def log_message(self, *_args):
            return

        def _write(self, status, payload):
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return EmbeddingHandler
