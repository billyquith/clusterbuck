#!/usr/bin/env python3
"""A tiny OpenAI-compatible model server for tests and CI.

Serves POST /v1/chat/completions and returns a deterministic canned completion, so the
end-to-end loop (submit → queue → worker → model server → result → poll) can be proven
with zero model weight and no GPU. In real dev the worker points at Ollama instead
(protocols.md §3 — the worker only speaks the wire protocol, so they are interchangeable).

Usage:  python fake_model_server.py [--port 11434]
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


# Model ids whose pull should fail, so failure handling is testable too (--fail-pulls).
FAIL_PULLS: set[str] = set()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:  # quiet
        pass

    # Models this stub claims to have, and which are "warm". Overridden via --models/--loaded
    # so discovery tests can assert specific inventories.
    models: list[str] = ["fake"]
    loaded_models: list[str] = []

    def _json(self, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = self.path.rstrip("/")
        # Generic OpenAI discovery endpoint — the portable way a worker learns what's here.
        if path == "/v1/models":
            self._json({"object": "list", "data": [
                {"id": m, "object": "model", "owned_by": "fake"} for m in self.models]})
            return
        # Ollama-native shapes, so the vendor adapter path is testable without Ollama.
        if path == "/api/ps":
            self._json({"models": [{"name": m} for m in self.loaded_models]})
            return
        if path == "/api/tags":
            self._json({"models": [
                {"name": m, "digest": f"sha256:{'0' * 60}{i:04d}"}
                for i, m in enumerate(self.models)]})
            return
        # Readiness probe: any other GET returns 200.
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or "{}")

    def do_DELETE(self) -> None:
        # Ollama-native model removal, so the reclaim path is testable without Ollama.
        if self.path.rstrip("/") == "/api/delete":
            model = self._read_json().get("model")
            if model in self.models:
                self.models.remove(model)
                if model in self.loaded_models:
                    self.loaded_models.remove(model)
                self._json({"status": "success"})
            else:
                self.send_error(404, "model not found")
            return
        self.send_error(404, "not found")

    def do_POST(self) -> None:
        path = self.path.rstrip("/")
        # Ollama-native model install, so the approved-pull path is testable without Ollama.
        if path == "/api/pull":
            model = self._read_json().get("model")
            if not model:
                self._json({"error": "no model given"})
                return
            if model in FAIL_PULLS:
                self._json({"error": f"simulated pull failure for {model}"})
                return
            if model not in self.models:
                self.models.append(model)
            self._json({"status": "success"})
            return
        if path != "/v1/chat/completions":
            self.send_error(404, "not found")
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length) or "{}")
        model = body.get("model", "fake-model")
        messages = body.get("messages", [])
        last_user = next(
            (m.get("content", "") for m in reversed(messages) if m.get("role") == "user"),
            "",
        )

        completion = {
            "id": f"chatcmpl-fake-{int(time.time()*1000)}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {
                        "role": "assistant",
                        "content": f"[fake:{model}] echo: {last_user}",
                    },
                }
            ],
            "usage": {
                "prompt_tokens": len(last_user.split()),
                "completion_tokens": 4,
                "total_tokens": len(last_user.split()) + 4,
            },
        }
        payload = json.dumps(completion).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=11434)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--models", default="fake",
                    help="comma-separated model ids to advertise on /v1/models")
    ap.add_argument("--loaded", default="",
                    help="comma-separated model ids to report warm on /api/ps")
    ap.add_argument("--fail-pulls", default="",
                    help="comma-separated model ids whose /api/pull should fail")
    args = ap.parse_args()
    Handler.models = [m for m in args.models.split(",") if m]
    Handler.loaded_models = [m for m in args.loaded.split(",") if m]
    FAIL_PULLS.update(m for m in args.fail_pulls.split(",") if m)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"fake model server on http://{args.host}:{args.port}/v1/chat/completions")
    server.serve_forever()


if __name__ == "__main__":
    main()
