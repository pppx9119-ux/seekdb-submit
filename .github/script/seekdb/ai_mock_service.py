#!/usr/bin/env python3
"""Deterministic mock AI service for the seekdb AI-function mysqltests.

The test suite under tools/deploy/mysql_test/test_suite/ai_function creates
model endpoints whose URLs point at http://0.0.0.0:8000/... and covers the
OpenAI compatible chat/embedding protocol, the SiliconFlow rerank protocol and
the native Ollama generate/embed protocol.

The service keeps no state.  Embedding and rerank responses deliberately list
their elements in reverse order: every element still carries the index of its
position inside the request, so a client that reassembles the answer by index
(the behaviour these tests protect) produces the correct per-row results.

Only the Python standard library is used.
"""

import argparse
import json
import math
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEFAULT_EMBED_DIMENSION = 768


def embedding_for(text, dimension):
    """Return a deterministic, finite embedding for one input string."""
    seed = sum(ord(ch) for ch in text) or 1
    return [round(math.sin((seed + i + 1) * 0.017), 6) for i in range(dimension)]


def requested_inputs(body):
    """Normalize the OpenAI/Ollama "input" member to a list of strings."""
    value = body.get("input", [])
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)
                for item in value]
    return []


def requested_dimension(body):
    dimension = body.get("dimensions")
    if isinstance(dimension, int) and not isinstance(dimension, bool) and dimension > 0:
        return dimension
    return DEFAULT_EMBED_DIMENSION


def rerank_results(query, documents):
    """Score documents so that documents containing the query rank first."""
    ranked = []
    for index, document in enumerate(documents):
        text = document if isinstance(document, str) else json.dumps(document, ensure_ascii=False)
        ranked.append((0 if query in text else 1, index))
    ranked.sort(key=lambda item: (item[0], item[1]))
    total = len(ranked)
    results = []
    for rank, (_, index) in enumerate(ranked):
        results.append({
            "index": index,
            "relevance_score": round(1.0 - rank / float(total + 1), 6),
        })
    return results


class MockAIHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        sys.stderr.write("[ai-mock] %s %s\n" % (self.address_string(), fmt % args))

    def _read_json_body(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            body = {}
        return body if isinstance(body, dict) else {}

    def _send_json(self, payload, status=200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path in ("/", "/health", "/healthz"):
            self._send_json({"status": "ok"})
        else:
            self._send_json({"error": "not found"}, status=404)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        body = self._read_json_body()
        try:
            if path.endswith("/chat/completions"):
                self._handle_chat_completion(body)
            elif path.endswith("/embeddings"):
                self._handle_embeddings(body, ollama=False)
            elif path.endswith("/rerank"):
                self._handle_rerank(body)
            elif path.endswith("/api/generate"):
                self._handle_ollama_generate(body)
            elif path.endswith("/api/embed") or path.endswith("/api/embeddings"):
                self._handle_embeddings(body, ollama=True)
            else:
                self._send_json({"error": "unknown path: %s" % path}, status=404)
        except Exception as exc:  # keep the service alive for the rest of the suite
            sys.stderr.write("[ai-mock] error while handling %s: %r\n" % (path, exc))
            self._send_json({"error": "internal mock error"}, status=500)

    def _handle_chat_completion(self, body):
        model = body.get("model", "mock-model")
        content = "mock completion for %s" % model
        sys.stderr.write("[ai-mock] chat completion model=%s\n" % model)
        self._send_json({
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": 0,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        })

    def _handle_embeddings(self, body, ollama):
        inputs = requested_inputs(body)
        dimension = requested_dimension(body)
        model = body.get("model", "mock-model")
        sys.stderr.write("[ai-mock] embeddings model=%s inputs=%d dimension=%d protocol=%s\n"
                         % (model, len(inputs), dimension, "ollama" if ollama else "openai"))
        if ollama:
            self._send_json({
                "model": model,
                "embeddings": [embedding_for(text, dimension) for text in inputs],
            })
            return
        data = [{
            "object": "embedding",
            "index": index,
            "embedding": embedding_for(text, dimension),
        } for index, text in enumerate(inputs)]
        # Return the elements in a different order than the request.  The index
        # member is what lets the client map them back onto the input rows.
        data.reverse()
        self._send_json({
            "object": "list",
            "model": model,
            "data": data,
            "usage": {"prompt_tokens": 0, "total_tokens": 0},
        })

    def _handle_rerank(self, body):
        query = body.get("query", "")
        documents = body.get("documents", [])
        if not isinstance(documents, list):
            documents = []
        sys.stderr.write("[ai-mock] rerank query=%r documents=%d\n" % (query, len(documents)))
        results = rerank_results(query, documents)
        # Return the results out of order; the engine has to sort by score and
        # map index back to the original document.
        results.reverse()
        self._send_json({
            "id": "rerank-mock",
            "model": body.get("model", "mock-model"),
            "results": results,
            "usage": {"total_tokens": 0},
        })

    def _handle_ollama_generate(self, body):
        model = body.get("model", "mock-model")
        sys.stderr.write("[ai-mock] ollama generate model=%s stream=%r\n"
                         % (model, body.get("stream")))
        self._send_json({
            "model": model,
            "created_at": "2026-01-01T00:00:00Z",
            "response": "mock ollama completion for %s" % model,
            "done": True,
            "done_reason": "stop",
        })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), MockAIHandler)
    server.daemon_threads = True
    print("[ai-mock] listening on %s:%d" % (args.host, args.port), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
