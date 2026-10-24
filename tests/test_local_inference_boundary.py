"""Exercise the client against local fake HTTP servers, never a model or provider."""
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "local_boundary", Path(__file__).resolve().parents[1] / "scripts/local_inference.py")
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        state = Path(self.tmp.name)
        (state / "api-key").write_text("test-key")
        p = patch.object(client, "STATE", state)
        p.start()
        self.addCleanup(p.stop)

    def server(self, status=200, body=b'{}', location=None):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append((self.path, self.headers.get("Authorization")))
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.send_response(status)
                if location:
                    self.send_header("Location", location)
                self.end_headers()
                self.wfile.write(body)

            do_GET = do_POST

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close():
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.addCleanup(close)
        return "http://127.0.0.1:" + str(server.server_port), requests

    def test_environment_proxy_never_receives_prompt_or_bearer(self):
        proxy, proxy_calls = self.server()
        endpoint, local_calls = self.server(body=b'{"local":true}')
        with patch.object(client, "COMPLETIONS_URL", endpoint + "/v1/chat/completions"), patch.dict(
            os.environ, {"http_proxy": proxy, "HTTP_PROXY": proxy,
                         "all_proxy": proxy, "ALL_PROXY": proxy,
                         "no_proxy": "", "NO_PROXY": ""}
        ):
            self.assertEqual(client.local_completion({"prompt": "private"}), {"local": True})
        self.assertEqual(proxy_calls, [])
        self.assertEqual(local_calls, [("/v1/chat/completions", "Bearer test-key")])

    def test_redirect_does_not_forward_request_or_bearer(self):
        target, target_calls = self.server()
        endpoint, local_calls = self.server(status=302, location=target + "/leak")
        with patch.object(client, "COMPLETIONS_URL", endpoint), self.assertRaises(urllib.error.HTTPError):
            client.local_completion({"prompt": "private"})
        self.assertEqual(len(local_calls), 1)
        self.assertEqual(target_calls, [])

    def test_unavailable_local_endpoint_is_one_attempt_with_no_fallback(self):
        with patch.object(client.urllib.request.OpenerDirector, "open",
                          side_effect=urllib.error.URLError("offline")) as request:
            with self.assertRaises(urllib.error.URLError):
                client.local_completion({"prompt": "private"})
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.args[0].full_url,
                         "http://127.0.0.1:18181/v1/chat/completions")

    def test_response_size_is_bounded(self):
        with patch.object(client.urllib.request.OpenerDirector, "open") as request:
            request.return_value.__enter__.return_value = io.BytesIO(b"x" * (client.MAX_RESPONSE_BYTES + 1))
            with self.assertRaisesRegex(ValueError, "exceeds limit"):
                client.local_completion({"prompt": "private"})


if __name__ == "__main__":
    unittest.main()
