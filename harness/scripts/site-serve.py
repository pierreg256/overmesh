#!/usr/bin/env python3
"""Serve the built site the way GitHub Pages does.

`python3 -m http.server` answers unmatched paths with its own built-in error
page, so `site/public/404.html` -- the one page a reader only ever reaches by
accident -- cannot be reviewed locally. This handler serves that file instead,
with a 404 status, which is what GitHub Pages does.
"""

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class PagesRequestHandler(SimpleHTTPRequestHandler):
    def send_error(self, code, message=None, explain=None):
        if code != 404:
            return super().send_error(code, message, explain)
        document = Path(self.directory) / "404.html"
        if not document.is_file():
            return super().send_error(code, message, explain)
        body = document.read_bytes()
        self.send_response(404, "Not Found")
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1111)
    parser.add_argument("--directory", default="site/public")
    args = parser.parse_args()
    handler = partial(PagesRequestHandler, directory=args.directory)
    server = ThreadingHTTPServer((args.bind, args.port), handler)
    print(f"serving {args.directory} on http://{args.bind}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
