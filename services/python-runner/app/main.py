import importlib
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import metadata


STARTED_AT = time.time()

DEPENDENCIES = [
    {"package": "pandas", "module": "pandas"},
    {"package": "openpyxl", "module": "openpyxl"},
    {"package": "requests", "module": "requests"},
    {"package": "beautifulsoup4", "module": "bs4"},
    {"package": "lxml", "module": "lxml"},
    {"package": "python-dotenv", "module": "dotenv"},
    {"package": "httpx", "module": "httpx"},
    {"package": "aiohttp", "module": "aiohttp"},
    {"package": "selectolax", "module": "selectolax"},
    {"package": "parsel", "module": "parsel"},
    {"package": "orjson", "module": "orjson"},
    {"package": "tenacity", "module": "tenacity"},
    {"package": "aiolimiter", "module": "aiolimiter"},
    {"package": "requests-cache", "module": "requests_cache"},
    {"package": "polars", "module": "polars"},
    {"package": "fastexcel", "module": "fastexcel"},
    {"package": "python-calamine", "module": "python_calamine"},
    {"package": "xlsxwriter", "module": "xlsxwriter"},
    {"package": "duckdb", "module": "duckdb"},
    {"package": "playwright", "module": "playwright"},
]


def dependency_status():
    results = []

    for dependency in DEPENDENCIES:
        package = dependency["package"]
        module = dependency["module"]

        try:
            importlib.import_module(module)
            try:
                version = metadata.version(package)
            except metadata.PackageNotFoundError:
                version = None

            results.append(
                {
                    "package": package,
                    "module": module,
                    "installed": True,
                    "version": version,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "package": package,
                    "module": module,
                    "installed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    missing = [result["package"] for result in results if not result["installed"]]

    return {
        "ok": len(missing) == 0,
        "missing": missing,
        "dependencies": results,
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "python-runner/1.0"

    def do_GET(self):
        if self.path == "/health":
            self.respond(
                200,
                {
                    "ok": True,
                    "service": "python-runner",
                    "uptime_seconds": round(time.time() - STARTED_AT, 3),
                },
            )
            return

        if self.path == "/dependencies":
            status = dependency_status()
            self.respond(200 if status["ok"] else 500, status)
            return

        self.respond(
            404,
            {
                "ok": False,
                "error": "not_found",
                "available_paths": ["/health", "/dependencies"],
            },
        )

    def log_message(self, format, *args):
        print(
            json.dumps(
                {
                    "client": self.address_string(),
                    "message": format % args,
                }
            ),
            flush=True,
        )

    def respond(self, status_code, payload):
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"python-runner listening on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
