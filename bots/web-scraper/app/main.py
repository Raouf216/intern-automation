import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from playwright.sync_api import sync_playwright


STARTED_AT = time.time()

CONFIG_KEYS = [
    "WEB_SCRAPER_API_KEY",
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "DOKTORABC_LOGIN_URL",
    "DOKTORABC_PRODUCTS_URL",
    "DOKTORABC_USERNAME",
    "DOKTORABC_PASSWORD",
    "DOKTORABC_HEADLESS",
]


def bool_env(name, default=True):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def config_status():
    return {
        key: {
            "configured": bool(os.environ.get(key)),
            "value": os.environ.get(key)
            if key in {"SUPABASE_URL", "DOKTORABC_LOGIN_URL", "DOKTORABC_PRODUCTS_URL", "DOKTORABC_HEADLESS"}
            else None,
        }
        for key in CONFIG_KEYS
    }


def require_job_key(handler):
    expected = os.environ.get("WEB_SCRAPER_API_KEY")
    provided = handler.headers.get("X-Web-Scraper-Key")
    return bool(expected) and provided == expected


class Handler(BaseHTTPRequestHandler):
    server_version = "web-scraper/1.0"

    def do_GET(self):
        if self.path == "/health":
            self.respond(
                200,
                {
                    "ok": True,
                    "service": "web-scraper",
                    "uptime_seconds": round(time.time() - STARTED_AT, 3),
                    "config": config_status(),
                },
            )
            return

        if self.path == "/browser-check":
            self.handle_browser_check()
            return

        self.respond(
            404,
            {
                "ok": False,
                "error": "not_found",
                "available_paths": ["/health", "/browser-check", "/jobs/product-prices"],
            },
        )

    def do_POST(self):
        if self.path != "/jobs/product-prices":
            self.respond(404, {"ok": False, "error": "not_found"})
            return

        if not require_job_key(self):
            self.respond(401, {"ok": False, "error": "unauthorized"})
            return

        self.respond(
            501,
            {
                "ok": False,
                "error": "not_implemented",
                "message": "DoktorABC product price scraping is scaffolded, but selectors/login flow still need to be implemented.",
                "next_step": "Inspect DoktorABC product page with Playwright, then map rows to products/product_prices.",
                "config": config_status(),
            },
        )

    def handle_browser_check(self):
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
                page = browser.new_page()
                page.goto("data:text/html,<title>browser-ok</title><h1>ok</h1>")
                title = page.title()
                browser.close()

            self.respond(200, {"ok": title == "browser-ok", "browser": "chromium", "title": title})
        except Exception as exc:
            self.respond(500, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

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
    print(f"web-scraper listening on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
