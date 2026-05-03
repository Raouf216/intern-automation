import json
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")


def bool_env(name, default=True):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def login_and_screenshot():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-{timestamp}.png")

    print("trying to login ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = browser.new_context(viewport={"width": 1365, "height": 900})
        page = context.new_page()

        try:
            page.goto(os.environ["DOKTORABC_LOGIN_URL"], wait_until="domcontentloaded")
            page.get_by_placeholder("Email").fill(os.environ["DOKTORABC_USERNAME"])
            page.get_by_placeholder("Password").fill(os.environ["DOKTORABC_PASSWORD"])
            page.get_by_text("Pharmacist", exact=True).click()
            page.get_by_role("button", name="Login").click()
            page.goto(os.environ["DOKTORABC_PRODUCTS_URL"], wait_until="domcontentloaded")
            page.screenshot(path=screenshot_path, full_page=True)

            return {
                "ok": True,
                "current_url": page.url,
                "screenshot_path": screenshot_path,
            }
        finally:
            context.close()
            browser.close()


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
                },
            )
            return

        self.respond(404, {"ok": False, "error": "not_found"})

    def do_POST(self):
        if self.path != "/jobs/product-prices":
            self.respond(404, {"ok": False, "error": "not_found"})
            return

        try:
            self.respond(200, login_and_screenshot())
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
