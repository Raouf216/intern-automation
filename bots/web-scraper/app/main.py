import json
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")

CONFIG_KEYS = [
    "WEB_SCRAPER_API_KEY",
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "DOKTORABC_LOGIN_URL",
    "DOKTORABC_PRODUCTS_URL",
    "DOKTORABC_USERNAME",
    "DOKTORABC_PASSWORD",
    "DOKTORABC_ROLE",
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
            if key in {
                "SUPABASE_URL",
                "DOKTORABC_LOGIN_URL",
                "DOKTORABC_PRODUCTS_URL",
                "DOKTORABC_ROLE",
                "DOKTORABC_HEADLESS",
            }
            else None,
        }
        for key in CONFIG_KEYS
    }


def require_job_key(handler):
    expected = os.environ.get("WEB_SCRAPER_API_KEY")
    provided = handler.headers.get("X-Web-Scraper-Key")
    return bool(expected) and provided == expected


def required_env_values():
    required_keys = [
        "DOKTORABC_LOGIN_URL",
        "DOKTORABC_PRODUCTS_URL",
        "DOKTORABC_USERNAME",
        "DOKTORABC_PASSWORD",
        "DOKTORABC_ROLE",
    ]
    missing = [key for key in required_keys if not os.environ.get(key)]
    return missing


def click_role(page, role_name):
    role_label = role_name.strip().lower()
    role_candidates = {
        "pharmacy": ["Pharmacy"],
        "e-prescription": ["E-Prescription", "E Prescription", "EPrescription"],
        "pharmacist": ["Pharmacist"],
    }

    labels = role_candidates.get(role_label, [role_name])
    for label in labels:
        try:
            page.get_by_label(label, exact=True).check(timeout=2_000)
            return label
        except PlaywrightError:
            pass

    for label in labels:
        try:
            page.get_by_text(label, exact=True).click(timeout=2_000)
            return label
        except PlaywrightError:
            pass

    raise RuntimeError(f"Could not select DoktorABC role: {role_name}")


def run_login_check():
    missing = required_env_values()
    if missing:
        return {
            "ok": False,
            "error": "missing_env",
            "missing": missing,
        }

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-login-{timestamp}.png")
    login_url = os.environ["DOKTORABC_LOGIN_URL"]
    products_url = os.environ["DOKTORABC_PRODUCTS_URL"]

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = browser.new_context(viewport={"width": 1365, "height": 900})
        page = context.new_page()

        try:
            page.goto(login_url, wait_until="domcontentloaded", timeout=60_000)
            page.get_by_placeholder("Email").fill(os.environ["DOKTORABC_USERNAME"], timeout=15_000)
            page.get_by_placeholder("Password").fill(os.environ["DOKTORABC_PASSWORD"], timeout=15_000)
            selected_role = click_role(page, os.environ["DOKTORABC_ROLE"])
            page.get_by_role("button", name="Login").click(timeout=15_000)

            try:
                page.wait_for_url(lambda url: "login" not in url.lower(), timeout=30_000)
            except PlaywrightTimeoutError:
                pass

            try:
                page.goto(products_url, wait_until="networkidle", timeout=60_000)
            except PlaywrightTimeoutError:
                page.goto(products_url, wait_until="domcontentloaded", timeout=60_000)

            page.screenshot(path=screenshot_path, full_page=True)

            current_url = page.url
            title = page.title()
            login_still_visible = page.get_by_role("button", name="Login").count() > 0

            return {
                "ok": not login_still_visible and "login" not in current_url.lower(),
                "selected_role": selected_role,
                "current_url": current_url,
                "title": title,
                "login_still_visible": login_still_visible,
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
                    "config": config_status(),
                },
            )
            return

        self.respond(
            404,
            {
                "ok": False,
                "error": "not_found",
                "available_paths": ["/health", "/jobs/product-prices"],
            },
        )

    def do_POST(self):
        if self.path != "/jobs/product-prices":
            self.respond(404, {"ok": False, "error": "not_found"})
            return

        if not require_job_key(self):
            self.respond(401, {"ok": False, "error": "unauthorized"})
            return

        try:
            result = run_login_check()
            self.respond(200 if result.get("ok") else 500, result)
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
