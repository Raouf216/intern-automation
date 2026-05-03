import json
import os
import time
from contextlib import suppress
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
    "DOKTORABC_DEBUG_SCREENSHOTS",
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


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def safe_filename(value):
    return "".join(char if char.isalnum() else "-" for char in value).strip("-").lower()


def log_event(payload):
    print(json.dumps(payload, sort_keys=True), flush=True)


def add_step(steps, name, status, started_at=None, **details):
    item = {
        "at": utc_now_iso(),
        "name": name,
        "status": status,
    }
    if started_at is not None:
        item["duration_ms"] = round((time.time() - started_at) * 1000)
    item.update({key: value for key, value in details.items() if value is not None})
    steps.append(item)
    log_event({"event": "scraper_step", **item})
    return item


def locator_count(locator):
    with suppress(Exception):
        return locator.count()
    return None


def page_debug(page):
    return {
        "current_url": page.url,
        "title": page.title(),
        "email_inputs": locator_count(page.get_by_placeholder("Email")),
        "password_inputs": locator_count(page.get_by_placeholder("Password")),
        "login_buttons": locator_count(page.get_by_role("button", name="Login")),
        "pharmacist_texts": locator_count(page.get_by_text("Pharmacist", exact=True)),
    }


def save_screenshot(page, timestamp, step_name):
    if not bool_env("DOKTORABC_DEBUG_SCREENSHOTS", True):
        return None

    path = os.path.join(
        ARTIFACTS_DIR,
        f"doktorabc-{timestamp}-{safe_filename(step_name)}.png",
    )
    page.screenshot(path=path, full_page=True)
    return path


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


def run_page_step(steps, page, timestamp, name, action, screenshot=False):
    started_at = time.time()
    add_step(steps, name, "started")
    try:
        result = action()
        screenshot_path = save_screenshot(page, timestamp, name) if screenshot else None
        add_step(
            steps,
            name,
            "ok",
            started_at,
            screenshot_path=screenshot_path,
            **page_debug(page),
        )
        return result
    except Exception as exc:
        screenshot_path = None
        screenshot_error = None
        with suppress(Exception):
            screenshot_path = save_screenshot(page, timestamp, f"failed-{name}")
        if screenshot_path is None and bool_env("DOKTORABC_DEBUG_SCREENSHOTS", True):
            screenshot_error = "screenshot_not_saved"

        debug = {}
        with suppress(Exception):
            debug = page_debug(page)

        add_step(
            steps,
            name,
            "failed",
            started_at,
            error=f"{type(exc).__name__}: {exc}",
            screenshot_path=screenshot_path,
            screenshot_error=screenshot_error,
            **debug,
        )
        raise


def run_login_check():
    steps = []
    missing = required_env_values()
    if missing:
        return {
            "ok": False,
            "error": "missing_env",
            "missing": missing,
            "steps": steps,
        }

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    login_url = os.environ["DOKTORABC_LOGIN_URL"]
    products_url = os.environ["DOKTORABC_PRODUCTS_URL"]

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = browser.new_context(viewport={"width": 1365, "height": 900})
        page = context.new_page()

        try:
            run_page_step(
                steps,
                page,
                timestamp,
                "open_login_page",
                lambda: page.goto(login_url, wait_until="domcontentloaded", timeout=60_000),
                screenshot=True,
            )
            run_page_step(
                steps,
                page,
                timestamp,
                "fill_email",
                lambda: page.get_by_placeholder("Email").fill(
                    os.environ["DOKTORABC_USERNAME"],
                    timeout=15_000,
                ),
                screenshot=True,
            )
            run_page_step(
                steps,
                page,
                timestamp,
                "fill_password",
                lambda: page.get_by_placeholder("Password").fill(
                    os.environ["DOKTORABC_PASSWORD"],
                    timeout=15_000,
                ),
                screenshot=True,
            )
            selected_role = run_page_step(
                steps,
                page,
                timestamp,
                "select_role",
                lambda: click_role(page, os.environ["DOKTORABC_ROLE"]),
                screenshot=True,
            )
            run_page_step(
                steps,
                page,
                timestamp,
                "click_login",
                lambda: page.get_by_role("button", name="Login").click(timeout=15_000),
                screenshot=True,
            )

            try:
                run_page_step(
                    steps,
                    page,
                    timestamp,
                    "wait_after_login",
                    lambda: page.wait_for_url(
                        lambda url: "login" not in url.lower(),
                        timeout=30_000,
                    ),
                    screenshot=True,
                )
            except PlaywrightTimeoutError:
                add_step(
                    steps,
                    "wait_after_login",
                    "warning",
                    error="Login did not redirect within 30 seconds. Continuing to supplies URL.",
                    **page_debug(page),
                )

            try:
                run_page_step(
                    steps,
                    page,
                    timestamp,
                    "open_products_page_networkidle",
                    lambda: page.goto(products_url, wait_until="networkidle", timeout=60_000),
                    screenshot=True,
                )
            except PlaywrightTimeoutError:
                add_step(
                    steps,
                    "open_products_page_networkidle",
                    "warning",
                    error="Network did not become idle. Retrying with domcontentloaded.",
                    **page_debug(page),
                )
                run_page_step(
                    steps,
                    page,
                    timestamp,
                    "open_products_page_domcontentloaded",
                    lambda: page.goto(products_url, wait_until="domcontentloaded", timeout=60_000),
                    screenshot=True,
                )

            current_url = page.url
            title = page.title()
            login_still_visible = page.get_by_role("button", name="Login").count() > 0
            final_screenshot_path = save_screenshot(page, timestamp, "final")

            return {
                "ok": not login_still_visible and "login" not in current_url.lower(),
                "selected_role": selected_role,
                "current_url": current_url,
                "title": title,
                "login_still_visible": login_still_visible,
                "screenshot_path": final_screenshot_path,
                "steps": steps,
            }
        except Exception as exc:
            return {
                "ok": False,
                "error": "login_check_failed",
                "detail": f"{type(exc).__name__}: {exc}",
                "steps": steps,
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
