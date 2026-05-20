import os
import re
import time
import traceback
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from playwright.sync_api import expect
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
SERVICE_NAME = "cannaflow-scan"
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")
SESSION_STATE_PATH = os.environ.get(
    "CANNAFLOW_SESSION_STATE_PATH",
    os.path.join(ARTIFACTS_DIR, "cannaflow-storage-state.json"),
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)

app = FastAPI(title=SERVICE_NAME)

cors_origins = [
    origin.strip()
    for origin in os.environ.get(
        "CANNAFLOW_SCAN_CORS_ORIGINS",
        "http://localhost:8040,http://127.0.0.1:8040,http://178.104.144.30:8040",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class BotStepError(RuntimeError):
    def __init__(self, message, **details):
        super().__init__(message)
        self.details = details


def bool_env(name, default=True):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def int_env(name, default):
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer environment variable {name}={value!r}") from exc


def required_env(name):
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def login_url():
    return (os.environ.get("CANNAFLOW_LOGIN_URL") or "").strip() or "https://erp.cannaflow.de/auth/login"


def inventory_url():
    return (os.environ.get("CANNAFLOW_INVENTORY_URL") or "").strip() or "https://erp.cannaflow.de/inventory"


def ready_text():
    return (os.environ.get("CANNAFLOW_READY_TEXT") or "Inventar").strip() or "Inventar"


def inventory_page_size():
    return int_env("CANNAFLOW_PAGE_SIZE", 200)


def user_agent():
    return (os.environ.get("CANNAFLOW_USER_AGENT") or DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT


def browser_context_options():
    return {
        "user_agent": user_agent(),
        "viewport": {"width": 1365, "height": 900},
        "locale": "de-DE",
        "timezone_id": os.environ.get("TZ", "Europe/Berlin"),
    }


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def base_url_from_request(request):
    return str(request.base_url).rstrip("/")


def safe_screenshot_filename(filename):
    if not re.match(r"^[A-Za-z0-9_.-]+\.png$", filename or ""):
        raise RuntimeError("Invalid screenshot filename.")
    return filename


def latest_screenshot_path():
    if not os.path.isdir(ARTIFACTS_DIR):
        return None

    screenshot_paths = [
        os.path.join(ARTIFACTS_DIR, filename)
        for filename in os.listdir(ARTIFACTS_DIR)
        if filename.lower().endswith(".png")
    ]
    if not screenshot_paths:
        return None
    return max(screenshot_paths, key=os.path.getmtime)


def screenshot_file_url(base_url, screenshot_path):
    return f"{base_url}/screenshots/{os.path.basename(screenshot_path)}"


def screenshot_response_links(base_url, screenshot_path):
    return {
        "screenshot_url": screenshot_file_url(base_url, screenshot_path),
        "latest_screenshot_url": f"{base_url}/screenshots/latest",
    }


def capture_debug_screenshot(page, reason, trace=None):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_reason = re.sub(r"[^A-Za-z0-9_.-]+", "-", reason).strip("-") or "debug"
    path = os.path.join(ARTIFACTS_DIR, f"cannaflow-{safe_reason}-{timestamp}.png")

    try:
        page.screenshot(path=path, full_page=True)
        trace_step(trace, "capture_debug_screenshot", path=path, reason=reason)
        return path
    except Exception as exc:
        trace_step(trace, "capture_debug_screenshot_failed", reason=reason, error=f"{type(exc).__name__}: {exc}")
        return None


def page_text_excerpt(page, limit=500):
    try:
        text = page.locator("body").inner_text(timeout=2_000)
    except Exception:
        return ""
    return re.sub(r"\s+", " ", text or "").strip()[:limit]


def trace_step(trace, name, **fields):
    if trace:
        trace(name, **fields)


def wait_for_inventory_ready(page, trace=None):
    timeout_ms = int_env("CANNAFLOW_READY_TIMEOUT_MS", 60_000)
    text = ready_text()
    trace_step(trace, "wait_for_inventory_ready", ready_text=text, timeout_ms=timeout_ms)

    try:
        page.get_by_text(text, exact=False).first.wait_for(state="visible", timeout=timeout_ms)
    except Exception as exc:
        screenshot_path = capture_debug_screenshot(page, "inventory-not-ready", trace=trace)
        details = {
            "current_url": page.url,
            "page_title": page.title(),
            "ready_text": text,
            "body_excerpt": page_text_excerpt(page, limit=800),
            "debug_screenshot_path": screenshot_path,
        }
        raise BotStepError("Inventory page did not become ready.", **details) from exc

    return {
        "ready_text": text,
        "body_excerpt": page_text_excerpt(page),
    }


def page_size_input(page):
    return page.locator("xpath=//*[normalize-space()='Seitengröße']/following::input[1]").first


def ensure_inventory_page_size(page, trace=None):
    target_size = inventory_page_size()
    timeout_ms = int_env("CANNAFLOW_PAGE_SIZE_TIMEOUT_MS", 30_000)
    page_size = page_size_input(page)

    trace_step(trace, "ensure_inventory_page_size", target_size=target_size, timeout_ms=timeout_ms)

    try:
        page_size.wait_for(state="visible", timeout=timeout_ms)
        current_value = (page_size.input_value(timeout=timeout_ms) or "").strip()
    except Exception as exc:
        screenshot_path = capture_debug_screenshot(page, "page-size-input-not-found", trace=trace)
        raise BotStepError(
            "Cannaflow page size input was not visible.",
            current_url=page.url,
            page_title=page.title(),
            body_excerpt=page_text_excerpt(page, limit=800),
            debug_screenshot_path=screenshot_path,
        ) from exc

    trace_step(trace, "inventory_page_size_current", current_value=current_value, target_size=target_size)

    try:
        current_size = int(current_value)
    except ValueError:
        current_size = 0

    if current_size >= target_size:
        return {
            "target": target_size,
            "before": current_value,
            "after": current_value,
            "changed": False,
        }

    try:
        page_size.scroll_into_view_if_needed(timeout=timeout_ms)
        page_size.click(timeout=timeout_ms)
        page.get_by_role("option", name=str(target_size), exact=True).click(timeout=timeout_ms)
        expect(page_size).to_have_value(str(target_size), timeout=timeout_ms)
    except Exception as exc:
        screenshot_path = capture_debug_screenshot(page, "page-size-select-failed", trace=trace)
        raise BotStepError(
            "Could not set Cannaflow page size.",
            current_url=page.url,
            page_title=page.title(),
            page_size_before=current_value,
            page_size_target=target_size,
            body_excerpt=page_text_excerpt(page, limit=800),
            debug_screenshot_path=screenshot_path,
        ) from exc

    wait_for_inventory_ready(page, trace=trace)
    after_value = (page_size.input_value(timeout=timeout_ms) or "").strip()
    trace_step(trace, "inventory_page_size_changed", before=current_value, after=after_value)

    return {
        "target": target_size,
        "before": current_value,
        "after": after_value,
        "changed": True,
    }


def page_says_not_authenticated(page):
    return "Nicht angemeldet" in page_text_excerpt(page, limit=1_000)


def click_inventory(page, trace=None):
    trace_step(trace, "open_inventory", url=inventory_url())

    inventory_link = page.get_by_role("link", name="Inventar")
    try:
        if inventory_link.count() > 0 and inventory_link.first.is_visible(timeout=2_000):
            trace_step(trace, "click_inventory_link")
            inventory_link.first.click(timeout=10_000)
        else:
            trace_step(trace, "goto_inventory_url")
            page.goto(inventory_url(), wait_until="domcontentloaded", timeout=60_000)
    except Exception:
        trace_step(trace, "inventory_link_failed_goto_url")
        page.goto(inventory_url(), wait_until="domcontentloaded", timeout=60_000)

    return wait_for_inventory_ready(page, trace=trace)


def login_form_visible(page):
    try:
        email = page.get_by_role("textbox", name="E-Mail", exact=True)
        password = page.get_by_role("textbox", name="Passwort", exact=True)
        return email.count() > 0 and password.count() > 0 and email.first.is_visible(timeout=2_000)
    except Exception:
        return False


def fill_login_form(page, trace=None):
    username = required_env("CANNAFLOW_USERNAME")
    password = required_env("CANNAFLOW_PASSWORD")

    trace_step(trace, "fill_login_form")
    page.get_by_role("textbox", name="E-Mail", exact=True).first.fill(username, timeout=15_000)
    page.get_by_role("textbox", name="Passwort", exact=True).first.fill(password, timeout=15_000)


def submit_login_form(page, trace=None):
    trace_step(trace, "submit_login_form")
    page.get_by_role("button", name="Anmelden", exact=True).click(timeout=15_000)


def wait_for_authenticated_shell(page, trace=None):
    timeout_ms = int_env("CANNAFLOW_POST_LOGIN_TIMEOUT_MS", 30_000)
    deadline = time.time() + (timeout_ms / 1000)
    trace_step(trace, "wait_for_authenticated_shell", timeout_ms=timeout_ms)

    while time.time() < deadline:
        if page_says_not_authenticated(page):
            break

        try:
            inventory_link = page.get_by_role("link", name="Inventar")
            if inventory_link.count() > 0 and inventory_link.first.is_visible(timeout=1_000):
                trace_step(trace, "authenticated_shell_visible", current_url=page.url)
                return
        except Exception:
            pass

        if "/auth/login" not in page.url:
            try:
                page.get_by_text("Inventar", exact=False).first.wait_for(state="visible", timeout=1_000)
                trace_step(trace, "authenticated_text_visible", current_url=page.url)
                return
            except Exception:
                pass

        page.wait_for_timeout(500)

    screenshot_path = capture_debug_screenshot(page, "post-login-not-authenticated", trace=trace)
    raise BotStepError(
        "Login did not reach the authenticated Cannaflow app.",
        current_url=page.url,
        page_title=page.title(),
        body_excerpt=page_text_excerpt(page, limit=800),
        debug_screenshot_path=screenshot_path,
    )


def open_context_with_saved_session(browser, trace=None):
    if not os.path.exists(SESSION_STATE_PATH):
        trace_step(trace, "saved_session_missing", path=SESSION_STATE_PATH)
        return None, None, False

    trace_step(trace, "open_saved_session", path=SESSION_STATE_PATH)
    context = browser.new_context(storage_state=SESSION_STATE_PATH, **browser_context_options())
    page = context.new_page()

    try:
        page.goto(inventory_url(), wait_until="domcontentloaded", timeout=60_000)
        wait_for_inventory_ready(page, trace=trace)
        return context, page, True
    except Exception as exc:
        trace_step(trace, "saved_session_not_ready", error=f"{type(exc).__name__}: {exc}", current_url=page.url)
        context.close()
        return None, None, False


def open_fresh_session(browser, trace=None):
    trace_step(trace, "open_fresh_session")
    context = browser.new_context(**browser_context_options())
    page = context.new_page()

    trace_step(trace, "goto_login_url", url=login_url())
    page.goto(login_url(), wait_until="domcontentloaded", timeout=60_000)
    trace_step(
        trace,
        "login_page_loaded",
        current_url=page.url,
        page_title=page.title(),
        body_excerpt=page_text_excerpt(page),
    )

    if not login_form_visible(page):
        trace_step(trace, "login_form_not_visible_try_inventory")
        page.goto(inventory_url(), wait_until="domcontentloaded", timeout=60_000)
    else:
        fill_login_form(page, trace=trace)
        submit_login_form(page, trace=trace)
        wait_for_authenticated_shell(page, trace=trace)

    click_inventory(page, trace=trace)
    os.makedirs(os.path.dirname(SESSION_STATE_PATH), exist_ok=True)
    context.storage_state(path=SESSION_STATE_PATH)
    trace_step(trace, "saved_session", path=SESSION_STATE_PATH)

    return context, page, False


def open_authenticated_inventory(browser, trace=None):
    context, page, reused_session = open_context_with_saved_session(browser, trace=trace)
    if context and page:
        return context, page, reused_session

    return open_fresh_session(browser, trace=trace)


def login_only(base_url, trace=None):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"cannaflow-after-login-{timestamp}.png")
    wait_ms = int_env("CANNAFLOW_AFTER_LOGIN_WAIT_MS", 5_000)

    trace_step(trace, "start_browser", headless=bool_env("CANNAFLOW_HEADLESS", True))
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("CANNAFLOW_HEADLESS", True))
        context = None
        try:
            context, page, reused_session = open_authenticated_inventory(browser, trace=trace)
            page_size_result = ensure_inventory_page_size(page, trace=trace)
            trace_step(trace, "wait_before_screenshot", wait_ms=wait_ms)
            page.wait_for_timeout(wait_ms)
            trace_step(trace, "capture_screenshot", path=screenshot_path)
            page.screenshot(path=screenshot_path, full_page=True)

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "ready_text": ready_text(),
                "page_size": page_size_result,
                "session_state_path": SESSION_STATE_PATH,
                "session_state_exists": os.path.exists(SESSION_STATE_PATH),
                "screenshot_path": screenshot_path,
                "screenshot_wait_ms": wait_ms,
                **screenshot_response_links(base_url, screenshot_path),
            }
        finally:
            if context:
                context.close()
            browser.close()


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
        "login_url_configured": bool((os.environ.get("CANNAFLOW_LOGIN_URL") or "").strip()),
        "inventory_url_configured": bool((os.environ.get("CANNAFLOW_INVENTORY_URL") or "").strip()),
        "session_state_path": SESSION_STATE_PATH,
        "session_state_exists": os.path.exists(SESSION_STATE_PATH),
        "ready_text": ready_text(),
        "page_size_target": inventory_page_size(),
        "screenshot_wait_ms": int_env("CANNAFLOW_AFTER_LOGIN_WAIT_MS", 5_000),
        "ready_timeout_ms": int_env("CANNAFLOW_READY_TIMEOUT_MS", 60_000),
    }


@app.post("/jobs/login")
def login_job(request: Request):
    base_url = base_url_from_request(request)
    try:
        return login_only(base_url)
    except Exception as exc:
        details = getattr(exc, "details", {}) or {}
        debug_screenshot_path = details.get("debug_screenshot_path")
        if debug_screenshot_path:
            details.update(screenshot_response_links(base_url, debug_screenshot_path))

        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                **details,
                "traceback": traceback.format_exc().splitlines()[-12:],
            },
        )


@app.post("/jobs/login/run")
def login_job_run(request: Request):
    return login_job(request)


@app.get("/screenshots/latest", name="get_latest_screenshot")
def get_latest_screenshot():
    screenshot_path = latest_screenshot_path()
    if not screenshot_path:
        return JSONResponse(status_code=404, content={"ok": False, "error": "no_screenshot_found"})
    return FileResponse(screenshot_path, media_type="image/png", filename=os.path.basename(screenshot_path))


@app.get("/screenshots/{filename}", name="get_screenshot")
def get_screenshot(filename: str):
    screenshot_path = os.path.join(ARTIFACTS_DIR, safe_screenshot_filename(filename))
    if not os.path.exists(screenshot_path):
        return JSONResponse(status_code=404, content={"ok": False, "error": "screenshot_not_found"})
    return FileResponse(screenshot_path, media_type="image/png", filename=os.path.basename(screenshot_path))
