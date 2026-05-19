import os
import re
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
SERVICE_NAME = "wawican-scan"
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")
SESSION_STATE_PATH = os.environ.get(
    "WAWICAN_SESSION_STATE_PATH",
    os.path.join(ARTIFACTS_DIR, "wawican-storage-state.json"),
)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
WAWICAN_USER_AGENT = (
    os.environ.get("WAWICAN_USER_AGENT", DEFAULT_USER_AGENT).strip()
    or DEFAULT_USER_AGENT
)
LOGIN_READY_TIMEOUT_MS = 45_000
PAGE_READY_TIMEOUT_MS = 30_000
FILTER_TIMEOUT_MS = 10_000


app = FastAPI(title=SERVICE_NAME)

cors_origins = [
    origin.strip()
    for origin in os.environ.get(
        "WAWICAN_SCAN_CORS_ORIGINS",
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


def inventory_url():
    return (os.environ.get("WAWICAN_INVENTORY_URL") or "").strip() or required_env("WAWICAN_LOGIN_URL")


def login_url():
    return (os.environ.get("WAWICAN_LOGIN_URL") or "").strip() or inventory_url()


def ready_text():
    return (os.environ.get("WAWICAN_READY_TEXT") or "Verfügbarkeit").strip() or "Verfügbarkeit"


def browser_context_options():
    return {
        "user_agent": WAWICAN_USER_AGENT,
        "viewport": {"width": 1365, "height": 900},
        "locale": "de-DE",
        "timezone_id": os.environ.get("TZ", "Europe/Berlin"),
    }


def screenshot_wait_ms():
    return int_env("WAWICAN_SCREENSHOT_WAIT_MS", 3_000)


def safe_artifact_filename(filename):
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


def screenshot_file_url(request, screenshot_path):
    filename = os.path.basename(screenshot_path)
    return str(request.url_for("get_screenshot", filename=filename))


def screenshot_response_links(request, screenshot_path):
    return {
        "screenshot_url": screenshot_file_url(request, screenshot_path),
        "latest_screenshot_url": str(request.url_for("get_latest_screenshot")),
    }


def capture_screenshot_after_wait(page, path):
    wait_ms = screenshot_wait_ms()

    if wait_ms > 0:
        page.wait_for_timeout(wait_ms)

    page.screenshot(path=path, full_page=True)

    return {
        "screenshot_path": path,
        "screenshot_wait_ms": wait_ms,
    }


def first_visible_locator(page, selectors, timeout=5_000):
    for selector in selectors:
        selector = (selector or "").strip()
        if not selector:
            continue

        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=timeout)
            return locator
        except PlaywrightTimeoutError:
            continue

    return None


def wait_for_inventory_page(page, timeout=PAGE_READY_TIMEOUT_MS):
    signals = [
        lambda: page.get_by_text(ready_text(), exact=False).first.wait_for(timeout=timeout),
        lambda: page.locator(".inventory-table-component").first.wait_for(state="visible", timeout=timeout),
        lambda: page.locator("table.q-table").first.wait_for(state="visible", timeout=timeout),
        lambda: page.locator("th", has_text=re.compile(r"Verf.gbarkeit", re.I)).first.wait_for(
            state="visible",
            timeout=timeout,
        ),
    ]

    errors = []
    for wait_signal in signals:
        try:
            wait_signal()
            return True
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")

    raise RuntimeError(
        f"Inventory page did not become ready. Expected text: {ready_text()!r}. "
        f"Last wait errors: {errors[-2:]}"
    )


def login_form_is_visible(page):
    username_selector = (os.environ.get("WAWICAN_USERNAME_SELECTOR") or "").strip()
    password_selector = (os.environ.get("WAWICAN_PASSWORD_SELECTOR") or "").strip()

    username = first_visible_locator(
        page,
        [
            username_selector,
            'input[placeholder*="Email" i]',
            'input[placeholder*="E-Mail" i]',
            'input[placeholder*="Benutzer" i]',
            'input[placeholder*="Username" i]',
            'input[type="email"]',
            'input[name*="email" i]',
            'input[name*="user" i]',
        ],
        timeout=1_000,
    )
    password = first_visible_locator(
        page,
        [
            password_selector,
            'input[placeholder*="Password" i]',
            'input[placeholder*="Passwort" i]',
            'input[type="password"]',
            'input[name*="password" i]',
            'input[name*="passwort" i]',
        ],
        timeout=1_000,
    )

    return username is not None and password is not None


def fill_login_form(page):
    username = required_env("WAWICAN_USERNAME")
    password = required_env("WAWICAN_PASSWORD")
    username_selector = (os.environ.get("WAWICAN_USERNAME_SELECTOR") or "").strip()
    password_selector = (os.environ.get("WAWICAN_PASSWORD_SELECTOR") or "").strip()

    username_input = first_visible_locator(
        page,
        [
            username_selector,
            'input[placeholder*="Email" i]',
            'input[placeholder*="E-Mail" i]',
            'input[placeholder*="Benutzer" i]',
            'input[placeholder*="Username" i]',
            'input[type="email"]',
            'input[name*="email" i]',
            'input[name*="user" i]',
            'input:not([type="hidden"])',
        ],
    )
    password_input = first_visible_locator(
        page,
        [
            password_selector,
            'input[placeholder*="Password" i]',
            'input[placeholder*="Passwort" i]',
            'input[type="password"]',
            'input[name*="password" i]',
            'input[name*="passwort" i]',
        ],
    )

    if username_input is None or password_input is None:
        raise RuntimeError(
            "Could not find login inputs. Set WAWICAN_USERNAME_SELECTOR and "
            "WAWICAN_PASSWORD_SELECTOR in Dokploy."
        )

    username_input.fill(username)
    password_input.fill(password)

    extra_click_text = (os.environ.get("WAWICAN_LOGIN_EXTRA_CLICK_TEXT") or "").strip()
    if extra_click_text:
        try:
            page.get_by_text(extra_click_text, exact=True).click(timeout=5_000)
        except PlaywrightTimeoutError:
            print(f"extra login click text was not visible: {extra_click_text!r}", flush=True)


def submit_login_form(page):
    login_button_selector = (os.environ.get("WAWICAN_LOGIN_BUTTON_SELECTOR") or "").strip()

    button = first_visible_locator(
        page,
        [
            login_button_selector,
            'button[type="submit"]',
            'button:has-text("Login")',
            'button:has-text("Log in")',
            'button:has-text("Einloggen")',
            'button:has-text("Anmelden")',
            'input[type="submit"]',
        ],
        timeout=5_000,
    )

    if button is not None:
        button.click(timeout=10_000)
    else:
        page.keyboard.press("Enter")


def save_session_state(context):
    session_state_dir = os.path.dirname(SESSION_STATE_PATH)

    if session_state_dir:
        os.makedirs(session_state_dir, exist_ok=True)

    temp_path = f"{SESSION_STATE_PATH}.{os.getpid()}.tmp"
    try:
        context.storage_state(path=temp_path)
        os.replace(temp_path, SESSION_STATE_PATH)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def open_saved_session(browser):
    if not os.path.exists(SESSION_STATE_PATH):
        return None

    print("trying saved Wawican session ...", flush=True)

    try:
        context = browser.new_context(
            storage_state=SESSION_STATE_PATH,
            **browser_context_options(),
        )
    except Exception as exc:
        print(f"saved Wawican session could not be opened: {type(exc).__name__}: {exc}", flush=True)
        return None

    page = context.new_page()

    try:
        page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
        wait_for_inventory_page(page, timeout=12_000)
        return context, page, True
    except Exception as exc:
        context.close()
        print(f"saved Wawican session is expired or invalid: {type(exc).__name__}: {exc}", flush=True)
        return None


def open_fresh_session(browser, before_login_path=None):
    print("trying fresh Wawican login ...", flush=True)

    context = browser.new_context(**browser_context_options())
    page = context.new_page()

    page.goto(login_url(), wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_load_state("domcontentloaded", timeout=10_000)

    if not login_form_is_visible(page):
        try:
            page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
            wait_for_inventory_page(page, timeout=8_000)
            save_session_state(context)
            return context, page, False
        except Exception:
            page.goto(login_url(), wait_until="domcontentloaded", timeout=30_000)

    fill_login_form(page)

    if before_login_path:
        page.screenshot(path=before_login_path, full_page=True)

    submit_login_form(page)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except PlaywrightTimeoutError:
        pass

    page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
    wait_for_inventory_page(page, timeout=LOGIN_READY_TIMEOUT_MS)
    save_session_state(context)

    return context, page, False


def open_authenticated_inventory_page(browser, before_login_path=None):
    saved_session = open_saved_session(browser)

    if saved_session is not None:
        return saved_session

    return open_fresh_session(browser, before_login_path=before_login_path)


def click_availability_filter_button(page):
    result = page.evaluate(
        """
        () => {
          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
          const headers = Array.from(document.querySelectorAll('th'));
          const header = headers.find((element) => normalize(element.innerText).includes('Verfügbarkeit'));

          if (!header) {
            return { ok: false, error: 'availability_header_not_found' };
          }

          const icons = Array.from(header.querySelectorAll('i'));
          const icon = icons.find((element) => normalize(element.textContent) === 'filter_alt');
          const button = icon ? icon.closest('button') : null;

          if (!button) {
            return { ok: false, error: 'availability_filter_button_not_found' };
          }

          button.click();
          return { ok: true };
        }
        """
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "availability_filter_button_failed")

    return result


def ensure_available_checkbox_checked(page):
    page.get_by_text("verfügbar", exact=True).first.wait_for(timeout=FILTER_TIMEOUT_MS)

    result = page.evaluate(
        """
        () => {
          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
          const all = Array.from(document.querySelectorAll('.q-checkbox, [role="checkbox"], label, div'));
          const label = all.find((element) => normalize(element.innerText) === 'verfügbar');

          if (!label) {
            return { ok: false, error: 'available_checkbox_not_found' };
          }

          const checkbox =
            label.closest('.q-checkbox') ||
            label.closest('[role="checkbox"]') ||
            label.querySelector('.q-checkbox') ||
            label.querySelector('[role="checkbox"]') ||
            label;

          const nativeInput = checkbox.querySelector('input[type="checkbox"]');
          const ariaChecked =
            checkbox.getAttribute('aria-checked') ||
            (checkbox.querySelector('[aria-checked]') || {}).getAttribute?.('aria-checked');
          const className = String(checkbox.className || '');
          const checked =
            (nativeInput && nativeInput.checked) ||
            ariaChecked === 'true' ||
            className.includes('--truthy') ||
            className.includes('q-checkbox__inner--truthy');

          if (!checked) {
            checkbox.click();
          }

          return { ok: true, was_checked: Boolean(checked) };
        }
        """
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "available_checkbox_failed")

    return result


def apply_available_filter(page):
    wait_for_inventory_page(page)
    click_result = click_availability_filter_button(page)
    page.wait_for_timeout(300)
    checkbox_result = ensure_available_checkbox_checked(page)
    page.wait_for_timeout(1_000)

    return {
        "filter_button": click_result,
        "available_checkbox": checkbox_result,
        "visible_rows": page.locator("tbody tr").count(),
    }


def login_only(request):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-after-login-{timestamp}.png")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("WAWICAN_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_inventory_page(
                browser,
                before_login_path=before_login_path,
            )
            screenshot_result = capture_screenshot_after_wait(page, after_login_path)

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "session_state_exists": os.path.exists(SESSION_STATE_PATH),
                "before_login_path": None if reused_session else before_login_path,
                "after_login_path": after_login_path,
                **screenshot_result,
                **screenshot_response_links(request, after_login_path),
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_and_filter_available(request):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-filter-before-login-{timestamp}.png")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"wawican-filter-available-{timestamp}.png")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("WAWICAN_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_inventory_page(
                browser,
                before_login_path=before_login_path,
            )
            filter_result = apply_available_filter(page)
            screenshot_result = capture_screenshot_after_wait(page, screenshot_path)

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                **filter_result,
                **screenshot_result,
                **screenshot_response_links(request, screenshot_path),
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
        "inventory_url_configured": bool((os.environ.get("WAWICAN_INVENTORY_URL") or "").strip()),
        "login_url_configured": bool((os.environ.get("WAWICAN_LOGIN_URL") or "").strip()),
        "session_state_path": SESSION_STATE_PATH,
        "session_state_exists": os.path.exists(SESSION_STATE_PATH),
        "screenshot_wait_ms": screenshot_wait_ms(),
    }


@app.post("/jobs/login")
def login_job(request: Request):
    try:
        return login_only(request)
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/filter-available")
def filter_available_job(request: Request):
    try:
        return login_and_filter_available(request)
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.get("/screenshots/latest", name="get_latest_screenshot")
def get_latest_screenshot():
    screenshot_path = latest_screenshot_path()

    if not screenshot_path:
        return JSONResponse(status_code=404, content={"ok": False, "error": "no_screenshot_found"})

    return FileResponse(
        screenshot_path,
        media_type="image/png",
        filename=os.path.basename(screenshot_path),
    )


@app.get("/screenshots/{filename}", name="get_screenshot")
def get_screenshot(filename: str):
    try:
        safe_filename = safe_artifact_filename(filename)
    except RuntimeError as exc:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})

    screenshot_path = os.path.join(ARTIFACTS_DIR, safe_filename)

    if not os.path.exists(screenshot_path):
        return JSONResponse(status_code=404, content={"ok": False, "error": "screenshot_not_found"})

    return FileResponse(
        screenshot_path,
        media_type="image/png",
        filename=safe_filename,
    )


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})


def main():
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
