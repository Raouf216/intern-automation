import os
import re
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")
SESSION_STATE_PATH = os.environ.get(
    "DOKTORABC_SESSION_STATE_PATH",
    os.path.join(ARTIFACTS_DIR, "doktorabc-storage-state.json"),
)
DEFAULT_DOKTORABC_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
DOKTORABC_USER_AGENT = (
    os.environ.get("DOKTORABC_USER_AGENT", DEFAULT_DOKTORABC_USER_AGENT).strip()
    or DEFAULT_DOKTORABC_USER_AGENT
)
DEFAULT_END_OF_DAY_URL = "https://pharmacies.doktorabc.com/end-of-day"
EOD_READY_TIMEOUT_MS = int(os.environ.get("EOD_READY_TIMEOUT_MS", "120000"))


app = FastAPI(title="end-of-day-scraper")


END_OF_DAY_SCRAPER_CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "END_OF_DAY_SCRAPER_CORS_ORIGINS",
        "http://localhost:8040,http://127.0.0.1:8040,http://178.104.144.30:8040",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=END_OF_DAY_SCRAPER_CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def bool_env(name, default=True):
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def required_env(name):
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def end_of_day_url():
    return os.environ.get("DOKTORABC_END_OF_DAY_URL") or DEFAULT_END_OF_DAY_URL


def browser_context_options():
    return {
        "user_agent": DOKTORABC_USER_AGENT,
        "viewport": {"width": 1365, "height": 900},
    }


def visible_login_form(page):
    if "login" in page.url.lower():
        return True

    for selector in (
        'input[placeholder*="Email" i]',
        'input[type="email"]',
        'input[name*="email" i]',
    ):
        try:
            page.locator(selector).first.wait_for(state="visible", timeout=800)
            return True
        except PlaywrightTimeoutError:
            continue

    return False


def fill_first_visible(page, selectors, value):
    for selector in selectors:
        try:
            field = page.locator(selector).first
            field.wait_for(state="visible", timeout=5_000)
            field.fill(value)
            return
        except PlaywrightTimeoutError:
            continue

    raise RuntimeError(f"Could not find visible field for selectors: {', '.join(selectors)}")


def click_pharmacist_role(page):
    for clicker in (
        lambda: page.get_by_text("Pharmacist", exact=True).click(timeout=5_000),
        lambda: page.get_by_label("Pharmacist").click(timeout=5_000),
        lambda: page.locator('text=/pharmacist/i').first.click(timeout=5_000),
    ):
        try:
            clicker()
            return
        except Exception:
            continue

    print("could not explicitly click Pharmacist role; continuing login attempt", flush=True)


def click_login_button(page):
    for clicker in (
        lambda: page.get_by_role("button", name=re.compile("login", re.I)).click(timeout=10_000),
        lambda: page.locator('button:has-text("Login")').first.click(timeout=10_000),
        lambda: page.locator('input[type="submit"]').first.click(timeout=10_000),
    ):
        try:
            clicker()
            return
        except Exception:
            continue

    raise RuntimeError("Could not find DoktorABC login button.")


def wait_for_load_states(page):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=30_000)
    except PlaywrightTimeoutError:
        pass

    try:
        page.wait_for_load_state("load", timeout=30_000)
    except PlaywrightTimeoutError:
        pass

    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except PlaywrightTimeoutError:
        print("networkidle did not arrive; falling back to DOM stability wait", flush=True)


def page_render_snapshot(page):
    return page.evaluate(
        """
        () => {
          const body = document.body;
          const text = body?.innerText || "";
          const visible = (element) => {
            const style = window.getComputedStyle(element);
            const rect = element.getBoundingClientRect();
            return (
              style.display !== "none" &&
              style.visibility !== "hidden" &&
              Number(style.opacity) !== 0 &&
              rect.width > 0 &&
              rect.height > 0
            );
          };
          const loaderSelectors = [
            '[aria-busy="true"]',
            '[role="progressbar"]',
            '[data-loading="true"]',
            '.ant-spin',
            '.MuiCircularProgress-root',
            '[class*="spinner" i]',
            '[class*="loader" i]',
            '[class*="loading" i]'
          ];
          const visibleLoaders = loaderSelectors
            .flatMap((selector) => Array.from(document.querySelectorAll(selector)))
            .filter(visible);
          const disabledBusyButtons = Array.from(document.querySelectorAll("button[disabled]"))
            .filter((button) => /loading|laden|wait|bitte/i.test(button.innerText || ""))
            .filter(visible);

          return {
            readyState: document.readyState,
            url: window.location.href,
            title: document.title,
            textLength: text.trim().length,
            textBucket: Math.round(text.trim().length / 100) * 100,
            tableRows: document.querySelectorAll("tr").length,
            inputs: document.querySelectorAll("input,select,textarea").length,
            buttons: document.querySelectorAll("button").length,
            links: document.querySelectorAll("a").length,
            scrollHeightBucket: Math.round((body?.scrollHeight || 0) / 100) * 100,
            visibleLoaderCount: visibleLoaders.length,
            busyButtonCount: disabledBusyButtons.length,
          };
        }
        """
    )


def wait_for_render_stability(page, timeout_ms=120_000, stable_ms=4_000, poll_ms=700):
    deadline = time.monotonic() + timeout_ms / 1000
    stable_since = None
    previous_key = None
    last_snapshot = None

    while time.monotonic() < deadline:
        snapshot = page_render_snapshot(page)
        last_snapshot = snapshot
        ready = (
            snapshot["readyState"] in {"interactive", "complete"}
            and snapshot["textLength"] >= 20
            and snapshot["visibleLoaderCount"] == 0
            and snapshot["busyButtonCount"] == 0
        )
        stability_key = (
            snapshot["readyState"],
            snapshot["textBucket"],
            snapshot["tableRows"],
            snapshot["inputs"],
            snapshot["buttons"],
            snapshot["links"],
            snapshot["scrollHeightBucket"],
            snapshot["visibleLoaderCount"],
            snapshot["busyButtonCount"],
        )

        now = time.monotonic()
        if ready and stability_key == previous_key:
            if stable_since is not None and (now - stable_since) * 1000 >= stable_ms:
                return {
                    "stable": True,
                    "stable_ms": stable_ms,
                    "snapshot": snapshot,
                }
        else:
            stable_since = now if ready else None
            previous_key = stability_key

        page.wait_for_timeout(poll_ms)

    raise RuntimeError(f"End-of-Day page did not finish rendering in time. Last snapshot: {last_snapshot}")


def wait_for_end_of_day_page(page, timeout_ms=EOD_READY_TIMEOUT_MS):
    if not page.url.startswith(end_of_day_url()):
        page.goto(end_of_day_url(), wait_until="domcontentloaded", timeout=60_000)

    wait_for_load_states(page)

    if visible_login_form(page):
        raise RuntimeError("DoktorABC session is not authenticated; login page is visible.")

    stability = wait_for_render_stability(page, timeout_ms=timeout_ms)
    page.wait_for_timeout(1_000)
    final_snapshot = page_render_snapshot(page)

    return {
        **stability,
        "final_snapshot": final_snapshot,
    }


def open_saved_session(browser):
    if not os.path.exists(SESSION_STATE_PATH):
        return None

    print("trying saved DoktorABC session for End-of-Day ...", flush=True)

    context = browser.new_context(
        storage_state=SESSION_STATE_PATH,
        **browser_context_options(),
    )
    page = context.new_page()

    try:
        page.goto(end_of_day_url(), wait_until="domcontentloaded", timeout=60_000)
        wait_result = wait_for_end_of_day_page(page)
        return context, page, True, wait_result
    except Exception as exc:
        context.close()
        print(f"saved DoktorABC session is expired or not ready: {type(exc).__name__}: {exc}", flush=True)
        return None


def open_fresh_session(browser, before_login_path=None):
    print("trying fresh DoktorABC login for End-of-Day ...", flush=True)

    context = browser.new_context(**browser_context_options())
    page = context.new_page()

    page.goto(required_env("DOKTORABC_LOGIN_URL"), wait_until="domcontentloaded", timeout=60_000)
    wait_for_load_states(page)

    fill_first_visible(
        page,
        ('input[placeholder*="Email" i]', 'input[type="email"]', 'input[name*="email" i]'),
        required_env("DOKTORABC_USERNAME"),
    )
    fill_first_visible(
        page,
        ('input[placeholder*="Password" i]', 'input[type="password"]', 'input[name*="password" i]'),
        required_env("DOKTORABC_PASSWORD"),
    )
    click_pharmacist_role(page)

    if before_login_path:
        page.screenshot(path=before_login_path, full_page=True)

    click_login_button(page)
    page.wait_for_timeout(2_000)
    wait_for_load_states(page)
    page.goto(end_of_day_url(), wait_until="domcontentloaded", timeout=60_000)
    wait_result = wait_for_end_of_day_page(page)

    session_state_dir = os.path.dirname(SESSION_STATE_PATH)
    if session_state_dir:
        os.makedirs(session_state_dir, exist_ok=True)

    context.storage_state(path=SESSION_STATE_PATH)

    return context, page, False, wait_result


def open_authenticated_end_of_day_page(browser, before_login_path=None):
    saved_session = open_saved_session(browser)

    if saved_session is not None:
        return saved_session

    return open_fresh_session(browser, before_login_path=before_login_path)


def login_end_of_day():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-after-login-{timestamp}.png")

    print("trying to open DoktorABC End-of-Day page ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, wait_result = open_authenticated_end_of_day_page(
                browser,
                before_login_path=before_login_path,
            )
            page.screenshot(path=after_login_path, full_page=True)

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                "after_login_path": after_login_path,
                "wait_result": wait_result,
            }
        finally:
            if context:
                context.close()
            browser.close()


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "end-of-day-scraper",
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
        "target_url": end_of_day_url(),
        "session_state_path": SESSION_STATE_PATH,
    }


@app.post("/jobs/end-of-day/login")
def end_of_day_login():
    try:
        return login_end_of_day()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/end-of-day/session-check")
def end_of_day_session_check():
    try:
        return login_end_of_day()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})
