import os
import re
import threading
import time
import traceback
import uuid
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
INVENTORY_READY_TIMEOUT_MS = int(os.environ.get("WAWICAN_INVENTORY_READY_TIMEOUT_MS", "180000"))
LOGIN_SUBMIT_TIMEOUT_MS = int(os.environ.get("WAWICAN_LOGIN_SUBMIT_TIMEOUT_MS", "90000"))
FILTER_TIMEOUT_MS = int(os.environ.get("WAWICAN_FILTER_TIMEOUT_MS", "60000"))
MAX_STORED_JOBS = 50
JOB_LOCK = threading.Lock()
JOBS = {}


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
    return 0


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


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def base_url_from_request(request):
    return str(request.base_url).rstrip("/")


def screenshot_file_url(base_url, screenshot_path):
    filename = os.path.basename(screenshot_path)
    return f"{base_url}/screenshots/{filename}"


def screenshot_response_links(base_url, screenshot_path):
    return {
        "screenshot_url": screenshot_file_url(base_url, screenshot_path),
        "latest_screenshot_url": f"{base_url}/screenshots/latest",
    }


def trace_step(trace, name, **fields):
    if trace:
        trace(name, **fields)


def capture_screenshot_now(page, path, trace=None):
    trace_step(trace, "capture_screenshot", path=path)
    page.screenshot(path=path, full_page=True)

    return {
        "screenshot_path": path,
        "screenshot_wait_ms": 0,
        "screenshot_trigger": "inventory_ready_visible",
    }


def page_text_excerpt(page, limit=500):
    try:
        text = page.locator("body").inner_text(timeout=2_000)
    except Exception:
        return ""

    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:limit]


def trace_page_state(trace, name, page, **fields):
    try:
        title = page.title()
    except Exception:
        title = ""

    trace_step(
        trace,
        name,
        current_url=page.url,
        page_title=title,
        body_excerpt=page_text_excerpt(page),
        **fields,
    )


def capture_debug_screenshot(page, path, trace, step_name):
    try:
        page.screenshot(path=path, full_page=True)
        trace_step(trace, step_name, path=path, current_url=page.url)
    except Exception as exc:
        trace_step(trace, f"{step_name}_failed", error=f"{type(exc).__name__}: {exc}")


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


def wait_for_next_render_frame(page):
    page.evaluate(
        """
        () => new Promise((resolve) => {
          requestAnimationFrame(() => requestAnimationFrame(resolve));
        })
        """
    )


def wait_for_inventory_page(page, timeout=INVENTORY_READY_TIMEOUT_MS, trace=None):
    trace_step(trace, "wait_for_inventory_page", timeout_ms=timeout, ready_text=ready_text())
    page.wait_for_function(
        """
        (targetText) => {
          const fold = (value) => (value || '')
            .normalize('NFD')
            .replace(/[\\u0300-\\u036f]/g, '')
            .replace(/\\s+/g, ' ')
            .trim()
            .toLowerCase();
          const target = fold(targetText);
          const selectors = [
            'th',
            '[role="columnheader"]',
            '.q-table thead *',
            '.inventory-table-component thead *',
            '.inventory-table-component [class*="table"] *'
          ];
          const candidates = Array.from(document.querySelectorAll(selectors.join(',')));
          const match = candidates.find((element) => {
            const text = fold(element.textContent);
            if (!text.includes(target)) {
              return false;
            }

            const inInventory =
              element.closest('.inventory-table-component') ||
              element.closest('table.q-table') ||
              element.closest('.q-table') ||
              element.closest('thead');

            if (!inInventory) {
              return false;
            }

            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);

            return (
              rect.width > 0 &&
              rect.height > 0 &&
              style.visibility !== 'hidden' &&
              style.display !== 'none' &&
              style.opacity !== '0'
            );
          });

          if (!match) {
            return false;
          }

          const table =
            match.closest('table') ||
            document.querySelector('table.q-table') ||
            document.querySelector('.inventory-table-component');
          const tableRect = table ? table.getBoundingClientRect() : { width: 0, height: 0 };

          return tableRect.width > 0 && tableRect.height > 0;
        }
        """,
        arg=ready_text(),
        timeout=timeout,
    )
    wait_for_next_render_frame(page)
    trace_page_state(trace, "inventory_ready_visible", page)

    return True


def login_form_is_visible(page):
    username_selector = (os.environ.get("WAWICAN_USERNAME_SELECTOR") or "").strip()
    password_selector = (os.environ.get("WAWICAN_PASSWORD_SELECTOR") or "").strip()

    username = first_visible_locator(
        page,
        [
            username_selector,
            '[data-testid="login-email-input"]',
            'input[aria-label*="E-Mail" i]',
            'input[aria-label*="Mail" i]',
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
            '[data-testid="login-password-input"]',
            'input[aria-label*="Passwort" i]',
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
            '[data-testid="login-email-input"]',
            'input[aria-label*="E-Mail" i]',
            'input[aria-label*="Mail" i]',
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
            '[data-testid="login-password-input"]',
            'input[aria-label*="Passwort" i]',
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


def wait_after_login_submit(page, trace=None):
    trace_step(trace, "wait_after_login_submit", timeout_ms=LOGIN_SUBMIT_TIMEOUT_MS)
    try:
        page.wait_for_function(
            """
            () => !window.location.pathname.includes('/auth/login')
            """,
            timeout=LOGIN_SUBMIT_TIMEOUT_MS,
        )
        trace_step(trace, "login_submit_condition_met")
    except PlaywrightTimeoutError:
        trace_step(trace, "login_submit_condition_timeout")

    trace_page_state(trace, "after_login_submit_state", page)


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


def open_saved_session(browser, trace=None):
    if not os.path.exists(SESSION_STATE_PATH):
        trace_step(trace, "saved_session_missing", path=SESSION_STATE_PATH)
        return None

    trace_step(trace, "open_saved_session", path=SESSION_STATE_PATH)
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
        trace_step(trace, "goto_inventory_with_saved_session", url=inventory_url())
        page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
        wait_for_inventory_page(page, trace=trace)
        trace_step(trace, "saved_session_valid")
        return context, page, True, True
    except Exception as exc:
        context.close()
        trace_step(trace, "saved_session_invalid", error=f"{type(exc).__name__}: {exc}")
        print(f"saved Wawican session is expired or invalid: {type(exc).__name__}: {exc}", flush=True)
        return None


def open_fresh_session(browser, before_login_path=None, trace=None):
    trace_step(trace, "open_fresh_session")
    print("trying fresh Wawican login ...", flush=True)

    context = browser.new_context(**browser_context_options())
    page = context.new_page()

    trace_step(trace, "goto_login_url", url=login_url())
    page.goto(login_url(), wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_load_state("domcontentloaded", timeout=10_000)
    trace_page_state(trace, "login_page_loaded", page)

    login_form_visible = login_form_is_visible(page)
    trace_step(trace, "login_form_visibility_checked", visible=login_form_visible)

    if not login_form_visible:
        if before_login_path:
            capture_debug_screenshot(
                page,
                before_login_path,
                trace,
                "login_form_not_detected_screenshot",
            )

        try:
            trace_step(trace, "try_inventory_without_login_form", url=inventory_url())
            page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
            trace_page_state(trace, "inventory_without_login_form_loaded", page)
            wait_for_inventory_page(page, trace=trace)
            save_session_state(context)
            trace_step(trace, "session_saved", path=SESSION_STATE_PATH)
            return context, page, False, True
        except Exception as exc:
            trace_step(trace, "inventory_without_login_form_failed", error=f"{type(exc).__name__}: {exc}")
            trace_step(trace, "return_to_login_url", url=login_url())
            page.goto(login_url(), wait_until="domcontentloaded", timeout=30_000)
            trace_page_state(trace, "login_page_reloaded", page)

    trace_step(trace, "fill_login_form")
    fill_login_form(page)

    if before_login_path:
        trace_step(trace, "capture_before_login_screenshot", path=before_login_path)
        page.screenshot(path=before_login_path, full_page=True)

    trace_step(trace, "submit_login_form")
    submit_login_form(page)
    wait_after_login_submit(page, trace=trace)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=10_000)
    except PlaywrightTimeoutError:
        pass

    trace_step(trace, "goto_inventory_after_login", url=inventory_url())
    page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
    wait_for_inventory_page(page, trace=trace)
    save_session_state(context)
    trace_step(trace, "session_saved", path=SESSION_STATE_PATH)

    return context, page, False, True


def open_authenticated_inventory_page(browser, before_login_path=None, trace=None):
    saved_session = open_saved_session(browser, trace=trace)

    if saved_session is not None:
        return saved_session

    return open_fresh_session(browser, before_login_path=before_login_path, trace=trace)


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


def apply_available_filter(page, trace=None):
    wait_for_inventory_page(page, trace=trace)
    trace_step(trace, "click_availability_filter")
    click_result = click_availability_filter_button(page)
    trace_step(trace, "ensure_available_checkbox_checked")
    checkbox_result = ensure_available_checkbox_checked(page)

    return {
        "filter_button": click_result,
        "available_checkbox": checkbox_result,
        "visible_rows": page.locator("tbody tr").count(),
    }


def login_only(base_url, trace=None):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-after-login-{timestamp}.png")

    trace_step(trace, "start_browser", headless=bool_env("WAWICAN_HEADLESS", True))
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("WAWICAN_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, inventory_ready = open_authenticated_inventory_page(
                browser,
                before_login_path=before_login_path,
                trace=trace,
            )
            screenshot_result = capture_screenshot_now(page, after_login_path, trace=trace)

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "inventory_ready": inventory_ready,
                "session_state_path": SESSION_STATE_PATH,
                "session_state_exists": os.path.exists(SESSION_STATE_PATH),
                "before_login_path": None if reused_session else before_login_path,
                "after_login_path": after_login_path,
                **screenshot_result,
                **screenshot_response_links(base_url, after_login_path),
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_and_filter_available(base_url, trace=None):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-filter-before-login-{timestamp}.png")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"wawican-filter-available-{timestamp}.png")

    trace_step(trace, "start_browser", headless=bool_env("WAWICAN_HEADLESS", True))
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("WAWICAN_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, inventory_ready = open_authenticated_inventory_page(
                browser,
                before_login_path=before_login_path,
                trace=trace,
            )
            filter_result = apply_available_filter(page, trace=trace)
            screenshot_result = capture_screenshot_now(page, screenshot_path, trace=trace)

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "inventory_ready": inventory_ready,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                **filter_result,
                **screenshot_result,
                **screenshot_response_links(base_url, screenshot_path),
            }
        finally:
            if context:
                context.close()
            browser.close()


def json_safe(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]

    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}

    return str(value)


def prune_jobs_locked():
    if len(JOBS) <= MAX_STORED_JOBS:
        return

    sorted_job_ids = sorted(
        JOBS,
        key=lambda job_id: JOBS[job_id].get("created_at", ""),
    )
    for job_id in sorted_job_ids[: len(JOBS) - MAX_STORED_JOBS]:
        JOBS.pop(job_id, None)


def create_job(name):
    job_id = uuid.uuid4().hex
    job = {
        "ok": True,
        "job_id": job_id,
        "name": name,
        "status": "queued",
        "created_at": utc_now_iso(),
        "started_at": None,
        "finished_at": None,
        "last_step": None,
        "steps": [],
    }

    with JOB_LOCK:
        JOBS[job_id] = job
        prune_jobs_locked()

    return job


def append_job_step(job_id, name, **fields):
    step = {
        "name": name,
        "at": utc_now_iso(),
        **{key: json_safe(value) for key, value in fields.items()},
    }

    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return

        job.setdefault("steps", []).append(step)
        job["last_step"] = name


def update_job(job_id, **fields):
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return

        job.update({key: json_safe(value) for key, value in fields.items()})


def get_job_snapshot(job_id):
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return None

        return json_safe(job)


def list_job_snapshots():
    with JOB_LOCK:
        jobs = sorted(
            JOBS.values(),
            key=lambda job: job.get("created_at", ""),
            reverse=True,
        )
        return [json_safe(job) for job in jobs]


def run_background_job(job_id, base_url, work):
    update_job(job_id, status="running", started_at=utc_now_iso())
    append_job_step(job_id, "job_started")

    try:
        result = work(base_url, trace=lambda name, **fields: append_job_step(job_id, name, **fields))
        update_job(
            job_id,
            status="done",
            finished_at=utc_now_iso(),
            result=result,
        )
        append_job_step(job_id, "job_done")
    except Exception as exc:
        update_job(
            job_id,
            ok=False,
            status="error",
            finished_at=utc_now_iso(),
            error=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc().splitlines()[-12:],
        )
        append_job_step(job_id, "job_error", error=f"{type(exc).__name__}: {exc}")


def start_background_job(request, name, work):
    base_url = base_url_from_request(request)
    job = create_job(name)
    thread = threading.Thread(
        target=run_background_job,
        args=(job["job_id"], base_url, work),
        daemon=True,
        name=f"{SERVICE_NAME}-{job['job_id'][:8]}",
    )
    thread.start()

    return JSONResponse(
        status_code=202,
        content={
            "ok": True,
            "job_id": job["job_id"],
            "status": "queued",
            "status_url": f"{base_url}/jobs/{job['job_id']}",
        },
    )


@app.get("/health")
def health():
    with JOB_LOCK:
        running_jobs = sum(1 for job in JOBS.values() if job.get("status") in {"queued", "running"})

    return {
        "ok": True,
        "service": SERVICE_NAME,
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
        "inventory_url_configured": bool((os.environ.get("WAWICAN_INVENTORY_URL") or "").strip()),
        "login_url_configured": bool((os.environ.get("WAWICAN_LOGIN_URL") or "").strip()),
        "session_state_path": SESSION_STATE_PATH,
        "session_state_exists": os.path.exists(SESSION_STATE_PATH),
        "screenshot_wait_ms": screenshot_wait_ms(),
        "screenshot_mode": "capture_immediately_when_inventory_ready_visible",
        "inventory_ready_timeout_ms": INVENTORY_READY_TIMEOUT_MS,
        "login_submit_timeout_ms": LOGIN_SUBMIT_TIMEOUT_MS,
        "running_jobs": running_jobs,
    }


@app.post("/jobs/login")
def login_job(request: Request):
    try:
        return login_only(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/login/start")
def login_job_start(request: Request):
    return start_background_job(request, "login", login_only)


@app.post("/jobs/login/run")
def login_job_sync(request: Request):
    try:
        return login_only(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/filter-available")
def filter_available_job(request: Request):
    try:
        return login_and_filter_available(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/filter-available/start")
def filter_available_job_start(request: Request):
    return start_background_job(request, "filter-available", login_and_filter_available)


@app.post("/jobs/filter-available/run")
def filter_available_job_sync(request: Request):
    try:
        return login_and_filter_available(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.get("/jobs")
def list_jobs():
    return {"ok": True, "jobs": list_job_snapshots()}


@app.get("/jobs/{job_id}", name="get_job")
def get_job(job_id: str):
    job = get_job_snapshot(job_id)

    if not job:
        return JSONResponse(status_code=404, content={"ok": False, "error": "job_not_found"})

    return job


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
