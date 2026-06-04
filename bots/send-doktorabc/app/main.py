import json
import os
import re
import time
from contextvars import ContextVar
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")
SESSION_STATE_PATH = os.environ.get(
    "DOKTORABC_SESSION_STATE_PATH",
    os.path.join(ARTIFACTS_DIR, "doktorabc-send-storage-state.json"),
)

DEFAULT_DOKTORABC_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
DOKTORABC_USER_AGENT = (
    os.environ.get("DOKTORABC_USER_AGENT", DEFAULT_DOKTORABC_USER_AGENT).strip()
    or DEFAULT_DOKTORABC_USER_AGENT
)


def int_env(name, default):
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def bool_env(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


NAVIGATION_TIMEOUT_MS = int_env("SEND_DOKTORABC_NAVIGATION_TIMEOUT_MS", 30_000)
LOGIN_READY_TIMEOUT_MS = int_env("SEND_DOKTORABC_LOGIN_READY_TIMEOUT_MS", 45_000)
LOGIN_SUCCESS_TIMEOUT_MS = int_env("SEND_DOKTORABC_LOGIN_SUCCESS_TIMEOUT_MS", 45_000)
PRODUCTS_READY_TIMEOUT_MS = int_env("SEND_DOKTORABC_PRODUCTS_READY_TIMEOUT_MS", 70_000)
PRODUCTS_STABLE_MS = int_env("SEND_DOKTORABC_PRODUCTS_STABLE_MS", 3_500)
PRODUCTS_POLL_MS = int_env("SEND_DOKTORABC_PRODUCTS_POLL_MS", 400)
PRODUCT_SEARCH_READY_TIMEOUT_MS = int_env("SEND_DOKTORABC_PRODUCT_SEARCH_READY_TIMEOUT_MS", 45_000)
PRODUCT_SEARCH_STABLE_MS = int_env("SEND_DOKTORABC_PRODUCT_SEARCH_STABLE_MS", 2_500)
WAIT_FOR_NETWORKIDLE = bool_env("SEND_DOKTORABC_WAIT_FOR_NETWORKIDLE", False)

MIN_PRODUCT_CELL_COUNT = 11

CURRENT_RUN_SCREENSHOTS = ContextVar("CURRENT_RUN_SCREENSHOTS", default=None)

os.makedirs(ARTIFACTS_DIR, exist_ok=True)
app = FastAPI(title="send-doktorabc")
app.mount("/artifacts", StaticFiles(directory=ARTIFACTS_DIR), name="artifacts")

CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "SEND_DOKTORABC_CORS_ORIGINS",
        "http://localhost:8075,http://127.0.0.1:8075",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def log_event(event, **fields):
    payload = {
        "event": event,
        "service": "send-doktorabc",
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


def public_base_url(request):
    configured_url = os.environ.get("SEND_DOKTORABC_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if configured_url:
        return configured_url

    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    if not host:
        return str(request.base_url).rstrip("/")

    return f"{proto}://{host}".rstrip("/")


def artifact_url(path, base_url):
    if not path:
        return None

    filename = os.path.basename(path)
    return f"{base_url}/artifacts/{filename}"


def screenshot_entries(base_url):
    paths = CURRENT_RUN_SCREENSHOTS.get() or []
    entries = []
    seen = set()

    for path in paths:
        if not path or path in seen:
            continue
        seen.add(path)
        entries.append(
            {
                "filename": os.path.basename(path),
                "path": path,
                "url": artifact_url(path, base_url),
            }
        )

    return entries


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def login_url():
    return os.environ.get("DOKTORABC_LOGIN_URL") or "https://pharmacies.doktorabc.com/login"


def products_url():
    return (
        os.environ.get("DOKTORABC_PRODUCTS_URL")
        or "https://pharmacies.doktorabc.com/manage-supplies?tab=active&subTab=cannabis&sortField=price&sortOrder=asc&page=1"
    )


def login_url_candidates(target_url):
    candidates = [target_url, login_url()]
    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            unique.append(candidate)
            seen.add(candidate)
    return unique


def browser_context_options():
    return {
        "user_agent": DOKTORABC_USER_AGENT,
        "viewport": {"width": 1365, "height": 900},
    }


def capture_optional_screenshot(page, path, label):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        page.screenshot(path=path, full_page=True)
        screenshots = CURRENT_RUN_SCREENSHOTS.get()
        if screenshots is not None:
            screenshots.append(path)
        log_event("screenshot_saved", label=label, path=path, url=page.url)
        return path, None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        log_event("screenshot_failed", label=label, path=path, error=error)
        return None, error


def capture_failure_screenshot(page, label):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(ARTIFACTS_DIR, f"send-doktorabc-{label}-{timestamp}.png")
    screenshot_path, _ = capture_optional_screenshot(page, path, label)
    return screenshot_path


def goto_page(page, url):
    log_event("goto_start", url=url)
    page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
    page.wait_for_load_state("domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
    log_event("goto_domcontentloaded", url=page.url)


def wait_for_load_states(page):
    for state in ("domcontentloaded", "load"):
        try:
            log_event("load_state_wait_start", state=state, url=page.url)
            page.wait_for_load_state(state, timeout=10_000)
            log_event("load_state_wait_ok", state=state, url=page.url)
        except PlaywrightTimeoutError:
            log_event("load_state_wait_timeout", state=state, url=page.url)

    if WAIT_FOR_NETWORKIDLE:
        try:
            log_event("load_state_wait_start", state="networkidle", url=page.url)
            page.wait_for_load_state("networkidle", timeout=5_000)
            log_event("load_state_wait_ok", state="networkidle", url=page.url)
        except PlaywrightTimeoutError:
            log_event("load_state_wait_timeout", state="networkidle", url=page.url)


LOGIN_FORM_READY_JS = """
() => {
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const visible = (element) => {
    if (!element) return false;
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
  const emailVisible = Array.from(
    document.querySelectorAll('input[placeholder*="Email" i], input[type="email"], input[name*="email" i]')
  ).some(visible);
  const passwordVisible = Array.from(
    document.querySelectorAll('input[placeholder*="Password" i], input[type="password"], input[name*="password" i]')
  ).some(visible);
  const loginButtonVisible = Array.from(
    document.querySelectorAll("button, input[type='submit'], [role='button']")
  ).some((element) => visible(element) && /login/i.test(normalize(element.innerText || element.value || element.textContent)));

  if (emailVisible && passwordVisible && loginButtonVisible) {
    return {
      status: "login_ready",
      url: window.location.href,
      readyState: document.readyState,
      emailVisible,
      passwordVisible,
      loginButtonVisible,
    };
  }

  return false;
}
"""


PAGE_DIAGNOSTICS_JS = """
() => {
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const visible = (element) => {
    if (!element) return false;
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
  const text = normalize(document.body?.innerText || "");
  const emailVisible = Array.from(
    document.querySelectorAll('input[placeholder*="Email" i], input[type="email"], input[name*="email" i]')
  ).some(visible);
  const passwordVisible = Array.from(
    document.querySelectorAll('input[placeholder*="Password" i], input[type="password"], input[name*="password" i]')
  ).some(visible);
  const loginButtonVisible = Array.from(
    document.querySelectorAll("button, input[type='submit'], [role='button']")
  ).some((element) => visible(element) && /login/i.test(normalize(element.innerText || element.value || element.textContent)));
  const headerText = Array.from(document.querySelectorAll("th"))
    .map((element) => normalize(element.innerText || element.textContent))
    .join(" | ");
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
  const visibleLoaderCount = loaderSelectors
    .flatMap((selector) => Array.from(document.querySelectorAll(selector)))
    .filter(visible)
    .length;
  const rows = Array.from(document.querySelectorAll("tr"));
  const validProductRows = rows
    .map((row) => Array.from(row.querySelectorAll("td")).map((cell) => normalize(cell.innerText || cell.textContent)))
    .filter((cells) => (
      cells.length >= 11 &&
      Boolean(cells[1]) &&
      Boolean(cells[3]) &&
      /\\d/.test(cells[6] || "") &&
      /\\d/.test(cells[7] || "")
    ));

  return {
    url: window.location.href,
    title: document.title,
    readyState: document.readyState,
    textLength: text.length,
    textBucket: Math.round(text.length / 100) * 100,
    tableRows: rows.length,
    validProductRowCount: validProductRows.length,
    headerVisible: /Product\\s+name/i.test(headerText) && /PZN/i.test(headerText) && /Availability/i.test(headerText),
    addDecreaseVisible: /Add\\/Decrease/i.test(headerText || text),
    emptyStateVisible: /No\\s+stock\\s+supplies\\s+found/i.test(text),
    visibleLoaderCount,
    emailVisible,
    passwordVisible,
    loginButtonVisible,
    sampleProducts: validProductRows.slice(0, 3).map((cells) => ({
      product_name: cells[1],
      pzn: cells[3],
      strain: cells[4],
      quantity: cells[6],
      price_per_g_incl_vat: cells[7],
      availability: cells[10],
    })),
  };
}
"""


def page_diagnostics(page):
    try:
        return page.evaluate(PAGE_DIAGNOSTICS_JS)
    except Exception as exc:
        return {"diagnostics_error": f"{type(exc).__name__}: {exc}", "url": getattr(page, "url", None)}


def wait_for_rendered_login_form(page, timeout_ms=LOGIN_READY_TIMEOUT_MS):
    log_event("login_form_wait_start", timeout_ms=timeout_ms, url=page.url)
    try:
        handle = page.wait_for_function(LOGIN_FORM_READY_JS, timeout=timeout_ms, polling=250)
        result = handle.json_value()
        log_event("login_form_wait_ready", result=result)
        return result
    except PlaywrightTimeoutError as exc:
        diagnostics = page_diagnostics(page)
        log_event("login_form_wait_timeout", diagnostics=diagnostics, timeout_ms=timeout_ms)
        raise RuntimeError(
            "DoktorABC login form was not visible. Expected rendered email field, password field, and Login button. "
            f"Diagnostics: {diagnostics}"
        ) from exc


def visible_login_form(page):
    try:
        wait_for_rendered_login_form(page, timeout_ms=2_000)
        return True
    except Exception:
        return False


def is_login_url(url):
    return bool(re.search(r"(^|/|[?&])login($|[/?#=&])", (url or "").lower()))


def fill_first_visible(page, selectors, value):
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=4_000)
            locator.fill(value)
            return selector
        except PlaywrightTimeoutError:
            continue
    raise RuntimeError(f"Could not find visible input for selectors: {selectors}")


def click_pharmacist_role(page):
    candidates = (
        page.get_by_text("Pharmacist", exact=True).first,
        page.locator('label:has-text("Pharmacist")').first,
        page.locator('button:has-text("Pharmacist")').first,
        page.locator('[role="radio"]:has-text("Pharmacist")').first,
    )

    for locator in candidates:
        try:
            locator.wait_for(state="visible", timeout=2_500)
            locator.click(timeout=2_500)
            log_event("pharmacist_role_clicked")
            return True
        except Exception:
            continue

    log_event("pharmacist_role_not_visible")
    return False


def click_login_button(page):
    candidates = (
        page.get_by_role("button", name=re.compile("login", re.I)).first,
        page.locator('button:has-text("Login")').first,
        page.locator('input[type="submit"][value*="Login" i]').first,
    )

    for locator in candidates:
        try:
            locator.wait_for(state="visible", timeout=5_000)
            locator.click(timeout=5_000)
            log_event("login_button_clicked")
            return True
        except Exception:
            continue

    raise RuntimeError("Could not find DoktorABC login button.")


def save_session_state(context):
    session_state_dir = os.path.dirname(SESSION_STATE_PATH)
    if session_state_dir:
        os.makedirs(session_state_dir, exist_ok=True)

    temp_path = f"{SESSION_STATE_PATH}.{os.getpid()}.tmp"
    try:
        context.storage_state(path=temp_path)
        os.replace(temp_path, SESSION_STATE_PATH)
        log_event("session_state_saved", path=SESSION_STATE_PATH)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def product_page_ready(snapshot):
    no_loaders = snapshot.get("visibleLoaderCount") == 0
    has_real_rows = (snapshot.get("validProductRowCount") or 0) > 0
    has_real_empty_state = bool(snapshot.get("emptyStateVisible"))
    has_table_identity = bool(snapshot.get("headerVisible") or snapshot.get("addDecreaseVisible"))
    return no_loaders and has_table_identity and (has_real_rows or has_real_empty_state)


def product_stability_key(snapshot):
    return (
        snapshot.get("readyState"),
        snapshot.get("textBucket"),
        snapshot.get("tableRows"),
        snapshot.get("validProductRowCount"),
        snapshot.get("headerVisible"),
        snapshot.get("addDecreaseVisible"),
        snapshot.get("emptyStateVisible"),
        snapshot.get("visibleLoaderCount"),
    )


def product_search_snapshot(page, product_name):
    return page.evaluate(
        """
        (productName) => {
          const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
          const visible = (element) => {
            if (!element) return false;
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
          const searchInput = Array.from(
            document.querySelectorAll('input[placeholder="Search by medication name or PZN"], input[placeholder*="medication" i], input[placeholder*="PZN" i]')
          ).find(visible);
          const rows = Array.from(document.querySelectorAll("tr"));
          const productRows = rows
            .map((row, index) => {
              const cells = Array.from(row.querySelectorAll("td")).map((cell) => normalize(cell.innerText || cell.textContent));
              if (cells.length < 11) return null;
              const buttons = Array.from(row.querySelectorAll("button")).filter((button) => visible(button));
              const hasAddDecreaseButton = buttons.some((button) => normalize(button.innerText || button.textContent) === "Add/Decrease");
              return {
                index,
                product_name: cells[1],
                pzn: cells[3],
                strain: cells[4],
                quantity: cells[6],
                price_per_g_incl_vat: cells[7],
                availability: cells[10],
                hasAddDecreaseButton,
              };
            })
            .filter(Boolean);
          const exactRows = productRows.filter((row) => row.product_name === productName);
          const allAddDecreaseButtons = Array.from(document.querySelectorAll("button"))
            .filter((button) => visible(button) && normalize(button.innerText || button.textContent) === "Add/Decrease");
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
          const visibleLoaderCount = loaderSelectors
            .flatMap((selector) => Array.from(document.querySelectorAll(selector)))
            .filter(visible)
            .length;

          return {
            url: window.location.href,
            readyState: document.readyState,
            searchValue: searchInput ? searchInput.value : null,
            productRowCount: productRows.length,
            exactRowCount: exactRows.length,
            allAddDecreaseButtonCount: allAddDecreaseButtons.length,
            visibleLoaderCount,
            exactRows,
            emptyStateVisible: /No\\s+stock\\s+supplies\\s+found/i.test(normalize(document.body?.innerText || "")),
          };
        }
        """,
        product_name,
    )


def product_search_ready(snapshot, product_name):
    return (
        snapshot.get("readyState") in {"interactive", "complete"}
        and snapshot.get("searchValue") == product_name
        and snapshot.get("visibleLoaderCount") == 0
        and snapshot.get("exactRowCount") == 1
        and len(snapshot.get("exactRows") or []) == 1
        and bool((snapshot.get("exactRows") or [{}])[0].get("hasAddDecreaseButton"))
    )


def product_search_stability_key(snapshot):
    exact_rows = snapshot.get("exactRows") or []
    exact_row = exact_rows[0] if exact_rows else {}
    return (
        snapshot.get("readyState"),
        snapshot.get("searchValue"),
        snapshot.get("productRowCount"),
        snapshot.get("exactRowCount"),
        snapshot.get("allAddDecreaseButtonCount"),
        snapshot.get("visibleLoaderCount"),
        exact_row.get("product_name"),
        exact_row.get("pzn"),
        exact_row.get("quantity"),
        exact_row.get("price_per_g_incl_vat"),
        exact_row.get("availability"),
    )


def find_search_input(page):
    candidates = (
        page.get_by_placeholder("Search by medication name or PZN", exact=True),
        page.locator('input[placeholder*="medication" i]').first,
        page.locator('input[placeholder*="PZN" i]').first,
    )

    for locator in candidates:
        try:
            locator.wait_for(state="visible", timeout=5_000)
            return locator
        except PlaywrightTimeoutError:
            continue

    raise RuntimeError("Could not find DoktorABC product search input.")


def search_exact_product(page, product_name):
    clean_product_name = " ".join(product_name.split())
    if not clean_product_name:
        raise RuntimeError("Missing product_name.")

    search_input = find_search_input(page)
    log_event("product_search_fill", product_name=clean_product_name)
    search_input.fill(clean_product_name, timeout=10_000)

    deadline = time.monotonic() + PRODUCT_SEARCH_READY_TIMEOUT_MS / 1000
    stable_since = None
    previous_key = None
    last_snapshot = None

    log_event(
        "product_search_wait_start",
        product_name=clean_product_name,
        timeout_ms=PRODUCT_SEARCH_READY_TIMEOUT_MS,
        stable_ms=PRODUCT_SEARCH_STABLE_MS,
    )

    while time.monotonic() < deadline:
        snapshot = product_search_snapshot(page, clean_product_name)
        last_snapshot = snapshot
        ready = product_search_ready(snapshot, clean_product_name)
        key = product_search_stability_key(snapshot)
        now = time.monotonic()

        if ready and key == previous_key:
            if stable_since is not None and (now - stable_since) * 1000 >= PRODUCT_SEARCH_STABLE_MS:
                log_event("product_search_wait_ready", snapshot=snapshot)
                return clean_product_name, snapshot
        else:
            stable_since = now if ready else None
            previous_key = key

        page.wait_for_timeout(PRODUCTS_POLL_MS)

    screenshot_path = capture_failure_screenshot(page, "product-search-timeout")
    log_event(
        "product_search_wait_timeout",
        product_name=clean_product_name,
        screenshot_path=screenshot_path,
        last_snapshot=last_snapshot,
    )
    raise RuntimeError(
        "DoktorABC product search did not become safe to click. "
        "Expected exactly one stable exact product row with one Add/Decrease button. "
        f"Screenshot: {screenshot_path}. Last snapshot: {last_snapshot}"
    )


def click_add_decrease_for_exact_product(page, product_name):
    row_locator = page.locator("tr").filter(has=page.get_by_text(product_name, exact=True))
    row_count = row_locator.count()
    if row_count != 1:
        raise RuntimeError(f"Expected one exact product row before click, found {row_count}.")

    button_locator = row_locator.get_by_role("button", name="Add/Decrease", exact=True)
    button_count = button_locator.count()
    if button_count != 1:
        raise RuntimeError(f"Expected one Add/Decrease button in exact product row, found {button_count}.")

    log_event("add_decrease_about_to_click", product_name=product_name)
    button_locator.click(timeout=10_000)
    log_event("add_decrease_click_done", product_name=product_name)


def wait_for_products_page_usable(page, timeout_ms=PRODUCTS_READY_TIMEOUT_MS, stable_ms=PRODUCTS_STABLE_MS):
    wait_for_load_states(page)

    if visible_login_form(page) or is_login_url(page.url):
        screenshot_path = capture_failure_screenshot(page, "login-visible-while-waiting-products")
        raise RuntimeError(
            "DoktorABC session is not authenticated; login page is visible. "
            f"Screenshot: {screenshot_path}. Diagnostics: {page_diagnostics(page)}"
        )

    log_event("products_page_wait_start", timeout_ms=timeout_ms, stable_ms=stable_ms, url=page.url)
    deadline = time.monotonic() + timeout_ms / 1000
    stable_since = None
    previous_key = None
    last_snapshot = None

    while time.monotonic() < deadline:
        snapshot = page_diagnostics(page)
        last_snapshot = snapshot
        ready = product_page_ready(snapshot)
        key = product_stability_key(snapshot)
        now = time.monotonic()

        if ready and key == previous_key:
            if stable_since is not None and (now - stable_since) * 1000 >= stable_ms:
                result = {
                    "ready": True,
                    "waited_for": "product_rows_or_empty_state_render_stability",
                    "stable_ms": stable_ms,
                    "snapshot": snapshot,
                }
                log_event("products_page_wait_ready", result=result)
                return result
        else:
            stable_since = now if ready else None
            previous_key = key

        page.wait_for_timeout(PRODUCTS_POLL_MS)

    screenshot_path = capture_failure_screenshot(page, "products-page-ready-timeout")
    log_event("products_page_wait_timeout", screenshot_path=screenshot_path, last_snapshot=last_snapshot)
    raise RuntimeError(
        "DoktorABC products page did not become usable. "
        "Expected stable rendered product rows or the explicit empty-state, not only table headers. "
        f"Screenshot: {screenshot_path}. Last snapshot: {last_snapshot}"
    )


def perform_login(page, context, target_url, before_login_path=None):
    login_state = wait_for_rendered_login_form(page)
    log_event("login_required", url=page.url, target_url=target_url, login_state=login_state)

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
        capture_optional_screenshot(page, before_login_path, "before-login")

    click_login_button(page)
    wait_for_load_states(page)

    log_event("login_success_wait_start", timeout_ms=LOGIN_SUCCESS_TIMEOUT_MS, url=page.url)
    deadline = time.monotonic() + LOGIN_SUCCESS_TIMEOUT_MS / 1000
    last_snapshot = None
    while time.monotonic() < deadline:
        snapshot = page_diagnostics(page)
        last_snapshot = snapshot
        login_visible = snapshot.get("emailVisible") or snapshot.get("passwordVisible") or snapshot.get("loginButtonVisible")
        if not login_visible and not is_login_url(snapshot.get("url")):
            log_event("login_success_wait_ready", snapshot=snapshot)
            break
        page.wait_for_timeout(500)
    else:
        screenshot_path = capture_failure_screenshot(page, "login-success-timeout")
        raise RuntimeError(
            "DoktorABC login did not complete. "
            f"Screenshot: {screenshot_path}. Last snapshot: {last_snapshot}"
        )

    if not page.url.startswith(target_url.split("?")[0]):
        goto_page(page, target_url)

    wait_result = wait_for_products_page_usable(page)
    save_session_state(context)
    return wait_result


def open_saved_session(browser, target_url):
    if not os.path.exists(SESSION_STATE_PATH):
        return None

    log_event("saved_session_try", path=SESSION_STATE_PATH)

    try:
        context = browser.new_context(storage_state=SESSION_STATE_PATH, **browser_context_options())
    except Exception as exc:
        log_event("saved_session_open_failed", error=f"{type(exc).__name__}: {exc}")
        return None

    page = context.new_page()

    try:
        goto_page(page, target_url)
        if visible_login_form(page) or is_login_url(page.url):
            raise RuntimeError("saved session reached login page")
        wait_result = wait_for_products_page_usable(page)
        return context, page, True, wait_result
    except Exception as exc:
        context.close()
        log_event("saved_session_invalid", error=f"{type(exc).__name__}: {exc}")
        return None


def open_fresh_session(browser, target_url, before_login_path=None):
    log_event("fresh_session_try")
    context = browser.new_context(**browser_context_options())
    page = context.new_page()
    last_error = None

    for candidate_url in login_url_candidates(target_url):
        try:
            log_event("fresh_session_candidate_start", url=candidate_url)
            goto_page(page, candidate_url)

            if not visible_login_form(page):
                if not is_login_url(page.url):
                    goto_page(page, login_url())
                else:
                    page.reload(wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
                    wait_for_load_states(page)

            wait_result = perform_login(page, context, target_url, before_login_path=before_login_path)
            return context, page, False, wait_result
        except Exception as exc:
            last_error = exc
            log_event(
                "fresh_session_candidate_failed",
                url=candidate_url,
                error=f"{type(exc).__name__}: {exc}",
                diagnostics=page_diagnostics(page),
            )

    context.close()
    raise RuntimeError(f"Could not open authenticated DoktorABC products page. Last error: {last_error}") from last_error


def open_authenticated_products_page(browser, before_login_path=None):
    target_url = products_url()
    saved = open_saved_session(browser, target_url)
    if saved is not None:
        return saved
    return open_fresh_session(browser, target_url, before_login_path=before_login_path)


def login_check(base_url):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"send-doktorabc-before-login-{timestamp}.png")
    ready_screenshot_path = os.path.join(ARTIFACTS_DIR, f"send-doktorabc-products-ready-{timestamp}.png")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, wait_result = open_authenticated_products_page(
                browser,
                before_login_path=before_login_path,
            )
            ready_screenshot_path, ready_screenshot_error = capture_optional_screenshot(
                page,
                ready_screenshot_path,
                "products-ready",
            )
            return {
                "ok": True,
                "current_url": page.url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                "before_login_url": None if reused_session else artifact_url(before_login_path, base_url),
                "ready_screenshot_path": ready_screenshot_path,
                "ready_screenshot_url": artifact_url(ready_screenshot_path, base_url),
                "ready_screenshot_error": ready_screenshot_error,
                "screenshots": screenshot_entries(base_url),
                "wait_result": wait_result,
            }
        finally:
            if context:
                context.close()
            browser.close()


def open_add_decrease_preview(product_name, base_url):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"send-doktorabc-before-login-{timestamp}.png")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"send-doktorabc-add-decrease-{timestamp}.png")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, wait_result = open_authenticated_products_page(
                browser,
                before_login_path=before_login_path,
            )
            clean_product_name, search_result = search_exact_product(page, product_name)
            click_add_decrease_for_exact_product(page, clean_product_name)

            # Let DoktorABC render the Add/Decrease surface, but do not touch its fields.
            page.wait_for_timeout(1_500)
            screenshot_path, screenshot_error = capture_optional_screenshot(
                page,
                screenshot_path,
                "add-decrease-preview",
            )

            return {
                "ok": True,
                "current_url": page.url,
                "product_name": clean_product_name,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                "before_login_url": None if reused_session else artifact_url(before_login_path, base_url),
                "screenshot_path": screenshot_path,
                "screenshot_url": artifact_url(screenshot_path, base_url),
                "screenshot_error": screenshot_error,
                "screenshots": screenshot_entries(base_url),
                "products_page_wait_result": wait_result,
                "search_result": search_result,
            }
        finally:
            if context:
                context.close()
            browser.close()


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "send-doktorabc",
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
        "session_state_path": SESSION_STATE_PATH,
        "session_state_exists": os.path.exists(SESSION_STATE_PATH),
    }


@app.post("/jobs/login-check")
def job_login_check(request: Request):
    token = CURRENT_RUN_SCREENSHOTS.set([])
    base_url = public_base_url(request)
    try:
        return login_check(base_url)
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "screenshots": screenshot_entries(base_url),
            },
        )
    finally:
        CURRENT_RUN_SCREENSHOTS.reset(token)


@app.post("/jobs/open-products-page")
def job_open_products_page(request: Request):
    return job_login_check(request)


@app.post("/jobs/add-decrease-preview")
async def job_add_decrease_preview(request: Request):
    token = CURRENT_RUN_SCREENSHOTS.set([])
    base_url = public_base_url(request)

    try:
        payload = await request.json()
        product_name = payload.get("product_name") if isinstance(payload, dict) else None
        return open_add_decrease_preview(product_name or "", base_url)
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "screenshots": screenshot_entries(base_url),
            },
        )
    finally:
        CURRENT_RUN_SCREENSHOTS.reset(token)
