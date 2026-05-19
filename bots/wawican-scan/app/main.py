import os
import json
import re
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

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
INVENTORY_READY_TIMEOUT_MS = int(os.environ.get("WAWICAN_INVENTORY_READY_TIMEOUT_MS", "60000"))
POST_LOGIN_READY_TIMEOUT_MS = int(os.environ.get("WAWICAN_POST_LOGIN_READY_TIMEOUT_MS", "15000"))
FILTER_TIMEOUT_MS = int(os.environ.get("WAWICAN_FILTER_TIMEOUT_MS", "60000"))
SCRAPE_PAGE_READY_TIMEOUT_MS = int(os.environ.get("WAWICAN_SCRAPE_PAGE_READY_TIMEOUT_MS", "30000"))
SCRAPE_MAX_PAGES = int(os.environ.get("WAWICAN_SCRAPE_MAX_PAGES", "250"))
SUPABASE_TIMEOUT_SECONDS = int(os.environ.get("WAWICAN_SUPABASE_TIMEOUT_SECONDS", "60"))
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


def capture_screenshot_now(page, path, trace=None, trigger="inventory_ready_visible"):
    trace_step(trace, "capture_screenshot", path=path)
    page.screenshot(path=path, full_page=True)

    return {
        "screenshot_path": path,
        "screenshot_wait_ms": 0,
        "screenshot_trigger": trigger,
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
            'span',
            'div',
            'button',
            'label',
            '.q-table *',
            '.inventory-table-component *'
          ];
          const candidates = Array.from(document.querySelectorAll(selectors.join(',')));
          return candidates.some((element) => {
            const text = fold(element.textContent);
            if (!text.includes(target)) {
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


def wait_for_inventory_after_login_submit(page, trace=None):
    try:
        trace_step(trace, "wait_for_inventory_after_submit", timeout_ms=POST_LOGIN_READY_TIMEOUT_MS)
        wait_for_inventory_page(page, timeout=POST_LOGIN_READY_TIMEOUT_MS, trace=trace)
        return
    except Exception as exc:
        trace_page_state(
            trace,
            "inventory_not_visible_after_submit",
            page,
            error=f"{type(exc).__name__}: {exc}",
        )

    trace_step(trace, "goto_inventory_after_login", url=inventory_url())
    page.goto(inventory_url(), wait_until="domcontentloaded", timeout=30_000)
    wait_for_inventory_page(page, trace=trace)


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
    wait_for_inventory_after_login_submit(page, trace=trace)
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
          const isVisible = (element) => {
            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return (
              rect.width > 0 &&
              rect.height > 0 &&
              style.visibility !== 'hidden' &&
              style.display !== 'none' &&
              style.opacity !== '0'
            );
          };
          const headers = Array.from(document.querySelectorAll('th'));
          const header = headers.find((element) => (
            normalize(element.innerText).includes('Verfügbarkeit') && isVisible(element)
          ));

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
          return { ok: true, header_text: normalize(header.innerText) };
        }
        """
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "availability_filter_button_failed")

    return result


def wait_for_availability_filter_menu(page, trace=None):
    trace_step(trace, "wait_for_availability_filter_menu", timeout_ms=FILTER_TIMEOUT_MS)
    page.wait_for_function(
        """
        () => {
          const fold = (value) => (value || '')
            .normalize('NFD')
            .replace(/[\\u0300-\\u036f]/g, '')
            .replace(/\\s+/g, ' ')
            .trim()
            .toLowerCase();
          const candidates = Array.from(document.querySelectorAll(
            '.q-menu *, .q-position-engine *, .q-checkbox, [role="checkbox"], label, span, div'
          ));

          return candidates.some((element) => {
            if (element.closest('th')) {
              return false;
            }

            if (fold(element.textContent) !== 'verfugbar') {
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
        }
        """,
        timeout=FILTER_TIMEOUT_MS,
    )
    wait_for_next_render_frame(page)
    trace_page_state(trace, "availability_filter_menu_visible", page)

    return {"ok": True, "menu_item": "verfügbar"}


def ensure_available_checkbox_checked(page):
    page.locator('[role="checkbox"][aria-label="verfügbar"]').first.wait_for(
        state="visible",
        timeout=FILTER_TIMEOUT_MS,
    )

    result = page.evaluate(
        """
        () => {
          const checkbox = document.querySelector('[role="checkbox"][aria-label="verfügbar"]');

          if (!checkbox) {
            return { ok: false, error: 'available_checkbox_not_found' };
          }

          const nativeInput = checkbox.querySelector('input[type="checkbox"]');
          const ariaChecked = checkbox.getAttribute('aria-checked');
          const className = String(checkbox.className || '');
          const checked =
            (nativeInput && nativeInput.checked) ||
            ariaChecked === 'true' ||
            className.includes('--truthy') ||
            Boolean(checkbox.querySelector('.q-checkbox__inner--truthy'));

          if (!checked) {
            const box =
              checkbox.querySelector('.q-checkbox__inner') ||
              checkbox.querySelector('.q-checkbox__bg') ||
              checkbox;
            box.click();
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
    wait_for_availability_filter_menu(page, trace=trace)
    trace_step(trace, "ensure_available_checkbox_checked")
    checkbox_result = ensure_available_checkbox_checked(page)

    return {
        "filter_button": click_result,
        "available_checkbox": checkbox_result,
        "visible_rows": page.locator("tbody tr").count(),
    }


def normalize_space(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_decimal(value):
    text = normalize_space(value)

    if not text or text.lower() in {"n.a.", "na", "n/a", "-"} or "<" in text:
        return None

    text = text.replace("€", "").replace(" ", "").replace(".", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)

    if not match:
        return None

    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def parse_date(value):
    text = normalize_space(value)

    if not text or text.lower() in {"n.a.", "na", "n/a", "-"}:
        return None

    match = re.fullmatch(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
    if not match:
        return None

    day, month, year = map(int, match.groups())
    if year < 1900:
        return None

    try:
        return datetime(year, month, day).date()
    except ValueError:
        return None


def postgres_url():
    value = (
        os.environ.get("SUPABASE_DB_URL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URL")
        or ""
    ).strip()

    if not value:
        raise RuntimeError(
            "Missing SUPABASE_DB_URL. Use the Supabase direct Postgres or pooler URL "
            "with sslmode=require."
        )

    if value.startswith(("postgresql://", "postgres://")) and "sslmode=" not in value:
        separator = "&" if "?" in value else "?"
        value = f"{value}{separator}sslmode=require"

    return value


def products_schema():
    return (
        os.environ.get("WAWICAN_PRODUCTS_SCHEMA")
        or os.environ.get("SUPABASE_SCHEMA")
        or "private"
    ).strip() or "private"


def products_table():
    return (os.environ.get("WAWICAN_PRODUCTS_TABLE") or "wawican_products").strip() or "wawican_products"


def supabase_url():
    return (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")


def supabase_service_role_key():
    return (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()


def supabase_rest_configured():
    return bool(supabase_url() and supabase_service_role_key())


def supabase_table_url():
    return f"{supabase_url()}/rest/v1/{quote(products_table(), safe='')}"


def supabase_headers():
    service_role_key = required_env("SUPABASE_SERVICE_ROLE_KEY")
    schema_name = products_schema()

    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Accept-Profile": schema_name,
        "Content-Profile": schema_name,
        "Content-Type": "application/json",
    }


def response_preview(response):
    body = response.text

    if len(body) > 1000:
        body = f"{body[:1000]}..."

    return {
        "status_code": response.status_code,
        "ok": response.status_code < 400,
        "body": body,
    }


def validate_identifier(value, label):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise RuntimeError(f"Invalid {label}: {value!r}")

    return value


def scrape_current_inventory_page(page, page_number=None, trace=None):
    trace_step(trace, "wait_for_inventory_table", timeout_ms=SCRAPE_PAGE_READY_TIMEOUT_MS)
    page.locator("table.q-table tbody").first.wait_for(
        state="visible",
        timeout=SCRAPE_PAGE_READY_TIMEOUT_MS,
    )

    result = page.evaluate(
        """
        () => {
          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
          const fold = (value) => normalize(value)
            .normalize('NFD')
            .replace(/[\\u0300-\\u036f]/g, '')
            .toLowerCase();
          const removeNoise = (element) => {
            const clone = element.cloneNode(true);
            clone.querySelectorAll(
              'button, svg, img, .q-focus-helper, .q-tooltip, .q-menu, .q-position-engine'
            ).forEach((node) => node.remove());
            return normalize(clone.textContent);
          };
          const switchValue = (cell) => {
            const switchElement = cell.querySelector('[role="switch"]');
            if (!switchElement) {
              return null;
            }

            return switchElement.getAttribute('aria-checked') === 'true';
          };
          const mapHeader = (header) => {
            const key = fold(header);
            if (key.includes('blute')) return 'product_name';
            if (key === 'preis pro g') return 'price_per_g_text';
            if (key.includes('netto ek')) return 'net_purchase_price_per_g_text';
            if (key.includes('verfugbarkeit')) return 'availability_status';
            if (key.includes('ist bestand')) return 'actual_stock_text';
            if (key.includes('virtueller bestand')) return 'virtual_stock_text';
            if (key.includes('preisberechnung')) return 'price_calculation_enabled';
            if (key.includes('immer verfugbar')) return 'always_available';
            if (key.includes('restmenge')) return 'remaining_quantity_text';
            if (key.includes('kultivar')) return 'cultivar';
            if (key.includes('genetik')) return 'genetics';
            if (key.includes('dominanz')) return 'dominance';
            if (key === 'thc') return 'thc';
            if (key === 'cbd') return 'cbd';
            if (key.includes('reservierung beim lieferanten')) return 'supplier_reserved_quantity_text';
            if (key.includes('verfall')) return 'expiry_date_text';
            if (key.includes('nicht anzeigen')) return 'hidden';
            return key.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
          };

          const table = document.querySelector('table.q-table');
          if (!table) {
            return { ok: false, error: 'inventory_table_not_found' };
          }

          const headers = Array.from(table.querySelectorAll('thead th')).map((header) => ({
            label: removeNoise(header),
            field: mapHeader(removeNoise(header)),
          }));
          const rows = Array.from(table.querySelectorAll('tbody tr')).map((row, rowIndex) => {
            const record = {
              row_index: rowIndex + 1,
              raw_cells: [],
            };

            Array.from(row.querySelectorAll('td')).forEach((cell, cellIndex) => {
              const header = headers[cellIndex] || {
                label: `column_${cellIndex + 1}`,
                field: `column_${cellIndex + 1}`,
              };
              const text = removeNoise(cell);
              const iconTexts = Array.from(cell.querySelectorAll('i')).map((icon) => normalize(icon.textContent));
              const toggle = switchValue(cell);
              const rawCell = {
                index: cellIndex + 1,
                header: header.label,
                field: header.field,
                text,
                icon_texts: iconTexts,
                switch_value: toggle,
              };

              record.raw_cells.push(rawCell);

              if (header.field === 'availability_status') {
                const hasPositive = Boolean(cell.querySelector('.text-positive'));
                const hasNegative = Boolean(
                  cell.querySelector('.text-negative, .text-red, .text-red-7, .text-red-8')
                );
                record.available = hasPositive ? true : (hasNegative ? false : null);
                record.availability_status = hasPositive ? 'verfügbar' : (hasNegative ? 'nicht verfügbar' : text);
                return;
              }

              if (
                header.field === 'price_calculation_enabled' ||
                header.field === 'always_available' ||
                header.field === 'hidden'
              ) {
                record[header.field] = toggle;
                return;
              }

              record[header.field] = text;
            });

            return record;
          });

          return {
            ok: true,
            headers,
            row_count: rows.length,
            rows,
          };
        }
        """
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "inventory_scrape_failed")

    trace_step(
        trace,
        "scraped_inventory_page",
        page_number=page_number,
        row_count=result.get("row_count", 0),
    )

    return result


def get_pagination_state(page):
    return page.evaluate(
        """
        () => {
          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
          const parsePageLabel = (label) => {
            const match = normalize(label).match(/(\\d+)\\s*\\/\\s*(\\d+)/);
            if (!match) {
              return { current: null, total: null };
            }

            return { current: Number(match[1]), total: Number(match[2]) };
          };
          const pagination = document.querySelector('.q-pagination');
          const input = pagination ? pagination.querySelector('input[type="number"], input') : null;
          const label = input
            ? normalize(input.value || input.getAttribute('placeholder') || '')
            : normalize(pagination ? pagination.textContent : '');
          const parsed = parsePageLabel(label);
          const nextIcon = Array.from(document.querySelectorAll('.q-pagination i'))
            .find((icon) => normalize(icon.textContent) === 'keyboard_arrow_right');
          const nextButton = nextIcon ? nextIcon.closest('button') : null;
          const nextDisabled = !nextButton || nextButton.disabled ||
            nextButton.getAttribute('aria-disabled') === 'true' ||
            nextButton.classList.contains('disabled');
          const firstRow = document.querySelector('table.q-table tbody tr');

          return {
            label,
            current_page: parsed.current,
            total_pages: parsed.total,
            has_next: !nextDisabled,
            first_row_text: normalize(firstRow ? firstRow.textContent : ''),
          };
        }
        """
    )


def click_next_inventory_page(page, before_state, trace=None):
    trace_step(trace, "click_next_inventory_page", before_state=before_state)
    result = page.evaluate(
        """
        () => {
          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
          const nextIcon = Array.from(document.querySelectorAll('.q-pagination i'))
            .find((icon) => normalize(icon.textContent) === 'keyboard_arrow_right');
          const nextButton = nextIcon ? nextIcon.closest('button') : null;

          if (!nextButton || nextButton.disabled ||
              nextButton.getAttribute('aria-disabled') === 'true' ||
              nextButton.classList.contains('disabled')) {
            return { ok: true, clicked: false, reason: 'next_button_disabled' };
          }

          nextButton.click();
          return { ok: true, clicked: true };
        }
        """
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "next_page_click_failed")

    if not result.get("clicked"):
        return result

    page.wait_for_function(
        """
        (before) => {
          const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
          const pagination = document.querySelector('.q-pagination');
          const input = pagination ? pagination.querySelector('input[type="number"], input') : null;
          const label = input
            ? normalize(input.value || input.getAttribute('placeholder') || '')
            : normalize(pagination ? pagination.textContent : '');
          const match = label.match(/(\\d+)\\s*\\/\\s*(\\d+)/);
          const current = match ? Number(match[1]) : null;
          const firstRow = document.querySelector('table.q-table tbody tr');
          const firstRowText = normalize(firstRow ? firstRow.textContent : '');

          if (before.current_page && current && current !== before.current_page) {
            return true;
          }

          return Boolean(before.first_row_text && firstRowText && firstRowText !== before.first_row_text);
        }
        """,
        arg=before_state,
        timeout=SCRAPE_PAGE_READY_TIMEOUT_MS,
    )
    wait_for_inventory_page(page, timeout=SCRAPE_PAGE_READY_TIMEOUT_MS, trace=trace)

    return result


def normalize_scraped_product(raw_product, page_number, source_url, scraped_at):
    product = {
        "product_name": normalize_space(raw_product.get("product_name")),
        "price_per_g_text": normalize_space(raw_product.get("price_per_g_text")),
        "price_per_g": parse_decimal(raw_product.get("price_per_g_text")),
        "net_purchase_price_per_g_text": normalize_space(raw_product.get("net_purchase_price_per_g_text")),
        "net_purchase_price_per_g": parse_decimal(raw_product.get("net_purchase_price_per_g_text")),
        "availability_status": normalize_space(raw_product.get("availability_status")),
        "available": raw_product.get("available"),
        "actual_stock_text": normalize_space(raw_product.get("actual_stock_text")),
        "actual_stock": parse_decimal(raw_product.get("actual_stock_text")),
        "virtual_stock_text": normalize_space(raw_product.get("virtual_stock_text")),
        "virtual_stock": parse_decimal(raw_product.get("virtual_stock_text")),
        "price_calculation_enabled": raw_product.get("price_calculation_enabled"),
        "always_available": raw_product.get("always_available"),
        "remaining_quantity_text": normalize_space(raw_product.get("remaining_quantity_text")),
        "remaining_quantity": parse_decimal(raw_product.get("remaining_quantity_text")),
        "cultivar": normalize_space(raw_product.get("cultivar")),
        "genetics": normalize_space(raw_product.get("genetics")),
        "dominance": normalize_space(raw_product.get("dominance")),
        "thc": normalize_space(raw_product.get("thc")),
        "cbd": normalize_space(raw_product.get("cbd")),
        "supplier_reserved_quantity_text": normalize_space(raw_product.get("supplier_reserved_quantity_text")),
        "supplier_reserved_quantity": parse_decimal(raw_product.get("supplier_reserved_quantity_text")),
        "expiry_date_text": normalize_space(raw_product.get("expiry_date_text")),
        "expiry_date": parse_date(raw_product.get("expiry_date_text")),
        "expires_at": parse_date(raw_product.get("expiry_date_text")),
        "hidden": raw_product.get("hidden"),
        "do_not_show": raw_product.get("hidden"),
        "page_number": page_number,
        "row_index": raw_product.get("row_index"),
        "source_url": source_url,
        "scraped_at": scraped_at,
        "raw_data": raw_product,
    }

    return product


def normalize_products_for_db(raw_products, source_url, scraped_at):
    return [
        normalize_scraped_product(raw_product, raw_product.get("page_number"), source_url, scraped_at)
        for raw_product in raw_products
        if normalize_space(raw_product.get("product_name"))
    ]


def get_db_columns(cursor, schema_name, table_name):
    cursor.execute(
        """
        select column_name, data_type, udt_name
        from information_schema.columns
        where table_schema = %s
          and table_name = %s
        order by ordinal_position
        """,
        (schema_name, table_name),
    )
    columns = cursor.fetchall()

    if not columns:
        raise RuntimeError(f"Table {schema_name}.{table_name} was not found or has no columns.")

    return {
        row[0]: {
            "data_type": row[1],
            "udt_name": row[2],
        }
        for row in columns
    }


def value_for_column(column_name, column_info, product, jsonb_wrapper):
    value = product.get(column_name)
    data_type = column_info.get("data_type")
    udt_name = column_info.get("udt_name")

    if value is None:
        return None

    if data_type in {"json", "jsonb"} or udt_name in {"json", "jsonb"}:
        return jsonb_wrapper(value)

    if data_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on", "verfügbar", "verfugbar"}
        return bool(value)

    if data_type in {"integer", "bigint", "smallint"}:
        parsed = parse_decimal(value)
        return int(parsed) if parsed is not None else None

    if data_type in {"numeric", "decimal", "real", "double precision"}:
        return parse_decimal(value) if not isinstance(value, Decimal) else value

    if data_type == "date":
        return parse_date(value) if isinstance(value, str) else value

    if data_type.startswith("timestamp"):
        return value

    if data_type in {"text", "character varying", "character"}:
        if isinstance(value, (datetime,)):
            return value.isoformat()
        if hasattr(value, "isoformat") and not isinstance(value, str):
            return value.isoformat()
        return str(value)

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)

    if isinstance(value, Decimal):
        return str(value)

    return value


def json_ready(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if hasattr(value, "isoformat") and not isinstance(value, str):
        return value.isoformat()

    if isinstance(value, list):
        return [json_ready(item) for item in value]

    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}

    return value


def product_payload_for_rest(product):
    return {
        key: json_ready(value)
        for key, value in product.items()
        if value is not None
    }


def chunks(items, size):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def write_products_to_supabase_rest(products, trace=None):
    replace_all = bool_env("WAWICAN_PRODUCTS_REPLACE_ALL", True)
    dry_run = bool_env("WAWICAN_PRODUCTS_DRY_RUN", False)
    schema_name = validate_identifier(products_schema(), "WAWICAN_PRODUCTS_SCHEMA")
    table_name = validate_identifier(products_table(), "WAWICAN_PRODUCTS_TABLE")

    if dry_run:
        return {
            "enabled": False,
            "dry_run": True,
            "schema": schema_name,
            "table": table_name,
            "inserted_rows": 0,
            "scraped_rows": len(products),
        }

    import httpx

    trace_step(
        trace,
        "write_products_to_supabase_rest",
        schema=schema_name,
        table=table_name,
        rows=len(products),
        replace_all=replace_all,
    )

    if replace_all:
        response = httpx.delete(
            supabase_table_url(),
            headers={
                **supabase_headers(),
                "Prefer": "return=minimal",
            },
            params={
                "product_name": "not.is.null",
            },
            timeout=SUPABASE_TIMEOUT_SECONDS,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Supabase cleanup failed: {response_preview(response)}")

    inserted_rows = 0
    for product_chunk in chunks([product_payload_for_rest(product) for product in products], 500):
        if not product_chunk:
            continue

        response = httpx.post(
            supabase_table_url(),
            headers={
                **supabase_headers(),
                "Prefer": "return=minimal",
            },
            json=product_chunk,
            timeout=SUPABASE_TIMEOUT_SECONDS,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Supabase insert failed: {response_preview(response)}")

        inserted_rows += len(product_chunk)

    return {
        "enabled": True,
        "method": "supabase_rest",
        "schema": schema_name,
        "table": table_name,
        "replace_all": replace_all,
        "inserted_rows": inserted_rows,
        "supabase_url": supabase_url(),
    }


def write_products_to_postgres(products, trace=None):
    replace_all = bool_env("WAWICAN_PRODUCTS_REPLACE_ALL", True)
    dry_run = bool_env("WAWICAN_PRODUCTS_DRY_RUN", False)
    schema_name = validate_identifier(products_schema(), "WAWICAN_PRODUCTS_SCHEMA")
    table_name = validate_identifier(products_table(), "WAWICAN_PRODUCTS_TABLE")

    if dry_run:
        return {
            "enabled": False,
            "dry_run": True,
            "schema": schema_name,
            "table": table_name,
            "inserted_rows": 0,
            "scraped_rows": len(products),
        }

    from psycopg import connect, sql
    from psycopg.types.json import Jsonb

    trace_step(
        trace,
        "write_products_to_postgres",
        schema=schema_name,
        table=table_name,
        rows=len(products),
        replace_all=replace_all,
    )

    with connect(postgres_url()) as connection:
        with connection.cursor() as cursor:
            columns = get_db_columns(cursor, schema_name, table_name)

            if not products:
                if replace_all:
                    cursor.execute(
                        sql.SQL("delete from {}.{}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                        )
                    )

                return {
                    "enabled": True,
                    "dry_run": False,
                    "schema": schema_name,
                    "table": table_name,
                    "replace_all": replace_all,
                    "inserted_rows": 0,
                    "insert_columns": [],
                }

            insert_columns = [column for column in columns if any(column in product for product in products)]

            if not insert_columns:
                raise RuntimeError(
                    f"No matching columns found in {schema_name}.{table_name}. "
                    "Add product_name/raw_data columns or run the setup SQL."
                )

            if replace_all:
                cursor.execute(
                    sql.SQL("delete from {}.{}").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                    )
                )

            insert_statement = sql.SQL("insert into {}.{} ({}) values ({})").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(column) for column in insert_columns),
                sql.SQL(", ").join(sql.Placeholder() for _ in insert_columns),
            )
            values = [
                [
                    value_for_column(column, columns[column], product, Jsonb)
                    for column in insert_columns
                ]
                for product in products
            ]

            if values:
                cursor.executemany(insert_statement, values)

    return {
        "enabled": True,
        "method": "postgres",
        "dry_run": False,
        "schema": schema_name,
        "table": table_name,
        "replace_all": replace_all,
        "inserted_rows": len(products),
        "insert_columns": insert_columns,
    }


def write_products_to_database(products, trace=None):
    if supabase_rest_configured():
        return write_products_to_supabase_rest(products, trace=trace)

    return write_products_to_postgres(products, trace=trace)


def scrape_all_available_products(base_url, trace=None):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-scrape-before-login-{timestamp}.png")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"wawican-scrape-products-{timestamp}.png")
    scraped_at = datetime.now(timezone.utc)

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
            page.keyboard.press("Escape")
            wait_for_inventory_page(page, trace=trace)

            raw_products = []
            pages = []

            for page_loop_index in range(1, SCRAPE_MAX_PAGES + 1):
                pagination = get_pagination_state(page)
                page_number = pagination.get("current_page") or page_loop_index
                page_result = scrape_current_inventory_page(
                    page,
                    page_number=page_number,
                    trace=trace,
                )
                page_rows = page_result.get("rows", [])

                for row in page_rows:
                    row["page_number"] = page_number
                    raw_products.append(row)

                pages.append(
                    {
                        "page": page_number,
                        "label": pagination.get("label"),
                        "rows": len(page_rows),
                    }
                )

                pagination = get_pagination_state(page)
                current_page = pagination.get("current_page")
                total_pages = pagination.get("total_pages")
                has_next = pagination.get("has_next")

                if not has_next or (current_page and total_pages and current_page >= total_pages):
                    break

                click_next_inventory_page(page, pagination, trace=trace)
            else:
                raise RuntimeError(f"Stopped after WAWICAN_SCRAPE_MAX_PAGES={SCRAPE_MAX_PAGES}")

            products = normalize_products_for_db(raw_products, page.url, scraped_at)
            db_result = write_products_to_database(products, trace=trace)
            screenshot_result = capture_screenshot_now(
                page,
                screenshot_path,
                trace=trace,
                trigger="scrape_finished",
            )

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "inventory_ready": inventory_ready,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                "filter": filter_result,
                "pages_scraped": pages,
                "products_scraped": len(products),
                "database": db_result,
                **screenshot_result,
                **screenshot_response_links(base_url, screenshot_path),
            }
        finally:
            if context:
                context.close()
            browser.close()


def open_availability_filter_menu(page, trace=None):
    wait_for_inventory_page(page, trace=trace)
    trace_step(trace, "click_availability_filter")
    click_result = click_availability_filter_button(page)
    menu_result = wait_for_availability_filter_menu(page, trace=trace)

    return {
        "filter_button": click_result,
        "filter_menu": menu_result,
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


def login_and_open_availability_filter(base_url, trace=None):
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"wawican-open-filter-before-login-{timestamp}.png")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"wawican-open-availability-filter-{timestamp}.png")

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
            filter_result = open_availability_filter_menu(page, trace=trace)
            screenshot_result = capture_screenshot_now(
                page,
                screenshot_path,
                trace=trace,
                trigger="availability_filter_menu_visible",
            )

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
        "post_login_ready_timeout_ms": POST_LOGIN_READY_TIMEOUT_MS,
        "scrape_max_pages": SCRAPE_MAX_PAGES,
        "supabase_rest_configured": supabase_rest_configured(),
        "database_url_configured": supabase_rest_configured() or bool(
            (
                os.environ.get("SUPABASE_DB_URL")
                or os.environ.get("DATABASE_URL")
                or os.environ.get("POSTGRES_URL")
                or ""
            ).strip()
        ),
        "products_schema": products_schema(),
        "products_table": products_table(),
        "products_replace_all": bool_env("WAWICAN_PRODUCTS_REPLACE_ALL", True),
        "supabase_timeout_seconds": SUPABASE_TIMEOUT_SECONDS,
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


@app.post("/jobs/open-availability-filter")
def open_availability_filter_job(request: Request):
    try:
        return login_and_open_availability_filter(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/open-availability-filter/start")
def open_availability_filter_job_start(request: Request):
    return start_background_job(
        request,
        "open-availability-filter",
        login_and_open_availability_filter,
    )


@app.post("/jobs/open-availability-filter/run")
def open_availability_filter_job_sync(request: Request):
    try:
        return login_and_open_availability_filter(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/scrape-products")
def scrape_products_job(request: Request):
    try:
        return scrape_all_available_products(base_url_from_request(request))
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/scrape-products/start")
def scrape_products_job_start(request: Request):
    return start_background_job(request, "scrape-products", scrape_all_available_products)


@app.post("/jobs/scrape-products/run")
def scrape_products_job_sync(request: Request):
    try:
        return scrape_all_available_products(base_url_from_request(request))
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
