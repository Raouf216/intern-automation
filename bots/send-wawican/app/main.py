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
from pydantic import BaseModel


STARTED_AT = time.time()
SERVICE_NAME = "send-wawican"
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")
SESSION_STATE_PATH = (
    os.environ.get("SEND_WAWICAN_SESSION_STATE_PATH")
    or os.environ.get("WAWICAN_SESSION_STATE_PATH")
    or os.path.join(ARTIFACTS_DIR, "wawican-send-storage-state.json")
)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
WAWICAN_USER_AGENT = (
    os.environ.get("WAWICAN_USER_AGENT", DEFAULT_USER_AGENT).strip()
    or DEFAULT_USER_AGENT
)

CURRENT_RUN_SCREENSHOTS = ContextVar("CURRENT_RUN_SCREENSHOTS", default=None)


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


NAVIGATION_TIMEOUT_MS = int_env("SEND_WAWICAN_NAVIGATION_TIMEOUT_MS", 30_000)
INVENTORY_READY_TIMEOUT_MS = int_env("SEND_WAWICAN_INVENTORY_READY_TIMEOUT_MS", 60_000)
POST_LOGIN_READY_TIMEOUT_MS = int_env("SEND_WAWICAN_POST_LOGIN_READY_TIMEOUT_MS", 15_000)
WAIT_FOR_NETWORKIDLE = bool_env("SEND_WAWICAN_WAIT_FOR_NETWORKIDLE", False)
FILTER_TIMEOUT_MS = int_env("SEND_WAWICAN_FILTER_TIMEOUT_MS", 60_000)

os.makedirs(ARTIFACTS_DIR, exist_ok=True)
app = FastAPI(title=SERVICE_NAME)
app.mount("/artifacts", StaticFiles(directory=ARTIFACTS_DIR), name="artifacts")

CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "SEND_WAWICAN_CORS_ORIGINS",
        "http://localhost:8076,http://127.0.0.1:8076",
    ).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class AvailabilityFilterPayload(BaseModel):
    availability: str


def log_event(event, **fields):
    print(
        json.dumps(
            {
                "event": event,
                "service": SERVICE_NAME,
                **fields,
            },
            ensure_ascii=False,
            default=str,
        ),
        flush=True,
    )


def required_env(name):
    value = (os.environ.get(name) or "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def inventory_url():
    configured = (os.environ.get("WAWICAN_INVENTORY_URL") or "").strip()
    if configured:
        return configured
    return required_env("WAWICAN_LOGIN_URL")


def login_url():
    return (os.environ.get("WAWICAN_LOGIN_URL") or "").strip() or inventory_url()


def ready_text():
    return (os.environ.get("WAWICAN_READY_TEXT") or "Verfügbarkeit").strip() or "Verfügbarkeit"


def public_base_url(request):
    configured_url = os.environ.get("SEND_WAWICAN_PUBLIC_BASE_URL", "").strip().rstrip("/")
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
    return f"{base_url}/artifacts/{os.path.basename(path)}"


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


def browser_context_options():
    return {
        "user_agent": WAWICAN_USER_AGENT,
        "viewport": {"width": 1365, "height": 900},
        "locale": "de-DE",
        "timezone_id": os.environ.get("TZ", "Europe/Berlin"),
    }


def page_text_excerpt(page, limit=500):
    try:
        text = page.locator("body").inner_text(timeout=2_000)
    except Exception:
        return ""

    text = re.sub(r"\s+", " ", text or "").strip()
    return text[:limit]


def page_diagnostics(page):
    try:
        title = page.title()
    except Exception:
        title = ""

    return {
        "url": page.url,
        "title": title,
        "body_excerpt": page_text_excerpt(page),
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
    path = os.path.join(ARTIFACTS_DIR, f"send-wawican-{label}-{timestamp}.png")
    screenshot_path, _ = capture_optional_screenshot(page, path, label)
    return screenshot_path


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


def goto_page(page, url):
    log_event("goto_start", url=url)
    page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
    page.wait_for_load_state("domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
    wait_for_load_states(page)
    log_event("goto_domcontentloaded", url=page.url)


def wait_for_inventory_page(page, timeout=INVENTORY_READY_TIMEOUT_MS):
    target_text = ready_text()
    log_event("inventory_wait_start", timeout_ms=timeout, ready_text=target_text, url=page.url)
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
        arg=target_text,
        timeout=timeout,
    )
    wait_for_next_render_frame(page)
    log_event("inventory_ready_visible", diagnostics=page_diagnostics(page))
    return True


def click_availability_filter_button(page):
    log_event("availability_filter_click_start", url=page.url)
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

    log_event("availability_filter_click_done", result=result)
    return result


def wait_for_availability_filter_menu(page):
    log_event("availability_filter_menu_wait_start", timeout_ms=FILTER_TIMEOUT_MS)
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
    log_event("availability_filter_menu_visible", diagnostics=page_diagnostics(page))
    return {"ok": True, "menu_item": "verfügbar"}


def set_availability_checkbox_state(page, label, should_be_checked):
    page.locator(f'[role="checkbox"][aria-label="{label}"]').first.wait_for(
        state="visible",
        timeout=FILTER_TIMEOUT_MS,
    )

    log_event("availability_checkbox_set_start", label=label, target_checked=should_be_checked)
    result = page.evaluate(
        """
        ({ label, shouldBeChecked }) => {
          const checkbox = document.querySelector(`[role="checkbox"][aria-label="${label}"]`);

          if (!checkbox) {
            return { ok: false, error: 'availability_checkbox_not_found', label };
          }

          const isChecked = (element) => {
            const nativeInput = element.querySelector('input[type="checkbox"]');
            const ariaChecked = element.getAttribute('aria-checked');
            const className = String(element.className || '');
            return Boolean(
              (nativeInput && nativeInput.checked) ||
              ariaChecked === 'true' ||
              className.includes('--truthy') ||
              element.querySelector('.q-checkbox__inner--truthy')
            );
          };

          const before = isChecked(checkbox);

          if (before !== shouldBeChecked) {
            const box =
              checkbox.querySelector('.q-checkbox__inner') ||
              checkbox.querySelector('.q-checkbox__bg') ||
              checkbox;
            box.click();
          }

          return {
            ok: true,
            label,
            was_checked: before,
            target_checked: shouldBeChecked,
          };
        }
        """,
        arg={"label": label, "shouldBeChecked": should_be_checked},
    )

    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "availability_checkbox_failed")

    wait_for_next_render_frame(page)

    verify_result = page.evaluate(
        """
        ({ label, shouldBeChecked }) => {
          const checkbox = document.querySelector(`[role="checkbox"][aria-label="${label}"]`);

          if (!checkbox) {
            return { ok: false, error: 'availability_checkbox_not_found_after_click', label };
          }

          const nativeInput = checkbox.querySelector('input[type="checkbox"]');
          const ariaChecked = checkbox.getAttribute('aria-checked');
          const className = String(checkbox.className || '');
          const checked = Boolean(
            (nativeInput && nativeInput.checked) ||
            ariaChecked === 'true' ||
            className.includes('--truthy') ||
            checkbox.querySelector('.q-checkbox__inner--truthy')
          );

          return {
            ok: checked === shouldBeChecked,
            label,
            checked,
            target_checked: shouldBeChecked,
            error: checked === shouldBeChecked ? null : 'availability_checkbox_wrong_state',
          };
        }
        """,
        arg={"label": label, "shouldBeChecked": should_be_checked},
    )

    if not verify_result.get("ok"):
        raise RuntimeError(
            f"{verify_result.get('error') or 'availability_checkbox_verify_failed'}: "
            f"{label} checked={verify_result.get('checked')} expected={should_be_checked}"
        )

    final_result = {**result, "checked": verify_result.get("checked")}
    log_event("availability_checkbox_set_done", result=final_result)
    return final_result


def normalize_availability_mode(value):
    mode = (value or "").strip().lower()
    mode = mode.replace("ü", "u")
    mode = re.sub(r"\s+", "", mode)

    if mode in {"ver", "available", "verfugbar", "verfuegbar"}:
        return "ver"
    if mode in {"unver", "unavailable", "nichtverfugbar", "nichtverfuegbar", "nicht-verfugbar"}:
        return "unver"

    raise RuntimeError("availability must be 'ver' or 'unver'.")


def apply_availability_filter(page, availability):
    mode = normalize_availability_mode(availability)
    wait_for_inventory_page(page)
    click_result = click_availability_filter_button(page)
    menu_result = wait_for_availability_filter_menu(page)

    if mode == "ver":
        available_checkbox_result = set_availability_checkbox_state(page, "verfügbar", True)
        unavailable_checkbox_result = set_availability_checkbox_state(page, "nicht verfügbar", False)
        selected_label = "verfügbar"
    else:
        unavailable_checkbox_result = set_availability_checkbox_state(page, "nicht verfügbar", True)
        available_checkbox_result = set_availability_checkbox_state(page, "verfügbar", False)
        selected_label = "nicht verfügbar"

    page.keyboard.press("Escape")
    wait_for_inventory_page(page)

    return {
        "mode": mode,
        "selected_label": selected_label,
        "filter_button": click_result,
        "filter_menu": menu_result,
        "available_checkbox": available_checkbox_result,
        "unavailable_checkbox": unavailable_checkbox_result,
    }


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
            log_event("login_extra_click_start", text=extra_click_text)
            page.get_by_text(extra_click_text, exact=True).click(timeout=5_000)
            log_event("login_extra_click_done", text=extra_click_text)
        except PlaywrightTimeoutError:
            log_event("login_extra_click_not_visible", text=extra_click_text)


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
        log_event("login_submit_button_click_start")
        button.click(timeout=10_000)
        log_event("login_submit_button_click_done")
    else:
        log_event("login_submit_enter_start")
        page.keyboard.press("Enter")
        log_event("login_submit_enter_done")


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

    log_event("session_state_saved", path=SESSION_STATE_PATH)


def wait_for_inventory_after_login_submit(page):
    try:
        log_event("inventory_after_submit_wait_start", timeout_ms=POST_LOGIN_READY_TIMEOUT_MS)
        wait_for_inventory_page(page, timeout=POST_LOGIN_READY_TIMEOUT_MS)
        return
    except Exception as exc:
        log_event("inventory_after_submit_not_ready", error=f"{type(exc).__name__}: {exc}", diagnostics=page_diagnostics(page))

    log_event("goto_inventory_after_login", url=inventory_url())
    goto_page(page, inventory_url())
    wait_for_inventory_page(page)


def open_saved_session(browser, target_url):
    if not os.path.exists(SESSION_STATE_PATH):
        log_event("saved_session_missing", path=SESSION_STATE_PATH)
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
        wait_for_inventory_page(page)
        log_event("saved_session_valid")
        return context, page, True, True
    except Exception as exc:
        failure_screenshot_path = capture_failure_screenshot(page, "saved-session-invalid")
        context.close()
        log_event(
            "saved_session_invalid",
            error=f"{type(exc).__name__}: {exc}",
            failure_screenshot_path=failure_screenshot_path,
            diagnostics=page_diagnostics(page),
        )
        return None


def open_fresh_session(browser, target_url, before_login_path=None):
    log_event("fresh_session_try")
    context = browser.new_context(**browser_context_options())
    page = context.new_page()

    try:
        goto_page(page, login_url())
        log_event("login_page_loaded", diagnostics=page_diagnostics(page))

        if not login_form_is_visible(page):
            if before_login_path:
                capture_optional_screenshot(page, before_login_path, "login-form-not-detected")

            try:
                log_event("try_inventory_without_login_form", url=target_url)
                goto_page(page, target_url)
                wait_for_inventory_page(page)
                save_session_state(context)
                return context, page, False, True
            except Exception as exc:
                log_event("inventory_without_login_form_failed", error=f"{type(exc).__name__}: {exc}")
                goto_page(page, login_url())

        fill_login_form(page)

        if before_login_path:
            capture_optional_screenshot(page, before_login_path, "before-login")

        submit_login_form(page)
        wait_for_inventory_after_login_submit(page)
        save_session_state(context)
        return context, page, False, True
    except Exception as exc:
        failure_screenshot_path = capture_failure_screenshot(page, "fresh-session-failed")
        log_event(
            "fresh_session_failed",
            error=f"{type(exc).__name__}: {exc}",
            failure_screenshot_path=failure_screenshot_path,
            diagnostics=page_diagnostics(page),
        )
        context.close()
        raise


def open_authenticated_inventory_page(browser, before_login_path=None):
    target_url = inventory_url()
    saved = open_saved_session(browser, target_url)
    if saved is not None:
        return saved
    return open_fresh_session(browser, target_url, before_login_path=before_login_path)


def login_check(base_url):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"send-wawican-before-login-{timestamp}.png")
    ready_screenshot_path = os.path.join(ARTIFACTS_DIR, f"send-wawican-inventory-ready-{timestamp}.png")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("WAWICAN_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, inventory_ready = open_authenticated_inventory_page(
                browser,
                before_login_path=before_login_path,
            )
            ready_screenshot_path, ready_screenshot_error = capture_optional_screenshot(
                page,
                ready_screenshot_path,
                "inventory-ready",
            )
            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "inventory_ready": inventory_ready,
                "session_state_path": SESSION_STATE_PATH,
                "session_state_exists": os.path.exists(SESSION_STATE_PATH),
                "before_login_path": None if reused_session else before_login_path,
                "before_login_url": None if reused_session else artifact_url(before_login_path, base_url),
                "ready_screenshot_path": ready_screenshot_path,
                "ready_screenshot_url": artifact_url(ready_screenshot_path, base_url),
                "ready_screenshot_error": ready_screenshot_error,
                "screenshots": screenshot_entries(base_url),
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_and_filter_availability(availability, base_url):
    mode = normalize_availability_mode(availability)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"send-wawican-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"send-wawican-after-login-{timestamp}.png")
    after_filter_path = os.path.join(ARTIFACTS_DIR, f"send-wawican-filter-{mode}-{timestamp}.png")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("WAWICAN_HEADLESS", True))
        context = None

        try:
            context, page, reused_session, inventory_ready = open_authenticated_inventory_page(
                browser,
                before_login_path=before_login_path,
            )
            after_login_path, after_login_error = capture_optional_screenshot(
                page,
                after_login_path,
                "after-login-inventory-ready",
            )
            filter_result = apply_availability_filter(page, mode)
            after_filter_path, after_filter_error = capture_optional_screenshot(
                page,
                after_filter_path,
                f"after-filter-{mode}",
            )

            return {
                "ok": True,
                "availability": mode,
                "selected_label": filter_result["selected_label"],
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "inventory_ready": inventory_ready,
                "session_state_path": SESSION_STATE_PATH,
                "session_state_exists": os.path.exists(SESSION_STATE_PATH),
                "before_login_path": None if reused_session else before_login_path,
                "before_login_url": None if reused_session else artifact_url(before_login_path, base_url),
                "after_login_screenshot_path": after_login_path,
                "after_login_screenshot_url": artifact_url(after_login_path, base_url),
                "after_login_screenshot_error": after_login_error,
                "after_filter_screenshot_path": after_filter_path,
                "after_filter_screenshot_url": artifact_url(after_filter_path, base_url),
                "after_filter_screenshot_error": after_filter_error,
                "filter_result": filter_result,
                "screenshots": screenshot_entries(base_url),
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
        "artifacts_dir": ARTIFACTS_DIR,
        "artifacts_dir_exists": os.path.isdir(ARTIFACTS_DIR),
        "inventory_ready_timeout_ms": INVENTORY_READY_TIMEOUT_MS,
        "post_login_ready_timeout_ms": POST_LOGIN_READY_TIMEOUT_MS,
        "filter_timeout_ms": FILTER_TIMEOUT_MS,
        "wait_for_networkidle": WAIT_FOR_NETWORKIDLE,
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


@app.post("/jobs/open-inventory-page")
def job_open_inventory_page(request: Request):
    return job_login_check(request)


@app.post("/jobs/login")
def job_login(request: Request):
    return job_login_check(request)


@app.post("/jobs/filter-availability")
def job_filter_availability(payload: AvailabilityFilterPayload, request: Request):
    token = CURRENT_RUN_SCREENSHOTS.set([])
    base_url = public_base_url(request)
    try:
        return login_and_filter_availability(payload.availability, base_url)
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
