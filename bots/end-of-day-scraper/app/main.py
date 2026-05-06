import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
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
EOD_MAX_PAGES = int(os.environ.get("EOD_MAX_PAGES", "100"))
SUPABASE_SCHEMA = os.environ.get("SUPABASE_SCHEMA", "private")
SUPABASE_EOD_ORDERS_TABLE = os.environ.get("SUPABASE_EOD_ORDERS_TABLE", "doktorabc_eod_bot_orders")


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


def supabase_table_url():
    supabase_url = required_env("SUPABASE_URL").rstrip("/")
    table_name = quote(SUPABASE_EOD_ORDERS_TABLE, safe="")

    return f"{supabase_url}/rest/v1/{table_name}"


def supabase_headers():
    service_role_key = required_env("SUPABASE_SERVICE_ROLE_KEY")

    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Accept-Profile": SUPABASE_SCHEMA,
        "Content-Profile": SUPABASE_SCHEMA,
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


def upsert_supabase_eod_orders(orders):
    if not orders:
        return {
            "sent_to_supabase": 0,
            "supabase_status_code": None,
        }

    response = httpx.post(
        f"{supabase_table_url()}?on_conflict=order_reference",
        headers={
            **supabase_headers(),
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=orders,
        timeout=90,
    )

    if response.status_code >= 400:
        raise RuntimeError(f"Supabase upsert failed: {response_preview(response)}")

    return {
        "sent_to_supabase": len(orders),
        "supabase_status_code": response.status_code,
    }


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


def parse_date_to_iso(value):
    if not value:
        return None

    clean_value = str(value).strip()

    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(clean_value, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def clean_text(value):
    if value is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def join_pipe(values):
    cleaned = [clean_text(value) for value in values if clean_text(value)]
    return " | ".join(cleaned)


def normalize_price(value):
    cleaned = clean_text(value)

    if not cleaned:
        return None

    cleaned = cleaned.replace(",", ".")
    match = re.search(r"\d+(?:\.\d+)?", cleaned)

    return match.group(0) if match else cleaned


def normalize_scraped_order(order):
    products = order.get("product_details") or []
    product_names = [product.get("product") for product in products]
    pzns = [join_pipe(product.get("pzns") or []) for product in products]
    prices = [normalize_price(product.get("price")) for product in products]
    quantities = [product.get("quantity") for product in products]

    return {
        "order_reference": clean_text(order.get("order_reference")),
        "prescription_date": parse_date_to_iso(order.get("prescription_date")),
        "tracking_id": clean_text(order.get("tracking_id")),
        "products": join_pipe(product_names),
        "pzns": join_pipe(pzns),
        "prices": join_pipe(prices),
        "quantities": join_pipe(quantities),
        "patient_name": clean_text(order.get("patient_name")),
        "patient_birth_date": parse_date_to_iso(order.get("patient_birth_date")),
        "address": clean_text(order.get("address")),
        "gender": clean_text(order.get("gender")),
    }


def validate_orders(rows, raw_orders):
    invalid = []
    warnings = []

    for index, row in enumerate(rows):
        missing_required = [
            field
            for field in ("order_reference", "prescription_date", "products")
            if not row.get(field)
        ]

        if missing_required:
            invalid.append(
                {
                    "index": index,
                    "order_reference": row.get("order_reference"),
                    "missing": missing_required,
                    "raw": raw_orders[index],
                }
            )
            continue

        missing_product_parts = [
            field
            for field in ("pzns", "prices", "quantities")
            if not row.get(field)
        ]

        if missing_product_parts:
            warnings.append(
                {
                    "index": index,
                    "order_reference": row.get("order_reference"),
                    "missing": missing_product_parts,
                }
            )

    return invalid, warnings


def wait_for_order_cards(page, timeout_ms=60_000):
    page.locator('button[id$="-mark-order"]').first.wait_for(state="visible", timeout=timeout_ms)
    return wait_for_render_stability(page, timeout_ms=timeout_ms, stable_ms=1_500)


def get_pagination_state(page):
    return page.evaluate(
        """
        () => {
          const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
          const current = document.querySelector('nav[aria-label="pagination"] a[aria-current="page"]');
          const next = document.querySelector('nav[aria-label="pagination"] a[aria-label="Go to next page"]');
          const orderRefs = Array.from(document.querySelectorAll('button[id$="-mark-order"]'))
            .map((button) => normalize(button.id).replace(/-mark-order$/, ""))
            .filter(Boolean);

          let hasEnabledNext = false;
          if (next) {
            const nextStyle = window.getComputedStyle(next);
            const nextParent = next.closest("li");
            const disabledByAttribute =
              next.getAttribute("aria-disabled") === "true" ||
              next.getAttribute("data-disabled") === "true" ||
              next.hasAttribute("disabled");
            const disabledByClass =
              /disabled|pointer-events-none|opacity-50/.test(next.className || "") ||
              /disabled|pointer-events-none|opacity-50/.test(nextParent?.className || "");
            hasEnabledNext =
              !disabledByAttribute &&
              !disabledByClass &&
              nextStyle.pointerEvents !== "none" &&
              nextStyle.display !== "none" &&
              nextStyle.visibility !== "hidden";
          }

          return {
            current_page: Number(normalize(current?.innerText)) || null,
            order_refs: orderRefs,
            first_order_reference: orderRefs[0] || null,
            order_count: orderRefs.length,
            has_next: hasEnabledNext,
          };
        }
        """
    )


def select_100_rows(page):
    debug = {"clicked": False, "before": get_pagination_state(page)}
    rows_100 = page.locator("#rows-per-page-container li").filter(has_text=re.compile(r"^\s*100\s*$")).first
    rows_100.wait_for(state="visible", timeout=30_000)
    rows_100.click(timeout=10_000)
    page.wait_for_timeout(1_000)
    debug["wait_result"] = wait_for_order_cards(page)
    debug["after"] = get_pagination_state(page)
    debug["clicked"] = True

    return debug


def click_next_page(page, before_state):
    next_link = page.locator('nav[aria-label="pagination"] a[aria-label="Go to next page"]').first
    next_link.wait_for(state="visible", timeout=10_000)
    next_link.click(timeout=10_000)

    deadline = time.monotonic() + 30
    last_state = before_state

    while time.monotonic() < deadline:
        page.wait_for_timeout(500)
        state = get_pagination_state(page)
        last_state = state

        if (
            state.get("current_page") != before_state.get("current_page")
            or state.get("first_order_reference") != before_state.get("first_order_reference")
        ):
            wait_for_order_cards(page)
            return True, state

    return False, last_state


SCRAPE_EOD_ORDERS_JS = """
async () => {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
  const lines = (value) => (value || "").split(/\\n+/).map(normalize).filter(Boolean);
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
  const unique = (items) => Array.from(new Set(items.filter(Boolean)));
  const findOrderRoot = (marker) => {
    let node = marker;
    for (let depth = 0; depth < 14 && node; depth += 1) {
      const text = node.innerText || "";
      if (text.includes("Products") && text.includes("Documents")) {
        return node;
      }
      node = node.parentElement;
    }
    return null;
  };
  const valueAfterLabel = (root, label) => {
    const candidates = Array.from(root.querySelectorAll("span,div"));
    const labelNode = candidates.find((element) => normalize(element.innerText) === label);
    if (!labelNode) return "";

    let container = labelNode.parentElement;
    for (let depth = 0; depth < 4 && container; depth += 1) {
      const foundLines = lines(container.innerText).filter((line) => line !== label);
      if (foundLines.length) return foundLines.join(", ");
      container = container.parentElement;
    }

    return "";
  };
  const smallestProductCards = (root) => {
    const candidates = Array.from(root.querySelectorAll("div")).filter((element) => {
      const text = normalize(element.innerText);
      return /\\b\\d+\\s*PZN\\b/i.test(text) && /Price:\\s*[\\d.,]+\\s*\\u20ac/i.test(text) && element.querySelector("h3");
    });

    return candidates.filter((candidate) => {
      return !candidates.some((other) => other !== candidate && candidate.contains(other));
    });
  };
  const getBadges = (card) => {
    return Array.from(card.querySelectorAll("div,span"))
      .map((element) => normalize(element.innerText))
      .filter(Boolean);
  };
  const readPznsAfterClick = async (trigger) => {
    if (!trigger) return [];
    trigger.scrollIntoView({ block: "center", inline: "center" });
    trigger.click();
    await delay(350);

    const popupSelectors = '[role="dialog"],[role="tooltip"],[data-radix-popper-content-wrapper]';
    const popupText = Array.from(document.querySelectorAll(popupSelectors))
      .filter(visible)
      .map((element) => element.innerText || "")
      .join("\\n");
    let codes = unique(popupText.match(/\\b\\d{7,8}\\b/g) || []);

    if (!codes.length) {
      codes = unique((document.body.innerText || "").match(/\\b\\d{7,8}\\b/g) || []);
    }

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    document.body.click();
    await delay(100);

    return codes;
  };
  const productDetails = async (root) => {
    const cards = smallestProductCards(root);
    const products = [];

    for (const card of cards) {
      const badges = getBadges(card);
      const title = normalize(card.querySelector("h3")?.innerText);
      const quantity = badges.find((badge) => /^\\d+(?:[,.]\\d+)?\\s*gramm$/i.test(badge)) || "";
      const priceBadge = badges.find((badge) => /^Price:\\s*[\\d.,]+\\s*\\u20ac$/i.test(badge)) || "";
      const priceMatch = priceBadge.match(/[\\d.,]+/);
      const pznTrigger = Array.from(card.querySelectorAll('[aria-haspopup],button,[type="button"],div'))
        .find((element) => /\\b\\d+\\s*PZN\\b/i.test(normalize(element.innerText)));
      const pzns = await readPznsAfterClick(pznTrigger);

      products.push({
        product: title,
        quantity,
        price: priceMatch ? priceMatch[0].replace(",", ".") : "",
        pzns,
      });
    }

    return products;
  };

  const markers = Array.from(document.querySelectorAll('button[id$="-mark-order"]'));
  const roots = unique(markers.map(findOrderRoot));
  const orders = [];

  for (const root of roots) {
    if (!root) continue;

    const marker = root.querySelector('button[id$="-mark-order"]');
    const orderReference = normalize(marker?.id).replace(/-mark-order$/, "");
    const rootText = root.innerText || "";
    const allDates = rootText.match(/\\b\\d{2}\\/\\d{2}\\/\\d{4}\\b/g) || [];
    const birthMatch = rootText.match(/\\b(\\d{2}\\/\\d{2}\\/\\d{4})\\s*\\(\\d+\\s+years?\\s+old\\)/i);
    const trackingLink = root.querySelector('a[href*="piececode="]');

    orders.push({
      order_reference: orderReference,
      prescription_date: allDates[0] || "",
      patient_name: normalize(root.querySelector("h2")?.innerText),
      patient_birth_date: birthMatch ? birthMatch[1] : "",
      address: valueAfterLabel(root, "Address"),
      gender: valueAfterLabel(root, "Gender"),
      tracking_id: normalize(trackingLink?.innerText || valueAfterLabel(root, "Tracking ID")),
      product_details: await productDetails(root),
    });
  }

  return orders;
}
"""


def scrape_orders_on_current_page(page):
    return page.evaluate(SCRAPE_EOD_ORDERS_JS)


def scrape_all_eod_orders(page):
    steps = []
    rows_debug = select_100_rows(page)
    steps.append({"name": "select_100_rows", "ok": True, **rows_debug})

    all_orders = []
    pages = []
    duplicate_order_references = []
    seen_order_references = set()

    for page_index in range(1, EOD_MAX_PAGES + 1):
        state_before = get_pagination_state(page)
        raw_orders = scrape_orders_on_current_page(page)
        page_order_refs = [order.get("order_reference") for order in raw_orders if order.get("order_reference")]

        for order in raw_orders:
            order_reference = order.get("order_reference")
            if order_reference and order_reference in seen_order_references:
                duplicate_order_references.append(order_reference)
                continue

            if order_reference:
                seen_order_references.add(order_reference)

            all_orders.append(order)

        pages.append(
            {
                "page_index": page_index,
                "pagination_page": state_before.get("current_page"),
                "orders_found": len(raw_orders),
                "first_order_reference": page_order_refs[0] if page_order_refs else None,
                "last_order_reference": page_order_refs[-1] if page_order_refs else None,
                "has_next_before_click": state_before.get("has_next"),
            }
        )

        if not state_before.get("has_next"):
            steps.append({"name": "pagination_finished", "ok": True, "reason": "next_disabled_or_missing"})
            break

        changed, state_after = click_next_page(page, state_before)
        steps.append(
            {
                "name": "click_next_page",
                "ok": changed,
                "from_page": state_before.get("current_page"),
                "to_page": state_after.get("current_page"),
                "from_first_order": state_before.get("first_order_reference"),
                "to_first_order": state_after.get("first_order_reference"),
            }
        )

        if not changed:
            steps.append({"name": "pagination_finished", "ok": True, "reason": "next_click_did_not_change_page"})
            break
    else:
        steps.append({"name": "pagination_finished", "ok": False, "reason": "max_pages_reached"})

    return {
        "orders": all_orders,
        "pages": pages,
        "steps": steps,
        "duplicate_order_references": duplicate_order_references,
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


def sync_end_of_day_orders():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-after-sync-{timestamp}.png")
    failure_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-sync-failure-{timestamp}.png")
    steps = []

    print("trying to sync DoktorABC End-of-Day orders ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None
        page = None

        try:
            steps.append({"name": "open_authenticated_end_of_day_page", "ok": None})
            context, page, reused_session, wait_result = open_authenticated_end_of_day_page(
                browser,
                before_login_path=before_login_path,
            )
            steps[-1] = {
                "name": "open_authenticated_end_of_day_page",
                "ok": True,
                "reused_session": reused_session,
                "current_url": page.url,
                "wait_result": wait_result,
            }

            steps.append({"name": "scrape_all_pages", "ok": None})
            scrape_result = scrape_all_eod_orders(page)
            raw_orders = scrape_result["orders"]
            rows = [normalize_scraped_order(order) for order in raw_orders]
            invalid_orders, warnings = validate_orders(rows, raw_orders)
            steps[-1] = {
                "name": "scrape_all_pages",
                "ok": True,
                "scraped": len(raw_orders),
                "pages": scrape_result["pages"],
                "scrape_steps": scrape_result["steps"],
                "duplicate_order_references": scrape_result["duplicate_order_references"],
                "warnings": warnings,
                "invalid_count": len(invalid_orders),
            }

            page.screenshot(path=after_login_path, full_page=True)

            if invalid_orders:
                return JSONResponse(
                    status_code=422,
                    content={
                        "ok": False,
                        "failed_step": "validate_scraped_orders",
                        "error": "Some scraped orders are missing required fields. Supabase was not changed.",
                        "current_url": page.url,
                        "reused_session": reused_session,
                        "session_state_path": SESSION_STATE_PATH,
                        "supabase_schema": SUPABASE_SCHEMA,
                        "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                        "scraped": len(raw_orders),
                        "invalid_count": len(invalid_orders),
                        "invalid_examples": invalid_orders[:10],
                        "warnings": warnings[:20],
                        "steps": steps,
                        "screenshot_path": after_login_path,
                    },
                )

            if not rows:
                return JSONResponse(
                    status_code=422,
                    content={
                        "ok": False,
                        "failed_step": "scrape_all_pages",
                        "error": "No End-of-Day orders were found. Supabase was not changed.",
                        "current_url": page.url,
                        "reused_session": reused_session,
                        "session_state_path": SESSION_STATE_PATH,
                        "steps": steps,
                        "screenshot_path": after_login_path,
                    },
                )

            steps.append({"name": "upsert_supabase", "ok": None, "rows": len(rows)})
            supabase_result = upsert_supabase_eod_orders(rows)
            steps[-1] = {"name": "upsert_supabase", "ok": True, **supabase_result}

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "supabase_schema": SUPABASE_SCHEMA,
                "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                "scraped": len(raw_orders),
                "saved": len(rows),
                "warnings_count": len(warnings),
                "warnings": warnings[:20],
                "duplicate_order_references": scrape_result["duplicate_order_references"],
                "pages": scrape_result["pages"],
                "sample_orders": rows[:3],
                "steps": steps,
                "screenshot_path": after_login_path,
                **supabase_result,
            }
        except Exception as exc:
            failure_screenshot_path = None
            if page:
                try:
                    page.screenshot(path=failure_path, full_page=True)
                    failure_screenshot_path = failure_path
                except Exception:
                    failure_screenshot_path = None

            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "failed_step": next((step["name"] for step in reversed(steps) if step.get("ok") is None), "unknown"),
                    "error": f"{type(exc).__name__}: {exc}",
                    "current_url": page.url if page else None,
                    "session_state_path": SESSION_STATE_PATH,
                    "supabase_schema": SUPABASE_SCHEMA,
                    "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                    "steps": steps,
                    "screenshot_path": failure_screenshot_path,
                },
            )
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


@app.post("/jobs/end-of-day/orders/sync")
def end_of_day_orders_sync():
    return sync_end_of_day_orders()


@app.post("/jobs/end-of-day/orders")
def end_of_day_orders():
    return sync_end_of_day_orders()


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})
