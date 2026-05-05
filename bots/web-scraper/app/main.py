import os
import re
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")
SESSION_STATE_PATH = os.environ.get(
    "DOKTORABC_SESSION_STATE_PATH",
    os.path.join(ARTIFACTS_DIR, "doktorabc-storage-state.json"),
)


app = FastAPI(title="web-scraper")


MIN_PRODUCT_CELL_COUNT = 11
WEB_SCRAPER_CORS_ORIGINS = [
    origin.strip()
    for origin in os.environ.get(
        "WEB_SCRAPER_CORS_ORIGINS",
        "http://localhost:8040,http://127.0.0.1:8040,http://178.104.144.30:8040",
    ).split(",")
    if origin.strip()
]
SUPABASE_SCHEMA = os.environ.get("SUPABASE_SCHEMA", "private")
SUPABASE_PRODUCTS_TABLE = os.environ.get("SUPABASE_PRODUCTS_TABLE", "doktorabc_products")
PRODUCT_FIELDS = [
    "product_name",
    "pzn",
    "strain",
    "quantity",
    "price_per_g_incl_vat",
    "additional_cost",
    "site_price",
    "availability",
]
NUMERIC_PRODUCT_FIELDS = {
    "quantity",
    "price_per_g_incl_vat",
    "additional_cost",
    "site_price",
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=WEB_SCRAPER_CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


def bool_env(name, default=True):
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def doktorabc_products_url():
    return os.environ.get("DOKTORABC_PRODUCTS_URL") or os.environ["DOKTORABC_LOGIN_URL"]


def wait_for_products_page(page, timeout=60_000):
    page.get_by_text("Add/Decrease", exact=True).first.wait_for(timeout=timeout)


def clean_cell_text(value):
    return " ".join(value.replace("\xa0", " ").split())


def parse_decimal(value):
    text = clean_cell_text(value)

    if not text:
        return None

    match = re.search(r"-?\d+(?:[.,]\d+)*", text)

    if not match:
        return None

    number = match.group(0)

    if "," in number and "." in number:
        comma_position = number.rfind(",")
        dot_position = number.rfind(".")

        if comma_position > dot_position:
            number = number.replace(".", "").replace(",", ".")
        else:
            number = number.replace(",", "")
    else:
        number = number.replace(",", ".")

    try:
        return float(number)
    except ValueError:
        return None


def parse_availability(value):
    text = clean_cell_text(value).lower()

    if text in {"yes", "ja", "true", "available"}:
        return True

    if text in {"no", "nein", "false", "unavailable"}:
        return False

    return None


def required_env(name):
    value = os.environ.get(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


def decimal_equal(left, right):
    if left is None and right is None:
        return True

    if left is None or right is None:
        return False

    try:
        return Decimal(str(left)) == Decimal(str(right))
    except (InvalidOperation, ValueError):
        return False


def product_values_equal(existing_product, scraped_product):
    for field_name in PRODUCT_FIELDS:
        existing_value = existing_product.get(field_name)
        scraped_value = scraped_product.get(field_name)

        if field_name in NUMERIC_PRODUCT_FIELDS:
            if not decimal_equal(existing_value, scraped_value):
                return False
        elif field_name == "availability":
            if existing_value != scraped_value:
                return False
        else:
            if (existing_value or "") != (scraped_value or ""):
                return False

    return True


def product_changes(existing_product, scraped_product):
    changes = {}

    for field_name in PRODUCT_FIELDS:
        existing_value = existing_product.get(field_name)
        scraped_value = scraped_product.get(field_name)

        if field_name in NUMERIC_PRODUCT_FIELDS:
            if decimal_equal(existing_value, scraped_value):
                continue
        elif field_name == "availability":
            if existing_value == scraped_value:
                continue
        elif (existing_value or "") == (scraped_value or ""):
            continue

        changes[field_name] = {
            "old": existing_value,
            "new": scraped_value,
        }

    return changes


def supabase_table_url():
    supabase_url = required_env("SUPABASE_URL").rstrip("/")
    table_name = quote(SUPABASE_PRODUCTS_TABLE, safe="")

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


def fetch_existing_supabase_products():
    response = httpx.get(
        supabase_table_url(),
        headers=supabase_headers(),
        params={"select": ",".join(PRODUCT_FIELDS)},
        timeout=30,
    )
    response.raise_for_status()

    return {str(product["pzn"]): product for product in response.json()}


def upsert_supabase_products(products):
    if not products:
        return

    response = httpx.post(
        f"{supabase_table_url()}?on_conflict=pzn",
        headers={
            **supabase_headers(),
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=products,
        timeout=60,
    )
    response.raise_for_status()


def sync_products_to_supabase(products):
    existing_products = fetch_existing_supabase_products()
    products_to_upsert = []
    inserted = 0
    updated = 0
    unchanged = 0
    new_products = []
    changed_products = []

    for product in products:
        existing_product = existing_products.get(product["pzn"])

        if existing_product is None:
            inserted += 1
            new_products.append(product)
            products_to_upsert.append(product)
            continue

        changes = product_changes(existing_product, product)

        if not changes:
            unchanged += 1
            continue

        updated += 1
        changed_products.append(
            {
                "pzn": product["pzn"],
                "product_name": product["product_name"],
                "changes": changes,
                "before": existing_product,
                "after": product,
            }
        )
        products_to_upsert.append(product)

    upsert_supabase_products(products_to_upsert)

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "sent_to_supabase": len(products_to_upsert),
        "new_products": new_products,
        "changed_products": changed_products,
    }


def response_preview(response):
    return {
        "status_code": response.status_code,
        "body": response.text[:1000],
    }


def supabase_debug_check():
    debug_product = {
        "product_name": "__debug_product__",
        "pzn": "__debug_pzn__",
        "strain": "debug",
        "quantity": 0,
        "price_per_g_incl_vat": 0,
        "additional_cost": 0,
        "site_price": 0,
        "availability": False,
    }
    debug_pzn = quote(debug_product["pzn"], safe="")
    steps = []

    timeout = httpx.Timeout(20, connect=5)

    with httpx.Client(timeout=timeout) as client:
        read_response = client.get(
            supabase_table_url(),
            headers=supabase_headers(),
            params={"select": "pzn", "limit": "1"},
        )
        steps.append({"name": "read_table", **response_preview(read_response)})

        if read_response.status_code >= 400:
            return {"ok": False, "step": "read_table", "steps": steps}

        upsert_response = client.post(
            f"{supabase_table_url()}?on_conflict=pzn",
            headers={
                **supabase_headers(),
                "Prefer": "resolution=merge-duplicates,return=representation",
            },
            json=[debug_product],
        )
        steps.append({"name": "upsert_debug_row", **response_preview(upsert_response)})

        if upsert_response.status_code >= 400:
            return {"ok": False, "step": "upsert_debug_row", "steps": steps}

        delete_response = client.delete(
            supabase_table_url(),
            headers=supabase_headers(),
            params={"pzn": f"eq.{debug_pzn}"},
        )
        steps.append({"name": "delete_debug_row", **response_preview(delete_response)})

        if delete_response.status_code >= 400:
            return {"ok": False, "step": "delete_debug_row", "steps": steps}

    return {"ok": True, "steps": steps}


def open_saved_session(browser):
    if not os.path.exists(SESSION_STATE_PATH):
        return None

    print("trying saved DoktorABC session ...", flush=True)

    context = browser.new_context(
        storage_state=SESSION_STATE_PATH,
        viewport={"width": 1365, "height": 900},
    )
    page = context.new_page()

    try:
        page.goto(doktorabc_products_url(), wait_until="domcontentloaded")
        wait_for_products_page(page, timeout=15_000)

        return context, page, True
    except Exception as exc:
        context.close()
        print(f"saved DoktorABC session is expired or invalid: {type(exc).__name__}", flush=True)
        return None


def open_fresh_session(browser, before_login_path=None):
    print("trying fresh DoktorABC login ...", flush=True)

    context = browser.new_context(viewport={"width": 1365, "height": 900})
    page = context.new_page()

    page.goto(os.environ["DOKTORABC_LOGIN_URL"], wait_until="domcontentloaded")

    page.get_by_placeholder("Email").fill(os.environ["DOKTORABC_USERNAME"])
    page.get_by_placeholder("Password").fill(os.environ["DOKTORABC_PASSWORD"])

    page.get_by_text("Pharmacist", exact=True).click()

    if before_login_path:
        page.screenshot(path=before_login_path, full_page=True)

    page.get_by_role("button", name="Login").click()
    wait_for_products_page(page)

    session_state_dir = os.path.dirname(SESSION_STATE_PATH)

    if session_state_dir:
        os.makedirs(session_state_dir, exist_ok=True)

    context.storage_state(path=SESSION_STATE_PATH)

    return context, page, False


def open_authenticated_products_page(browser, before_login_path=None):
    saved_session = open_saved_session(browser)

    if saved_session is not None:
        return saved_session

    return open_fresh_session(browser, before_login_path=before_login_path)


def product_from_cells(cells):
    return {
        "product_name": cells[1],
        "pzn": cells[3],
        "strain": cells[4],
        "quantity": parse_decimal(cells[6]),
        "price_per_g_incl_vat": parse_decimal(cells[7]),
        "additional_cost": parse_decimal(cells[8]),
        "site_price": parse_decimal(cells[9]),
        "availability": parse_availability(cells[10]),
    }


def product_is_valid(product):
    return (
        bool(product["product_name"])
        and bool(product["pzn"])
        and product["quantity"] is not None
        and product["price_per_g_incl_vat"] is not None
        and product["additional_cost"] is not None
        and product["site_price"] is not None
        and product["availability"] is not None
    )


def products_from_visible_rows(page):
    rows = page.locator("tr")
    row_count = rows.count()
    products_by_pzn = {}
    invalid_products = []

    for row_index in range(row_count):
        row = rows.nth(row_index)
        cells_locator = row.locator("td")
        cell_count = cells_locator.count()

        if cell_count < MIN_PRODUCT_CELL_COUNT:
            continue

        cells = [
            clean_cell_text(cells_locator.nth(cell_index).inner_text())
            for cell_index in range(cell_count)
        ]
        product = product_from_cells(cells)

        if not product["pzn"]:
            continue

        if not product_is_valid(product):
            invalid_products.append(
                {
                    "row_index": row_index,
                    "raw_cells": cells,
                    "product": product,
                }
            )
            continue

        products_by_pzn[product["pzn"]] = product

    return products_by_pzn, invalid_products


def scroll_products_page(page):
    page.mouse.wheel(0, 1600)
    return page.evaluate(
        """
        () => {
          const scrollableElements = Array.from(document.querySelectorAll('*'))
            .filter((element) => {
              const style = window.getComputedStyle(element);
              const overflowY = style.overflowY;
              return (
                (overflowY === 'auto' || overflowY === 'scroll') &&
                element.scrollHeight > element.clientHeight + 20
              );
            });

          for (const element of scrollableElements) {
            element.scrollTop = Math.min(
              element.scrollTop + Math.floor(element.clientHeight * 0.9),
              element.scrollHeight
            );
          }

          window.scrollBy(0, Math.floor(window.innerHeight * 0.9));

          return scrollableElements.length;
        }
        """
    )


def scrape_all_product_rows(page, max_scroll_rounds=80, stable_round_limit=5):
    products_by_pzn = {}
    invalid_products = []
    stable_rounds = 0
    scroll_rounds = 0

    for scroll_round in range(max_scroll_rounds + 1):
        visible_products, visible_invalid_products = products_from_visible_rows(page)
        previous_count = len(products_by_pzn)

        products_by_pzn.update(visible_products)
        invalid_products.extend(visible_invalid_products)

        if len(products_by_pzn) == previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= stable_round_limit:
            break

        scroll_rounds = scroll_round + 1
        scroll_products_page(page)
        page.wait_for_timeout(700)

    products = sorted(products_by_pzn.values(), key=lambda product: product["pzn"])

    return {
        "products": products,
        "invalid_products": invalid_products,
        "scroll_rounds": scroll_rounds,
        "visible_tr_count": page.locator("tr").count(),
    }


def extract_first_visible_product_row(page):
    rows = page.locator("tr")
    row_count = rows.count()

    for row_index in range(row_count):
        row = rows.nth(row_index)
        cells_locator = row.locator("td")
        cell_count = cells_locator.count()

        if cell_count < MIN_PRODUCT_CELL_COUNT:
            continue

        cells = [
            clean_cell_text(cells_locator.nth(cell_index).inner_text())
            for cell_index in range(cell_count)
        ]

        if not cells[1] or not cells[3]:
            continue

        return {
            "row_index": row_index,
            "raw_cells": cells,
            "sample": product_from_cells(cells),
        }

    return None


def inspect_visible_product_rows(page, limit=20):
    rows = page.locator("tr")
    row_count = rows.count()
    inspected_rows = []
    td_count_summary = {}

    for row_index in range(row_count):
        row = rows.nth(row_index)
        cells_locator = row.locator("td")
        headers_locator = row.locator("th")
        cell_count = cells_locator.count()
        header_count = headers_locator.count()

        td_count_summary[str(cell_count)] = td_count_summary.get(str(cell_count), 0) + 1

        if len(inspected_rows) >= limit and cell_count == 0:
            continue

        cells = [
            clean_cell_text(cells_locator.nth(cell_index).inner_text())
            for cell_index in range(cell_count)
        ]
        headers = [
            clean_cell_text(headers_locator.nth(header_index).inner_text())
            for header_index in range(header_count)
        ]

        inspected_rows.append(
            {
                "row_index": row_index,
                "td_count": cell_count,
                "th_count": header_count,
                "headers": headers,
                "cells": cells,
                "row_text_preview": clean_cell_text(row.inner_text())[:500],
            }
        )

        if len(inspected_rows) >= limit and cell_count > 0:
            break

    return {
        "visible_tr_count": row_count,
        "td_count_summary": td_count_summary,
        "rows": inspected_rows,
    }


def login_and_screenshot():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-after-login-{timestamp}.png")

    print("trying to open DoktorABC supplies page ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_products_page(
                browser,
                before_login_path=before_login_path,
            )

            page.screenshot(path=after_login_path, full_page=True)

            return {
                "ok": True,
                "current_url": page.url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                "after_login_path": after_login_path,
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_and_sample_product_row():
    print("trying to open DoktorABC and read one product row ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_products_page(browser)

            product_row = extract_first_visible_product_row(page)

            if product_row is None:
                return {
                    "ok": False,
                    "current_url": page.url,
                    "reused_session": reused_session,
                    "session_state_path": SESSION_STATE_PATH,
                    "error": "No visible product row with at least 11 td cells was found.",
                    "visible_tr_count": page.locator("tr").count(),
                }

            return {
                "ok": True,
                "current_url": page.url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "visible_tr_count": page.locator("tr").count(),
                **product_row,
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_and_check_session():
    print("trying to open DoktorABC supplies page for session check ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_products_page(browser)

            return {
                "ok": True,
                "current_url": page.url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "visible_tr_count": page.locator("tr").count(),
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_and_debug_rows():
    print("trying to inspect DoktorABC table rows ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_products_page(browser)
            row_debug = inspect_visible_product_rows(page)

            return {
                "ok": True,
                "current_url": page.url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                **row_debug,
            }
        finally:
            if context:
                context.close()
            browser.close()


def login_scrape_and_sync_products():
    print("trying to scrape and sync DoktorABC products ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
        context = None

        try:
            context, page, reused_session = open_authenticated_products_page(browser)
            scrape_result = scrape_all_product_rows(page)
            products = scrape_result["products"]
            invalid_products = scrape_result["invalid_products"]

            if invalid_products:
                return {
                    "ok": False,
                    "current_url": page.url,
                    "reused_session": reused_session,
                    "session_state_path": SESSION_STATE_PATH,
                    "error": "Scrape found invalid product rows. Supabase was not changed.",
                    "invalid_count": len(invalid_products),
                    "invalid_examples": invalid_products[:5],
                    "scraped_valid_count": len(products),
                    "scroll_rounds": scrape_result["scroll_rounds"],
                    "visible_tr_count": scrape_result["visible_tr_count"],
                }

            if not products:
                return {
                    "ok": False,
                    "current_url": page.url,
                    "reused_session": reused_session,
                    "session_state_path": SESSION_STATE_PATH,
                    "error": "No valid products were scraped. Supabase was not changed.",
                    "scroll_rounds": scrape_result["scroll_rounds"],
                    "visible_tr_count": scrape_result["visible_tr_count"],
                }

            supabase_result = sync_products_to_supabase(products)

            return {
                "ok": True,
                "current_url": page.url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "supabase_schema": SUPABASE_SCHEMA,
                "supabase_table": SUPABASE_PRODUCTS_TABLE,
                "scraped": len(products),
                "scroll_rounds": scrape_result["scroll_rounds"],
                "visible_tr_count": scrape_result["visible_tr_count"],
                **supabase_result,
            }
        finally:
            if context:
                context.close()
            browser.close()


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "web-scraper",
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
    }


@app.post("/jobs/product-prices")
def product_prices():
    try:
        return login_scrape_and_sync_products()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/product-prices/sample")
def product_prices_sample():
    try:
        return login_and_sample_product_row()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/product-prices/session-check")
def product_prices_session_check():
    try:
        return login_and_check_session()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/product-prices/debug-rows")
def product_prices_debug_rows():
    try:
        return login_and_debug_rows()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
        )


@app.post("/jobs/product-prices/supabase-check")
def product_prices_supabase_check():
    try:
        return supabase_debug_check()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"{type(exc).__name__}: {exc}"},
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
