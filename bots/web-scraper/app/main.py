import os
import re
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")


app = FastAPI(title="web-scraper")


PRODUCT_CELL_COUNT = 11


def bool_env(name, default=True):
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


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


def extract_first_visible_product_row(page):
    rows = page.locator("tr")
    row_count = rows.count()

    for row_index in range(row_count):
        row = rows.nth(row_index)
        cells_locator = row.locator("td")
        cell_count = cells_locator.count()

        if cell_count != PRODUCT_CELL_COUNT:
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


def login_and_screenshot():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-before-login-{timestamp}.png")
    after_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-after-login-{timestamp}.png")

    print("trying to login ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))

        context = browser.new_context(viewport={"width": 1365, "height": 900})
        page = context.new_page()

        try:
            page.goto(os.environ["DOKTORABC_LOGIN_URL"], wait_until="domcontentloaded")

            page.get_by_placeholder("Email").fill(os.environ["DOKTORABC_USERNAME"])
            page.get_by_placeholder("Password").fill(os.environ["DOKTORABC_PASSWORD"])

            page.get_by_text("Pharmacist", exact=True).click()
            page.screenshot(path=before_login_path, full_page=True)
            page.get_by_role("button", name="Login").click()#login

            page.get_by_text("Add/Decrease", exact=True).first.wait_for(timeout=60_000)

            

            page.screenshot(path=after_login_path, full_page=True)

            return {
                "ok": True,
                "current_url": page.url,
                "before_login_path": before_login_path,
                "after_login_path": after_login_path,
            }
        finally:
            context.close()
            browser.close()


def login_and_sample_product_row():
    print("trying to login and read one product row ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))

        context = browser.new_context(viewport={"width": 1365, "height": 900})
        page = context.new_page()

        try:
            page.goto(os.environ["DOKTORABC_LOGIN_URL"], wait_until="domcontentloaded")

            page.get_by_placeholder("Email").fill(os.environ["DOKTORABC_USERNAME"])
            page.get_by_placeholder("Password").fill(os.environ["DOKTORABC_PASSWORD"])

            page.get_by_text("Pharmacist", exact=True).click()
            page.get_by_role("button", name="Login").click()

            page.get_by_text("Add/Decrease", exact=True).first.wait_for(timeout=60_000)

            product_row = extract_first_visible_product_row(page)

            if product_row is None:
                return {
                    "ok": False,
                    "current_url": page.url,
                    "error": "No visible product row with 11 td cells was found.",
                    "visible_tr_count": page.locator("tr").count(),
                }

            return {
                "ok": True,
                "current_url": page.url,
                "visible_tr_count": page.locator("tr").count(),
                **product_row,
            }
        finally:
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
        return login_and_screenshot()
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


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})


def main():
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
