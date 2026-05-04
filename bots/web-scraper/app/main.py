import os
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from playwright.sync_api import sync_playwright


STARTED_AT = time.time()
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "/app/artifacts")


app = FastAPI(title="web-scraper")


def bool_env(name, default=True):
    value = os.environ.get(name)

    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


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


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})


def main():
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
