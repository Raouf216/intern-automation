import mimetypes
import os
import re
import random
import threading
import time
import zipfile
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
import xml.etree.ElementTree as ElementTree

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
PERSISTENT_CONTEXT_DIR = os.environ.get(
    "DOKTORABC_PERSISTENT_CONTEXT_DIR",
    os.path.join(ARTIFACTS_DIR, "doktorabc-pickup-ready-profile"),
)
DEFAULT_DOKTORABC_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)


def int_env(name, default):
    value = os.environ.get(name)

    if value is None or not value.strip():
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer environment variable {name}={value!r}") from exc


DOKTORABC_USER_AGENT = (
    os.environ.get("DOKTORABC_USER_AGENT", DEFAULT_DOKTORABC_USER_AGENT).strip()
    or DEFAULT_DOKTORABC_USER_AGENT
)
DEFAULT_END_OF_DAY_URL = "https://pharmacies.doktorabc.com/end-of-day"
EOD_ORDER_TYPE = "eod"
SELF_PICKUP_ORDER_TYPE = "self pickup"
EOD_ORDER_LIST_TYPE = "eod"
PICKUP_READY_ORDER_LIST_TYPE = "pickup_ready"
COMBINED_ORDER_LIST_TYPE = "eod_and_pickup"
EXCEL_EXPORT_ORDER_LIST_TYPE = "excel_export"
EOD_MAX_PAGES = int_env("EOD_MAX_PAGES", 100)
EOD_AFTER_GOTO_WAIT_MS = int_env("EOD_AFTER_GOTO_WAIT_MS", 2_000)
EOD_AFTER_READY_FOR_CUSTOMER_CLICK_WAIT_MS = int_env("EOD_AFTER_READY_FOR_CUSTOMER_CLICK_WAIT_MS", 2_000)
EOD_AFTER_SELECT_100_CLICK_WAIT_MS = int_env("EOD_AFTER_SELECT_100_CLICK_WAIT_MS", 1_500)
EOD_AFTER_NEXT_CLICK_WAIT_MS = int_env("EOD_AFTER_NEXT_CLICK_WAIT_MS", 1_500)
EOD_READY_TIMEOUT_MS = 20_000
EOD_NAVIGATION_TIMEOUT_MS = 20_000
EOD_SUPABASE_TIMEOUT_SECONDS = 30
EOD_N8N_UPLOAD_TIMEOUT_SECONDS = 30
EOD_NOTIFICATION_TIMEOUT_SECONDS = 30
EOD_EXPORT_DOWNLOAD_TIMEOUT_MS = 20_000
EOD_EXPORT_BUTTON_VISIBLE_TIMEOUT_MS = 5_000
EOD_EXPORT_BUTTON_CLICK_TIMEOUT_MS = 10_000
EOD_LOGIN_FORM_CHECK_TIMEOUT_MS = 800
EOD_LOGIN_FIELD_TIMEOUT_MS = 5_000
EOD_PHARMACIST_ROLE_CLICK_TIMEOUT_MS = 5_000
EOD_LOGIN_BUTTON_CLICK_TIMEOUT_MS = 10_000
EOD_LOGIN_SUCCESS_TIMEOUT_MS = int_env("EOD_LOGIN_SUCCESS_TIMEOUT_MS", 45_000)
EOD_LOAD_STATE_DOMCONTENTLOADED_TIMEOUT_MS = 10_000
EOD_LOAD_STATE_LOAD_TIMEOUT_MS = 10_000
EOD_LOAD_STATE_NETWORKIDLE_TIMEOUT_MS = 5_000
EOD_RENDER_STABILITY_TIMEOUT_MS = 20_000
EOD_RENDER_STABILITY_STABLE_MS = 4_000
EOD_RENDER_STABILITY_POLL_MS = 700
EOD_ORDER_LIST_TIMEOUT_MS = 20_000
EOD_ORDER_LIST_STABLE_MS = 1_500
EOD_ROWS_100_TIMEOUT_MS = 20_000
EOD_ROWS_100_PROBE_TIMEOUT_MS = 500
EOD_EMPTY_LIST_MIN_WAIT_MS = 5_000
EOD_EMPTY_LIST_STABLE_MS = 1_500
EOD_EMPTY_LIST_POLL_MS = 500
EOD_READY_FOR_CUSTOMER_VISIBLE_TIMEOUT_MS = 20_000
EOD_READY_FOR_CUSTOMER_CLICK_TIMEOUT_MS = 10_000
EOD_SELECT_100_VISIBLE_TIMEOUT_MS = 5_000
EOD_SELECT_100_CLICK_TIMEOUT_MS = 10_000
EOD_NEXT_VISIBLE_TIMEOUT_MS = 10_000
EOD_NEXT_CLICK_TIMEOUT_MS = 10_000
EOD_NEXT_CHANGE_TIMEOUT_MS = 20_000
EOD_NEXT_CHANGE_POLL_MS = 500
EOD_AFTER_LOGIN_CLICK_WAIT_MS = 2_000
EOD_PZN_POPUP_WAIT_MS = 350
EOD_PZN_CLOSE_WAIT_MS = 100
PICKUP_DONE_MAX_ORDER_REFERENCES = int_env("PICKUP_DONE_MAX_ORDER_REFERENCES", 50)
PICKUP_DONE_ORDER_PROBE_TIMEOUT_MS = int_env("PICKUP_DONE_ORDER_PROBE_TIMEOUT_MS", 800)
PICKUP_DONE_BUTTON_VISIBLE_TIMEOUT_MS = int_env("PICKUP_DONE_BUTTON_VISIBLE_TIMEOUT_MS", 10_000)
PICKUP_DONE_BUTTON_CLICK_TIMEOUT_MS = int_env("PICKUP_DONE_BUTTON_CLICK_TIMEOUT_MS", 10_000)
PICKUP_DONE_AFTER_CLICK_WAIT_MS = int_env("PICKUP_DONE_AFTER_CLICK_WAIT_MS", 2_000)
PICKUP_READY_AUTO_SYNC_MIN_MINUTES = int_env("PICKUP_READY_AUTO_SYNC_MIN_MINUTES", 27)
PICKUP_READY_AUTO_SYNC_MAX_MINUTES = int_env("PICKUP_READY_AUTO_SYNC_MAX_MINUTES", 33)
PICKUP_READY_AUTO_SYNC_INITIAL_DELAY_SECONDS = int_env("PICKUP_READY_AUTO_SYNC_INITIAL_DELAY_SECONDS", 15)
PICKUP_READY_BEFORE_OPEN_DELAY_MIN_SECONDS = int_env("PICKUP_READY_BEFORE_OPEN_DELAY_MIN_SECONDS", 3)
PICKUP_READY_BEFORE_OPEN_DELAY_MAX_SECONDS = int_env("PICKUP_READY_BEFORE_OPEN_DELAY_MAX_SECONDS", 18)
SUPABASE_SCHEMA = os.environ.get("SUPABASE_SCHEMA", "private")
SUPABASE_EOD_ORDERS_TABLE = os.environ.get("SUPABASE_EOD_ORDERS_TABLE", "doktorabc_eod_bot_orders")
END_OF_DAY_EXPORT_N8N_WEBHOOK_URL = (os.environ.get("END_OF_DAY_EXPORT_N8N_WEBHOOK_URL") or "").strip()
END_OF_DAY_NOTIFICATION_WEBHOOK_URL = (os.environ.get("END_OF_DAY_NOTIFICATION_WEBHOOK_URL") or "").strip()
SERVICE_NAME = (os.environ.get("DOKTORABC_SCRAPER_SERVICE_NAME") or "end-of-day-scraper").strip() or "end-of-day-scraper"
SELF_PICKUP_ORDERS_API_FRAGMENT = "incoming-self-pickup"
SELF_PICKUP_ORDERS_API_LIMIT = 15
SELF_PICKUP_ORDERS_API_QUERY = (
    "incoming-self-pickup?limit=15&offset=0&sort=false&productIDs=&status=ready-for-customer&search="
)


app = FastAPI(title=SERVICE_NAME)


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

AUTO_SYNC_STOP_EVENT = threading.Event()
AUTO_SYNC_THREAD = None
AUTO_SYNC_STATE = {
    "enabled": False,
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_result": None,
    "last_error": None,
    "next_run_at": None,
    "skipped_reason": None,
}
BROWSER_RUN_LOCK = threading.Lock()


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
        f"{supabase_table_url()}?on_conflict=order_type,order_reference",
        headers={
            **supabase_headers(),
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
        json=orders,
        timeout=EOD_SUPABASE_TIMEOUT_SECONDS,
    )

    if response.status_code >= 400:
        raise RuntimeError(f"Supabase upsert failed: {response_preview(response)}")

    return {
        "sent_to_supabase": len(orders),
        "supabase_status_code": response.status_code,
    }


def export_button_locator(page):
    role_button = page.get_by_role("button", name=re.compile(r"^\s*Export\s*$", re.I)).first
    text_button = page.locator('button:has-text("Export")').first

    for locator in (role_button, text_button):
        try:
            locator.wait_for(state="visible", timeout=EOD_EXPORT_BUTTON_VISIBLE_TIMEOUT_MS)
            return locator
        except PlaywrightTimeoutError:
            continue

    raise RuntimeError('Could not find visible "Export" button.')


def send_export_to_n8n(download_path, metadata):
    if not END_OF_DAY_EXPORT_N8N_WEBHOOK_URL:
        return {
            "sent_to_n8n": False,
            "n8n_skipped_reason": "END_OF_DAY_EXPORT_N8N_WEBHOOK_URL is not configured",
        }

    filename = os.path.basename(download_path)
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    with open(download_path, "rb") as file_handle:
        response = httpx.post(
            END_OF_DAY_EXPORT_N8N_WEBHOOK_URL,
            data={key: "" if value is None else str(value) for key, value in metadata.items()},
            files={"file": (filename, file_handle, content_type)},
            timeout=EOD_N8N_UPLOAD_TIMEOUT_SECONDS,
        )

    if response.status_code >= 400:
        raise RuntimeError(f"n8n export upload failed: {response_preview(response)}")

    return {
        "sent_to_n8n": True,
        "n8n_status_code": response.status_code,
    }


def send_notification(payload):
    if not END_OF_DAY_NOTIFICATION_WEBHOOK_URL:
        return {
            "sent_to_notification_app": False,
            "notification_skipped_reason": "END_OF_DAY_NOTIFICATION_WEBHOOK_URL is not configured",
        }

    try:
        response = httpx.post(
            END_OF_DAY_NOTIFICATION_WEBHOOK_URL,
            json=payload,
            timeout=EOD_NOTIFICATION_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as exc:
        return {
            "sent_to_notification_app": False,
            "notification_error": f"{type(exc).__name__}: {exc}",
        }

    if response.status_code >= 400:
        return {
            "sent_to_notification_app": False,
            "notification_status_code": response.status_code,
            "notification_error": response_preview(response),
        }

    return {
        "sent_to_notification_app": True,
        "notification_status_code": response.status_code,
    }


def notification_order_list_type(order_type):
    if order_type == SELF_PICKUP_ORDER_TYPE:
        return PICKUP_READY_ORDER_LIST_TYPE

    return EOD_ORDER_LIST_TYPE


def notification_order_label(order_list_type):
    if order_list_type == PICKUP_READY_ORDER_LIST_TYPE:
        return "Self Pickup READY"

    if order_list_type == EXCEL_EXPORT_ORDER_LIST_TYPE:
        return "Excel Export"

    if order_list_type == COMBINED_ORDER_LIST_TYPE:
        return "EOD und Self Pickup"

    return "EOD"


def notification_order_snapshot(row):
    billing_date = row.get("billing_date")

    return {
        "order_id": row.get("order_reference"),
        "order_reference": row.get("order_reference"),
        "created_date": row.get("prescription_date"),
        "billing_date": billing_date,
        "prescription_date": row.get("prescription_date"),
        "products": row.get("products"),
        "prices": row.get("prices"),
        "quantities": row.get("quantities"),
    }


def notification_order_list_payload(order_type, rows):
    order_list_type = notification_order_list_type(order_type)

    return {
        "order_type": order_type,
        "order_list_type": order_list_type,
        "label": notification_order_label(order_list_type),
        "order_count": len(rows),
        "orders": [notification_order_snapshot(row) for row in rows],
    }


def notification_order_lists_payload(rows_by_order_type, targets):
    return {
        notification_order_list_type(target["order_type"]): notification_order_list_payload(
            target["order_type"],
            rows_by_order_type.get(target["order_type"], []),
        )
        for target in targets
    }


def send_orders_sync_notification(rows_by_order_type, targets, timestamp, supabase_result):
    order_lists = notification_order_lists_payload(rows_by_order_type, targets)
    eod_count = order_lists.get(EOD_ORDER_LIST_TYPE, {}).get("order_count", 0)
    pickup_count = order_lists.get(PICKUP_READY_ORDER_LIST_TYPE, {}).get("order_count", 0)
    total_count = sum(order_list.get("order_count", 0) for order_list in order_lists.values())

    return send_notification(
        {
            "event": "doktorabc_eod_pickup_orders_success",
            "status": "success",
            "section": "doktorabc_sync",
            "sync_type": "doktorabc_eod_bot",
            "service": SERVICE_NAME,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": timestamp,
            "order_list_type": COMBINED_ORDER_LIST_TYPE,
            "order_count": total_count,
            "eod_order_count": eod_count,
            "pickup_ready_order_count": pickup_count,
            "order_lists": order_lists,
            "summary": {
                "orders": total_count,
                "eod_orders": eod_count,
                "pickup_ready_orders": pickup_count,
                "sent_to_supabase": supabase_result.get("sent_to_supabase", 0),
            },
        }
    )


def send_excel_export_notification(export_result, timestamp):
    return send_notification(
        {
            "event": "doktorabc_eod_excel_export_success",
            "status": "success",
            "section": "upload",
            "upload_type": "doktorabc_eod_excel_export",
            "sync_type": "doktorabc_eod_bot",
            "service": SERVICE_NAME,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": timestamp,
            "order_list_type": EXCEL_EXPORT_ORDER_LIST_TYPE,
            "filename": export_result.get("download_filename"),
            "path": export_result.get("download_path"),
            "size_bytes": export_result.get("download_size_bytes"),
            "download_filename": export_result.get("download_filename"),
            "download_path": export_result.get("download_path"),
            "download_size_bytes": export_result.get("download_size_bytes"),
            "excel_row_count": export_result.get("excel_row_count"),
            "export_date": export_result.get("export_date"),
            "sent_to_n8n": export_result.get("sent_to_n8n"),
            "n8n_status_code": export_result.get("n8n_status_code"),
            "n8n_skipped_reason": export_result.get("n8n_skipped_reason"),
            "summary": {
                "excel_files": 1,
                "excel_rows": export_result.get("excel_row_count"),
                "sent_to_n8n": 1 if export_result.get("sent_to_n8n") else 0,
            },
        }
    )


def send_failure_notification(
    failure_part,
    timestamp,
    error,
    failed_step,
    page_url=None,
    screenshot_path=None,
    rows_by_order_type=None,
    extra=None,
):
    rows_by_order_type = rows_by_order_type or {}
    extra = extra or {}
    order_type = None
    section = "doktorabc_sync"
    upload_type = None

    if failure_part == PICKUP_READY_ORDER_LIST_TYPE:
        event = "doktorabc_pickup_ready_orders_failure"
        order_type = SELF_PICKUP_ORDER_TYPE
    elif failure_part == EXCEL_EXPORT_ORDER_LIST_TYPE:
        event = "doktorabc_eod_excel_export_failure"
        section = "upload"
        upload_type = "doktorabc_eod_excel_export"
    else:
        event = "doktorabc_eod_orders_failure"
        failure_part = EOD_ORDER_LIST_TYPE
        order_type = EOD_ORDER_TYPE

    payload = {
        "event": event,
        "status": "failure",
        "section": section,
        "sync_type": "doktorabc_eod_bot",
        "service": SERVICE_NAME,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": timestamp,
        "order_type": order_type,
        "order_list_type": failure_part,
        "label": notification_order_label(failure_part),
        "error": error,
        "failed_step": failed_step,
        "current_url": page_url,
        "screenshot_path": screenshot_path,
        **extra,
    }

    if upload_type:
        payload["upload_type"] = upload_type

    if order_type:
        rows = rows_by_order_type.get(order_type, [])
        payload["order_count"] = len(rows)
        payload["orders"] = [notification_order_snapshot(row) for row in rows]

    return send_notification(payload)


def failure_parts_for_step(failed_step, steps, targets):
    if failed_step in {"export_eod_excel_to_n8n", "send_excel_export_notification"}:
        return [EXCEL_EXPORT_ORDER_LIST_TYPE]

    pending_order_type = next(
        (
            step.get("order_type")
            for step in reversed(steps)
            if step.get("ok") is None and step.get("order_type")
        ),
        None,
    )

    if pending_order_type:
        return [notification_order_list_type(pending_order_type)]

    if failed_step == "upsert_supabase":
        return list(dict.fromkeys(notification_order_list_type(target["order_type"]) for target in targets))

    last_order_type = next(
        (step.get("order_type") for step in reversed(steps) if step.get("order_type")),
        None,
    )

    return [notification_order_list_type(last_order_type)] if last_order_type else [EOD_ORDER_LIST_TYPE]


def local_timezone():
    return german_timezone_for_utc(datetime.now(timezone.utc))


def local_today_iso():
    return datetime.now(local_timezone()).date().isoformat()


def worksheet_sort_key(name):
    match = re.search(r"sheet(\d+)\.xml$", name)
    return int(match.group(1)) if match else 10_000


def count_xlsx_rows(path):
    if not path.lower().endswith(".xlsx"):
        return None

    try:
        with zipfile.ZipFile(path) as workbook:
            worksheets = sorted(
                [
                    name
                    for name in workbook.namelist()
                    if re.match(r"xl/worksheets/sheet\d+\.xml$", name)
                ],
                key=worksheet_sort_key,
            )

            if not worksheets:
                return None

            row_count = 0
            with workbook.open(worksheets[0]) as sheet:
                for _, element in ElementTree.iterparse(sheet, events=("end",)):
                    if element.tag.rsplit("}", 1)[-1] == "row" and any(
                        child.tag.rsplit("}", 1)[-1] == "c" for child in element
                    ):
                        row_count += 1
                    element.clear()

            return row_count
    except Exception as exc:
        print(f"could not count rows in exported Excel file: {type(exc).__name__}: {exc}", flush=True)
        return None


def export_end_of_day_excel_to_n8n(page, timestamp, metadata):
    wait_for_orders_page(page, end_of_day_url())

    export_button = export_button_locator(page)

    with page.expect_download(timeout=EOD_EXPORT_DOWNLOAD_TIMEOUT_MS) as download_info:
        export_button.click(timeout=EOD_EXPORT_BUTTON_CLICK_TIMEOUT_MS)

    download = download_info.value
    suggested_filename = os.path.basename(download.suggested_filename or f"doktorabc-eod-export-{timestamp}.xlsx")
    safe_filename = re.sub(r"[^A-Za-z0-9_. -]+", "_", suggested_filename).strip() or f"doktorabc-eod-export-{timestamp}.xlsx"
    download_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-export-{timestamp}-{safe_filename}")
    download.save_as(download_path)
    download_size_bytes = os.path.getsize(download_path)
    excel_row_count = count_xlsx_rows(download_path)
    export_date = local_today_iso()

    n8n_result = send_export_to_n8n(
        download_path,
        {
            **metadata,
            "source": "doktorabc_end_of_day_export",
            "source_url": end_of_day_url(),
            "download_filename": safe_filename,
            "download_path": download_path,
            "download_size_bytes": download_size_bytes,
            "excel_row_count": excel_row_count,
            "export_date": export_date,
            "exported_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    return {
        "downloaded": True,
        "download_filename": safe_filename,
        "download_path": download_path,
        "download_size_bytes": download_size_bytes,
        "excel_row_count": excel_row_count,
        "export_date": export_date,
        **n8n_result,
    }


def maybe_export_end_of_day_excel_to_n8n(page, timestamp, metadata, rows_by_order_type):
    eod_order_count = len(rows_by_order_type.get(EOD_ORDER_TYPE, []))

    if eod_order_count == 0:
        result = {
            "skipped": True,
            "skipped_reason": "no_eod_orders",
            "eod_order_count": eod_order_count,
            "sent_to_n8n": False,
            "n8n_skipped_reason": "No EOD orders; Excel export was not clicked.",
        }
        log_event("eod_excel_export_skipped", **result)
        return result

    return {
        "skipped": False,
        "eod_order_count": eod_order_count,
        **export_end_of_day_excel_to_n8n(page, timestamp, metadata),
    }


def end_of_day_url():
    return os.environ.get("DOKTORABC_END_OF_DAY_URL") or DEFAULT_END_OF_DAY_URL


def login_url():
    return (os.environ.get("DOKTORABC_LOGIN_URL") or "").strip() or end_of_day_url()


def self_pickup_url():
    return (os.environ.get("DOKTORABC_SELF_PICKUP_URL") or "").strip() or None


def configured_order_target_mode():
    value = (os.environ.get("EOD_ORDER_TARGET_MODE") or "combined").strip().lower()
    aliases = {
        "combined": "combined",
        "all": "combined",
        "eod_and_pickup": "combined",
        "eod": EOD_ORDER_LIST_TYPE,
        "end_of_day": EOD_ORDER_LIST_TYPE,
        "end-of-day": EOD_ORDER_LIST_TYPE,
        "pickup": PICKUP_READY_ORDER_LIST_TYPE,
        "pickup_ready": PICKUP_READY_ORDER_LIST_TYPE,
        "pickup-ready": PICKUP_READY_ORDER_LIST_TYPE,
        "self_pickup": PICKUP_READY_ORDER_LIST_TYPE,
        "self-pickup": PICKUP_READY_ORDER_LIST_TYPE,
    }

    if value not in aliases:
        allowed = ", ".join(sorted(aliases))
        raise RuntimeError(f"Invalid EOD_ORDER_TARGET_MODE={value!r}. Expected one of: {allowed}")

    return aliases[value]


def browser_context_options():
    return {
        "accept_downloads": True,
        "user_agent": DOKTORABC_USER_AGENT,
        "viewport": {"width": 1365, "height": 900},
    }


def launch_doktorabc_persistent_context(playwright):
    os.makedirs(PERSISTENT_CONTEXT_DIR, exist_ok=True)
    log_event("persistent_context_launch", user_data_dir=PERSISTENT_CONTEXT_DIR)

    return playwright.chromium.launch_persistent_context(
        PERSISTENT_CONTEXT_DIR,
        headless=bool_env("DOKTORABC_HEADLESS", True),
        **browser_context_options(),
    )


def log_event(event, **details):
    payload = {
        "event": event,
        "service": SERVICE_NAME,
        **details,
    }
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


def bounded_random_seconds(min_seconds, max_seconds):
    low = min(min_seconds, max_seconds)
    high = max(min_seconds, max_seconds)

    if high <= 0:
        return 0

    return random.uniform(max(0, low), high)


def wait_before_opening_page():
    seconds = bounded_random_seconds(
        PICKUP_READY_BEFORE_OPEN_DELAY_MIN_SECONDS,
        PICKUP_READY_BEFORE_OPEN_DELAY_MAX_SECONDS,
    )

    if seconds <= 0:
        return 0

    log_event("pickup_ready_before_open_wait", seconds=round(seconds, 3))
    time.sleep(seconds)

    return seconds


def failure_screenshot_path(label):
    safe_label = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-") or "failure"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(ARTIFACTS_DIR, f"doktorabc-{safe_label}-{timestamp}.png")


def capture_failure_screenshot(page, label):
    path = failure_screenshot_path(label)

    try:
        os.makedirs(ARTIFACTS_DIR, exist_ok=True)
        page.screenshot(path=path, full_page=True)
        log_event("screenshot_saved", label=label, path=path, url=page.url)
        return path
    except Exception as exc:
        log_event("screenshot_failed", label=label, error=f"{type(exc).__name__}: {exc}")
        return None


def page_readiness_diagnostics(page):
    try:
        return page.evaluate(
            """
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
              const rows100Visible = Array.from(
                document.querySelectorAll("#pagination-container #rows-per-page-container li")
              ).some((element) => visible(element) && /^100$/.test(normalize(element.innerText)));
              const errorText = Array.from(
                document.querySelectorAll('[role="alert"], [class*="error" i], [class*="danger" i], [class*="invalid" i]')
              )
                .filter(visible)
                .map((element) => normalize(element.innerText))
                .filter(Boolean)
                .join(" | ");

              return {
                url: window.location.href,
                title: document.title,
                readyState: document.readyState,
                emailVisible,
                passwordVisible,
                rows100Visible,
                paginationVisible: visible(document.querySelector("#pagination-container")),
                orderMarkerCount: document.querySelectorAll('button[id$="-mark-order"], [id^="order-"][id$="-badge"]').length,
                errorText,
              };
            }
            """
        )
    except Exception as exc:
        return {"diagnostics_error": f"{type(exc).__name__}: {exc}", "url": getattr(page, "url", None)}


def goto_page(page, url):
    log_event("goto_start", url=url)
    page.goto(url, wait_until="domcontentloaded", timeout=EOD_NAVIGATION_TIMEOUT_MS)
    page.wait_for_load_state("domcontentloaded")
    log_event("goto_domcontentloaded", url=page.url)


def visible_login_form(page):
    if "login" in page.url.lower():
        return True

    for selector in (
        'input[placeholder*="Email" i]',
        'input[type="email"]',
        'input[name*="email" i]',
    ):
        try:
            page.locator(selector).first.wait_for(state="visible", timeout=EOD_LOGIN_FORM_CHECK_TIMEOUT_MS)
            return True
        except PlaywrightTimeoutError:
            continue

    return False


def fill_first_visible(page, selectors, value):
    for selector in selectors:
        try:
            field = page.locator(selector).first
            field.wait_for(state="visible", timeout=EOD_LOGIN_FIELD_TIMEOUT_MS)
            field.fill(value)
            return
        except PlaywrightTimeoutError:
            continue

    raise RuntimeError(f"Could not find visible field for selectors: {', '.join(selectors)}")


def click_pharmacist_role(page):
    for clicker in (
        lambda: page.get_by_label("Pharmacist").click(timeout=EOD_PHARMACIST_ROLE_CLICK_TIMEOUT_MS),
        lambda: page.get_by_text("Pharmacist", exact=True).click(timeout=EOD_PHARMACIST_ROLE_CLICK_TIMEOUT_MS),
        lambda: page.locator('text=/pharmacist/i').first.click(timeout=EOD_PHARMACIST_ROLE_CLICK_TIMEOUT_MS),
    ):
        try:
            clicker()
            return
        except Exception:
            continue

    print("could not explicitly click Pharmacist role; continuing login attempt", flush=True)


def click_login_button(page):
    for clicker in (
        lambda: page.get_by_role("button", name=re.compile("login", re.I)).click(timeout=EOD_LOGIN_BUTTON_CLICK_TIMEOUT_MS),
        lambda: page.locator('button:has-text("Login")').first.click(timeout=EOD_LOGIN_BUTTON_CLICK_TIMEOUT_MS),
        lambda: page.locator('input[type="submit"]').first.click(timeout=EOD_LOGIN_BUTTON_CLICK_TIMEOUT_MS),
    ):
        try:
            clicker()
            return
        except Exception:
            continue

    raise RuntimeError("Could not find DoktorABC login button.")


def save_session_state(context):
    session_state_dir = os.path.dirname(SESSION_STATE_PATH)
    if session_state_dir:
        os.makedirs(session_state_dir, exist_ok=True)

    context.storage_state(path=SESSION_STATE_PATH)
    log_event("session_state_saved", path=SESSION_STATE_PATH)


LOGIN_AUTHENTICATED_JS = """
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
  const onLoginUrl = /login/i.test(window.location.href);
  const loginButtonVisible = Array.from(document.querySelectorAll("button, input[type='submit']"))
    .some((element) => visible(element) && /login/i.test(normalize(element.innerText || element.value)));
  const loginVisible = emailVisible || passwordVisible || loginButtonVisible;
  const rows100Visible = Array.from(
    document.querySelectorAll("#pagination-container #rows-per-page-container li")
  ).some((element) => visible(element) && /^100$/.test(normalize(element.innerText)));
  const orderMarkerCount = document.querySelectorAll('button[id$="-mark-order"], [id^="order-"][id$="-badge"]').length;
  const paginationVisible = visible(document.querySelector("#pagination-container"));
  const errorText = Array.from(
    document.querySelectorAll('[role="alert"], [class*="error" i], [class*="danger" i], [class*="invalid" i]')
  )
    .filter(visible)
    .map((element) => normalize(element.innerText))
    .filter(Boolean)
    .join(" | ");
  const bodyText = normalize(document.body?.innerText || "");
  const credentialError =
    loginVisible &&
    (
      Boolean(errorText) ||
      /invalid|incorrect|wrong|failed|denied|unauthorized|ungueltig|falsch|fehler/i.test(bodyText)
    );

  if (credentialError) {
    return {
      status: "login_error",
      url: window.location.href,
      readyState: document.readyState,
      errorText: errorText || bodyText.slice(0, 500),
    };
  }

  if ((!loginVisible && !onLoginUrl) || rows100Visible || orderMarkerCount > 0 || paginationVisible) {
    return {
      status: "authenticated",
      url: window.location.href,
      readyState: document.readyState,
      rows100Visible,
      orderMarkerCount,
      paginationVisible,
    };
  }

  return false;
}
"""


def wait_for_business_condition(page, name, script, timeout_ms, ready_statuses, screenshot_label=None):
    log_event("business_wait_start", name=name, timeout_ms=timeout_ms, url=page.url)

    try:
        handle = page.wait_for_function(script, timeout=timeout_ms, polling=250)
        result = handle.json_value()
    except PlaywrightTimeoutError as exc:
        screenshot_path = capture_failure_screenshot(page, screenshot_label or name)
        diagnostics = page_readiness_diagnostics(page)
        log_event(
            "business_wait_timeout",
            name=name,
            timeout_ms=timeout_ms,
            screenshot_path=screenshot_path,
            diagnostics=diagnostics,
        )
        raise RuntimeError(
            f"{name} did not become ready in {timeout_ms}ms. "
            f"Screenshot: {screenshot_path}. Diagnostics: {diagnostics}"
        ) from exc

    status = result.get("status") if isinstance(result, dict) else None
    if status not in ready_statuses:
        screenshot_path = capture_failure_screenshot(page, screenshot_label or name)
        diagnostics = page_readiness_diagnostics(page)
        log_event(
            "business_wait_failed",
            name=name,
            status=status,
            result=result,
            screenshot_path=screenshot_path,
            diagnostics=diagnostics,
        )
        raise RuntimeError(
            f"{name} failed with status {status}. "
            f"Result: {result}. Screenshot: {screenshot_path}. Diagnostics: {diagnostics}"
        )

    log_event("business_wait_ready", name=name, result=result)
    return result


def login_if_needed(page, context, target_url, before_login_path=None):
    if not visible_login_form(page):
        log_event("login_not_needed", url=page.url)
        return False

    log_event("login_required", url=page.url, target_url=target_url)

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
    wait_for_load_states(page)
    wait_for_business_condition(
        page,
        "doktorabc_login_success",
        LOGIN_AUTHENTICATED_JS,
        EOD_LOGIN_SUCCESS_TIMEOUT_MS,
        ready_statuses={"authenticated"},
        screenshot_label="eod-login-success-timeout",
    )

    if not page.url.startswith(target_url):
        goto_page(page, target_url)
    else:
        page.wait_for_load_state("domcontentloaded")

    save_session_state(context)

    return True


def wait_for_load_states(page):
    try:
        log_event("load_state_wait_start", state="domcontentloaded", url=page.url)
        page.wait_for_load_state("domcontentloaded", timeout=EOD_LOAD_STATE_DOMCONTENTLOADED_TIMEOUT_MS)
        log_event("load_state_wait_ok", state="domcontentloaded", url=page.url)
    except PlaywrightTimeoutError:
        log_event("load_state_wait_timeout", state="domcontentloaded", url=page.url)
        pass

    try:
        log_event("load_state_wait_start", state="load", url=page.url)
        page.wait_for_load_state("load", timeout=EOD_LOAD_STATE_LOAD_TIMEOUT_MS)
        log_event("load_state_wait_ok", state="load", url=page.url)
    except PlaywrightTimeoutError:
        log_event("load_state_wait_timeout", state="load", url=page.url)
        pass

    try:
        log_event("load_state_wait_start", state="networkidle", url=page.url)
        page.wait_for_load_state("networkidle", timeout=EOD_LOAD_STATE_NETWORKIDLE_TIMEOUT_MS)
        log_event("load_state_wait_ok", state="networkidle", url=page.url)
    except PlaywrightTimeoutError:
        log_event("load_state_wait_timeout", state="networkidle", url=page.url)


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


def wait_for_render_stability(
    page,
    timeout_ms=EOD_RENDER_STABILITY_TIMEOUT_MS,
    stable_ms=EOD_RENDER_STABILITY_STABLE_MS,
    poll_ms=EOD_RENDER_STABILITY_POLL_MS,
):
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

    raise RuntimeError(f"DoktorABC orders page did not finish rendering in time. Last snapshot: {last_snapshot}")


def wait_for_orders_page(page, target_url, timeout_ms=None):
    if not page.url.startswith(target_url):
        goto_page(page, target_url)

    wait_for_load_states(page)

    if visible_login_form(page):
        screenshot_path = capture_failure_screenshot(page, "eod-login-visible-while-waiting-for-orders")
        diagnostics = page_readiness_diagnostics(page)
        raise RuntimeError(
            "DoktorABC session is not authenticated; login page is visible. "
            f"Screenshot: {screenshot_path}. Diagnostics: {diagnostics}"
        )

    log_event("business_wait_start", name="doktorabc_orders_page_ready", timeout_ms=timeout_ms or EOD_ROWS_100_TIMEOUT_MS, url=page.url)
    try:
        ready_result = wait_for_rows_100_control(page, timeout_ms=timeout_ms or EOD_ROWS_100_TIMEOUT_MS)
    except Exception as exc:
        screenshot_path = capture_failure_screenshot(page, "eod-orders-page-ready-timeout")
        diagnostics = page_readiness_diagnostics(page)
        log_event(
            "business_wait_failed",
            name="doktorabc_orders_page_ready",
            screenshot_path=screenshot_path,
            diagnostics=diagnostics,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise RuntimeError(
            f"DoktorABC orders page did not become usable. Screenshot: {screenshot_path}. "
            f"Diagnostics: {diagnostics}. Original error: {type(exc).__name__}: {exc}"
        ) from exc
    log_event("business_wait_ready", name="doktorabc_orders_page_ready", result=ready_result)

    return {
        **ready_result,
        "final_snapshot": page_render_snapshot(page),
    }


def wait_for_end_of_day_page(page, timeout_ms=EOD_READY_TIMEOUT_MS):
    return wait_for_orders_page(page, end_of_day_url(), timeout_ms=timeout_ms)


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


def last_sunday(year, month):
    date = datetime(year, month + 1, 1) - timedelta(days=1) if month < 12 else datetime(year, 12, 31)
    return date - timedelta(days=(date.weekday() + 1) % 7)


def german_timezone_for_utc(value):
    utc_value = value.astimezone(timezone.utc)
    year = utc_value.year
    dst_start = last_sunday(year, 3).replace(hour=1, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    dst_end = last_sunday(year, 10).replace(hour=1, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

    if dst_start <= utc_value < dst_end:
        return timezone(timedelta(hours=2))

    return timezone(timedelta(hours=1))


def to_german_datetime(value):
    if not value.tzinfo:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(german_timezone_for_utc(value))


def parse_datetime_to_second_iso(value):
    if not value:
        return None

    clean_value = str(value).strip()

    if not clean_value:
        return None

    try:
        parsed = datetime.fromisoformat(clean_value.replace("Z", "+00:00"))
    except ValueError:
        return clean_value

    parsed = parsed.replace(microsecond=0)
    if not parsed.tzinfo:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return to_german_datetime(parsed).isoformat()


def parse_datetime_to_local_date_iso(value):
    if not value:
        return None

    clean_value = str(value).strip()
    if not clean_value:
        return None

    try:
        parsed = datetime.fromisoformat(clean_value.replace("Z", "+00:00"))
    except ValueError:
        return parse_date_to_iso(clean_value)

    if not parsed.tzinfo:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return to_german_datetime(parsed).date().isoformat()


def clean_text(value):
    if value is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def order_reference_key(value):
    cleaned = clean_text(value)
    if not cleaned:
        return None

    return cleaned.strip("#").upper()


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


class SelfPickupBillingDateCollector:
    def __init__(self):
        self.billing_dates_by_reference = {}
        self.responses = []
        self.errors = []
        self.fetched_urls = set()

    def capture_response(self, response):
        if SELF_PICKUP_ORDERS_API_FRAGMENT not in response.url:
            return

        try:
            self.capture_payload(response.url, response.status, response.json())
        except Exception as exc:
            self.capture_error(response.url, getattr(response, "status", None), exc)

    def capture_payload(self, url, status, payload, source="response"):
        snapshot = {
            "url": url,
            "status": status,
            "source": source,
            "results": 0,
            "captured_billing_dates": 0,
        }
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list):
            self.responses.append(snapshot)
            return

        snapshot["results"] = len(results)
        for item in results:
            if not isinstance(item, dict):
                continue

            key = order_reference_key(item.get("hashID"))
            billing_date = parse_datetime_to_second_iso(item.get("createdAt"))
            if key and billing_date:
                self.billing_dates_by_reference[key] = billing_date
                snapshot["captured_billing_dates"] += 1

        self.responses.append(snapshot)

    def capture_error(self, url, status, exc):
        snapshot = {
            "url": url,
            "status": status,
            "source": "response",
            "results": 0,
            "captured_billing_dates": 0,
            "error": f"{type(exc).__name__}: {exc}",
        }
        self.responses.append(snapshot)
        self.errors.append(snapshot["error"])

    def collect_from_performance(self, page):
        try:
            entries = page.evaluate(
                """
                async (fragment) => {
                  const urls = Array.from(
                    new Set(
                      performance
                        .getEntriesByType("resource")
                        .map((entry) => entry.name)
                        .filter((url) => url.includes(fragment))
                    )
                  );
                  const responses = [];

                  for (const url of urls) {
                    try {
                      const response = await fetch(url, { credentials: "include" });
                      const payload = await response.json();
                      responses.push({ url, status: response.status, payload });
                    } catch (error) {
                      responses.push({
                        url,
                        status: null,
                        error: `${error?.name || "Error"}: ${error?.message || String(error)}`,
                      });
                    }
                  }

                  return responses;
                }
                """,
                SELF_PICKUP_ORDERS_API_FRAGMENT,
            )
        except Exception as exc:
            self.capture_error("performance", None, exc)
            return

        for entry in entries:
            url = entry.get("url")
            if not url or url in self.fetched_urls:
                continue

            self.fetched_urls.add(url)
            if entry.get("error"):
                self.responses.append(
                    {
                        "url": url,
                        "status": entry.get("status"),
                        "source": "performance_fetch",
                        "results": 0,
                        "captured_billing_dates": 0,
                        "error": entry["error"],
                    }
                )
                self.errors.append(entry["error"])
                continue

            self.capture_payload(url, entry.get("status"), entry.get("payload"), source="performance_fetch")

    def snapshot(self):
        return {
            "captured_billing_dates": len(self.billing_dates_by_reference),
            "response_count": len(self.responses),
            "responses": self.responses[-10:],
            "errors": self.errors[-10:],
        }


def normalize_scraped_order(order, order_type, billing_dates_by_reference=None):
    products = order.get("product_details") or []
    product_names = [product.get("product") for product in products]
    pzns = [join_pipe(product.get("pzns") or []) for product in products]
    prices = [normalize_price(product.get("price")) for product in products]
    quantities = [product.get("quantity") for product in products]
    order_reference = clean_text(order.get("order_reference"))
    billing_date = None

    if order_type == SELF_PICKUP_ORDER_TYPE:
        billing_date = (billing_dates_by_reference or {}).get(order_reference_key(order_reference))

    return {
        "order_type": order_type,
        "order_reference": order_reference,
        "billing_date": billing_date,
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


def normalize_gender(value):
    if value == 0:
        return "female"

    if value == 1:
        return "male"

    return clean_text(value)


def normalize_self_pickup_api_order(order):
    products = order.get("products") if isinstance(order.get("products"), list) else []
    product_names = [product.get("productTitle") for product in products if isinstance(product, dict)]
    pzns = [
        join_pipe(product.get("skus") or [])
        for product in products
        if isinstance(product, dict)
    ]
    prices = [
        str(product.get("supplyPrice"))
        for product in products
        if isinstance(product, dict) and product.get("supplyPrice") is not None
    ]
    quantities = [
        product.get("productQuantityTitle")
        for product in products
        if isinstance(product, dict)
    ]
    customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
    delivery = order.get("delivery") if isinstance(order.get("delivery"), dict) else {}
    prescription = order.get("prescription") if isinstance(order.get("prescription"), dict) else {}
    patient_name = join_pipe(
        [
            customer.get("firstName"),
            customer.get("middleName"),
            customer.get("lastName"),
        ]
    )
    address = join_pipe(
        [
            delivery.get("address"),
            delivery.get("zip"),
            delivery.get("city"),
            delivery.get("country"),
        ]
    )

    return {
        "order_type": SELF_PICKUP_ORDER_TYPE,
        "order_reference": clean_text(order.get("hashID")),
        "billing_date": parse_datetime_to_second_iso(order.get("createdAt")),
        "prescription_date": parse_datetime_to_local_date_iso(prescription.get("approvedAt")),
        "tracking_id": clean_text((order.get("shipping") or {}).get("trackingID")) if isinstance(order.get("shipping"), dict) else None,
        "products": join_pipe(product_names),
        "pzns": join_pipe(pzns),
        "prices": join_pipe(prices),
        "quantities": join_pipe(quantities),
        "patient_name": patient_name,
        "patient_birth_date": parse_datetime_to_local_date_iso(customer.get("birthday")),
        "address": address,
        "gender": normalize_gender(customer.get("gender")),
    }


def validate_orders(rows, raw_orders):
    invalid = []
    warnings = []

    for index, row in enumerate(rows):
        missing_required = [
            field
            for field in ("order_type", "order_reference", "prescription_date", "products")
            if not row.get(field)
        ]

        if row.get("order_type") not in {EOD_ORDER_TYPE, SELF_PICKUP_ORDER_TYPE}:
            missing_required.append("valid_order_type")

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

        missing_optional_parts = [
            field
            for field in ("pzns", "prices", "quantities")
            if not row.get(field)
        ]

        if row.get("order_type") == SELF_PICKUP_ORDER_TYPE and not row.get("billing_date"):
            missing_optional_parts.append("billing_date")

        if missing_optional_parts:
            warnings.append(
                {
                    "index": index,
                    "order_reference": row.get("order_reference"),
                    "missing": missing_optional_parts,
                }
            )

    return invalid, warnings


def wait_for_order_list(page, timeout_ms=EOD_ORDER_LIST_TIMEOUT_MS):
    stability = wait_for_render_stability(page, timeout_ms=timeout_ms, stable_ms=EOD_ORDER_LIST_STABLE_MS)
    pagination_state = get_pagination_state(page)
    order_count = pagination_state.get("order_count") or 0

    return {
        **stability,
        "waited_for": "order_list_render_stability",
        "order_count": order_count,
        "empty": order_count == 0,
        "pagination_state": pagination_state,
    }


def rows_100_locator(page):
    return page.locator("#pagination-container #rows-per-page-container li").filter(has_text=re.compile(r"^\s*100\s*$")).first


def render_ready(snapshot):
    return (
        snapshot["readyState"] in {"interactive", "complete"}
        and snapshot["textLength"] >= 20
        and snapshot["visibleLoaderCount"] == 0
        and snapshot["busyButtonCount"] == 0
    )


def render_stability_key(snapshot):
    return (
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


def wait_for_rows_100_control(page, timeout_ms=EOD_ROWS_100_TIMEOUT_MS):
    rows_100 = rows_100_locator(page)
    started_at = time.monotonic()
    deadline = time.monotonic() + timeout_ms / 1000
    stable_since = None
    previous_key = None
    last_snapshot = None
    last_pagination_state = None

    while time.monotonic() < deadline:
        try:
            rows_100.wait_for(state="visible", timeout=EOD_ROWS_100_PROBE_TIMEOUT_MS)
            return {
                "ready": True,
                "waited_for": "rows_100_control",
                "pagination_state": get_pagination_state(page),
            }
        except PlaywrightTimeoutError:
            pass

        snapshot = page_render_snapshot(page)
        pagination_state = get_pagination_state(page)
        last_snapshot = snapshot
        last_pagination_state = pagination_state
        stability_key = render_stability_key(snapshot)
        now = time.monotonic()

        empty_wait_elapsed = (now - started_at) * 1000 >= EOD_EMPTY_LIST_MIN_WAIT_MS
        if (
            empty_wait_elapsed
            and render_ready(snapshot)
            and (pagination_state.get("order_count") or 0) == 0
            and stability_key == previous_key
        ):
            if stable_since is not None and (now - stable_since) * 1000 >= EOD_EMPTY_LIST_STABLE_MS:
                return {
                    "ready": True,
                    "waited_for": "empty_order_list_render_stability",
                    "pagination_state": pagination_state,
                    "final_snapshot": snapshot,
                }
        else:
            stable_since = now if render_ready(snapshot) else None
            previous_key = stability_key

        page.wait_for_timeout(EOD_EMPTY_LIST_POLL_MS)

    raise RuntimeError(
        "DoktorABC orders page did not show the 100 rows control or a stable empty list. "
        f"Last snapshot: {last_snapshot}. Last pagination state: {last_pagination_state}"
    )


def click_ready_for_customer(page):
    candidates = (
        (
            "tab_role_name",
            page.get_by_role("tab", name=re.compile(r"\bReady\s+for\s+Customer\b", re.I)).first,
        ),
        (
            "ready_for_customer_tab_id",
            page.locator('button[role="tab"][id*="ready-for-customer"]').first,
        ),
        (
            "ready_for_customer_tab_text",
            page.locator('[role="tab"]:has-text("Ready for Customer")').first,
        ),
        (
            "ready_for_customer_button_text",
            page.locator('button:has-text("Ready for Customer")').first,
        ),
    )

    for name, locator in candidates:
        try:
            locator.wait_for(state="visible", timeout=EOD_READY_FOR_CUSTOMER_VISIBLE_TIMEOUT_MS)
            locator.click(timeout=EOD_READY_FOR_CUSTOMER_CLICK_TIMEOUT_MS)
            return name
        except PlaywrightTimeoutError:
            continue

    raise RuntimeError('Could not find visible "Ready for Customer" tab.')


def get_pagination_state(page):
    return page.evaluate(
        """
        () => {
          const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
          const orderReferenceFromMarker = (marker) => {
            const id = normalize(marker?.id);
            if (/-mark-order$/.test(id)) return id.replace(/-mark-order$/, "");
            const badgeMatch = id.match(/^order-(.+)-badge$/);
            if (badgeMatch) return badgeMatch[1];
            return normalize(marker?.innerText).replace(/^#/, "");
          };
          const pagination = document.querySelector("#pagination-container");
          const current = pagination?.querySelector('nav[aria-label="pagination"] a[aria-current="page"]');
          const next = pagination?.querySelector('nav[aria-label="pagination"] a[aria-label="Go to next page"]');
          const orderRefs = Array.from(document.querySelectorAll('button[id$="-mark-order"], [id^="order-"][id$="-badge"]'))
            .map(orderReferenceFromMarker)
            .filter(Boolean);

          let hasEnabledNext = false;
          let nextClass = "";
          let nextParentClass = "";
          if (next) {
            const nextStyle = window.getComputedStyle(next);
            const nextParent = next.closest("li");
            nextClass = next.className || "";
            nextParentClass = nextParent?.className || "";
            const classTokens = `${nextClass} ${nextParentClass}`.split(/\\s+/);
            const disabledByAttribute =
              next.getAttribute("aria-disabled") === "true" ||
              next.getAttribute("data-disabled") === "true" ||
              next.hasAttribute("disabled");
            const disabledByClass = classTokens.some((token) =>
              ["pointer-events-none", "opacity-50", "cursor-not-allowed"].includes(token)
            );
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
            last_order_reference: orderRefs[orderRefs.length - 1] || null,
            order_refs_signature: orderRefs.join("|"),
            order_count: orderRefs.length,
            has_next: hasEnabledNext,
            next_exists: Boolean(next),
            next_text: normalize(next?.innerText),
            next_class: nextClass,
            next_parent_class: nextParentClass,
            url: window.location.href,
          };
        }
        """
    )


def select_100_rows(page):
    debug = {"clicked": False, "before": get_pagination_state(page)}
    rows_100 = rows_100_locator(page)
    try:
        rows_100.wait_for(state="visible", timeout=EOD_SELECT_100_VISIBLE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        debug["wait_result"] = wait_for_order_list(page)
        debug["after"] = get_pagination_state(page)
        debug["skipped_reason"] = "rows_100_not_visible_on_empty_order_list"

        if not debug["wait_result"]["empty"]:
            raise RuntimeError("The 100 rows control is missing, but the order list is not empty.")

        return debug

    rows_100.click(timeout=EOD_SELECT_100_CLICK_TIMEOUT_MS)
    page.wait_for_load_state("domcontentloaded")
    debug["wait_result"] = wait_for_order_list(page)
    debug["after"] = get_pagination_state(page)
    debug["clicked"] = True

    return debug


def pagination_signature(state):
    return (
        state.get("current_page"),
        state.get("first_order_reference"),
        state.get("last_order_reference"),
        state.get("order_refs_signature"),
        state.get("order_count"),
        state.get("url"),
    )


def click_next_page(page, before_state):
    if not before_state.get("next_exists"):
        return False, before_state, "next_not_visible"

    if not before_state.get("has_next"):
        return False, before_state, "next_disabled"

    next_link = page.locator('#pagination-container nav[aria-label="pagination"] a[aria-label="Go to next page"]').first

    try:
        next_link.wait_for(state="visible", timeout=EOD_NEXT_VISIBLE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        return False, get_pagination_state(page), "next_not_visible"

    before_signature = pagination_signature(before_state)
    next_link.click(timeout=EOD_NEXT_CLICK_TIMEOUT_MS)
    page.wait_for_load_state("domcontentloaded")

    deadline = time.monotonic() + EOD_NEXT_CHANGE_TIMEOUT_MS / 1000
    last_state = before_state

    while time.monotonic() < deadline:
        page.wait_for_timeout(EOD_NEXT_CHANGE_POLL_MS)
        state = get_pagination_state(page)
        last_state = state

        if pagination_signature(state) != before_signature:
            wait_result = wait_for_order_list(page)
            return True, wait_result["pagination_state"], "page_changed"

    return False, last_state, "next_click_did_not_change_page"


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
  const orderReferenceFromMarker = (marker) => {
    const id = normalize(marker?.id);
    if (/-mark-order$/.test(id)) return id.replace(/-mark-order$/, "");
    const badgeMatch = id.match(/^order-(.+)-badge$/);
    if (badgeMatch) return badgeMatch[1];
    return normalize(marker?.innerText).replace(/^#/, "");
  };
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
    await delay(__PZN_POPUP_WAIT_MS__);

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
    await delay(__PZN_CLOSE_WAIT_MS__);

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

  const markers = Array.from(document.querySelectorAll('button[id$="-mark-order"], [id^="order-"][id$="-badge"]'));
  const roots = unique(markers.map(findOrderRoot));
  const orders = [];

  for (const root of roots) {
    if (!root) continue;

    const marker = root.querySelector('button[id$="-mark-order"], [id^="order-"][id$="-badge"]');
    const orderReference = orderReferenceFromMarker(marker);
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
SCRAPE_EOD_ORDERS_JS = (
    SCRAPE_EOD_ORDERS_JS.replace("__PZN_POPUP_WAIT_MS__", str(EOD_PZN_POPUP_WAIT_MS)).replace(
        "__PZN_CLOSE_WAIT_MS__",
        str(EOD_PZN_CLOSE_WAIT_MS),
    )
)


def scrape_orders_on_current_page(page):
    return page.evaluate(SCRAPE_EOD_ORDERS_JS)


def fetch_self_pickup_api_orders(page):
    result = page.evaluate(
        """
        async ({ query, limit }) => {
          const fragment = "incoming-self-pickup";
          const discoveredUrl = performance
            .getEntriesByType("resource")
            .map((entry) => entry.name)
            .find((url) => url.includes(fragment));
          const firstUrl = new URL(discoveredUrl || query, window.location.href);
          const pages = [];
          const orders = [];
          const seen = new Set();

          for (let offset = 0; offset < 10000; offset += limit) {
            const url = new URL(firstUrl.href);
            url.searchParams.set("limit", String(limit));
            url.searchParams.set("offset", String(offset));
            url.searchParams.set("sort", "false");
            url.searchParams.set("productIDs", "");
            url.searchParams.set("status", "ready-for-customer");
            url.searchParams.set("search", "");

            const response = await fetch(url.href, {
              credentials: "include",
              headers: { Accept: "application/json" },
            });
            const payload = await response.json();
            const results = Array.isArray(payload?.results) ? payload.results : [];

            pages.push({
              offset,
              url: url.href,
              status: response.status,
              count: payload?.count ?? null,
              results: results.length,
              first_order_reference: results[0]?.hashID ?? null,
              last_order_reference: results[results.length - 1]?.hashID ?? null,
            });

            for (const order of results) {
              const key = order?.hashID;
              if (!key || seen.has(key)) continue;
              seen.add(key);
              orders.push(order);
            }

            if (!response.ok || results.length < limit) {
              break;
            }

            const total = Number(payload?.count);
            if (Number.isFinite(total) && offset + limit >= total) {
              break;
            }
          }

          return { orders, pages };
        }
        """,
        {
            "query": SELF_PICKUP_ORDERS_API_QUERY,
            "limit": SELF_PICKUP_ORDERS_API_LIMIT,
        },
    )

    raw_orders = result.get("orders") if isinstance(result, dict) else []
    pages = result.get("pages") if isinstance(result, dict) else []

    return {
        "orders": raw_orders,
        "pages": pages,
        "steps": [
            {
                "name": "fetch_self_pickup_api_orders",
                "ok": True,
                "pages": pages,
                "orders_found": len(raw_orders),
            }
        ],
        "duplicate_order_references": [],
    }


def scrape_all_eod_orders(page, billing_date_collector=None):
    steps = []
    rows_debug = select_100_rows(page)
    steps.append({"name": "select_100_rows", "ok": True, **rows_debug})

    all_orders = []
    pages = []
    duplicate_order_references = []
    seen_order_references = set()

    for page_index in range(1, EOD_MAX_PAGES + 1):
        state_before = get_pagination_state(page)
        if billing_date_collector:
            billing_date_collector.collect_from_performance(page)

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
                "next_exists_before_click": state_before.get("next_exists"),
                "url_before_click": state_before.get("url"),
            }
        )

        changed, state_after, next_reason = click_next_page(page, state_before)
        steps.append(
            {
                "name": "click_next_page",
                "ok": changed,
                "reason": next_reason,
                "from_page": state_before.get("current_page"),
                "to_page": state_after.get("current_page"),
                "from_first_order": state_before.get("first_order_reference"),
                "to_first_order": state_after.get("first_order_reference"),
                "from_last_order": state_before.get("last_order_reference"),
                "to_last_order": state_after.get("last_order_reference"),
                "from_order_count": state_before.get("order_count"),
                "to_order_count": state_after.get("order_count"),
                "from_url": state_before.get("url"),
                "to_url": state_after.get("url"),
            }
        )

        if not changed:
            steps.append({"name": "pagination_finished", "ok": True, "reason": next_reason})
            break
    else:
        steps.append({"name": "pagination_finished", "ok": False, "reason": "max_pages_reached"})

    return {
        "orders": all_orders,
        "pages": pages,
        "steps": steps,
        "duplicate_order_references": duplicate_order_references,
    }


def open_saved_session(browser, target_url, order_type):
    if not os.path.exists(SESSION_STATE_PATH):
        return None

    print(f"trying saved DoktorABC session for {order_type} ...", flush=True)

    try:
        context = browser.new_context(
            storage_state=SESSION_STATE_PATH,
            **browser_context_options(),
        )
    except Exception as exc:
        print(
            f"saved DoktorABC session for {order_type} could not be opened: {type(exc).__name__}: {exc}",
            flush=True,
        )
        return None

    page = context.new_page()

    try:
        goto_page(page, target_url)
        logged_in = login_if_needed(page, context, target_url)
        wait_result = wait_for_orders_page(page, target_url)
        return context, page, not logged_in, wait_result
    except Exception as exc:
        context.close()
        print(f"saved DoktorABC session is expired or not ready: {type(exc).__name__}: {exc}", flush=True)
        return None


def open_fresh_session(browser, target_url, order_type, before_login_path=None):
    print(f"trying fresh DoktorABC login for {order_type} ...", flush=True)

    context = browser.new_context(**browser_context_options())
    page = context.new_page()

    goto_page(page, login_url())
    login_if_needed(page, context, target_url, before_login_path=before_login_path)
    wait_result = wait_for_orders_page(page, target_url)

    return context, page, False, wait_result


def open_authenticated_orders_page(browser, target_url, order_type, before_login_path=None):
    saved_session = open_saved_session(browser, target_url, order_type)

    if saved_session is not None:
        return saved_session

    return open_fresh_session(browser, target_url, order_type, before_login_path=before_login_path)


def open_authenticated_persistent_orders_page(context, target_url, order_type, before_login_path=None):
    print(f"trying persistent DoktorABC context for {order_type} ...", flush=True)

    page = context.pages[0] if context.pages else context.new_page()

    wait_before_opening_page()
    goto_page(page, target_url)
    logged_in = login_if_needed(page, context, target_url, before_login_path=before_login_path)
    wait_result = wait_for_orders_page(page, target_url)

    return context, page, not logged_in, wait_result


def open_authenticated_end_of_day_page(browser, before_login_path=None):
    return open_authenticated_orders_page(browser, end_of_day_url(), EOD_ORDER_TYPE, before_login_path=before_login_path)


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


def safe_slug(value):
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "orders"


def configured_order_targets():
    target_mode = configured_order_target_mode()
    configured_self_pickup_url = self_pickup_url()

    if target_mode == EOD_ORDER_LIST_TYPE:
        return [
            {
                "order_type": EOD_ORDER_TYPE,
                "target_url": end_of_day_url(),
            }
        ]

    if target_mode == PICKUP_READY_ORDER_LIST_TYPE:
        if not configured_self_pickup_url:
            raise RuntimeError("DOKTORABC_SELF_PICKUP_URL is required when EOD_ORDER_TARGET_MODE=pickup_ready")

        return [
            {
                "order_type": SELF_PICKUP_ORDER_TYPE,
                "target_url": configured_self_pickup_url,
            }
        ]

    targets = [
        {
            "order_type": EOD_ORDER_TYPE,
            "target_url": end_of_day_url(),
        }
    ]

    if configured_self_pickup_url:
        targets.append(
            {
                "order_type": SELF_PICKUP_ORDER_TYPE,
                "target_url": configured_self_pickup_url,
            }
        )

    return targets


def _sync_end_of_day_orders_unlocked():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-before-login-{timestamp}.png")
    failure_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-sync-failure-{timestamp}.png")
    steps = []
    targets = configured_order_targets()

    print("trying to sync DoktorABC order lists ...", flush=True)

    with sync_playwright() as playwright:
        browser = None
        context = None
        page = None
        all_rows = []
        target_results = []
        all_warnings = []
        all_duplicate_order_references = []
        screenshot_paths = []
        rows_by_order_type = {}
        notification_results = []

        try:
            first_target = targets[0]
            steps.append(
                {
                    "name": "open_authenticated_orders_page",
                    "ok": None,
                    "order_type": first_target["order_type"],
                    "target_url": first_target["target_url"],
                }
            )
            if bool_env("DOKTORABC_PERSISTENT_CONTEXT_ENABLED", True):
                context = launch_doktorabc_persistent_context(playwright)
                context, page, reused_session, wait_result = open_authenticated_persistent_orders_page(
                    context,
                    first_target["target_url"],
                    first_target["order_type"],
                    before_login_path=before_login_path,
                )
            else:
                browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
                wait_before_opening_page()
                context, page, reused_session, wait_result = open_authenticated_orders_page(
                    browser,
                    first_target["target_url"],
                    first_target["order_type"],
                    before_login_path=before_login_path,
                )
            steps[-1] = {
                "name": "open_authenticated_orders_page",
                "ok": True,
                "order_type": first_target["order_type"],
                "target_url": first_target["target_url"],
                "reused_session": reused_session,
                "current_url": page.url,
                "wait_result": wait_result,
            }

            for target in targets:
                order_type = target["order_type"]
                target_url = target["target_url"]
                billing_date_collector = None

                steps.append(
                    {
                        "name": "open_order_type_page",
                        "ok": None,
                        "order_type": order_type,
                        "target_url": target_url,
                    }
                )
                if order_type == SELF_PICKUP_ORDER_TYPE:
                    billing_date_collector = SelfPickupBillingDateCollector()
                    page.on("response", billing_date_collector.capture_response)

                goto_page(page, target_url)
                ready_for_customer_clicked = False
                ready_for_customer_click_strategy = None
                if order_type == SELF_PICKUP_ORDER_TYPE:
                    wait_for_load_states(page)
                    ready_for_customer_click_strategy = click_ready_for_customer(page)
                    ready_for_customer_clicked = True
                    page.wait_for_load_state("domcontentloaded")
                    target_wait_result = wait_for_rows_100_control(page)
                else:
                    target_wait_result = wait_for_orders_page(page, target_url)
                steps[-1] = {
                    "name": "open_order_type_page",
                    "ok": True,
                    "order_type": order_type,
                    "target_url": target_url,
                    "ready_for_customer_clicked": ready_for_customer_clicked,
                    "ready_for_customer_click_strategy": ready_for_customer_click_strategy,
                    "current_url": page.url,
                    "wait_result": target_wait_result,
                    "billing_date_network": billing_date_collector.snapshot() if billing_date_collector else None,
                }

                steps.append({"name": "scrape_all_pages", "ok": None, "order_type": order_type})
                if order_type == SELF_PICKUP_ORDER_TYPE:
                    scrape_result = fetch_self_pickup_api_orders(page)
                    fallback_reason = None
                    if not scrape_result["orders"] and (get_pagination_state(page).get("order_count") or 0) > 0:
                        fallback_reason = "api_fetch_empty_but_page_has_orders"
                    elif any((page_result.get("status") or 0) >= 400 for page_result in scrape_result.get("pages", [])):
                        fallback_reason = "api_fetch_failed"

                    if fallback_reason:
                        fallback_result = scrape_all_eod_orders(page, billing_date_collector=billing_date_collector)
                        scrape_result = {
                            **fallback_result,
                            "steps": [
                                *scrape_result.get("steps", []),
                                {
                                    "name": "fallback_to_dom_scrape",
                                    "ok": True,
                                    "reason": fallback_reason,
                                },
                                *fallback_result.get("steps", []),
                            ],
                            "api_pages": scrape_result.get("pages", []),
                        }
                else:
                    scrape_result = scrape_all_eod_orders(page, billing_date_collector=billing_date_collector)
                raw_orders = scrape_result["orders"]
                if order_type == SELF_PICKUP_ORDER_TYPE and raw_orders and raw_orders[0].get("hashID"):
                    rows = [normalize_self_pickup_api_order(order) for order in raw_orders]
                else:
                    billing_dates_by_reference = (
                        billing_date_collector.billing_dates_by_reference if billing_date_collector else None
                    )
                    rows = [
                        normalize_scraped_order(order, order_type, billing_dates_by_reference)
                        for order in raw_orders
                    ]
                invalid_orders, warnings = validate_orders(rows, raw_orders)
                screenshot_path = os.path.join(
                    ARTIFACTS_DIR,
                    f"doktorabc-{safe_slug(order_type)}-after-sync-{timestamp}.png",
                )
                page.screenshot(path=screenshot_path, full_page=True)
                screenshot_paths.append(screenshot_path)

                target_result = {
                    "order_type": order_type,
                    "target_url": target_url,
                    "current_url": page.url,
                    "scraped": len(raw_orders),
                    "valid_rows": len(rows) - len(invalid_orders),
                    "invalid_count": len(invalid_orders),
                    "warnings_count": len(warnings),
                    "warnings": warnings[:20],
                    "pages": scrape_result["pages"],
                    "scrape_steps": scrape_result["steps"],
                    "duplicate_order_references": scrape_result["duplicate_order_references"],
                    "billing_date_network": billing_date_collector.snapshot() if billing_date_collector else None,
                    "screenshot_path": screenshot_path,
                }
                target_results.append(target_result)
                all_warnings.extend(warnings)
                all_duplicate_order_references.extend(
                    f"{order_type}:{order_reference}"
                    for order_reference in scrape_result["duplicate_order_references"]
                )

                steps[-1] = {
                    "name": "scrape_all_pages",
                    "ok": True,
                    "order_type": order_type,
                    "scraped": len(raw_orders),
                    "pages": scrape_result["pages"],
                    "scrape_steps": scrape_result["steps"],
                    "duplicate_order_references": scrape_result["duplicate_order_references"],
                    "warnings": warnings,
                    "invalid_count": len(invalid_orders),
                    "billing_date_network": billing_date_collector.snapshot() if billing_date_collector else None,
                }

                if invalid_orders:
                    rows_by_order_type[order_type] = rows
                    error_message = "Some scraped orders are missing required fields. Supabase was not changed."
                    steps.append(
                        {
                            "name": "send_failure_notification",
                            "ok": None,
                            "order_type": order_type,
                            "failed_step": "validate_scraped_orders",
                        }
                    )
                    failure_notification_result = send_failure_notification(
                        notification_order_list_type(order_type),
                        timestamp,
                        error_message,
                        "validate_scraped_orders",
                        page_url=page.url,
                        screenshot_path=screenshot_path,
                        rows_by_order_type=rows_by_order_type,
                        extra={
                            "invalid_count": len(invalid_orders),
                            "invalid_examples": invalid_orders[:10],
                            "warnings": warnings[:20],
                        },
                    )
                    notification_results.append(
                        {
                            "order_type": order_type,
                            "order_list_type": notification_order_list_type(order_type),
                            **failure_notification_result,
                        }
                    )
                    steps[-1] = {
                        "name": "send_failure_notification",
                        "ok": True,
                        "order_type": order_type,
                        "order_list_type": notification_order_list_type(order_type),
                        **failure_notification_result,
                    }

                    return JSONResponse(
                        status_code=422,
                        content={
                            "ok": False,
                            "failed_step": "validate_scraped_orders",
                            "error": error_message,
                            "order_type": order_type,
                            "current_url": page.url,
                            "reused_session": reused_session,
                            "session_state_path": SESSION_STATE_PATH,
                            "supabase_schema": SUPABASE_SCHEMA,
                            "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                            "scraped": len(raw_orders),
                            "invalid_count": len(invalid_orders),
                            "invalid_examples": invalid_orders[:10],
                            "warnings": warnings[:20],
                            "targets": target_results,
                            "steps": steps,
                            "screenshot_paths": screenshot_paths,
                            "notifications": notification_results,
                        },
                    )

                rows_by_order_type[order_type] = rows
                all_rows.extend(rows)

            if not all_rows:
                order_notification_summary = {"sent_to_supabase": 0}
                steps.append({"name": "send_order_notification", "ok": None, "order_list_type": COMBINED_ORDER_LIST_TYPE})
                notification_result = send_orders_sync_notification(
                    rows_by_order_type,
                    targets,
                    timestamp,
                    order_notification_summary,
                )
                notification_results.append(
                    {
                        "order_list_type": COMBINED_ORDER_LIST_TYPE,
                        "order_count": 0,
                        **notification_result,
                    }
                )
                steps[-1] = {
                    "name": "send_order_notification",
                    "ok": True,
                    "order_list_type": COMBINED_ORDER_LIST_TYPE,
                    "order_count": 0,
                    **notification_result,
                }

                steps.append(
                    {
                        "name": "export_eod_excel_to_n8n",
                        "ok": None,
                        "eod_order_count": len(rows_by_order_type.get(EOD_ORDER_TYPE, [])),
                    }
                )
                export_result = maybe_export_end_of_day_excel_to_n8n(
                    page,
                    timestamp,
                    {
                        "scraped": 0,
                        "saved": 0,
                        "sent_to_supabase": 0,
                        "targets_count": len(target_results),
                    },
                    rows_by_order_type,
                )
                steps[-1] = {"name": "export_eod_excel_to_n8n", "ok": True, **export_result}

                if export_result.get("skipped"):
                    steps.append(
                        {
                            "name": "send_excel_export_notification",
                            "ok": True,
                            "skipped": True,
                            "skipped_reason": export_result.get("skipped_reason"),
                        }
                    )
                else:
                    steps.append({"name": "send_excel_export_notification", "ok": None})
                    excel_notification_result = send_excel_export_notification(export_result, timestamp)
                    notification_results.append(
                        {
                            "order_type": "excel_export",
                            **excel_notification_result,
                        }
                    )
                    steps[-1] = {"name": "send_excel_export_notification", "ok": True, **excel_notification_result}

                return {
                    "ok": True,
                    "current_url": page.url,
                    "page_title": page.title(),
                    "reused_session": reused_session,
                    "session_state_path": SESSION_STATE_PATH,
                    "supabase_schema": SUPABASE_SCHEMA,
                    "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                    "scraped": 0,
                    "saved": 0,
                    "sent_to_supabase": 0,
                    "warnings_count": len(all_warnings),
                    "warnings": all_warnings[:20],
                    "duplicate_order_references": all_duplicate_order_references,
                    "targets": target_results,
                    "sample_orders": [],
                    "steps": steps,
                    "screenshot_paths": screenshot_paths,
                    "export": export_result,
                    "notifications": notification_results,
                    "message": "No EOD or self pickup orders were found. Supabase was not changed.",
                }

            steps.append({"name": "upsert_supabase", "ok": None, "rows": len(all_rows)})
            supabase_result = upsert_supabase_eod_orders(all_rows)
            steps[-1] = {"name": "upsert_supabase", "ok": True, **supabase_result}

            steps.append({"name": "send_order_notification", "ok": None, "order_list_type": COMBINED_ORDER_LIST_TYPE})
            notification_result = send_orders_sync_notification(
                rows_by_order_type,
                targets,
                timestamp,
                supabase_result,
            )
            notification_results.append(
                {
                    "order_list_type": COMBINED_ORDER_LIST_TYPE,
                    "order_count": len(all_rows),
                    **notification_result,
                }
            )
            steps[-1] = {
                "name": "send_order_notification",
                "ok": True,
                "order_list_type": COMBINED_ORDER_LIST_TYPE,
                "order_count": len(all_rows),
                **notification_result,
            }

            steps.append(
                {
                    "name": "export_eod_excel_to_n8n",
                    "ok": None,
                    "eod_order_count": len(rows_by_order_type.get(EOD_ORDER_TYPE, [])),
                }
            )
            export_result = maybe_export_end_of_day_excel_to_n8n(
                page,
                timestamp,
                {
                    "scraped": len(all_rows),
                    "saved": len(all_rows),
                    "sent_to_supabase": supabase_result.get("sent_to_supabase"),
                    "targets_count": len(target_results),
                    "warnings_count": len(all_warnings),
                    "duplicate_order_references_count": len(all_duplicate_order_references),
                },
                rows_by_order_type,
            )
            steps[-1] = {"name": "export_eod_excel_to_n8n", "ok": True, **export_result}

            if export_result.get("skipped"):
                steps.append(
                    {
                        "name": "send_excel_export_notification",
                        "ok": True,
                        "skipped": True,
                        "skipped_reason": export_result.get("skipped_reason"),
                    }
                )
            else:
                steps.append({"name": "send_excel_export_notification", "ok": None})
                excel_notification_result = send_excel_export_notification(export_result, timestamp)
                notification_results.append(
                    {
                        "order_type": "excel_export",
                        **excel_notification_result,
                    }
                )
                steps[-1] = {"name": "send_excel_export_notification", "ok": True, **excel_notification_result}

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "supabase_schema": SUPABASE_SCHEMA,
                "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                "scraped": len(all_rows),
                "saved": len(all_rows),
                "warnings_count": len(all_warnings),
                "warnings": all_warnings[:20],
                "duplicate_order_references": all_duplicate_order_references,
                "targets": target_results,
                "sample_orders": all_rows[:3],
                "steps": steps,
                "screenshot_paths": screenshot_paths,
                "export": export_result,
                "notifications": notification_results,
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

            failed_step = next((step["name"] for step in reversed(steps) if step.get("ok") is None), "unknown")
            error_message = f"{type(exc).__name__}: {exc}"
            failure_notification_results = []
            for failure_part in failure_parts_for_step(failed_step, steps, targets):
                notification_result = send_failure_notification(
                    failure_part,
                    timestamp,
                    error_message,
                    failed_step,
                    page_url=page.url if page else None,
                    screenshot_path=failure_screenshot_path,
                    rows_by_order_type=rows_by_order_type,
                    extra={
                        "supabase_schema": SUPABASE_SCHEMA,
                        "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                    },
                )
                failure_notification_results.append(
                    {
                        "order_list_type": failure_part,
                        **notification_result,
                    }
                )
            notification_results.extend(failure_notification_results)

            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "failed_step": failed_step,
                    "error": error_message,
                    "current_url": page.url if page else None,
                    "session_state_path": SESSION_STATE_PATH,
                    "supabase_schema": SUPABASE_SCHEMA,
                    "supabase_table": SUPABASE_EOD_ORDERS_TABLE,
                    "steps": steps,
                    "screenshot_path": failure_screenshot_path,
                    "notifications": notification_results,
                },
            )
        finally:
            if context:
                context.close()
            if browser:
                browser.close()


def sync_end_of_day_orders():
    with BROWSER_RUN_LOCK:
        return _sync_end_of_day_orders_unlocked()


def payload_bool(value, default=False):
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return value != 0

    return str(value).strip().lower() in {"1", "true", "yes", "on", "dry_run", "dry-run"}


def normalize_pickup_done_order_references(payload):
    raw = (
        payload.get("order_references")
        or payload.get("orderReferences")
        or payload.get("order_ids")
        or payload.get("orderIds")
        or payload.get("orders")
        or payload.get("text")
    )
    values = raw if isinstance(raw, list) else re.split(r"[\s,;]+", raw) if isinstance(raw, str) else []
    seen = set()
    references = []

    for value in values:
        reference = order_reference_key(value)
        if not reference or reference in seen:
            continue

        seen.add(reference)
        references.append(reference)

    return references


def pickup_done_button_locator(page, order_reference):
    reference = order_reference_key(order_reference)
    marker_selector = (
        f'[id="order-{reference}-badge"], '
        f'[id="{reference}-mark-order"], '
        f'label[for="{reference}-mark-order"]'
    )
    button_xpath = (
        'xpath=.//button[@id="self-pickup-done-button" or '
        'contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), '
        '"self pickup done")]'
    )
    root_xpath = (
        'xpath=ancestor::div[.//button[@id="self-pickup-done-button" or '
        'contains(translate(normalize-space(.), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), '
        '"self pickup done")]][1]'
    )
    debug = {
        "order_reference": reference,
        "marker_selector": marker_selector,
        "button_selector": 'button#self-pickup-done-button or text "Self pickup done"',
    }

    marker = page.locator(marker_selector).first
    try:
        marker.wait_for(state="attached", timeout=PICKUP_DONE_ORDER_PROBE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        return None, {**debug, "marker_found": False}

    root = marker.locator(root_xpath)
    try:
        root.wait_for(state="attached", timeout=PICKUP_DONE_ORDER_PROBE_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        return None, {**debug, "marker_found": True, "card_with_button_found": False}

    button = root.locator(button_xpath).first
    return button, {**debug, "marker_found": True, "card_with_button_found": True}


def pickup_done_button_snapshot(button):
    try:
        return button.evaluate(
            """
            (element) => {
              const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim();
              const rect = element.getBoundingClientRect();
              return {
                id: element.id || null,
                text: normalize(element.innerText || element.textContent || ""),
                disabled: Boolean(element.disabled),
                aria_disabled: element.getAttribute("aria-disabled"),
                class_name: element.className || "",
                rect: {
                  x: Math.round(rect.x),
                  y: Math.round(rect.y),
                  width: Math.round(rect.width),
                  height: Math.round(rect.height),
                },
              };
            }
            """
        )
    except Exception as exc:
        return {"snapshot_error": f"{type(exc).__name__}: {exc}"}


def try_pickup_done_on_current_page(page, order_reference, dry_run):
    pagination_state = get_pagination_state(page)
    button, debug = pickup_done_button_locator(page, order_reference)

    if button is None:
        return {
            "order_reference": order_reference,
            "status": "not_found_on_page",
            "message": "Order card or Self pickup done button was not found on this page.",
            "dry_run": dry_run,
            "pagination_state": pagination_state,
            "debug": debug,
        }

    try:
        button.wait_for(state="visible", timeout=PICKUP_DONE_BUTTON_VISIBLE_TIMEOUT_MS)
        button.scroll_into_view_if_needed(timeout=PICKUP_DONE_BUTTON_VISIBLE_TIMEOUT_MS)
        button_snapshot = pickup_done_button_snapshot(button)
        button_visible = button.is_visible()
        button_enabled = button.is_enabled()
        button.click(timeout=PICKUP_DONE_BUTTON_CLICK_TIMEOUT_MS, trial=True)

        if dry_run:
            return {
                "order_reference": order_reference,
                "status": "clickable",
                "message": "Dry run passed. Playwright can click the Self pickup done button.",
                "dry_run": True,
                "would_click": True,
                "button_visible": button_visible,
                "button_enabled": button_enabled,
                "button": button_snapshot,
                "pagination_state": pagination_state,
                "debug": debug,
            }

        button.click(timeout=PICKUP_DONE_BUTTON_CLICK_TIMEOUT_MS)
        page.wait_for_timeout(PICKUP_DONE_AFTER_CLICK_WAIT_MS)
        still_visible = page.locator(f'[id="order-{order_reference_key(order_reference)}-badge"]').count() > 0

        return {
            "order_reference": order_reference,
            "status": "clicked_still_visible" if still_visible else "clicked",
            "message": (
                "Self pickup done was clicked, but the order is still visible."
                if still_visible
                else "Self pickup done was clicked and the order disappeared from the visible list."
            ),
            "dry_run": False,
            "would_click": True,
            "clicked": True,
            "order_still_visible": still_visible,
            "button_visible": button_visible,
            "button_enabled": button_enabled,
            "button": button_snapshot,
            "pagination_state": pagination_state,
            "debug": debug,
        }
    except Exception as exc:
        return {
            "order_reference": order_reference,
            "status": "not_clickable",
            "message": f"{type(exc).__name__}: {exc}",
            "dry_run": dry_run,
            "would_click": False,
            "button": pickup_done_button_snapshot(button),
            "pagination_state": pagination_state,
            "debug": debug,
        }


def prepare_pickup_done_page(page, target_url):
    goto_page(page, target_url)
    wait_for_load_states(page)
    ready_for_customer_click_strategy = click_ready_for_customer(page)

    if EOD_AFTER_READY_FOR_CUSTOMER_CLICK_WAIT_MS > 0:
        page.wait_for_timeout(EOD_AFTER_READY_FOR_CUSTOMER_CLICK_WAIT_MS)

    page.wait_for_load_state("domcontentloaded")
    wait_result = wait_for_rows_100_control(page)
    rows_debug = select_100_rows(page)

    return {
        "target_url": target_url,
        "current_url": page.url,
        "ready_for_customer_click_strategy": ready_for_customer_click_strategy,
        "wait_result": wait_result,
        "rows_debug": rows_debug,
    }


def find_and_try_pickup_done_order(page, order_reference, dry_run):
    checked_pages = []

    for page_index in range(1, EOD_MAX_PAGES + 1):
        result = try_pickup_done_on_current_page(page, order_reference, dry_run)
        result["page_index"] = page_index

        if result["status"] != "not_found_on_page":
            result["pages_checked"] = checked_pages
            return result

        state_before = result.get("pagination_state") or get_pagination_state(page)
        checked_pages.append(
            {
                "page_index": page_index,
                "current_page": state_before.get("current_page"),
                "order_count": state_before.get("order_count"),
                "first_order_reference": state_before.get("first_order_reference"),
                "last_order_reference": state_before.get("last_order_reference"),
                "has_next": state_before.get("has_next"),
                "next_exists": state_before.get("next_exists"),
            }
        )
        changed, state_after, next_reason = click_next_page(page, state_before)

        if not changed:
            return {
                "order_reference": order_reference,
                "status": "not_found",
                "message": "Order was not found after checking all reachable Self Pickup pages.",
                "dry_run": dry_run,
                "would_click": False,
                "page_index": page_index,
                "pages_checked": checked_pages,
                "pagination_state": state_after,
                "next_reason": next_reason,
            }

    return {
        "order_reference": order_reference,
        "status": "not_found",
        "message": f"Order was not found before EOD_MAX_PAGES={EOD_MAX_PAGES} was reached.",
        "dry_run": dry_run,
        "would_click": False,
        "pages_checked": checked_pages,
    }


def _mark_pickup_done_orders_unlocked(payload):
    order_references = normalize_pickup_done_order_references(payload)
    dry_run = payload_bool(
        payload.get("dry_run", payload.get("dryRun")),
        default=bool_env("PICKUP_DONE_DRY_RUN", True),
    )

    if not order_references:
        return JSONResponse(status_code=400, content={"ok": False, "error": "no_order_references"})

    if len(order_references) > PICKUP_DONE_MAX_ORDER_REFERENCES:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": f"too_many_order_references_max_{PICKUP_DONE_MAX_ORDER_REFERENCES}",
            },
        )

    target_url = self_pickup_url()
    if not target_url:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "DOKTORABC_SELF_PICKUP_URL is required."},
        )

    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    mode_slug = "dry-run" if dry_run else "click"
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-pickup-done-before-login-{timestamp}.png")
    screenshot_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-pickup-done-{mode_slug}-{timestamp}.png")
    failure_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-pickup-done-failure-{timestamp}.png")
    browser = None
    context = None
    page = None
    steps = []

    try:
        with sync_playwright() as playwright:
            steps.append(
                {
                    "name": "open_authenticated_self_pickup_page",
                    "ok": None,
                    "target_url": target_url,
                }
            )

            if bool_env("DOKTORABC_PERSISTENT_CONTEXT_ENABLED", True):
                context = launch_doktorabc_persistent_context(playwright)
                context, page, reused_session, wait_result = open_authenticated_persistent_orders_page(
                    context,
                    target_url,
                    SELF_PICKUP_ORDER_TYPE,
                    before_login_path=before_login_path,
                )
            else:
                browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
                wait_before_opening_page()
                context, page, reused_session, wait_result = open_authenticated_orders_page(
                    browser,
                    target_url,
                    SELF_PICKUP_ORDER_TYPE,
                    before_login_path=before_login_path,
                )

            steps[-1] = {
                "name": "open_authenticated_self_pickup_page",
                "ok": True,
                "target_url": target_url,
                "current_url": page.url,
                "reused_session": reused_session,
                "wait_result": wait_result,
            }

            results = []
            for order_reference in order_references:
                steps.append({"name": "prepare_pickup_done_page", "ok": None, "order_reference": order_reference})
                prepare_result = prepare_pickup_done_page(page, target_url)
                steps[-1] = {
                    "name": "prepare_pickup_done_page",
                    "ok": True,
                    "order_reference": order_reference,
                    **prepare_result,
                }

                steps.append({"name": "try_self_pickup_done", "ok": None, "order_reference": order_reference})
                result = find_and_try_pickup_done_order(page, order_reference, dry_run)
                result["prepare"] = prepare_result
                results.append(result)
                steps[-1] = {
                    "name": "try_self_pickup_done",
                    "ok": result["status"] in {"clickable", "clicked", "clicked_still_visible"},
                    "order_reference": order_reference,
                    "status": result["status"],
                    "message": result["message"],
                }

            try:
                page.screenshot(path=screenshot_path, full_page=True)
            except Exception as exc:
                screenshot_path = None
                steps.append(
                    {
                        "name": "screenshot",
                        "ok": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

            clickable = sum(1 for result in results if result["status"] == "clickable")
            clicked = sum(1 for result in results if result["status"] in {"clicked", "clicked_still_visible"})
            not_found = sum(1 for result in results if result["status"] == "not_found")
            errors = sum(1 for result in results if result["status"] not in {"clickable", "clicked", "clicked_still_visible"})

            return {
                "ok": True,
                "dry_run": dry_run,
                "checked": len(results),
                "clickable": clickable,
                "clicked": clicked,
                "not_found": not_found,
                "errors": errors,
                "current_url": page.url,
                "page_title": page.title(),
                "target_url": target_url,
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "persistent_context_dir": PERSISTENT_CONTEXT_DIR,
                "screenshot_path": screenshot_path,
                "results": results,
                "steps": steps,
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
                "dry_run": dry_run,
                "error": f"{type(exc).__name__}: {exc}",
                "current_url": page.url if page else None,
                "target_url": target_url,
                "session_state_path": SESSION_STATE_PATH,
                "screenshot_path": failure_screenshot_path,
                "steps": steps,
            },
        )
    finally:
        if context:
            try:
                context.close()
            except Exception as exc:
                log_event("browser_context_cleanup_failed", error=f"{type(exc).__name__}: {exc}")
        if browser:
            try:
                browser.close()
            except Exception as exc:
                log_event("browser_cleanup_failed", error=f"{type(exc).__name__}: {exc}")


def mark_pickup_done_orders(payload):
    with BROWSER_RUN_LOCK:
        return _mark_pickup_done_orders_unlocked(payload or {})


def parse_clock_minutes(value):
    cleaned = (value or "").strip()
    if not cleaned:
        return None

    match = re.match(r"^(\d{1,2}):(\d{2})$", cleaned)
    if not match:
        raise RuntimeError(f"Invalid closed-hours time {value!r}. Use HH:MM.")

    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        raise RuntimeError(f"Invalid closed-hours time {value!r}. Use HH:MM.")

    return hour * 60 + minute


def pickup_ready_auto_sync_closed_now():
    closed_from = parse_clock_minutes(os.environ.get("PICKUP_READY_AUTO_SYNC_CLOSED_FROM"))
    closed_to = parse_clock_minutes(os.environ.get("PICKUP_READY_AUTO_SYNC_CLOSED_TO"))

    if closed_from is None or closed_to is None:
        return False

    now = datetime.now(local_timezone())
    now_minutes = now.hour * 60 + now.minute

    if closed_from <= closed_to:
        return closed_from <= now_minutes < closed_to

    return now_minutes >= closed_from or now_minutes < closed_to


def pickup_ready_auto_sync_interval_seconds():
    return bounded_random_seconds(
        PICKUP_READY_AUTO_SYNC_MIN_MINUTES * 60,
        PICKUP_READY_AUTO_SYNC_MAX_MINUTES * 60,
    )


def response_to_auto_sync_summary(response):
    if isinstance(response, JSONResponse):
        try:
            content = json.loads(response.body.decode("utf-8"))
        except Exception:
            content = None

        return {
            "status_code": response.status_code,
            "ok": bool(content.get("ok")) if isinstance(content, dict) else response.status_code < 400,
            "content": content,
        }

    if isinstance(response, dict):
        return {
            "status_code": 200,
            "ok": bool(response.get("ok", True)),
            "scraped": response.get("scraped"),
            "saved": response.get("saved"),
            "sent_to_supabase": response.get("sent_to_supabase"),
        }

    return {"status_code": 200, "ok": True, "content_type": type(response).__name__}


def run_pickup_ready_auto_sync_once():
    started_at = datetime.now(timezone.utc)
    AUTO_SYNC_STATE.update(
        {
            "running": True,
            "last_started_at": started_at.isoformat(),
            "last_finished_at": None,
            "last_error": None,
            "skipped_reason": None,
        }
    )
    log_event("pickup_ready_auto_sync_started", started_at=started_at.isoformat())

    try:
        result = response_to_auto_sync_summary(sync_end_of_day_orders())
        AUTO_SYNC_STATE["last_result"] = result
        log_event("pickup_ready_auto_sync_finished", result=result)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        AUTO_SYNC_STATE["last_error"] = error
        log_event("pickup_ready_auto_sync_failed", error=error)
    finally:
        AUTO_SYNC_STATE["running"] = False
        AUTO_SYNC_STATE["last_finished_at"] = datetime.now(timezone.utc).isoformat()


def pickup_ready_auto_sync_loop():
    if PICKUP_READY_AUTO_SYNC_INITIAL_DELAY_SECONDS > 0:
        AUTO_SYNC_STOP_EVENT.wait(PICKUP_READY_AUTO_SYNC_INITIAL_DELAY_SECONDS)

    while not AUTO_SYNC_STOP_EVENT.is_set():
        try:
            if pickup_ready_auto_sync_closed_now():
                AUTO_SYNC_STATE["skipped_reason"] = "store_closed_window"
                log_event(
                    "pickup_ready_auto_sync_skipped",
                    reason="store_closed_window",
                    closed_from=os.environ.get("PICKUP_READY_AUTO_SYNC_CLOSED_FROM"),
                    closed_to=os.environ.get("PICKUP_READY_AUTO_SYNC_CLOSED_TO"),
                )
            else:
                run_pickup_ready_auto_sync_once()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            AUTO_SYNC_STATE["last_error"] = error
            log_event("pickup_ready_auto_sync_loop_error", error=error)

        interval_seconds = pickup_ready_auto_sync_interval_seconds()
        next_run_at = datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
        AUTO_SYNC_STATE["next_run_at"] = next_run_at.isoformat()
        log_event(
            "pickup_ready_auto_sync_next_run_scheduled",
            seconds=round(interval_seconds, 3),
            next_run_at=next_run_at.isoformat(),
        )
        AUTO_SYNC_STOP_EVENT.wait(interval_seconds)


@app.on_event("startup")
def start_pickup_ready_auto_sync():
    global AUTO_SYNC_THREAD

    enabled = bool_env("PICKUP_READY_AUTO_SYNC_ENABLED", True)
    AUTO_SYNC_STATE["enabled"] = enabled

    if not enabled:
        log_event("pickup_ready_auto_sync_disabled")
        return

    if AUTO_SYNC_THREAD and AUTO_SYNC_THREAD.is_alive():
        return

    AUTO_SYNC_STOP_EVENT.clear()
    AUTO_SYNC_THREAD = threading.Thread(
        target=pickup_ready_auto_sync_loop,
        name="pickup-ready-auto-sync",
        daemon=True,
    )
    AUTO_SYNC_THREAD.start()
    log_event(
        "pickup_ready_auto_sync_started_thread",
        interval_min_minutes=PICKUP_READY_AUTO_SYNC_MIN_MINUTES,
        interval_max_minutes=PICKUP_READY_AUTO_SYNC_MAX_MINUTES,
        initial_delay_seconds=PICKUP_READY_AUTO_SYNC_INITIAL_DELAY_SECONDS,
        closed_from=os.environ.get("PICKUP_READY_AUTO_SYNC_CLOSED_FROM"),
        closed_to=os.environ.get("PICKUP_READY_AUTO_SYNC_CLOSED_TO"),
    )


@app.on_event("shutdown")
def stop_pickup_ready_auto_sync():
    AUTO_SYNC_STOP_EVENT.set()
    if AUTO_SYNC_THREAD and AUTO_SYNC_THREAD.is_alive():
        AUTO_SYNC_THREAD.join(timeout=5)


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
        "target_url": end_of_day_url(),
        "self_pickup_url": self_pickup_url(),
        "order_target_mode": configured_order_target_mode(),
        "session_state_path": SESSION_STATE_PATH,
        "session_state_exists": os.path.exists(SESSION_STATE_PATH),
        "persistent_context_dir": PERSISTENT_CONTEXT_DIR,
        "persistent_context_enabled": bool_env("DOKTORABC_PERSISTENT_CONTEXT_ENABLED", True),
        "auto_sync": AUTO_SYNC_STATE,
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


@app.post("/jobs/pickup-ready/orders/sync")
def pickup_ready_orders_sync():
    return sync_end_of_day_orders()


@app.post("/jobs/pickup-ready/orders")
def pickup_ready_orders():
    return sync_end_of_day_orders()


@app.post("/jobs/pickup-ready/orders/mark-picked")
def pickup_ready_orders_mark_picked(payload: dict):
    return mark_pickup_done_orders(payload)


@app.post("/jobs/pickup-ready/orders/self-pickup-done")
def pickup_ready_orders_self_pickup_done(payload: dict):
    return mark_pickup_done_orders(payload)


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})
