import mimetypes
import glob
import hmac
import os
import re
import time
import zipfile
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import xml.etree.ElementTree as ElementTree

import httpx
from fastapi import FastAPI, Header
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
DEFAULT_DOKTORABC_LOGIN_URL = "https://pharmacies.doktorabc.com/manage-supplies"
DEFAULT_DOKTORABC_LOGIN_FORM_URL = "https://pharmacies.doktorabc.com/login"
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
EOD_STORAGE_UPLOAD_TIMEOUT_SECONDS = int_env("EOD_STORAGE_UPLOAD_TIMEOUT_SECONDS", 30)
EOD_N8N_UPLOAD_TIMEOUT_SECONDS = 30
EOD_NOTIFICATION_TIMEOUT_SECONDS = 30
EOD_DEDUPE_TIMEZONE = (os.environ.get("EOD_DEDUPE_TIMEZONE") or "Europe/Berlin").strip() or "Europe/Berlin"
EOD_EXPORT_DOWNLOAD_TIMEOUT_MS = 20_000
EOD_EXPORT_BUTTON_VISIBLE_TIMEOUT_MS = 5_000
EOD_EXPORT_BUTTON_CLICK_TIMEOUT_MS = 10_000
EOD_LOGIN_FORM_CHECK_TIMEOUT_MS = 800
EOD_LOGIN_UI_READY_TIMEOUT_MS = int_env("EOD_LOGIN_UI_READY_TIMEOUT_MS", 45_000)
EOD_LOGIN_UI_READY_RETRY_TIMEOUT_MS = int_env("EOD_LOGIN_UI_READY_RETRY_TIMEOUT_MS", 30_000)
EOD_LOGIN_FIELD_TIMEOUT_MS = 5_000
EOD_PHARMACIST_ROLE_CLICK_TIMEOUT_MS = 5_000
EOD_LOGIN_BUTTON_CLICK_TIMEOUT_MS = 10_000
EOD_LOGIN_SUCCESS_TIMEOUT_MS = int_env("EOD_LOGIN_SUCCESS_TIMEOUT_MS", 45_000)
EOD_LOAD_STATE_DOMCONTENTLOADED_TIMEOUT_MS = 10_000
EOD_LOAD_STATE_LOAD_TIMEOUT_MS = 10_000
EOD_LOAD_STATE_NETWORKIDLE_TIMEOUT_MS = 5_000
EOD_WAIT_FOR_NETWORKIDLE = (
    os.environ.get("EOD_WAIT_FOR_NETWORKIDLE", "false").strip().lower() in {"1", "true", "yes", "on"}
)
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
DOKTORABC_CLICK_BLOCKER_TIMEOUT_MS = int_env("DOKTORABC_CLICK_BLOCKER_TIMEOUT_MS", 5_000)
DOKTORABC_CLICK_BLOCKER_POLL_MS = int_env("DOKTORABC_CLICK_BLOCKER_POLL_MS", 100)
EOD_AFTER_LOGIN_CLICK_WAIT_MS = 2_000
EOD_PZN_POPUP_WAIT_MS = int_env("EOD_PZN_POPUP_WAIT_MS", 180)
EOD_PZN_CLOSE_WAIT_MS = int_env("EOD_PZN_CLOSE_WAIT_MS", 50)
EOD_API_FETCH_ENABLED = (
    os.environ.get("EOD_API_FETCH_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
)
EOD_API_LIMIT = int_env("EOD_API_LIMIT", 100)
SUPABASE_SCHEMA = os.environ.get("SUPABASE_SCHEMA", "private")
SUPABASE_EOD_ORDERS_TABLE = os.environ.get("SUPABASE_EOD_ORDERS_TABLE", "doktorabc_eod_bot_orders")
END_OF_DAY_EXPORT_STORAGE_BUCKET = (os.environ.get("END_OF_DAY_EXPORT_STORAGE_BUCKET") or "").strip()
END_OF_DAY_EXPORT_STORAGE_PREFIX = (
    os.environ.get("END_OF_DAY_EXPORT_STORAGE_PREFIX", "doktorabc-eod-export").strip().strip("/")
)
END_OF_DAY_EXPORT_N8N_WEBHOOK_URL = (os.environ.get("END_OF_DAY_EXPORT_N8N_WEBHOOK_URL") or "").strip()
END_OF_DAY_NOTIFICATION_WEBHOOK_URL = (os.environ.get("END_OF_DAY_NOTIFICATION_WEBHOOK_URL") or "").strip()
SERVICE_NAME = (os.environ.get("DOKTORABC_SCRAPER_SERVICE_NAME") or "end-of-day-scraper").strip() or "end-of-day-scraper"
DOKTORABC_SESSION_ADMIN_TOKEN = (os.environ.get("DOKTORABC_SESSION_ADMIN_TOKEN") or "").strip()
EOD_ORDERS_API_FRAGMENT = "end-of-day?"
EOD_ORDERS_API_QUERY = "end-of-day?limit=100&offset=0&sort=false&productIDs=&search="
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


def supabase_storage_object_url(bucket, object_path):
    supabase_url = required_env("SUPABASE_URL").rstrip("/")
    encoded_bucket = quote(bucket, safe="")
    encoded_path = quote(object_path, safe="/")
    return f"{supabase_url}/storage/v1/object/{encoded_bucket}/{encoded_path}"


def supabase_storage_upload_headers(content_type):
    service_role_key = required_env("SUPABASE_SERVICE_ROLE_KEY")

    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": content_type,
        "Cache-Control": "3600",
        "x-upsert": "true",
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


def utc_timestamp(value):
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def timestamptz_iso(value):
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def filename_timestamp(value):
    return timestamptz_iso(value).replace(":", "-")


def to_check_excel_filename(value):
    return f"to_check_{filename_timestamp(value)}.xlsx"


def to_check_excel_storage_filename(export_date):
    return f"to_check_{export_date}.xlsx"


def storage_object_path(filename, prefix):
    clean_prefix = (prefix or "").strip("/")
    return f"{clean_prefix}/{filename}" if clean_prefix else filename


def eod_dedupe_day_start(value):
    try:
        local_timezone = ZoneInfo(EOD_DEDUPE_TIMEZONE)
    except ZoneInfoNotFoundError:
        local_timezone = timezone.utc

    local_value = value.astimezone(local_timezone)
    local_start = local_value.replace(hour=0, minute=0, second=0, microsecond=0)
    return timestamptz_iso(local_start)


def postgrest_in_values(values):
    quoted_values = []
    for value in values:
        escaped_value = str(value).replace("\\", "\\\\").replace('"', '\\"')
        quoted_values.append(f'"{escaped_value}"')

    return f"in.({','.join(quoted_values)})"


def chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def existing_order_references_since(order_references, start_iso, order_type=EOD_ORDER_TYPE):
    references = sorted({reference for reference in order_references if reference})
    if not references:
        return set()

    existing_references = set()
    for reference_chunk in chunks(references, 100):
        response = httpx.get(
            supabase_table_url(),
            headers=supabase_headers(),
            params={
                "select": "order_reference",
                "order_type": f"eq.{order_type}",
                "order_reference": postgrest_in_values(reference_chunk),
                "scraped_at": f"gte.{start_iso}",
            },
            timeout=EOD_SUPABASE_TIMEOUT_SECONDS,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Supabase duplicate check failed: {response_preview(response)}")

        for row in response.json():
            order_reference = row.get("order_reference") if isinstance(row, dict) else None
            if order_reference:
                existing_references.add(order_reference)

    return existing_references


def delete_existing_order_references_since(order_references, start_iso, order_type=EOD_ORDER_TYPE):
    references = sorted({reference for reference in order_references if reference})
    if not references:
        return 0

    deleted_references = 0
    for reference_chunk in chunks(references, 100):
        response = httpx.delete(
            supabase_table_url(),
            headers={
                **supabase_headers(),
                "Prefer": "return=minimal",
            },
            params={
                "order_type": f"eq.{order_type}",
                "order_reference": postgrest_in_values(reference_chunk),
                "scraped_at": f"gte.{start_iso}",
            },
            timeout=EOD_SUPABASE_TIMEOUT_SECONDS,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Supabase same-day cleanup failed: {response_preview(response)}")

        deleted_references += len(reference_chunk)

    return deleted_references


def replace_orders_already_scraped_today(orders, scraped_at):
    today_start_iso = eod_dedupe_day_start(scraped_at)
    rows_with_scraped_at = [
        row
        for row in orders
        if row.get("order_reference") and row.get("scraped_at")
    ]
    existing_references = existing_order_references_since(
        [row.get("order_reference") for row in rows_with_scraped_at],
        today_start_iso,
    )

    if not existing_references:
        return {
            "dedupe_window_start": today_start_iso,
            "dedupe_existing_today": 0,
            "replaced_existing_today": 0,
            "replaced_existing_today_references": [],
        }

    replaced_count = delete_existing_order_references_since(existing_references, today_start_iso)

    return {
        "dedupe_window_start": today_start_iso,
        "dedupe_existing_today": len(existing_references),
        "replaced_existing_today": replaced_count,
        "replaced_existing_today_references": sorted(existing_references)[:20],
    }


def upsert_supabase_eod_orders(orders):
    if not orders:
        return {
            "sent_to_supabase": 0,
            "supabase_status_code": None,
        }

    response = httpx.post(
        f"{supabase_table_url()}?on_conflict=order_reference,scraped_at",
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


def upload_export_to_supabase_storage(download_path, storage_filename):
    if not END_OF_DAY_EXPORT_STORAGE_BUCKET:
        return {
            "sent_to_storage": False,
            "storage_skipped_reason": "END_OF_DAY_EXPORT_STORAGE_BUCKET is not configured",
        }

    object_path = storage_object_path(storage_filename, END_OF_DAY_EXPORT_STORAGE_PREFIX)
    content_type = mimetypes.guess_type(storage_filename)[0] or "application/octet-stream"

    with open(download_path, "rb") as file_handle:
        response = httpx.post(
            supabase_storage_object_url(END_OF_DAY_EXPORT_STORAGE_BUCKET, object_path),
            headers=supabase_storage_upload_headers(content_type),
            content=file_handle,
            timeout=EOD_STORAGE_UPLOAD_TIMEOUT_SECONDS,
        )

    if response.status_code >= 400:
        raise RuntimeError(f"Supabase Storage export upload failed: {response_preview(response)}")

    return {
        "sent_to_storage": True,
        "storage_status_code": response.status_code,
        "storage_bucket": END_OF_DAY_EXPORT_STORAGE_BUCKET,
        "storage_path": object_path,
        "storage_filename": storage_filename,
        "storage_replaced_existing": True,
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
            "bucket": export_result.get("storage_bucket"),
            "path": export_result.get("storage_path") or export_result.get("download_path"),
            "size_bytes": export_result.get("download_size_bytes"),
            "download_filename": export_result.get("download_filename"),
            "download_path": export_result.get("download_path"),
            "download_size_bytes": export_result.get("download_size_bytes"),
            "excel_row_count": export_result.get("excel_row_count"),
            "export_date": export_result.get("export_date"),
            "sent_to_storage": export_result.get("sent_to_storage"),
            "storage_bucket": export_result.get("storage_bucket"),
            "storage_path": export_result.get("storage_path"),
            "storage_filename": export_result.get("storage_filename"),
            "storage_status_code": export_result.get("storage_status_code"),
            "storage_replaced_existing": export_result.get("storage_replaced_existing"),
            "storage_skipped_reason": export_result.get("storage_skipped_reason"),
            "sent_to_n8n": export_result.get("sent_to_n8n"),
            "n8n_status_code": export_result.get("n8n_status_code"),
            "n8n_skipped_reason": export_result.get("n8n_skipped_reason"),
            "summary": {
                "excel_files": 1,
                "excel_rows": export_result.get("excel_row_count"),
                "sent_to_storage": 1 if export_result.get("sent_to_storage") else 0,
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


def export_end_of_day_excel_to_n8n(page, timestamp, metadata, scraped_at):
    wait_for_orders_page(page, end_of_day_url())

    export_button = export_button_locator(page)

    with page.expect_download(timeout=EOD_EXPORT_DOWNLOAD_TIMEOUT_MS) as download_info:
        export_button.click(timeout=EOD_EXPORT_BUTTON_CLICK_TIMEOUT_MS)

    download = download_info.value
    scraped_at_iso = timestamptz_iso(scraped_at)
    download_filename = to_check_excel_filename(scraped_at)
    download_path = os.path.join(ARTIFACTS_DIR, download_filename)
    download.save_as(download_path)
    download_size_bytes = os.path.getsize(download_path)
    excel_row_count = count_xlsx_rows(download_path)
    export_date = scraped_at.astimezone(german_timezone_for_utc(scraped_at)).date().isoformat()
    storage_filename = to_check_excel_storage_filename(export_date)
    storage_result = upload_export_to_supabase_storage(download_path, storage_filename)

    n8n_result = send_export_to_n8n(
        download_path,
        {
            **metadata,
            "source": "doktorabc_end_of_day_export",
            "source_url": end_of_day_url(),
            "download_filename": download_filename,
            "download_path": download_path,
            "download_size_bytes": download_size_bytes,
            "excel_row_count": excel_row_count,
            "export_date": export_date,
            "exported_at": scraped_at_iso,
            "scraped_at": scraped_at_iso,
            **storage_result,
        },
    )

    return {
        "downloaded": True,
        "download_filename": download_filename,
        "download_path": download_path,
        "download_size_bytes": download_size_bytes,
        "excel_row_count": excel_row_count,
        "export_date": export_date,
        "scraped_at": scraped_at_iso,
        **storage_result,
        **n8n_result,
    }


def maybe_export_end_of_day_excel_to_n8n(page, timestamp, metadata, rows_by_order_type, scraped_at):
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
        **export_end_of_day_excel_to_n8n(page, timestamp, metadata, scraped_at),
    }


def end_of_day_url():
    return os.environ.get("DOKTORABC_END_OF_DAY_URL") or DEFAULT_END_OF_DAY_URL


def login_url():
    return (os.environ.get("DOKTORABC_LOGIN_URL") or "").strip() or DEFAULT_DOKTORABC_LOGIN_URL


def login_url_candidates(target_url):
    candidates = [
        login_url(),
        DEFAULT_DOKTORABC_LOGIN_URL,
        DEFAULT_DOKTORABC_LOGIN_FORM_URL,
        target_url,
        end_of_day_url(),
    ]
    unique = []
    seen = set()

    for candidate in candidates:
        clean_candidate = (candidate or "").strip()
        if not clean_candidate or clean_candidate in seen:
            continue

        seen.add(clean_candidate)
        unique.append(clean_candidate)

    return unique


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


def log_event(event, **details):
    payload = {
        "event": event,
        "service": SERVICE_NAME,
        **details,
    }
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


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


def capture_optional_screenshot(page, path, label):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        page.screenshot(path=path, full_page=True)
        log_event("screenshot_saved", label=label, path=path, url=page.url)
        return path, None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        log_event("screenshot_failed", label=label, path=path, error=error)
        return None, error


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
              const loginButtonVisible = Array.from(
                document.querySelectorAll("button, input[type='submit'], [role='button']")
              ).some((element) => visible(element) && /login/i.test(normalize(element.innerText || element.value || element.textContent)));
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
                loginButtonVisible,
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


def is_login_url(url):
    return bool(re.search(r"(^|/|[?&])login($|[/?#=&])", (url or "").lower()))


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


def wait_for_rendered_login_form(page, timeout_ms=EOD_LOGIN_UI_READY_TIMEOUT_MS):
    log_event("login_form_wait_start", timeout_ms=timeout_ms, url=page.url)

    try:
        handle = page.wait_for_function(LOGIN_FORM_READY_JS, timeout=timeout_ms, polling=250)
        result = handle.json_value()
        log_event("login_form_wait_ready", result=result)
        return result
    except PlaywrightTimeoutError as exc:
        diagnostics = page_readiness_diagnostics(page)
        log_event("login_form_wait_timeout", timeout_ms=timeout_ms, diagnostics=diagnostics)
        raise RuntimeError(
            "DoktorABC login form was not visible. "
            "Expected rendered email field, password field, and Login button. "
            f"Diagnostics: {diagnostics}"
        ) from exc


def visible_login_form(page):
    try:
        wait_for_rendered_login_form(page, timeout_ms=EOD_LOGIN_FORM_CHECK_TIMEOUT_MS)
        return True
    except Exception:
        return False


def require_visible_login_form(page):
    try:
        return wait_for_rendered_login_form(page, timeout_ms=EOD_LOGIN_UI_READY_TIMEOUT_MS)
    except RuntimeError as first_exc:
        log_event(
            "login_form_wait_retry_reload",
            url=page.url,
            first_error=f"{type(first_exc).__name__}: {first_exc}",
        )

        try:
            page.reload(wait_until="domcontentloaded", timeout=EOD_NAVIGATION_TIMEOUT_MS)
            wait_for_load_states(page)
        except Exception as reload_exc:
            log_event("login_form_reload_failed", error=f"{type(reload_exc).__name__}: {reload_exc}")

        try:
            return wait_for_rendered_login_form(page, timeout_ms=EOD_LOGIN_UI_READY_RETRY_TIMEOUT_MS)
        except RuntimeError as second_exc:
            diagnostics = page_readiness_diagnostics(page)
            log_event(
                "login_form_wait_failed_after_retry",
                diagnostics=diagnostics,
                first_error=f"{type(first_exc).__name__}: {first_exc}",
                second_error=f"{type(second_exc).__name__}: {second_exc}",
            )
            raise RuntimeError(
                "DoktorABC login form was not visible. "
                "The page loaded, but the rendered Login button/email/password did not appear. "
                f"Diagnostics: {page_readiness_diagnostics(page)}"
            ) from second_exc


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

    temp_path = f"{SESSION_STATE_PATH}.{os.getpid()}.tmp"
    try:
        context.storage_state(path=temp_path)
        os.replace(temp_path, SESSION_STATE_PATH)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    log_event("session_state_saved", path=SESSION_STATE_PATH)


def session_admin_error(x_admin_token):
    if not DOKTORABC_SESSION_ADMIN_TOKEN:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "session_admin_token_not_configured",
                "hint": "Set DOKTORABC_SESSION_ADMIN_TOKEN on the EOD bot.",
            },
        )

    if not x_admin_token or not hmac.compare_digest(str(x_admin_token), DOKTORABC_SESSION_ADMIN_TOKEN):
        return JSONResponse(status_code=403, content={"ok": False, "error": "forbidden"})

    return None


def clear_shared_session_state():
    candidates = [SESSION_STATE_PATH, *glob.glob(f"{SESSION_STATE_PATH}.*.tmp")]
    removed = []
    missing = []
    errors = []

    for path in sorted(set(candidates)):
        try:
            if os.path.exists(path):
                os.remove(path)
                removed.append(path)
            else:
                missing.append(path)
        except Exception as exc:
            errors.append({"path": path, "error": f"{type(exc).__name__}: {exc}"})

    log_event("session_state_cleared", path=SESSION_STATE_PATH, removed=removed, errors=errors)

    return {
        "ok": not errors,
        "session_state_path": SESSION_STATE_PATH,
        "session_state_exists": os.path.exists(SESSION_STATE_PATH),
        "removed": removed,
        "missing": missing,
        "errors": errors,
    }


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


def perform_login(page, context, target_url, before_login_path=None):
    login_state = require_visible_login_form(page)
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
        capture_optional_screenshot(page, before_login_path, "eod-before-login")

    click_login_button(page)
    wait_for_load_states(page)
    wait_for_business_condition(
        page,
        "doktorabc_login_success",
        LOGIN_AUTHENTICATED_JS,
        EOD_LOGIN_SUCCESS_TIMEOUT_MS,
        {"authenticated"},
        screenshot_label="eod-login-success",
    )

    if not page.url.startswith(target_url):
        goto_page(page, target_url)
    else:
        page.wait_for_load_state("domcontentloaded")

    wait_result = wait_for_orders_page(page, target_url)
    save_session_state(context)

    return wait_result


def login_if_needed(page, context, target_url, before_login_path=None):
    if not visible_login_form(page) and not is_login_url(page.url):
        log_event("login_not_needed", url=page.url)
        return False

    perform_login(page, context, target_url, before_login_path=before_login_path)
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

    if EOD_WAIT_FOR_NETWORKIDLE:
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


class BillingDateCollector:
    def __init__(self, api_fragment):
        self.api_fragment = api_fragment
        self.billing_dates_by_reference = {}
        self.responses = []
        self.errors = []
        self.fetched_urls = set()

    def capture_response(self, response):
        if self.api_fragment not in response.url:
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
                self.api_fragment,
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


class SelfPickupBillingDateCollector(BillingDateCollector):
    def __init__(self):
        super().__init__(SELF_PICKUP_ORDERS_API_FRAGMENT)


def normalize_scraped_order(order, order_type, billing_dates_by_reference=None, scraped_at=None):
    products = order.get("product_details") or []
    product_names = [product.get("product") for product in products]
    pzns = [join_pipe(product.get("pzns") or []) for product in products]
    prices = [normalize_price(product.get("price")) for product in products]
    quantities = [product.get("quantity") for product in products]
    order_reference = clean_text(order.get("order_reference"))
    billing_date = (billing_dates_by_reference or {}).get(order_reference_key(order_reference))

    row = {
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

    if scraped_at is not None:
        row["scraped_at"] = scraped_at

    return row


def normalize_gender(value):
    if value == 0:
        return "female"

    if value == 1:
        return "male"

    return clean_text(value)


def list_value(value):
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def first_clean_text(*values):
    for value in values:
        cleaned = clean_text(value)
        if cleaned:
            return cleaned

    return None


def nested_dict(value, key):
    child = value.get(key) if isinstance(value, dict) else None
    return child if isinstance(child, dict) else {}


def api_product_title(product):
    nested_product = nested_dict(product, "product")
    return first_clean_text(
        product.get("productTitle"),
        product.get("productName"),
        product.get("title"),
        product.get("name"),
        nested_product.get("title"),
        nested_product.get("name"),
    )


def api_product_skus(product):
    values = []

    for key in ("skus", "sku", "pzn", "pzns", "PZN"):
        for value in list_value(product.get(key)):
            cleaned = clean_text(value)
            if cleaned:
                values.append(cleaned)

    nested_product = nested_dict(product, "product")
    for key in ("skus", "sku", "pzn", "pzns", "PZN"):
        for value in list_value(nested_product.get(key)):
            cleaned = clean_text(value)
            if cleaned:
                values.append(cleaned)

    return values


def api_product_price(product):
    return first_clean_text(
        product.get("supplyPrice"),
        product.get("price"),
        product.get("totalPrice"),
        product.get("unitPrice"),
        product.get("amount"),
    )


def api_product_quantity(product):
    return first_clean_text(
        product.get("productQuantityTitle"),
        product.get("quantityTitle"),
        product.get("quantity"),
        product.get("amountTitle"),
        product.get("unit"),
    )


def normalize_self_pickup_api_order(order):
    products = order.get("products") if isinstance(order.get("products"), list) else []
    product_names = [api_product_title(product) for product in products if isinstance(product, dict)]
    pzns = [
        join_pipe(api_product_skus(product))
        for product in products
        if isinstance(product, dict)
    ]
    prices = [
        api_product_price(product)
        for product in products
        if isinstance(product, dict)
    ]
    quantities = [
        api_product_quantity(product)
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


def normalize_eod_api_order(order, scraped_at=None):
    products = order.get("products") if isinstance(order.get("products"), list) else []
    product_names = [api_product_title(product) for product in products if isinstance(product, dict)]
    pzns = [
        join_pipe(api_product_skus(product))
        for product in products
        if isinstance(product, dict)
    ]
    prices = [
        api_product_price(product)
        for product in products
        if isinstance(product, dict)
    ]
    quantities = [
        api_product_quantity(product)
        for product in products
        if isinstance(product, dict)
    ]
    customer = order.get("customer") if isinstance(order.get("customer"), dict) else {}
    delivery = order.get("delivery") if isinstance(order.get("delivery"), dict) else {}
    prescription = order.get("prescription") if isinstance(order.get("prescription"), dict) else {}
    shipping = order.get("shipping") if isinstance(order.get("shipping"), dict) else {}
    patient_name = first_clean_text(
        order.get("patientName"),
        customer.get("fullName"),
        join_pipe(
            [
                customer.get("firstName"),
                customer.get("middleName"),
                customer.get("lastName"),
            ]
        ),
    )
    address = first_clean_text(
        order.get("address"),
        delivery.get("fullAddress"),
        join_pipe(
            [
                delivery.get("address"),
                delivery.get("street"),
                delivery.get("zip"),
                delivery.get("city"),
                delivery.get("country"),
            ]
        ),
    )
    row = {
        "order_type": EOD_ORDER_TYPE,
        "order_reference": clean_text(order.get("hashID") or order.get("orderReference") or order.get("reference")),
        "billing_date": parse_datetime_to_second_iso(order.get("createdAt") or order.get("billingDate")),
        "prescription_date": parse_datetime_to_local_date_iso(
            prescription.get("approvedAt")
            or prescription.get("createdAt")
            or order.get("approvedAt")
            or order.get("prescriptionDate")
        ),
        "tracking_id": clean_text(shipping.get("trackingID") or shipping.get("trackingId") or order.get("trackingID")),
        "products": join_pipe(product_names),
        "pzns": join_pipe(pzns),
        "prices": join_pipe(prices),
        "quantities": join_pipe(quantities),
        "patient_name": patient_name,
        "patient_birth_date": parse_datetime_to_local_date_iso(customer.get("birthday") or customer.get("birthDate")),
        "address": address,
        "gender": normalize_gender(customer.get("gender")),
    }

    if scraped_at is not None:
        row["scraped_at"] = scraped_at

    return row


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

        if row.get("order_type") in {EOD_ORDER_TYPE, SELF_PICKUP_ORDER_TYPE} and not row.get("billing_date"):
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


def click_blocker_state(page):
    return page.evaluate(
        """
        () => {
          const describe = (element) => {
            const style = window.getComputedStyle(element);
            const rect = element.getBoundingClientRect();
            return {
              id: element.id || "",
              class_name: String(element.className || ""),
              opacity: style.opacity,
              pointer_events: style.pointerEvents,
              position: style.position,
              rect: {
                x: Math.round(rect.x),
                y: Math.round(rect.y),
                width: Math.round(rect.width),
                height: Math.round(rect.height),
              },
            };
          };
          const candidates = Array.from(
            document.querySelectorAll('#loader-container, [id*="loader" i], [class*="loader" i], [class*="backdrop" i]')
          );
          const blocking = candidates.filter((element) => {
            const style = window.getComputedStyle(element);
            const rect = element.getBoundingClientRect();
            if (
              style.display === "none" ||
              style.visibility === "hidden" ||
              style.pointerEvents === "none" ||
              rect.width <= 0 ||
              rect.height <= 0 ||
              rect.right <= 0 ||
              rect.bottom <= 0 ||
              rect.left >= window.innerWidth ||
              rect.top >= window.innerHeight
            ) {
              return false;
            }

            const coversMostViewport =
              rect.width >= window.innerWidth * 0.75 &&
              rect.height >= window.innerHeight * 0.75;
            return style.position === "fixed" || coversMostViewport;
          });

          const cleared = [];
          for (const element of blocking) {
            const style = window.getComputedStyle(element);
            const className = String(element.className || "");
            const invisible = Number(style.opacity) === 0 || /opacity-0/.test(className);

            if (invisible) {
              element.style.pointerEvents = "none";
              element.setAttribute("data-bot-pointer-events-cleared", "true");
              cleared.push(describe(element));
            }
          }

          const remaining = blocking.filter((element) => window.getComputedStyle(element).pointerEvents !== "none");

          return {
            ready: remaining.length === 0,
            cleared,
            remaining: remaining.slice(0, 5).map(describe),
          };
        }
        """
    )


def wait_for_click_blockers_clear(page, timeout_ms=DOKTORABC_CLICK_BLOCKER_TIMEOUT_MS):
    deadline = time.monotonic() + timeout_ms / 1000
    last_state = None

    while time.monotonic() < deadline:
        last_state = click_blocker_state(page)
        if last_state.get("ready"):
            return last_state

        page.wait_for_timeout(DOKTORABC_CLICK_BLOCKER_POLL_MS)

    log_event("click_blocker_wait_timeout", state=last_state, timeout_ms=timeout_ms, url=page.url)
    return last_state or {"ready": False}


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
            locator.scroll_into_view_if_needed(timeout=EOD_READY_FOR_CUSTOMER_VISIBLE_TIMEOUT_MS)
            wait_for_click_blockers_clear(page)
            locator.click(timeout=EOD_READY_FOR_CUSTOMER_CLICK_TIMEOUT_MS, trial=True)
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

    rows_100.scroll_into_view_if_needed(timeout=EOD_SELECT_100_VISIBLE_TIMEOUT_MS)
    wait_for_click_blockers_clear(page)
    rows_100.click(timeout=EOD_SELECT_100_CLICK_TIMEOUT_MS, trial=True)
    rows_100.click(timeout=EOD_SELECT_100_CLICK_TIMEOUT_MS)
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
    next_link.scroll_into_view_if_needed(timeout=EOD_NEXT_VISIBLE_TIMEOUT_MS)
    wait_for_click_blockers_clear(page)
    next_link.click(timeout=EOD_NEXT_CLICK_TIMEOUT_MS, trial=True)
    next_link.click(timeout=EOD_NEXT_CLICK_TIMEOUT_MS)

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
    let codes = unique(popupText.match(/\\b\\d{7,8}[A-Z]*\\b/gi) || []);

    if (!codes.length) {
      codes = unique((document.body.innerText || "").match(/\\b\\d{7,8}[A-Z]*\\b/gi) || []);
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


def fetch_eod_api_orders(page):
    if not EOD_API_FETCH_ENABLED:
        return {
            "orders": [],
            "pages": [],
            "steps": [
                {
                    "name": "fetch_eod_api_orders",
                    "ok": True,
                    "skipped": True,
                    "skipped_reason": "EOD_API_FETCH_ENABLED=false",
                    "orders_found": 0,
                }
            ],
            "duplicate_order_references": [],
        }

    result = page.evaluate(
        """
        async ({ query, limit }) => {
          const fragment = "end-of-day?";
          const discoveredUrl = performance
            .getEntriesByType("resource")
            .map((entry) => entry.name)
            .reverse()
            .find((url) => url.includes(fragment));
          const firstUrl = new URL(discoveredUrl || query, window.location.href);
          const pages = [];
          const orders = [];
          const seen = new Set();
          const payloadResults = (payload) => {
            if (Array.isArray(payload?.results)) return payload.results;
            if (Array.isArray(payload?.data)) return payload.data;
            if (Array.isArray(payload?.orders)) return payload.orders;
            if (Array.isArray(payload)) return payload;
            return [];
          };
          const payloadTotal = (payload) => {
            const value = payload?.count ?? payload?.total ?? payload?.pagination?.total;
            const total = Number(value);
            return Number.isFinite(total) ? total : null;
          };

          let offset = 0;
          while (offset < 10000) {
            const url = new URL(firstUrl.href);
            url.searchParams.set("limit", String(limit));
            url.searchParams.set("offset", String(offset));
            if (!url.searchParams.has("sort")) url.searchParams.set("sort", "false");
            if (!url.searchParams.has("productIDs")) url.searchParams.set("productIDs", "");
            if (!url.searchParams.has("search")) url.searchParams.set("search", "");

            let response;
            let payload;
            try {
              response = await fetch(url.href, {
                credentials: "include",
                headers: { Accept: "application/json" },
              });
              payload = await response.json();
            } catch (error) {
              pages.push({
                offset,
                url: url.href,
                status: response?.status ?? null,
                count: null,
                results: 0,
                error: `${error?.name || "Error"}: ${error?.message || String(error)}`,
              });
              break;
            }

            const results = payloadResults(payload);
            const total = payloadTotal(payload);

            pages.push({
              offset,
              url: url.href,
              status: response.status,
              count: total,
              results: results.length,
              first_order_reference: results[0]?.hashID ?? null,
              last_order_reference: results[results.length - 1]?.hashID ?? null,
            });

            for (const order of results) {
              const key = order?.hashID || order?.orderReference || order?.reference;
              if (!key || seen.has(key)) continue;
              seen.add(key);
              orders.push(order);
            }

            if (!response.ok || results.length === 0) {
              break;
            }

            const nextOffset = offset + results.length;
            if (total !== null && total > results.length && nextOffset >= total) {
              break;
            }

            offset = nextOffset;
          }

          return { orders, pages, discovered_url: discoveredUrl || null };
        }
        """,
        {
            "query": EOD_ORDERS_API_QUERY,
            "limit": EOD_API_LIMIT,
        },
    )

    raw_orders = result.get("orders") if isinstance(result, dict) else []
    pages = result.get("pages") if isinstance(result, dict) else []

    return {
        "orders": raw_orders,
        "pages": pages,
        "steps": [
            {
                "name": "fetch_eod_api_orders",
                "ok": True,
                "enabled": EOD_API_FETCH_ENABLED,
                "limit": EOD_API_LIMIT,
                "discovered_url": result.get("discovered_url") if isinstance(result, dict) else None,
                "pages": pages,
                "orders_found": len(raw_orders),
            }
        ],
        "duplicate_order_references": [],
    }


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
    last_error = None

    for candidate_url in login_url_candidates(target_url):
        try:
            log_event("fresh_session_login_candidate_start", order_type=order_type, url=candidate_url)
            goto_page(page, candidate_url)
            wait_result = perform_login(page, context, target_url, before_login_path=before_login_path)
            return context, page, False, wait_result
        except Exception as exc:
            last_error = exc
            log_event(
                "fresh_session_login_candidate_failed",
                order_type=order_type,
                url=candidate_url,
                error=f"{type(exc).__name__}: {exc}",
                diagnostics=page_readiness_diagnostics(page),
            )

    context.close()
    raise RuntimeError(f"Could not open authenticated DoktorABC {order_type} page. Last error: {last_error}") from last_error


def open_authenticated_orders_page(browser, target_url, order_type, before_login_path=None):
    saved_session = open_saved_session(browser, target_url, order_type)

    if saved_session is not None:
        return saved_session

    return open_fresh_session(browser, target_url, order_type, before_login_path=before_login_path)


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
            after_login_path, after_login_error = capture_optional_screenshot(
                page,
                after_login_path,
                "eod-after-login",
            )

            return {
                "ok": True,
                "current_url": page.url,
                "page_title": page.title(),
                "reused_session": reused_session,
                "session_state_path": SESSION_STATE_PATH,
                "before_login_path": None if reused_session else before_login_path,
                "after_login_path": after_login_path,
                "after_login_screenshot_error": after_login_error,
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


def sync_end_of_day_orders():
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    scraped_at = datetime.now(timezone.utc)
    scraped_at_iso = timestamptz_iso(scraped_at)
    timestamp = utc_timestamp(scraped_at)
    before_login_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-before-login-{timestamp}.png")
    failure_path = os.path.join(ARTIFACTS_DIR, f"doktorabc-eod-sync-failure-{timestamp}.png")
    steps = []
    targets = configured_order_targets()

    print("trying to sync DoktorABC order lists ...", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=bool_env("DOKTORABC_HEADLESS", True))
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
                elif order_type == EOD_ORDER_TYPE:
                    billing_date_collector = BillingDateCollector(EOD_ORDERS_API_FRAGMENT)

                if billing_date_collector:
                    page.on("response", billing_date_collector.capture_response)

                if not page.url.startswith(target_url):
                    goto_page(page, target_url)
                else:
                    try:
                        page.wait_for_load_state("domcontentloaded", timeout=EOD_LOAD_STATE_DOMCONTENTLOADED_TIMEOUT_MS)
                    except PlaywrightTimeoutError:
                        pass
                ready_for_customer_clicked = False
                ready_for_customer_click_strategy = None
                if order_type == SELF_PICKUP_ORDER_TYPE:
                    wait_for_load_states(page)
                    ready_for_customer_click_strategy = click_ready_for_customer(page)
                    ready_for_customer_clicked = True
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
                rows = None
                invalid_orders = None
                warnings = None
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
                elif order_type == EOD_ORDER_TYPE:
                    api_scrape_result = fetch_eod_api_orders(page)
                    api_invalid_orders = []
                    api_warnings = []
                    fallback_reason = None

                    if api_scrape_result["orders"]:
                        api_rows = [
                            normalize_eod_api_order(order, scraped_at_iso)
                            for order in api_scrape_result["orders"]
                        ]
                        api_invalid_orders, api_warnings = validate_orders(api_rows, api_scrape_result["orders"])

                        if not api_invalid_orders:
                            scrape_result = api_scrape_result
                            raw_orders = scrape_result["orders"]
                            rows = api_rows
                            invalid_orders = api_invalid_orders
                            warnings = api_warnings
                        else:
                            fallback_reason = "api_rows_missing_required_fields"
                    else:
                        fallback_reason = "api_fetch_empty"

                    if fallback_reason:
                        fallback_result = scrape_all_eod_orders(page, billing_date_collector=billing_date_collector)
                        scrape_result = {
                            **fallback_result,
                            "steps": [
                                *api_scrape_result.get("steps", []),
                                {
                                    "name": "fallback_to_dom_scrape",
                                    "ok": True,
                                    "reason": fallback_reason,
                                    "api_invalid_count": len(api_invalid_orders),
                                    "api_invalid_examples": api_invalid_orders[:5],
                                    "api_warnings_count": len(api_warnings),
                                },
                                *fallback_result.get("steps", []),
                            ],
                            "api_pages": api_scrape_result.get("pages", []),
                        }
                else:
                    scrape_result = scrape_all_eod_orders(page, billing_date_collector=billing_date_collector)

                if rows is None:
                    raw_orders = scrape_result["orders"]
                    if order_type == SELF_PICKUP_ORDER_TYPE and raw_orders and raw_orders[0].get("hashID"):
                        rows = [normalize_self_pickup_api_order(order) for order in raw_orders]
                    else:
                        billing_dates_by_reference = (
                            billing_date_collector.billing_dates_by_reference if billing_date_collector else None
                        )
                        row_scraped_at = scraped_at_iso if order_type == EOD_ORDER_TYPE else None
                        rows = [
                            normalize_scraped_order(order, order_type, billing_dates_by_reference, row_scraped_at)
                            for order in raw_orders
                        ]

                if invalid_orders is None or warnings is None:
                    invalid_orders, warnings = validate_orders(rows, raw_orders)
                screenshot_path = os.path.join(
                    ARTIFACTS_DIR,
                    f"doktorabc-{safe_slug(order_type)}-after-sync-{timestamp}.png",
                )
                screenshot_path, screenshot_error = capture_optional_screenshot(
                    page,
                    screenshot_path,
                    f"{safe_slug(order_type)}-after-sync",
                )
                if screenshot_path:
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
                    "screenshot_error": screenshot_error,
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
                        "scraped_at": scraped_at_iso,
                    },
                    rows_by_order_type,
                    scraped_at,
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

            steps.append({"name": "replace_existing_today", "ok": None, "rows": len(all_rows)})
            dedupe_result = replace_orders_already_scraped_today(all_rows, scraped_at)
            steps[-1] = {"name": "replace_existing_today", "ok": True, **dedupe_result}

            steps.append({"name": "upsert_supabase", "ok": None, "rows": len(all_rows)})
            supabase_result = upsert_supabase_eod_orders(all_rows)
            steps[-1] = {"name": "upsert_supabase", "ok": True, **supabase_result, **dedupe_result}

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
                    "replaced_existing_today": dedupe_result.get("replaced_existing_today"),
                    "targets_count": len(target_results),
                    "warnings_count": len(all_warnings),
                    "duplicate_order_references_count": len(all_duplicate_order_references),
                    "scraped_at": scraped_at_iso,
                },
                rows_by_order_type,
                scraped_at,
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
                **dedupe_result,
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
            browser.close()


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
        "session_admin_enabled": bool(DOKTORABC_SESSION_ADMIN_TOKEN),
    }


@app.post("/admin/doktorabc/session/clear")
def clear_doktorabc_session(x_admin_token: str = Header(default="", alias="X-Admin-Token")):
    error_response = session_admin_error(x_admin_token)
    if error_response:
        return error_response

    return clear_shared_session_state()


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


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})
