import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import quote, urlencode

import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.requests import Request

try:
    from app.doktorabc_billing_parser import parse_doktorabc_billing_excel
    from app.oed_orders_parser import parse_oed_orders_excel
except ModuleNotFoundError:
    from doktorabc_billing_parser import parse_doktorabc_billing_excel
    from oed_orders_parser import parse_oed_orders_excel


STARTED_AT = time.time()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SUPABASE_STORAGE_BUCKET = os.environ.get("SUPABASE_STORAGE_BUCKET", "oed-files")
SUPABASE_STORAGE_PREFIX = os.environ.get("SUPABASE_STORAGE_PREFIX", "oed").strip("/")
UPLOAD_PASSWORD = os.environ.get("UPLOAD_PASSWORD", "")
ABRECHNUNG_STORAGE_BUCKET = os.environ.get("ABRECHNUNG_STORAGE_BUCKET", "abrechnung-files")
ABRECHNUNG_STORAGE_PREFIX = os.environ.get("ABRECHNUNG_STORAGE_PREFIX", "doktorabc-abrechnung").strip("/")
ABRECHNUNG_UPLOAD_PASSWORD = os.environ.get("ABRECHNUNG_UPLOAD_PASSWORD", "")
N8N_UPLOAD_WEBHOOK_URL = os.environ.get("N8N_UPLOAD_WEBHOOK_URL", "").strip()
SUPABASE_ORDERS_TABLE = os.environ.get("SUPABASE_ORDERS_TABLE", "orders_csv").strip()
SUPABASE_DB_SCHEMA = os.environ.get("SUPABASE_DB_SCHEMA", "public").strip()
SUPABASE_ABRECHNUNG_TABLE = os.environ.get("SUPABASE_ABRECHNUNG_TABLE", "doktorabc_billing").strip()
SUPABASE_ABRECHNUNG_SCHEMA = os.environ.get("SUPABASE_ABRECHNUNG_SCHEMA", "private").strip()
ABRECHNUNG_TIME_ZONE = os.environ.get("ABRECHNUNG_TIME_ZONE", os.environ.get("TZ", "Europe/Berlin")).strip()
SUPABASE_DB_INSERT_BATCH_SIZE = int(os.environ.get("SUPABASE_DB_INSERT_BATCH_SIZE", "1000"))
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "20"))
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024
ALLOWED_EXTENSIONS = {
    item.strip().lower()
    for item in os.environ.get("ALLOWED_EXTENSIONS", ".xlsx,.xls").split(",")
    if item.strip()
}

app = FastAPI(title="file-upload-ui")


BILLING_SOURCE_COLUMNS = (
    "hash_id",
    "sent_date",
    "stock",
    "type",
    "total_medication_cost_incl_vat",
    "supply_price_base",
    "additional_cost",
    "waybill_id",
    "uber_shipping_fee",
)


class AbrechnungValidationError(HTTPException):
    def __init__(
        self,
        detail: str,
        error_kind: str,
        extra: dict[str, object] | None = None,
    ):
        super().__init__(status_code=409, detail=detail)
        self.error_kind = error_kind
        self.extra = extra or {}


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    print(f"Unhandled error on {request.method} {request.url.path}: {type(exc).__name__}: {exc}", flush=True)
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "detail": f"Internal server error: {type(exc).__name__}: {exc}",
        },
    )


def oed_filename(selected_date: date, extension: str) -> str:
    return f"oed_{selected_date.isoformat()}{extension}"


def abrechnung_filename(period_from: date, period_to: date, extension: str) -> str:
    return f"doktorabc_abrechnung_{period_from.isoformat()}_bis_{period_to.isoformat()}{extension}"


def storage_path(filename: str, prefix: str) -> str:
    if prefix:
        return f"{prefix}/{filename}"
    return filename


def storage_object_url(bucket: str, object_path: str) -> str:
    encoded_path = quote(object_path, safe="/")
    return (
        f"{SUPABASE_URL}/storage/v1/object/"
        f"{quote(bucket, safe='')}/{encoded_path}"
    )


def storage_object_collection_url(bucket: str) -> str:
    return f"{SUPABASE_URL}/storage/v1/object/{quote(bucket, safe='')}"


def supabase_table_url(table: str, query: dict[str, str] | None = None) -> str:
    url = f"{SUPABASE_URL}/rest/v1/{quote(table, safe='')}"
    if query:
        return f"{url}?{urlencode(query)}"
    return url


def storage_upload_headers(content_type: str, upsert: bool | None = None) -> dict[str, str]:
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": content_type or "application/octet-stream",
    }
    if upsert is not None:
        headers["x-upsert"] = "true" if upsert else "false"
    return headers


def supabase_db_headers(
    return_representation: bool = False,
    ignore_duplicates: bool = False,
    schema: str | None = None,
) -> dict[str, str]:
    prefer = ["return=representation" if return_representation else "return=minimal"]
    if ignore_duplicates:
        prefer.append("resolution=ignore-duplicates")
    profile = SUPABASE_DB_SCHEMA if schema is None else schema

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": ",".join(prefer),
    }
    if profile:
        headers["Accept-Profile"] = profile
        headers["Content-Profile"] = profile
    return headers


def require_config():
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_ROLE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variable(s): {', '.join(missing)}",
        )


def require_password(password: str | None, expected_password: str, label: str):
    if not expected_password:
        raise HTTPException(status_code=500, detail=f"{label} upload password is not configured.")
    if expected_password and password != expected_password:
        raise HTTPException(status_code=401, detail="Wrong upload password.")


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Choose a valid date.") from exc


async def read_limited_file(upload: UploadFile) -> bytes:
    contents = await upload.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Choose an Excel file.")
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File is too large. Maximum size is {MAX_UPLOAD_MB} MB.",
        )
    return contents


def validate_extension(filename: str) -> str:
    extension = Path(filename).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        allowed = ", ".join(sorted(ALLOWED_EXTENSIONS))
        raise HTTPException(
            status_code=400,
            detail=f"Only these file types are allowed: {allowed}.",
        )
    return extension


def error_reason(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return str(exc.detail)
    return f"{type(exc).__name__}: {exc}"


def storage_error_detail(response: httpx.Response):
    try:
        return response.json()
    except ValueError:
        return response.text


def raise_database_insert_error(response: httpx.Response, error_detail):
    raise HTTPException(
        status_code=502,
        detail={
            "message": "Supabase database insert failed.",
            "status_code": response.status_code,
            "supabase_error": error_detail,
        },
    )


def raise_database_read_error(response: httpx.Response, error_detail):
    raise HTTPException(
        status_code=502,
        detail={
            "message": "Supabase database validation read failed.",
            "status_code": response.status_code,
            "supabase_error": error_detail,
        },
    )


def chunk_rows(rows: list[dict[str, object]], size: int):
    size = max(1, size)
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


def chunk_values(values: list[str], size: int):
    size = max(1, size)
    for index in range(0, len(values), size):
        yield values[index : index + size]


def inserted_rows_count(response: httpx.Response) -> int:
    try:
        rows = response.json()
    except ValueError:
        return 0

    return len(rows) if isinstance(rows, list) else 0


async def insert_orders_csv_rows(rows: list[dict[str, str | None]]) -> int:
    require_config()
    if not rows:
        return 0

    inserted_count = 0
    url = supabase_table_url(SUPABASE_ORDERS_TABLE, {"on_conflict": '"Order id"'})
    headers = supabase_db_headers(return_representation=True, ignore_duplicates=True)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            for batch in chunk_rows(rows, SUPABASE_DB_INSERT_BATCH_SIZE):
                response = await client.post(url, headers=headers, json=batch)
                if not response.is_success:
                    raise_database_insert_error(response, storage_error_detail(response))
                inserted_count += inserted_rows_count(response)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Supabase database at {SUPABASE_URL}: {type(exc).__name__}: {exc}",
        ) from exc

    return inserted_count


async def insert_abrechnung_rows(rows: list[dict[str, object]]) -> int:
    require_config()
    if not rows:
        return 0

    url = supabase_table_url(SUPABASE_ABRECHNUNG_TABLE)
    headers = supabase_db_headers(schema=SUPABASE_ABRECHNUNG_SCHEMA)

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.post(url, headers=headers, json=rows)
            if not response.is_success:
                raise_database_insert_error(response, storage_error_detail(response))
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Supabase database at {SUPABASE_URL}: {type(exc).__name__}: {exc}",
        ) from exc

    return len(rows)


async def ensure_abrechnung_can_be_imported(
    rows: list[dict[str, object]],
    source_row_numbers: list[int],
    billing_period_from: date,
    billing_period_to: date,
):
    ensure_no_duplicate_uploaded_billing_rows(rows, source_row_numbers)
    await ensure_no_overlapping_abrechnung_period(billing_period_from, billing_period_to)
    await ensure_no_existing_billing_rows(rows, source_row_numbers)


def ensure_no_duplicate_uploaded_billing_rows(rows: list[dict[str, object]], source_row_numbers: list[int]):
    seen: dict[tuple[object, ...], int] = {}

    for row, source_row_number in zip(rows, source_row_numbers, strict=True):
        key = billing_source_key(row)
        first_row_number = seen.get(key)
        if first_row_number is not None:
            raise AbrechnungValidationError(
                detail=(
                    "Abgelehnt: Die Datei enthaelt eine doppelte Abrechnungszeile "
                    f"(Excel-Zeilen {first_row_number} und {source_row_number}). Es wurde nichts gespeichert."
                ),
                error_kind="duplicate_row_in_file",
                extra={
                    "duplicate_excel_row": source_row_number,
                    "first_excel_row": first_row_number,
                    "hash_id": str(row.get("hash_id") or ""),
                },
            )
        seen[key] = source_row_number


async def ensure_no_overlapping_abrechnung_period(billing_period_from: date, billing_period_to: date):
    require_config()
    query = {
        "select": "id,billing_period_from,billing_period_to",
        "billing_period_from": f"lte.{billing_period_to.isoformat()}",
        "billing_period_to": f"gte.{billing_period_from.isoformat()}",
        "limit": "1",
    }
    url = supabase_table_url(SUPABASE_ABRECHNUNG_TABLE, query)
    headers = supabase_db_headers(schema=SUPABASE_ABRECHNUNG_SCHEMA)

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, headers=headers)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Supabase database at {SUPABASE_URL}: {type(exc).__name__}: {exc}",
        ) from exc

    if not response.is_success:
        raise_database_read_error(response, storage_error_detail(response))

    existing_periods = response.json()
    if not existing_periods:
        return

    existing = existing_periods[0]
    existing_from = str(existing.get("billing_period_from") or "?")
    existing_to = str(existing.get("billing_period_to") or "?")
    raise AbrechnungValidationError(
        detail=(
            "Abgelehnt: Der gewaehlte Zeitraum "
            f"{billing_period_from.isoformat()} bis {billing_period_to.isoformat()} ueberschneidet "
            f"bereits vorhandene Abrechnung {existing_from} bis {existing_to}. Es wurde nichts gespeichert."
        ),
        error_kind="overlapping_billing_period",
        extra={
            "billing_period_from": billing_period_from.isoformat(),
            "billing_period_to": billing_period_to.isoformat(),
            "overlap_period_from": existing_from,
            "overlap_period_to": existing_to,
            "overlap_row_id": existing.get("id"),
        },
    )


async def ensure_no_existing_billing_rows(rows: list[dict[str, object]], source_row_numbers: list[int]):
    require_config()
    hash_ids = sorted({str(row["hash_id"]) for row in rows if row.get("hash_id")})
    if not hash_ids:
        return

    uploaded_by_key = {
        billing_source_key(row): (row, source_row_number)
        for row, source_row_number in zip(rows, source_row_numbers, strict=True)
    }

    select_columns = ",".join(
        (
            "id",
            *BILLING_SOURCE_COLUMNS,
            "billing_period_from",
            "billing_period_to",
        )
    )
    headers = supabase_db_headers(schema=SUPABASE_ABRECHNUNG_SCHEMA)

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            for hash_batch in chunk_values(hash_ids, 120):
                query = {
                    "select": select_columns,
                    "hash_id": f"in.({','.join(format_postgrest_in_value(value) for value in hash_batch)})",
                }
                response = await client.get(supabase_table_url(SUPABASE_ABRECHNUNG_TABLE, query), headers=headers)
                if not response.is_success:
                    raise_database_read_error(response, storage_error_detail(response))

                for existing_row in response.json():
                    existing_key = billing_source_key(existing_row)
                    duplicate = uploaded_by_key.get(existing_key)
                    if not duplicate:
                        continue

                    uploaded_row, source_row_number = duplicate
                    raise AbrechnungValidationError(
                        detail=(
                            "Abgelehnt: Excel-Zeile "
                            f"{source_row_number} existiert bereits in der Datenbank "
                            f"(id {existing_row.get('id')}, Zeitraum "
                            f"{existing_row.get('billing_period_from')} bis {existing_row.get('billing_period_to')}). "
                            "Es wurde nichts gespeichert."
                        ),
                        error_kind="duplicate_row_in_database",
                        extra={
                            "duplicate_excel_row": source_row_number,
                            "existing_row_id": existing_row.get("id"),
                            "existing_billing_period_from": existing_row.get("billing_period_from"),
                            "existing_billing_period_to": existing_row.get("billing_period_to"),
                            "hash_id": str(uploaded_row.get("hash_id") or ""),
                        },
                    )
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Supabase database at {SUPABASE_URL}: {type(exc).__name__}: {exc}",
        ) from exc


def format_postgrest_in_value(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) + chr(34))}"'


def billing_source_key(row: dict[str, object]) -> tuple[object, ...]:
    return tuple(canonical_billing_value(column, row.get(column)) for column in BILLING_SOURCE_COLUMNS)


def canonical_billing_value(column: str, value: object) -> object:
    if column == "sent_date":
        return canonical_datetime(value)
    if column in {
        "total_medication_cost_incl_vat",
        "supply_price_base",
        "additional_cost",
        "uber_shipping_fee",
    }:
        return canonical_numeric(value)
    if value is None:
        return None
    return str(value).strip()


def canonical_datetime(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(microsecond=0).isoformat()


def canonical_numeric(value: object) -> str | None:
    if value is None:
        return None

    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return str(value).strip()

    return str(number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def storage_error_text(error_detail) -> str:
    return str(error_detail).lower()


def is_duplicate_storage_error(response: httpx.Response, error_detail) -> bool:
    error_text = storage_error_text(error_detail)
    return response.status_code == 409 or (
        response.status_code == 400
        and ("already exists" in error_text or "duplicate" in error_text)
    )


def is_missing_storage_object(response: httpx.Response, error_detail) -> bool:
    error_text = storage_error_text(error_detail)
    return response.status_code == 404 or (
        response.status_code == 400
        and (
            "not found" in error_text
            or "does not exist" in error_text
            or "no such key" in error_text
        )
    )


def raise_storage_upload_error(
    response: httpx.Response,
    error_detail,
    replace_existing: bool,
    duplicate_message: str,
):
    if is_duplicate_storage_error(response, error_detail) and not replace_existing:
        raise HTTPException(status_code=409, detail=duplicate_message)
    if is_duplicate_storage_error(response, error_detail) and replace_existing:
        raise HTTPException(
            status_code=409,
            detail="The existing file is still locked in Supabase Storage. Please try replacing it again.",
        )

    raise HTTPException(
        status_code=502,
        detail={
            "message": "Supabase Storage upload failed.",
            "status_code": response.status_code,
            "supabase_error": error_detail,
        },
    )


def raise_storage_delete_error(response: httpx.Response, error_detail):
    raise HTTPException(
        status_code=502,
        detail={
            "message": "Supabase Storage delete failed before replacement.",
            "status_code": response.status_code,
            "supabase_error": error_detail,
        },
    )


async def delete_existing_storage_object(
    client: httpx.AsyncClient,
    bucket: str,
    object_path: str,
):
    response = await client.request(
        "DELETE",
        storage_object_collection_url(bucket),
        headers=storage_upload_headers("application/json"),
        json={"prefixes": [object_path]},
    )
    if response.is_success:
        return

    error_detail = storage_error_detail(response)
    if is_missing_storage_object(response, error_detail):
        return

    raise_storage_delete_error(response, error_detail)


async def notify_n8n_upload_event(
    status: str,
    upload_type: str,
    filename: str,
    bucket: str,
    object_path: str,
    size_bytes: int,
    error: str | None = None,
    event: str | None = None,
    extra: dict[str, object] | None = None,
):
    if not N8N_UPLOAD_WEBHOOK_URL:
        return

    payload = {
        "event": event or f"upload_{status}",
        "status": status,
        "upload_type": upload_type,
        "filename": filename,
        "bucket": bucket,
        "path": object_path,
        "size_bytes": size_bytes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "file-upload-ui",
    }

    if error:
        payload["error"] = error

    if extra:
        payload.update(extra)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(N8N_UPLOAD_WEBHOOK_URL, json=payload)
            response.raise_for_status()
    except Exception as exc:
        print(
            f"Could not notify n8n about {status} for {filename}: {type(exc).__name__}: {exc}",
            flush=True,
        )


async def upload_to_supabase_with_notifications(
    upload_type: str,
    filename: str,
    bucket: str,
    object_path: str,
    contents: bytes,
    content_type: str,
    replace_existing: bool,
    duplicate_message: str,
):
    await notify_n8n_upload_event(
        status="triggered",
        upload_type=upload_type,
        filename=filename,
        bucket=bucket,
        object_path=object_path,
        size_bytes=len(contents),
    )

    try:
        await upload_to_supabase(
            bucket=bucket,
            object_path=object_path,
            contents=contents,
            content_type=content_type,
            replace_existing=replace_existing,
            duplicate_message=duplicate_message,
        )
    except Exception as exc:
        await notify_n8n_upload_event(
            status="failure",
            upload_type=upload_type,
            filename=filename,
            bucket=bucket,
            object_path=object_path,
            size_bytes=len(contents),
            error=error_reason(exc),
        )
        raise

    await notify_n8n_upload_event(
        status="success",
        upload_type=upload_type,
        filename=filename,
        bucket=bucket,
        object_path=object_path,
        size_bytes=len(contents),
    )


async def upload_to_supabase(
    bucket: str,
    object_path: str,
    contents: bytes,
    content_type: str,
    replace_existing: bool,
    duplicate_message: str,
):
    require_config()
    url = storage_object_url(bucket, object_path)
    create_headers = storage_upload_headers(content_type, upsert=False)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            if replace_existing:
                await delete_existing_storage_object(client, bucket, object_path)

            response = await client.post(url, headers=create_headers, content=contents)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach Supabase Storage at {SUPABASE_URL}: {type(exc).__name__}: {exc}",
        ) from exc

    if response.is_success:
        return

    error_detail = storage_error_detail(response)
    raise_storage_upload_error(
        response=response,
        error_detail=error_detail,
        replace_existing=replace_existing,
        duplicate_message=duplicate_message,
    )


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "file-upload-ui",
        "uptime_seconds": round(time.time() - STARTED_AT, 3),
        "configured": bool(
            SUPABASE_URL
            and SUPABASE_SERVICE_ROLE_KEY
            and UPLOAD_PASSWORD
            and ABRECHNUNG_UPLOAD_PASSWORD
        ),
        "n8n_upload_webhook_configured": bool(N8N_UPLOAD_WEBHOOK_URL),
        "oed_password_configured": bool(UPLOAD_PASSWORD),
        "abrechnung_password_configured": bool(ABRECHNUNG_UPLOAD_PASSWORD),
        "oed_bucket": SUPABASE_STORAGE_BUCKET,
        "oed_prefix": SUPABASE_STORAGE_PREFIX,
        "abrechnung_bucket": ABRECHNUNG_STORAGE_BUCKET,
        "abrechnung_prefix": ABRECHNUNG_STORAGE_PREFIX,
        "orders_table": SUPABASE_ORDERS_TABLE,
        "orders_schema": SUPABASE_DB_SCHEMA,
        "abrechnung_table": SUPABASE_ABRECHNUNG_TABLE,
        "abrechnung_schema": SUPABASE_ABRECHNUNG_SCHEMA,
    }


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


@app.post("/upload")
async def upload_file(
    upload_date: str = Form(...),
    file: UploadFile = File(...),
    password: str | None = Form(default=None),
    replace_existing: bool = Form(default=False),
):
    upload_type = "oed"
    filename = file.filename or "unknown-file"
    object_path = storage_path(filename, SUPABASE_STORAGE_PREFIX)
    size_bytes = 0
    upload_started = False
    orders_inserted = 0

    try:
        selected_date = parse_date(upload_date)
        extension = validate_extension(file.filename or "")
        filename = oed_filename(selected_date, extension)
        object_path = storage_path(filename, SUPABASE_STORAGE_PREFIX)
        require_password(password, UPLOAD_PASSWORD, "OED")
        contents = await read_limited_file(file)
        size_bytes = len(contents)
        try:
            order_rows = parse_oed_orders_excel(contents, extension)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        upload_started = True

        await upload_to_supabase_with_notifications(
            upload_type=upload_type,
            filename=filename,
            bucket=SUPABASE_STORAGE_BUCKET,
            object_path=object_path,
            contents=contents,
            content_type=file.content_type or "application/octet-stream",
            replace_existing=replace_existing,
            duplicate_message="A file for this date already exists. Enable replace and try again.",
        )
        try:
            orders_inserted = await insert_orders_csv_rows(order_rows)
        except Exception as exc:
            await notify_n8n_upload_event(
                status="failure",
                upload_type=upload_type,
                filename=filename,
                bucket=SUPABASE_STORAGE_BUCKET,
                object_path=object_path,
                size_bytes=size_bytes,
                error=error_reason(exc),
                event="orders_csv_insert_failure",
                extra={
                    "stage": "orders_csv_insert",
                    "orders_table": SUPABASE_ORDERS_TABLE,
                    "orders_schema": SUPABASE_DB_SCHEMA,
                    "rows_found": len(order_rows),
                    "rows_inserted": orders_inserted,
                },
            )
            raise

        await notify_n8n_upload_event(
            status="success",
            upload_type=upload_type,
            filename=filename,
            bucket=SUPABASE_STORAGE_BUCKET,
            object_path=object_path,
            size_bytes=size_bytes,
            event="orders_csv_insert_success",
            extra={
                "stage": "orders_csv_insert",
                "orders_table": SUPABASE_ORDERS_TABLE,
                "orders_schema": SUPABASE_DB_SCHEMA,
                "rows_found": len(order_rows),
                "rows_inserted": orders_inserted,
            },
        )
    except Exception as exc:
        if not upload_started:
            await notify_n8n_upload_event(
                status="failure",
                upload_type=upload_type,
                filename=filename,
                bucket=SUPABASE_STORAGE_BUCKET,
                object_path=object_path,
                size_bytes=size_bytes,
                error=error_reason(exc),
            )
        raise

    return {
        "ok": True,
        "filename": filename,
        "bucket": SUPABASE_STORAGE_BUCKET,
        "path": object_path,
        "size_bytes": len(contents),
        "orders_inserted": orders_inserted,
        "orders_table": SUPABASE_ORDERS_TABLE,
    }


@app.post("/upload-abrechnung")
async def upload_abrechnung_file(
    period_from: str = Form(...),
    period_to: str = Form(...),
    file: UploadFile = File(...),
    password: str | None = Form(default=None),
):
    upload_type = "doktorabc_abrechnung"
    filename = file.filename or "unknown-file"
    object_path = storage_path(filename, ABRECHNUNG_STORAGE_PREFIX)
    size_bytes = 0
    upload_started = False
    billing_inserted = 0
    billing_rows_found = 0
    billing_rows_skipped = 0

    try:
        start_date = parse_date(period_from)
        end_date = parse_date(period_to)
        if end_date < start_date:
            raise HTTPException(status_code=400, detail="The end date must be after the start date.")

        extension = validate_extension(file.filename or "")
        filename = abrechnung_filename(start_date, end_date, extension)
        object_path = storage_path(filename, ABRECHNUNG_STORAGE_PREFIX)
        require_password(password, ABRECHNUNG_UPLOAD_PASSWORD, "DoktorABC Abrechnung")
        contents = await read_limited_file(file)
        size_bytes = len(contents)
        try:
            billing_parse = parse_doktorabc_billing_excel(
                contents=contents,
                extension=extension,
                billing_period_from=start_date,
                billing_period_to=end_date,
                timezone_name=ABRECHNUNG_TIME_ZONE,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        billing_rows_found = len(billing_parse.rows)
        billing_rows_skipped = billing_parse.skipped_rows
        await ensure_abrechnung_can_be_imported(
            rows=billing_parse.rows,
            source_row_numbers=billing_parse.source_row_numbers,
            billing_period_from=start_date,
            billing_period_to=end_date,
        )
        upload_started = True

        await upload_to_supabase_with_notifications(
            upload_type=upload_type,
            filename=filename,
            bucket=ABRECHNUNG_STORAGE_BUCKET,
            object_path=object_path,
            contents=contents,
            content_type=file.content_type or "application/octet-stream",
            replace_existing=False,
            duplicate_message="A DoktorABC Abrechnung file for this period already exists.",
        )
        try:
            billing_inserted = await insert_abrechnung_rows(billing_parse.rows)
        except Exception as exc:
            await notify_n8n_upload_event(
                status="failure",
                upload_type=upload_type,
                filename=filename,
                bucket=ABRECHNUNG_STORAGE_BUCKET,
                object_path=object_path,
                size_bytes=size_bytes,
                error=error_reason(exc),
                event="doktorabc_abrechnung_insert_failure",
                extra={
                    "stage": "doktorabc_abrechnung_insert",
                    "billing_table": SUPABASE_ABRECHNUNG_TABLE,
                    "billing_schema": SUPABASE_ABRECHNUNG_SCHEMA,
                    "billing_period_from": start_date.isoformat(),
                    "billing_period_to": end_date.isoformat(),
                    "rows_found": billing_rows_found,
                    "rows_inserted": billing_inserted,
                    "rows_skipped": billing_rows_skipped,
                    "type_counts": billing_parse.type_counts,
                },
            )
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    await delete_existing_storage_object(client, ABRECHNUNG_STORAGE_BUCKET, object_path)
            except Exception as rollback_exc:
                print(
                    f"Could not delete uploaded Abrechnung file after DB insert failure: "
                    f"{type(rollback_exc).__name__}: {rollback_exc}",
                    flush=True,
                )
            raise

        await notify_n8n_upload_event(
            status="success",
            upload_type=upload_type,
            filename=filename,
            bucket=ABRECHNUNG_STORAGE_BUCKET,
            object_path=object_path,
            size_bytes=size_bytes,
            event="doktorabc_abrechnung_insert_success",
            extra={
                "stage": "doktorabc_abrechnung_insert",
                "billing_table": SUPABASE_ABRECHNUNG_TABLE,
                "billing_schema": SUPABASE_ABRECHNUNG_SCHEMA,
                "billing_period_from": start_date.isoformat(),
                "billing_period_to": end_date.isoformat(),
                "rows_found": billing_rows_found,
                "rows_inserted": billing_inserted,
                "rows_skipped": billing_rows_skipped,
                "type_counts": billing_parse.type_counts,
            },
        )
    except Exception as exc:
        if not upload_started:
            event = None
            extra = None
            if isinstance(exc, AbrechnungValidationError):
                event = "doktorabc_abrechnung_validation_failure"
                extra = {
                    "stage": "doktorabc_abrechnung_validation",
                    "validation_error_kind": exc.error_kind,
                    "billing_table": SUPABASE_ABRECHNUNG_TABLE,
                    "billing_schema": SUPABASE_ABRECHNUNG_SCHEMA,
                    "billing_period_from": period_from,
                    "billing_period_to": period_to,
                    "rows_found": billing_rows_found,
                    "rows_skipped": billing_rows_skipped,
                    **exc.extra,
                }
            await notify_n8n_upload_event(
                status="failure",
                upload_type=upload_type,
                filename=filename,
                bucket=ABRECHNUNG_STORAGE_BUCKET,
                object_path=object_path,
                size_bytes=size_bytes,
                error=error_reason(exc),
                event=event,
                extra=extra,
            )
        raise

    return {
        "ok": True,
        "filename": filename,
        "bucket": ABRECHNUNG_STORAGE_BUCKET,
        "path": object_path,
        "size_bytes": len(contents),
        "billing_rows_found": billing_rows_found,
        "billing_rows_inserted": billing_inserted,
        "billing_rows_skipped": billing_rows_skipped,
        "billing_table": SUPABASE_ABRECHNUNG_TABLE,
        "billing_schema": SUPABASE_ABRECHNUNG_SCHEMA,
    }


@app.api_route("/{path_name:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def not_found(path_name):
    return JSONResponse(status_code=404, content={"ok": False, "error": "not_found"})


def main():
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


HTML = """
<!doctype html>
<html lang="de">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Rats-Apotheke Upload</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f7f4f2;
        --panel: #ffffff;
        --text: #18202f;
        --muted: #667085;
        --line: #e3d8d4;
        --primary: #c91528;
        --primary-dark: #9f1020;
        --primary-soft: #fff0f1;
        --danger: #b42318;
        --ok: #027a48;
        --clinical: #e8f3f1;
        --clinical-deep: #0f766e;
        --ink: #0f172a;
        --shadow: 0 22px 60px rgba(74, 19, 28, 0.16);
      }

      body[data-theme="night"] {
        color-scheme: dark;
        --bg: #101418;
        --panel: #171c22;
        --text: #eef2f7;
        --muted: #a8b3c3;
        --line: #303946;
        --primary: #f04452;
        --primary-dark: #c91528;
        --primary-soft: rgba(240, 68, 82, 0.14);
        --danger: #ff8a7d;
        --ok: #68d391;
        --clinical: rgba(20, 184, 166, 0.14);
        --clinical-deep: #5eead4;
        --ink: #f8fafc;
        --shadow: 0 24px 70px rgba(0, 0, 0, 0.42);
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: Arial, Helvetica, sans-serif;
        background:
          linear-gradient(135deg, rgba(255, 255, 255, 0.92) 0%, rgba(251, 244, 242, 0.88) 42%, rgba(237, 247, 245, 0.9) 100%),
          url("data:image/svg+xml,%3Csvg width='96' height='96' viewBox='0 0 96 96' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' stroke='%230f766e' stroke-opacity='.09' stroke-width='2'%3E%3Cpath d='M48 18v60M18 48h60'/%3E%3C/g%3E%3C/svg%3E");
        color: var(--text);
      }

      body[data-theme="night"] {
        background:
          linear-gradient(135deg, rgba(13, 18, 24, 0.96) 0%, rgba(28, 22, 28, 0.94) 46%, rgba(10, 32, 34, 0.94) 100%),
          url("data:image/svg+xml,%3Csvg width='96' height='96' viewBox='0 0 96 96' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' stroke='%235eead4' stroke-opacity='.08' stroke-width='2'%3E%3Cpath d='M48 18v60M18 48h60'/%3E%3C/g%3E%3C/svg%3E");
      }

      main {
        width: min(1180px, calc(100% - 28px));
        margin: 0 auto;
        padding: 42px 0;
      }

      .shell {
        background: color-mix(in srgb, var(--panel) 96%, transparent);
        border: 1px solid rgba(201, 21, 40, 0.14);
        border-radius: 8px;
        box-shadow: var(--shadow);
        overflow: hidden;
      }

      header {
        display: flex;
        align-items: center;
        gap: 18px;
        padding: 26px 28px 22px;
        border-bottom: 1px solid var(--line);
        background: linear-gradient(90deg, #ffffff 0%, #fff7f5 54%, #f2fbf9 100%);
      }

      body[data-theme="night"] header {
        background: linear-gradient(90deg, #171c22 0%, #201a20 54%, #112729 100%);
      }

      .theme-toggle {
        margin-left: auto;
        min-width: 132px;
        min-height: 42px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        padding: 0 14px;
        border: 1px solid rgba(201, 21, 40, 0.18);
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.86);
        color: var(--ink);
        box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
        font-size: 13px;
        font-weight: 800;
        cursor: pointer;
      }

      body[data-theme="night"] .theme-toggle {
        border-color: rgba(94, 234, 212, 0.24);
        background: rgba(15, 23, 42, 0.82);
        color: var(--text);
      }

      .theme-icon {
        font-size: 16px;
        line-height: 1;
      }

      .brand-mark {
        width: 76px;
        height: 76px;
        flex: 0 0 auto;
        position: relative;
        display: grid;
        place-items: center;
        border-radius: 8px;
        background: #fff;
        box-shadow: inset 0 0 0 1px rgba(201, 21, 40, 0.12), 0 10px 26px rgba(201, 21, 40, 0.16);
      }

      body[data-theme="night"] .brand-mark {
        background: #111827;
        box-shadow: inset 0 0 0 1px rgba(240, 68, 82, 0.22), 0 10px 26px rgba(0, 0, 0, 0.34);
      }

      .brand-a {
        width: 52px;
        height: 52px;
        position: relative;
        background: var(--primary);
        clip-path: polygon(50% 0, 94% 28%, 94% 82%, 74% 82%, 74% 44%, 50% 29%, 26% 44%, 26% 82%, 6% 82%, 6% 28%);
      }

      .brand-a::before,
      .brand-a::after {
        content: "";
        position: absolute;
        background: #fff;
        left: 50%;
        top: 43%;
        transform: translate(-50%, -50%);
        border-radius: 2px;
      }

      .brand-a::before {
        width: 25px;
        height: 8px;
      }

      .brand-a::after {
        width: 8px;
        height: 25px;
      }

      .brand-copy {
        display: grid;
        gap: 3px;
        min-width: 0;
      }

      h1,
      h2 {
        margin: 0;
        color: var(--ink);
        letter-spacing: 0;
      }

      h1 {
        font-size: 34px;
        font-weight: 800;
      }

      h2 {
        font-size: 22px;
        font-weight: 800;
      }

      .subtitle {
        margin: 0;
        color: #697386;
        font-size: 18px;
        font-weight: 700;
        letter-spacing: 6px;
      }

      body[data-theme="night"] .subtitle {
        color: #a8b3c3;
      }

      .content {
        padding: 28px;
      }

      .upload-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 22px;
        align-items: start;
      }

      .upload-panel {
        min-height: 100%;
        padding: 20px;
        border: 1px solid rgba(217, 200, 197, 0.9);
        border-radius: 8px;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(255, 249, 248, 0.98) 100%);
        box-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
      }

      body[data-theme="night"] .upload-panel {
        border-color: rgba(240, 68, 82, 0.18);
        background: linear-gradient(180deg, rgba(23, 28, 34, 0.98) 0%, rgba(28, 25, 31, 0.98) 100%);
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.28);
      }

      .upload-panel[data-tone="clinical"] {
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.98) 0%, rgba(241, 250, 248, 0.98) 100%);
        border-color: rgba(15, 118, 110, 0.22);
      }

      body[data-theme="night"] .upload-panel[data-tone="clinical"] {
        background: linear-gradient(180deg, rgba(22, 29, 34, 0.98) 0%, rgba(12, 36, 38, 0.98) 100%);
        border-color: rgba(94, 234, 212, 0.2);
      }

      .panel-title {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 18px;
      }

      .panel-badge {
        width: 36px;
        height: 36px;
        display: grid;
        place-items: center;
        border-radius: 8px;
        background: var(--primary-soft);
        color: var(--primary);
        font-size: 17px;
        font-weight: 900;
      }

      .upload-panel[data-tone="clinical"] .panel-badge {
        background: var(--clinical);
        color: var(--clinical-deep);
      }

      form {
        display: grid;
        gap: 18px;
      }

      label {
        display: grid;
        gap: 8px;
        color: #344054;
        font-size: 14px;
        font-weight: 700;
      }

      body[data-theme="night"] label,
      body[data-theme="night"] .check {
        color: #d5dde8;
      }

      .date-row {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }

      input[type="date"],
      input[type="password"] {
        width: 100%;
        height: 50px;
        border: 1px solid #d9c8c5;
        border-radius: 8px;
        padding: 0 15px;
        color: var(--text);
        background: var(--panel);
        font: inherit;
        font-weight: 700;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
        transition: border-color 120ms ease, box-shadow 120ms ease;
      }

      input[type="date"]:focus,
      input[type="password"]:focus {
        outline: none;
        border-color: var(--primary);
        box-shadow: 0 0 0 4px rgba(201, 21, 40, 0.12);
      }

      .upload-panel[data-tone="clinical"] input[type="date"]:focus,
      .upload-panel[data-tone="clinical"] input[type="password"]:focus {
        border-color: var(--clinical-deep);
        box-shadow: 0 0 0 4px rgba(15, 118, 110, 0.12);
      }

      .dropzone {
        display: grid;
        place-items: center;
        min-height: 186px;
        padding: 22px;
        border: 1.5px dashed #d1404e;
        border-radius: 8px;
        background: linear-gradient(180deg, #fff 0%, #fff8f8 100%);
        cursor: pointer;
        text-align: center;
        transition: border-color 120ms ease, background 120ms ease, box-shadow 120ms ease, transform 120ms ease;
      }

      body[data-theme="night"] .dropzone {
        background: linear-gradient(180deg, #171c22 0%, #211b21 100%);
      }

      .upload-panel[data-tone="clinical"] .dropzone {
        border-color: #2c9a8e;
        background: linear-gradient(180deg, #fff 0%, #f3fbfa 100%);
      }

      body[data-theme="night"] .upload-panel[data-tone="clinical"] .dropzone {
        border-color: #3dd6c4;
        background: linear-gradient(180deg, #171c22 0%, #10292b 100%);
      }

      .dropzone[data-active="true"] {
        border-color: var(--primary);
        background: var(--primary-soft);
        box-shadow: 0 0 0 4px rgba(201, 21, 40, 0.1);
        transform: translateY(-1px);
      }

      .upload-panel[data-tone="clinical"] .dropzone[data-active="true"] {
        border-color: var(--clinical-deep);
        background: var(--clinical);
        box-shadow: 0 0 0 4px rgba(15, 118, 110, 0.1);
      }

      .file-icon {
        width: 58px;
        height: 58px;
        margin: 0 auto 12px;
        display: grid;
        place-items: center;
        border-radius: 8px;
        background: var(--primary-soft);
        color: var(--primary);
        border: 1px solid rgba(201, 21, 40, 0.16);
        font-size: 28px;
        font-weight: 800;
      }

      .upload-panel[data-tone="clinical"] .file-icon {
        background: var(--clinical);
        color: var(--clinical-deep);
        border-color: rgba(15, 118, 110, 0.16);
      }

      .drop-title {
        margin: 0;
        font-size: 17px;
        font-weight: 700;
      }

      .drop-meta,
      .filename-preview {
        margin: 6px 0 0;
        color: var(--muted);
        font-size: 14px;
        overflow-wrap: anywhere;
      }

      input[type="file"] {
        position: absolute;
        width: 1px;
        height: 1px;
        opacity: 0;
        pointer-events: none;
      }

      .row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        flex-wrap: wrap;
      }

      .row.single-action {
        justify-content: flex-end;
      }

      .check {
        display: inline-flex;
        align-items: center;
        gap: 9px;
        color: #344054;
        font-size: 14px;
        font-weight: 700;
      }

      body[data-theme="night"] .check {
        color: #d5dde8;
      }

      .check input {
        width: 18px;
        height: 18px;
        accent-color: var(--primary);
      }

      button {
        min-height: 52px;
        border: 1px solid rgba(111, 12, 24, 0.24);
        border-radius: 8px;
        padding: 0 22px;
        background: linear-gradient(180deg, #df1f34 0%, var(--primary) 100%);
        color: white;
        font: inherit;
        font-weight: 800;
        cursor: pointer;
        box-shadow: 0 12px 24px rgba(201, 21, 40, 0.26), inset 0 1px 0 rgba(255, 255, 255, 0.2);
        transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
      }

      .upload-panel[data-tone="clinical"] button {
        border-color: rgba(7, 89, 82, 0.24);
        background: linear-gradient(180deg, #129080 0%, var(--clinical-deep) 100%);
        box-shadow: 0 12px 24px rgba(15, 118, 110, 0.24), inset 0 1px 0 rgba(255, 255, 255, 0.2);
      }

      button:disabled {
        cursor: not-allowed;
        opacity: 0.55;
      }

      button:not(:disabled):hover {
        background: linear-gradient(180deg, #d51b30 0%, var(--primary-dark) 100%);
        box-shadow: 0 14px 28px rgba(201, 21, 40, 0.32), inset 0 1px 0 rgba(255, 255, 255, 0.2);
        transform: translateY(-1px);
      }

      .upload-panel[data-tone="clinical"] button:not(:disabled):hover {
        background: linear-gradient(180deg, #0f8275 0%, #0b5f58 100%);
        box-shadow: 0 14px 28px rgba(15, 118, 110, 0.3), inset 0 1px 0 rgba(255, 255, 255, 0.2);
      }

      button:not(:disabled):active {
        transform: translateY(0);
      }

      .status {
        min-height: 46px;
        display: none;
        align-items: center;
        border-radius: 8px;
        padding: 12px 14px;
        font-size: 14px;
        line-height: 1.35;
        animation: status-pop 180ms ease;
      }

      .status[data-kind="ok"] {
        display: flex;
        background: #ecfdf3;
        color: var(--ok);
      }

      .status[data-kind="error"] {
        display: flex;
        background: #fef3f2;
        color: var(--danger);
        border: 1px solid rgba(180, 35, 24, 0.16);
      }

      .toast {
        position: fixed;
        left: 50%;
        bottom: 24px;
        z-index: 10;
        width: min(460px, calc(100% - 28px));
        min-height: 54px;
        display: none;
        align-items: center;
        justify-content: center;
        padding: 14px 18px;
        border-radius: 8px;
        background: #7f1d1d;
        color: white;
        box-shadow: 0 18px 42px rgba(127, 29, 29, 0.35);
        font-size: 15px;
        font-weight: 800;
        text-align: center;
        transform: translateX(-50%);
      }

      .toast[data-visible="true"] {
        display: flex;
        animation: toast-pop 260ms ease;
      }

      .field-error {
        border-color: var(--primary) !important;
        box-shadow: 0 0 0 4px rgba(201, 21, 40, 0.14) !important;
        animation: shake 220ms ease;
      }

      @keyframes status-pop {
        from {
          opacity: 0.65;
          transform: translateY(4px);
        }
        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      @keyframes toast-pop {
        0% {
          opacity: 0;
          transform: translate(-50%, 8px) scale(0.98);
        }
        100% {
          opacity: 1;
          transform: translate(-50%, 0) scale(1);
        }
      }

      @keyframes shake {
        0%, 100% {
          transform: translateX(0);
        }
        35% {
          transform: translateX(-5px);
        }
        70% {
          transform: translateX(5px);
        }
      }

      @media (max-width: 900px) {
        .upload-grid {
          grid-template-columns: 1fr;
        }
      }

      @media (max-width: 560px) {
        main {
          width: min(100% - 18px, 680px);
          padding: 18px 0;
        }

        header,
        .content {
          padding-left: 18px;
          padding-right: 18px;
        }

        h1 {
          font-size: 27px;
        }

        h2 {
          font-size: 20px;
        }

        header {
          align-items: flex-start;
          flex-wrap: wrap;
        }

        .theme-toggle {
          width: 100%;
          margin-left: 0;
        }

        .brand-mark {
          width: 62px;
          height: 62px;
        }

        .brand-a {
          width: 43px;
          height: 43px;
        }

        .subtitle {
          font-size: 14px;
          letter-spacing: 4px;
        }

        .date-row {
          grid-template-columns: 1fr;
        }

        button {
          width: 100%;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="shell">
        <header>
          <div class="brand-mark" aria-hidden="true">
            <div class="brand-a"></div>
          </div>
          <div class="brand-copy">
            <h1>Rats-Apotheke</h1>
            <p class="subtitle">BLIESKASTEL</p>
          </div>
          <button id="theme-toggle" class="theme-toggle" type="button" aria-label="Darstellung wechseln">
            <span id="theme-icon" class="theme-icon">L</span>
            <span id="theme-label">Light</span>
          </button>
        </header>

        <div class="content">
          <div class="upload-grid">
            <section class="upload-panel">
              <div class="panel-title">
                <div class="panel-badge">O</div>
                <h2>OED Upload</h2>
              </div>

              <form id="oed-form">
                <label>
                  Datum
                  <input id="oed-date" name="upload_date" type="date" required />
                </label>

                <label>
                  Excel-Datei
                  <div id="oed-dropzone" class="dropzone" role="button" tabindex="0">
                    <div>
                      <div class="file-icon">X</div>
                      <p id="oed-drop-title" class="drop-title">Datei auswaehlen</p>
                      <p class="drop-meta">.xlsx oder .xls</p>
                      <p id="oed-preview" class="filename-preview"></p>
                    </div>
                  </div>
                  <input id="oed-file" name="file" type="file" accept=".xlsx,.xls" required />
                </label>

                <label>
                  Passwort
                  <input id="oed-password" name="password" type="password" autocomplete="current-password" />
                </label>

                <div class="row">
                  <label class="check">
                    <input id="oed-replace-existing" name="replace_existing" type="checkbox" />
                    Vorhandene Datei ersetzen
                  </label>
                  <button id="oed-submit" type="submit" disabled>Hochladen</button>
                </div>

                <div id="oed-status" class="status" aria-live="polite"></div>
              </form>
            </section>

            <section class="upload-panel" data-tone="clinical">
              <div class="panel-title">
                <div class="panel-badge">D</div>
                <h2>DoktorABC Abrechnung</h2>
              </div>

              <form id="abrechnung-form">
                <div class="date-row">
                  <label>
                    Von
                    <input id="abrechnung-from" name="period_from" type="date" required />
                  </label>
                  <label>
                    Bis
                    <input id="abrechnung-to" name="period_to" type="date" required />
                  </label>
                </div>

                <label>
                  Excel-Datei
                  <div id="abrechnung-dropzone" class="dropzone" role="button" tabindex="0">
                    <div>
                      <div class="file-icon">X</div>
                      <p id="abrechnung-drop-title" class="drop-title">Datei auswaehlen</p>
                      <p class="drop-meta">.xlsx oder .xls</p>
                      <p id="abrechnung-preview" class="filename-preview"></p>
                    </div>
                  </div>
                  <input id="abrechnung-file" name="file" type="file" accept=".xlsx,.xls" required />
                </label>

                <label>
                  Passwort
                  <input id="abrechnung-password" name="password" type="password" autocomplete="current-password" />
                </label>

                <div class="row single-action">
                  <button id="abrechnung-submit" type="submit" disabled>Abrechnung hochladen</button>
                </div>

                <div id="abrechnung-status" class="status" aria-live="polite"></div>
              </form>
            </section>
          </div>
        </div>
      </section>
    </main>
    <div id="toast" class="toast" role="alert" aria-live="assertive"></div>

    <script>
      const toast = document.querySelector("#toast");
      const themeToggle = document.querySelector("#theme-toggle");
      const themeIcon = document.querySelector("#theme-icon");
      const themeLabel = document.querySelector("#theme-label");
      let toastTimer;

      function applyTheme(theme) {
        document.body.dataset.theme = theme;
        themeIcon.textContent = theme === "night" ? "N" : "H";
        themeLabel.textContent = theme === "night" ? "Nacht" : "Hell";
        themeToggle.setAttribute("aria-pressed", theme === "night" ? "true" : "false");
        localStorage.setItem("upload-ui-theme", theme);
      }

      const savedTheme = localStorage.getItem("upload-ui-theme") || "light";
      applyTheme(savedTheme === "night" ? "night" : "light");

      themeToggle.addEventListener("click", () => {
        applyTheme(document.body.dataset.theme === "night" ? "light" : "night");
      });

      const today = new Date();
      const localToday = new Date(today.getTime() - today.getTimezoneOffset() * 60000)
        .toISOString()
        .slice(0, 10);
      const firstDay = new Date(today.getFullYear(), today.getMonth(), 1);
      const localFirstDay = new Date(firstDay.getTime() - firstDay.getTimezoneOffset() * 60000)
        .toISOString()
        .slice(0, 10);

      function fileExtension(fileInput) {
        if (!fileInput.files.length) return ".xlsx";
        const parts = fileInput.files[0].name.split(".");
        return parts.length > 1 ? "." + parts.pop().toLowerCase() : ".xlsx";
      }

      function setStatus(statusBox, kind, text) {
        statusBox.dataset.kind = kind;
        statusBox.textContent = text;
        statusBox.style.animation = "none";
        statusBox.offsetHeight;
        statusBox.style.animation = "";
      }

      function showToast(text) {
        clearTimeout(toastTimer);
        toast.dataset.visible = "false";
        toast.textContent = text;
        toast.offsetHeight;
        toast.dataset.visible = "true";
        toastTimer = setTimeout(() => {
          toast.dataset.visible = "false";
        }, 3200);
      }

      function markPasswordError(passwordInput) {
        passwordInput.classList.remove("field-error");
        passwordInput.offsetHeight;
        passwordInput.classList.add("field-error");
        passwordInput.focus();
        setTimeout(() => {
          passwordInput.classList.remove("field-error");
        }, 900);
      }

      function parseResultError(result) {
        const detail = result.detail || result.error || "Upload fehlgeschlagen.";
        return typeof detail === "string" ? detail : JSON.stringify(detail);
      }

      function attachDropzone(controller) {
        function pickFile(event) {
          if (event) {
            event.preventDefault();
            event.stopPropagation();
          }
          controller.fileInput.click();
        }

        controller.dropzone.addEventListener("click", pickFile);
        controller.fileInput.addEventListener("click", (event) => event.stopPropagation());
        controller.dropzone.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            pickFile();
          }
        });

        ["dragenter", "dragover"].forEach((eventName) => {
          controller.dropzone.addEventListener(eventName, (event) => {
            event.preventDefault();
            event.stopPropagation();
            controller.dropzone.dataset.active = "true";
          });
        });

        ["dragleave", "drop"].forEach((eventName) => {
          controller.dropzone.addEventListener(eventName, (event) => {
            event.preventDefault();
            event.stopPropagation();
            controller.dropzone.dataset.active = "false";
          });
        });

        controller.dropzone.addEventListener("drop", (event) => {
          const file = event.dataTransfer.files[0];
          if (!file) return;
          controller.fileInput.files = event.dataTransfer.files;
          controller.refresh();
        });
      }

      function createUploadController(config) {
        const controller = {
          form: document.querySelector(config.form),
          fileInput: document.querySelector(config.fileInput),
          dropzone: document.querySelector(config.dropzone),
          dropTitle: document.querySelector(config.dropTitle),
          preview: document.querySelector(config.preview),
          passwordInput: document.querySelector(config.passwordInput),
          submitButton: document.querySelector(config.submitButton),
          statusBox: document.querySelector(config.statusBox),
          refresh() {
            const targetName = config.targetName();
            this.preview.textContent = this.fileInput.files.length
              ? `${this.fileInput.files[0].name} -> ${targetName}`
              : `Zielname: ${targetName}`;
            this.dropTitle.textContent = this.fileInput.files.length
              ? this.fileInput.files[0].name
              : "Datei auswaehlen";
            this.submitButton.disabled = !config.isReady();
          },
        };

        attachDropzone(controller);
        controller.fileInput.addEventListener("change", () => controller.refresh());
        config.watch.forEach((input) => input.addEventListener("input", () => controller.refresh()));

        controller.form.addEventListener("submit", async (event) => {
          event.preventDefault();
          controller.submitButton.disabled = true;
          setStatus(controller.statusBox, "ok", "Upload laeuft...");

          try {
            const data = config.formData();
            const response = await fetch(config.endpoint, { method: "POST", body: data });
            const contentType = response.headers.get("content-type") || "";
            const result = contentType.includes("application/json")
              ? await response.json()
              : { detail: await response.text() };

            if (!response.ok || !result.ok) {
              const message = parseResultError(result);
              const isPasswordError = response.status === 401 || message.toLowerCase().includes("password");
              if (isPasswordError) {
                markPasswordError(controller.passwordInput);
                showToast(`${config.label}: falsches Passwort. Bitte erneut pruefen.`);
                throw new Error("Falsches Passwort. Bitte erneut pruefen.");
              }
              throw new Error(message);
            }

            const dbInfo = typeof result.billing_rows_inserted === "number"
              ? `, Abrechnungs-Zeilen: ${result.billing_rows_inserted}`
              : typeof result.orders_inserted === "number"
                ? `, DB-Zeilen: ${result.orders_inserted}`
                : "";
            const skippedInfo = typeof result.billing_rows_skipped === "number" && result.billing_rows_skipped > 0
              ? `, uebersprungen: ${result.billing_rows_skipped}`
              : "";
            setStatus(controller.statusBox, "ok", `Gespeichert: ${result.path}${dbInfo}${skippedInfo}`);
            config.afterSuccess();
            controller.refresh();
          } catch (error) {
            setStatus(controller.statusBox, "error", error.message);
          } finally {
            controller.submitButton.disabled = !config.isReady();
          }
        });

        controller.refresh();
        return controller;
      }

      const oedDate = document.querySelector("#oed-date");
      const oedFile = document.querySelector("#oed-file");
      const oedPassword = document.querySelector("#oed-password");
      const oedReplaceExisting = document.querySelector("#oed-replace-existing");

      const abrechnungFrom = document.querySelector("#abrechnung-from");
      const abrechnungTo = document.querySelector("#abrechnung-to");
      const abrechnungFile = document.querySelector("#abrechnung-file");
      const abrechnungPassword = document.querySelector("#abrechnung-password");

      oedDate.value = localToday;
      abrechnungFrom.value = localFirstDay;
      abrechnungTo.value = localToday;

      createUploadController({
        label: "OED",
        form: "#oed-form",
        endpoint: "/upload",
        fileInput: "#oed-file",
        dropzone: "#oed-dropzone",
        dropTitle: "#oed-drop-title",
        preview: "#oed-preview",
        passwordInput: "#oed-password",
        submitButton: "#oed-submit",
        statusBox: "#oed-status",
        watch: [oedDate],
        targetName: () => `oed_${oedDate.value || "YYYY-MM-DD"}${fileExtension(oedFile)}`,
        isReady: () => Boolean(oedDate.value && oedFile.files.length),
        formData: () => {
          const data = new FormData();
          data.append("upload_date", oedDate.value);
          data.append("file", oedFile.files[0]);
          data.append("password", oedPassword.value);
          if (oedReplaceExisting.checked) {
            data.append("replace_existing", "true");
          }
          return data;
        },
        afterSuccess: () => {
          document.querySelector("#oed-form").reset();
          oedDate.value = localToday;
        },
      });

      createUploadController({
        label: "DoktorABC Abrechnung",
        form: "#abrechnung-form",
        endpoint: "/upload-abrechnung",
        fileInput: "#abrechnung-file",
        dropzone: "#abrechnung-dropzone",
        dropTitle: "#abrechnung-drop-title",
        preview: "#abrechnung-preview",
        passwordInput: "#abrechnung-password",
        submitButton: "#abrechnung-submit",
        statusBox: "#abrechnung-status",
        watch: [abrechnungFrom, abrechnungTo],
        targetName: () => {
          const fromValue = abrechnungFrom.value || "YYYY-MM-DD";
          const toValue = abrechnungTo.value || "YYYY-MM-DD";
          return `doktorabc_abrechnung_${fromValue}_bis_${toValue}${fileExtension(abrechnungFile)}`;
        },
        isReady: () => Boolean(abrechnungFrom.value && abrechnungTo.value && abrechnungFile.files.length),
        formData: () => {
          const data = new FormData();
          data.append("period_from", abrechnungFrom.value);
          data.append("period_to", abrechnungTo.value);
          data.append("file", abrechnungFile.files[0]);
          data.append("password", abrechnungPassword.value);
          return data;
        },
        afterSuccess: () => {
          document.querySelector("#abrechnung-form").reset();
          abrechnungFrom.value = localFirstDay;
          abrechnungTo.value = localToday;
        },
      });
    </script>
  </body>
</html>
"""


if __name__ == "__main__":
    main()
