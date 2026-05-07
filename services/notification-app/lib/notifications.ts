export type NotificationSection = "upload" | "doktorabc_sync" | "doktorabc_orders" | "abrechnung_verification";
export type NotificationStatus = "triggered" | "success" | "failure" | "info" | "warning";

export type StoredNotification = {
  id: string;
  section: NotificationSection | string;
  event: string;
  status: NotificationStatus;
  title: string;
  message: string;
  filename: string | null;
  upload_type: string | null;
  bucket: string | null;
  path: string | null;
  size_bytes: number | null;
  error: string | null;
  source: string;
  payload: Record<string, unknown>;
  created_at: string;
};

export type UploadWebhookPayload = {
  event?: string;
  status?: string;
  section?: string;
  upload_type?: string;
  sync_type?: string;
  order_type?: string;
  order_list_type?: string;
  order_count?: number;
  orders?: Array<Record<string, unknown>>;
  run_id?: string;
  sent_to_n8n?: boolean;
  n8n_status_code?: number;
  n8n_skipped_reason?: string;
  download_filename?: string;
  download_path?: string;
  download_size_bytes?: number;
  filename?: string;
  bucket?: string;
  path?: string;
  size_bytes?: number;
  timestamp?: string;
  service?: string;
  error?: string;
  stage?: string;
  orders_table?: string;
  orders_schema?: string;
  billing_table?: string;
  billing_schema?: string;
  billing_period_from?: string;
  billing_period_to?: string;
  validation_error_kind?: string;
  rows_found?: number;
  rows_inserted?: number;
  rows_skipped?: number;
  started_at?: string | null;
  finished_at?: string;
  duration_ms?: number | null;
  endpoint?: string;
  summary?: {
    scraped?: number;
    inserted?: number;
    updated?: number;
    unchanged?: number;
    sent_to_supabase?: number;
  };
  logs?: Record<string, unknown>;
};

function requiredEnv(name: string) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function supabaseUrl() {
  return requiredEnv("SUPABASE_URL").replace(/\/$/, "");
}

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = process.env.SUPABASE_NOTIFICATIONS_SCHEMA || process.env.SUPABASE_SCHEMA || "public";

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function tableName() {
  return encodeURIComponent(process.env.SUPABASE_NOTIFICATIONS_TABLE || "notifications");
}

function tableUrl() {
  return `${supabaseUrl()}/rest/v1/${tableName()}`;
}

export function notificationConfigStatus() {
  return {
    configured: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
    table: process.env.SUPABASE_NOTIFICATIONS_TABLE || "notifications",
    schema: process.env.SUPABASE_NOTIFICATIONS_SCHEMA || process.env.SUPABASE_SCHEMA || "public",
  };
}

export function normalizeUploadNotification(payload: UploadWebhookPayload): Omit<StoredNotification, "id"> {
  const status = normalizeStatus(payload.status);
  const filename = String(payload.filename || "unknown-file");
  const uploadType = String(payload.upload_type || "upload");
  const event = String(payload.event || `upload_${status}`);
  const source = String(payload.service || "n8n");

  if (isProductSync(payload)) {
    return {
      section: "doktorabc_sync",
      event,
      status,
      title: productSyncTitle(status),
      message: productSyncMessage(status, payload),
      filename: null,
      upload_type: payload.sync_type ? String(payload.sync_type) : "doktorabc_products",
      bucket: null,
      path: payload.endpoint ? String(payload.endpoint) : null,
      size_bytes: null,
      error: payload.error ? String(payload.error) : null,
      source,
      payload: payload as Record<string, unknown>,
      created_at: timestampOrNow(payload.timestamp || payload.finished_at),
    };
  }

  if (isEodBotNotification(payload)) {
    return {
      section: "doktorabc_orders",
      event,
      status,
      title: eodBotTitle(status, payload),
      message: eodBotMessage(status, payload),
      filename: payload.filename || payload.download_filename ? String(payload.filename || payload.download_filename) : null,
      upload_type: payload.order_list_type ? String(payload.order_list_type) : "doktorabc_eod_bot",
      bucket: null,
      path: payload.path || payload.download_path ? String(payload.path || payload.download_path) : null,
      size_bytes:
        typeof payload.size_bytes === "number"
          ? payload.size_bytes
          : typeof payload.download_size_bytes === "number"
            ? payload.download_size_bytes
            : null,
      error: payload.error ? String(payload.error) : null,
      source,
      payload: payload as Record<string, unknown>,
      created_at: timestampOrNow(payload.timestamp || payload.finished_at),
    };
  }

  return {
    section: "upload",
    event,
    status,
    title: uploadTitle(status, uploadType, payload),
    message: uploadMessage(status, filename, payload),
    filename,
    upload_type: uploadType,
    bucket: payload.bucket ? String(payload.bucket) : null,
    path: payload.path ? String(payload.path) : null,
    size_bytes: typeof payload.size_bytes === "number" ? payload.size_bytes : null,
    error: payload.error ? String(payload.error) : null,
    source,
    payload: payload as Record<string, unknown>,
    created_at: timestampOrNow(payload.timestamp),
  };
}

export async function insertNotification(notification: Omit<StoredNotification, "id">) {
  const response = await fetch(tableUrl(), {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify(notification),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase insert failed (${response.status}): ${await response.text()}`);
  }

  const rows = (await response.json()) as StoredNotification[];
  return rows[0];
}

export async function listNotifications(limit = 100) {
  if (!notificationConfigStatus().configured) {
    return [];
  }

  const url = new URL(tableUrl());
  url.searchParams.set("select", "*");
  url.searchParams.set("order", "created_at.desc");
  url.searchParams.set("limit", String(limit));

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase read failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as StoredNotification[];
}

function normalizeStatus(status: string | undefined): NotificationStatus {
  if (status === "triggered" || status === "success" || status === "failure") {
    return status;
  }

  return "info";
}

function timestampOrNow(value: string | undefined) {
  if (!value) {
    return new Date().toISOString();
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function uploadTitle(status: NotificationStatus, uploadType: string, payload: UploadWebhookPayload) {
  if (isAbrechnungValidationFailure(payload)) {
    return "DoktorABC Abrechnung abgelehnt";
  }

  if (isAbrechnungDbInsert(payload)) {
    if (status === "success") {
      return "DoktorABC Abrechnung in Datenbank angekommen";
    }

    if (status === "failure") {
      return "DoktorABC Datenbankimport fehlgeschlagen";
    }

    return "DoktorABC Datenbankimport";
  }

  if (isOrdersCsvInsert(payload)) {
    if (status === "success") {
      return "OED Datenbankimport erfolgreich";
    }

    if (status === "failure") {
      return "OED Datenbankimport fehlgeschlagen";
    }

    return "OED Datenbankimport";
  }

  const label = uploadType === "doktorabc_abrechnung" ? "DoktorABC Abrechnung" : "OED Upload";

  if (uploadType === "doktorabc_abrechnung" && status === "success") {
    return "DoktorABC Abrechnung gespeichert";
  }

  if (status === "triggered") {
    return `${label} gestartet`;
  }

  if (status === "success") {
    return `${label} erfolgreich`;
  }

  if (status === "failure") {
    return `${label} fehlgeschlagen`;
  }

  return `${label} Meldung`;
}

function uploadMessage(status: NotificationStatus, filename: string, payload: UploadWebhookPayload) {
  if (isAbrechnungValidationFailure(payload)) {
    return payload.error ? String(payload.error) : `Abrechnung wurde nicht importiert: ${filename}`;
  }

  if (isAbrechnungDbInsert(payload)) {
    const table = payload.billing_table || "doktorabc_billing";
    const schema = payload.billing_schema || "private";
    const rowsInserted = typeof payload.rows_inserted === "number" ? payload.rows_inserted : null;

    if (status === "success") {
      const rowText = rowsInserted === null ? "DB-Zeilen" : `${rowsInserted} DB-Zeilen`;
      return `${rowText} in ${schema}.${table} gespeichert: ${filename}`;
    }

    if (status === "failure") {
      return `Datenbankimport fuer ${filename} fehlgeschlagen${payload.error ? `: ${payload.error}` : ""}`;
    }

    return `Datenbankimport fuer ${filename}`;
  }

  if (isOrdersCsvInsert(payload)) {
    const table = payload.orders_table || "orders_csv";
    const rowsInserted = typeof payload.rows_inserted === "number" ? payload.rows_inserted : null;
    const rowsFound = typeof payload.rows_found === "number" ? payload.rows_found : null;

    if (status === "success") {
      const rowText = rowsInserted === null ? "Zeilen" : `${rowsInserted} Zeilen`;
      return `${rowText} in ${table} eingefuegt: ${filename}`;
    }

    if (status === "failure") {
      const rowText = rowsFound === null ? "" : ` (${rowsFound} gelesene Zeilen)`;
      return `Datenbankimport fuer ${filename} fehlgeschlagen${rowText}${payload.error ? `: ${payload.error}` : ""}`;
    }

    return `Datenbankimport fuer ${filename}`;
  }

  if (status === "triggered") {
    return `Upload wurde gestartet: ${filename}`;
  }

  if (uploadTypeFromPayload(payload) === "doktorabc_abrechnung" && status === "success") {
    return "Datei wurde im Speicher abgelegt. Datenbankimport folgt.";
  }

  if (status === "success") {
    return `Upload wurde erfolgreich gespeichert: ${filename}`;
  }

  if (status === "failure") {
    return `Upload konnte nicht gespeichert werden: ${filename}${payload.error ? ` (${payload.error})` : ""}`;
  }

  return `Upload-Meldung fuer ${filename}`;
}

function isOrdersCsvInsert(payload: UploadWebhookPayload) {
  return (
    payload.stage === "orders_csv_insert" ||
    payload.event === "orders_csv_insert_success" ||
    payload.event === "orders_csv_insert_failure"
  );
}

function isAbrechnungDbInsert(payload: UploadWebhookPayload) {
  return (
    payload.stage === "doktorabc_abrechnung_insert" ||
    payload.event === "doktorabc_abrechnung_insert_success" ||
    payload.event === "doktorabc_abrechnung_insert_failure"
  );
}

function isAbrechnungValidationFailure(payload: UploadWebhookPayload) {
  return (
    payload.stage === "doktorabc_abrechnung_validation" ||
    payload.event === "doktorabc_abrechnung_validation_failure"
  );
}

function uploadTypeFromPayload(payload: UploadWebhookPayload) {
  return String(payload.upload_type || "upload");
}

function isProductSync(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  return (
    payload.section === "doktorabc_sync" ||
    payload.sync_type === "doktorabc_products" ||
    event === "doktorabc_sync_success" ||
    event === "doktorabc_sync_failure"
  );
}

function isEodBotNotification(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  return (
    payload.section === "doktorabc_orders" ||
    payload.sync_type === "doktorabc_eod_bot" ||
    event === "doktorabc_eod_orders_success" ||
    event === "doktorabc_pickup_ready_orders_success" ||
    event === "doktorabc_eod_excel_export_success"
  );
}

function productSyncTitle(status: NotificationStatus) {
  if (status === "success") {
    return "DoktorABC Synchronisierung erfolgreich";
  }

  if (status === "failure") {
    return "DoktorABC Synchronisierung fehlgeschlagen";
  }

  return "DoktorABC Synchronisierung";
}

function productSyncMessage(status: NotificationStatus, payload: UploadWebhookPayload) {
  if (status === "success") {
    const summary = payload.summary || {};
    return [
      `${numberOrZero(summary.scraped)} Produkte geprueft`,
      `${numberOrZero(summary.inserted)} neu`,
      `${numberOrZero(summary.updated)} geaendert`,
      `${numberOrZero(summary.unchanged)} unveraendert`,
    ].join(", ");
  }

  if (status === "failure") {
    return payload.error ? String(payload.error) : "Synchronisierung konnte nicht abgeschlossen werden.";
  }

  return "DoktorABC Sync-Meldung";
}

function eodBotTitle(status: NotificationStatus, payload: UploadWebhookPayload) {
  if (status === "failure") {
    return "DoktorABC Orders Bot fehlgeschlagen";
  }

  if (payload.order_list_type === "pickup_ready" || payload.event === "doktorabc_pickup_ready_orders_success") {
    return "DoktorABC Pickup READY gespeichert";
  }

  if (payload.order_list_type === "excel_export" || payload.event === "doktorabc_eod_excel_export_success") {
    return "DoktorABC Excel exportiert";
  }

  return "DoktorABC EOD gespeichert";
}

function eodBotMessage(status: NotificationStatus, payload: UploadWebhookPayload) {
  if (status === "failure") {
    return payload.error ? String(payload.error) : "DoktorABC Orders Bot konnte nicht abgeschlossen werden.";
  }

  if (payload.order_list_type === "pickup_ready" || payload.event === "doktorabc_pickup_ready_orders_success") {
    return `${numberOrZero(payload.order_count)} Pickup READY Orders gespeichert.`;
  }

  if (payload.order_list_type === "excel_export" || payload.event === "doktorabc_eod_excel_export_success") {
    const filename = payload.download_filename || payload.filename || "Excel-Datei";
    if (payload.sent_to_n8n) {
      return `Excel exportiert und an n8n gesendet: ${filename}`;
    }

    return `Excel exportiert: ${filename}`;
  }

  return `${numberOrZero(payload.order_count)} EOD Orders gespeichert.`;
}

function numberOrZero(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}
