export type NotificationSection = "upload" | "doktorabc_sync" | "check_bot" | "realtime_bot" | "abrechnung_verification";
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
  source?: string;
  upload_type?: string;
  sync_type?: string;
  order_type?: string;
  order_list_type?: string;
  order_count?: number;
  eod_order_count?: number;
  pickup_ready_order_count?: number;
  orders?: Array<Record<string, unknown>>;
  order_lists?: Record<string, Record<string, unknown>>;
  run_id?: string;
  sent_to_n8n?: boolean;
  n8n_status_code?: number;
  n8n_skipped_reason?: string;
  download_filename?: string;
  download_path?: string;
  download_size_bytes?: number;
  excel_row_count?: number | null;
  export_date?: string;
  filename?: string;
  bucket?: string;
  path?: string;
  size_bytes?: number;
  timestamp?: string;
  service?: string;
  error?: string;
  failed_step?: string;
  current_url?: string;
  screenshot_path?: string;
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
  scraped_at?: string;
  checked_orders?: number;
  db_rows_checked?: number;
  excel_rows_checked?: number;
  total_problems?: number;
  ordered_problem_sections?: Array<Record<string, unknown>>;
  payload?: Record<string, unknown>;
  summary?: {
    orders?: number;
    eod_orders?: number;
    pickup_ready_orders?: number;
    excel_rows?: number | null;
    excel_files?: number;
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
  payload = normalizeNestedNotificationPayload(payload);

  const status = normalizeStatus(payload.status, payload.event);
  const filename = String(payload.filename || "unknown-file");
  const uploadType = String(payload.upload_type || "upload");
  const event = String(payload.event || `upload_${status}`);
  const source = String(payload.service || payload.source || "n8n");

  if (isCheckBotNotification(payload)) {
    return {
      section: "check_bot",
      event: event.startsWith("check_") ? event : `check_${status}`,
      status,
      title: checkBotTitle(status),
      message: checkBotMessage(status, payload),
      filename: null,
      upload_type: "eod_reconciliation",
      bucket: null,
      path: null,
      size_bytes: null,
      error: payload.error ? String(payload.error) : null,
      source,
      payload: {
        ...payload,
        section: "check_bot",
        event: event.startsWith("check_") ? event : `check_${status}`,
        status,
      } as Record<string, unknown>,
      created_at: timestampOrNow(payload.timestamp || payload.finished_at),
    };
  }

  if (isEodBotNotification(payload)) {
    const isExcel = isEodExcelExport(payload);
    const section = isExcel ? "upload" : isRealtimeBotNotification(payload) ? "realtime_bot" : "doktorabc_sync";

    return {
      section,
      event,
      status,
      title: eodBotTitle(status, payload),
      message: eodBotMessage(status, payload),
      filename: payload.filename || payload.download_filename ? String(payload.filename || payload.download_filename) : null,
      upload_type: isExcel
        ? String(payload.upload_type || "doktorabc_eod_excel_export")
        : String(payload.order_list_type || "doktorabc_eod_bot"),
      bucket: null,
      path: payload.path || payload.download_path || payload.current_url ? String(payload.path || payload.download_path || payload.current_url) : null,
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

function normalizeStatus(status: string | undefined, event = ""): NotificationStatus {
  if (status === "triggered" || status === "success" || status === "failure") {
    return status;
  }

  const normalizedEvent = event.toLowerCase();

  if (normalizedEvent.includes("success")) {
    return "success";
  }

  if (normalizedEvent.includes("failure") || normalizedEvent.includes("failed")) {
    return "failure";
  }

  if (normalizedEvent.includes("triggered") || normalizedEvent.includes("started")) {
    return "triggered";
  }

  return "info";
}

function normalizeNestedNotificationPayload(payload: UploadWebhookPayload): UploadWebhookPayload {
  const nestedPayload = recordValue(payload.payload);

  if (!isCheckBotSection(payload.section)) {
    return payload;
  }

  if (!nestedPayload) {
    return {
      ...payload,
      section: "check_bot",
    };
  }

  const status = stringValue(payload.status) || stringValue(nestedPayload.status);
  const event = stringValue(payload.event) || stringValue(nestedPayload.event) || (status ? `check_${status}` : "check_result");

  return {
    ...(nestedPayload as UploadWebhookPayload),
    section: "check_bot",
    event,
    status,
    service: payload.service || stringValue(nestedPayload.service),
    source: payload.source || stringValue(nestedPayload.source),
    timestamp: payload.timestamp || stringValue(nestedPayload.timestamp),
  };
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
      return `Datenbankimport für ${filename} fehlgeschlagen${payload.error ? `: ${payload.error}` : ""}`;
    }

    return `Datenbankimport für ${filename}`;
  }

  if (isOrdersCsvInsert(payload)) {
    const table = payload.orders_table || "orders_csv";
    const rowsInserted = typeof payload.rows_inserted === "number" ? payload.rows_inserted : null;
    const rowsFound = typeof payload.rows_found === "number" ? payload.rows_found : null;

    if (status === "success") {
      const rowText = rowsInserted === null ? "Zeilen" : `${rowsInserted} Zeilen`;
      return `${rowText} in ${table} eingefügt: ${filename}`;
    }

    if (status === "failure") {
      const rowText = rowsFound === null ? "" : ` (${rowsFound} gelesene Zeilen)`;
      return `Datenbankimport für ${filename} fehlgeschlagen${rowText}${payload.error ? `: ${payload.error}` : ""}`;
    }

    return `Datenbankimport für ${filename}`;
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

  return `Upload-Meldung für ${filename}`;
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

function isCheckBotSection(section: string | undefined) {
  return section === "check_bot" || section === "bot_check";
}

function isCheckBotNotification(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  return (
    isCheckBotSection(payload.section) ||
    event === "check_success" ||
    event === "check_failure" ||
    Boolean(payload.ordered_problem_sections)
  );
}

function isProductSync(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  return (
    payload.sync_type !== "doktorabc_eod_bot" &&
    (payload.section === "doktorabc_sync" ||
      payload.sync_type === "doktorabc_products" ||
      event === "doktorabc_sync_success" ||
      event === "doktorabc_sync_failure")
  );
}

function isEodBotNotification(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  return (
    payload.section === "doktorabc_orders" ||
    payload.section === "realtime_bot" ||
    (payload.section === "doktorabc_sync" && payload.sync_type === "doktorabc_eod_bot") ||
    (payload.section === "upload" && payload.sync_type === "doktorabc_eod_bot") ||
    payload.sync_type === "doktorabc_eod_bot" ||
    payload.upload_type === "doktorabc_eod_excel_export" ||
    event === "doktorabc_eod_pickup_orders_success" ||
    event === "doktorabc_eod_orders_success" ||
    event === "doktorabc_eod_orders_failure" ||
    event === "doktorabc_pickup_ready_orders_success" ||
    event === "doktorabc_pickup_ready_orders_failure" ||
    event === "doktorabc_eod_excel_export_success" ||
    event === "doktorabc_eod_excel_export_failure"
  );
}

function isRealtimeBotNotification(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  const source = String(payload.service || payload.source || "").toLowerCase();

  return (
    payload.section === "realtime_bot" ||
    source.includes("pickup-ready") ||
    payload.order_list_type === "pickup_ready" ||
    event === "doktorabc_pickup_ready_orders_success" ||
    event === "doktorabc_pickup_ready_orders_failure"
  );
}

function isEodExcelExport(payload: UploadWebhookPayload) {
  const event = payload.event || "";
  return (
    payload.order_list_type === "excel_export" ||
    payload.upload_type === "doktorabc_eod_excel_export" ||
    event === "doktorabc_eod_excel_export_success" ||
    event === "doktorabc_eod_excel_export_failure"
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
      `${numberOrZero(summary.scraped)} Produkte geprüft`,
      `${numberOrZero(summary.inserted)} neu`,
      `${numberOrZero(summary.updated)} geändert`,
      `${numberOrZero(summary.unchanged)} unverändert`,
    ].join(", ");
  }

  if (status === "failure") {
    return payload.error ? String(payload.error) : "Synchronisierung konnte nicht abgeschlossen werden.";
  }

  return "DoktorABC Sync-Meldung";
}

function checkBotTitle(status: NotificationStatus) {
  if (status === "success") {
    return "Bot Check erfolgreich";
  }

  if (status === "failure") {
    return "Bot Check fehlgeschlagen";
  }

  return "Bot Check";
}

function checkBotMessage(status: NotificationStatus, payload: UploadWebhookPayload) {
  const checkedOrders = numberOrZero(payload.checked_orders);

  if (status === "success") {
    return `${checkedOrders} Orders geprüft - keine Abweichungen gefunden.`;
  }

  if (status === "failure") {
    return `${numberOrZero(payload.total_problems)} Probleme bei ${checkedOrders} geprüften Orders gefunden.`;
  }

  return "Bot Check Ergebnis empfangen.";
}

function eodBotTitle(status: NotificationStatus, payload: UploadWebhookPayload) {
  if (status === "failure") {
    if (isEodExcelExport(payload)) {
      return "DoktorABC Excel Export fehlgeschlagen";
    }

    if (payload.order_list_type === "pickup_ready" || payload.event === "doktorabc_pickup_ready_orders_failure") {
      return "DoktorABC Self Pickup fehlgeschlagen";
    }

    if (payload.order_list_type === "eod" || payload.event === "doktorabc_eod_orders_failure") {
      return "DoktorABC EOD fehlgeschlagen";
    }

    return "DoktorABC EOD/Self Pickup fehlgeschlagen";
  }

  if (payload.order_list_type === "pickup_ready" || payload.event === "doktorabc_pickup_ready_orders_success") {
    return "DoktorABC Pickup READY gespeichert";
  }

  if (isEodExcelExport(payload)) {
    return "DoktorABC Excel Export erfolgreich";
  }

  if (payload.order_list_type === "eod_and_pickup" || payload.event === "doktorabc_eod_pickup_orders_success") {
    return "DoktorABC EOD und Self Pickup gespeichert";
  }

  return "DoktorABC EOD gespeichert";
}

function eodBotMessage(status: NotificationStatus, payload: UploadWebhookPayload) {
  if (status === "failure") {
    const label = eodFailureLabel(payload);
    const step = payload.failed_step ? ` (${payload.failed_step})` : "";
    return payload.error
      ? `${label}${step}: ${payload.error}`
      : `${label}${step}: konnte nicht abgeschlossen werden.`;
  }

  if (payload.order_list_type === "pickup_ready" || payload.event === "doktorabc_pickup_ready_orders_success") {
    return `${numberOrZero(payload.order_count)} Pickup READY Orders gespeichert.`;
  }

  if (isEodExcelExport(payload)) {
    const rows = excelDataRowCount(payload.excel_row_count ?? payload.summary?.excel_rows);
    const exportDate = payload.export_date || formatDateOnly(payload.timestamp) || "heute";
    if (payload.sent_to_n8n) {
      return `${rows} Excel-Zeilen am ${exportDate} exportiert und an n8n gesendet.`;
    }

    return `${rows} Excel-Zeilen am ${exportDate} exportiert.`;
  }

  if (payload.order_list_type === "eod_and_pickup" || payload.event === "doktorabc_eod_pickup_orders_success") {
    const eodCount = numberOrZero(payload.eod_order_count ?? payload.summary?.eod_orders);
    const pickupCount = numberOrZero(payload.pickup_ready_order_count ?? payload.summary?.pickup_ready_orders);
    return `EOD: ${eodCount} Orders, Self Pickup: ${pickupCount} Orders gespeichert.`;
  }

  return `${numberOrZero(payload.order_count)} EOD Orders gespeichert.`;
}

function eodFailureLabel(payload: UploadWebhookPayload) {
  if (isEodExcelExport(payload)) {
    return "Excel Export";
  }

  if (payload.order_list_type === "pickup_ready" || payload.event === "doktorabc_pickup_ready_orders_failure") {
    return "Self Pickup";
  }

  if (payload.order_list_type === "eod" || payload.event === "doktorabc_eod_orders_failure") {
    return "EOD";
  }

  return "EOD/Self Pickup";
}

function formatDateOnly(value: string | undefined) {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toISOString().slice(0, 10);
}

function numberOrZero(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function excelDataRowCount(value: unknown) {
  return Math.max(numberOrZero(value) - 1, 0);
}

function recordValue(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim()) {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return "";
}
