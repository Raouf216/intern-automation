export type NotificationSection = "upload";
export type NotificationStatus = "triggered" | "success" | "failure" | "info" | "warning";

export type StoredNotification = {
  id: string;
  section: NotificationSection;
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
  upload_type?: string;
  filename?: string;
  bucket?: string;
  path?: string;
  size_bytes?: number;
  timestamp?: string;
  service?: string;
  error?: string;
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

  return {
    section: "upload",
    event,
    status,
    title: uploadTitle(status, uploadType),
    message: uploadMessage(status, filename, payload.error),
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

function uploadTitle(status: NotificationStatus, uploadType: string) {
  const label = uploadType === "doktorabc_abrechnung" ? "DoktorABC Abrechnung" : "OED Upload";

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

function uploadMessage(status: NotificationStatus, filename: string, error?: string) {
  if (status === "triggered") {
    return `Upload wurde gestartet: ${filename}`;
  }

  if (status === "success") {
    return `Upload wurde erfolgreich gespeichert: ${filename}`;
  }

  if (status === "failure") {
    return `Upload konnte nicht gespeichert werden: ${filename}${error ? ` (${error})` : ""}`;
  }

  return `Upload-Meldung fuer ${filename}`;
}
