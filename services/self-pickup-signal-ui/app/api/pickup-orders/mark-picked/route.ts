import { NextResponse } from "next/server";
import { createHmac, timingSafeEqual } from "crypto";

export const dynamic = "force-dynamic";

const SELF_PICKUP_ORDER_TYPE = "self pickup";
const MAX_ORDER_REFERENCES = 200;
const SESSION_COOKIE_NAME = "self_pickup_operator_session";
const SESSION_MAX_AGE_MS = 12 * 60 * 60 * 1000;

type SupabaseOrderRow = {
  id: string;
  order_reference: string;
  order_type: string | null;
  scraped_at: string | null;
  picked: boolean | null;
};

type PendingPickupOrderRow = SupabaseOrderRow & {
  patient_name: string | null;
  billing_date: string | null;
  products: string | null;
};

type PickupAttemptOrderSnapshot = {
  order_reference: string;
  patient_name: string | null;
  billing_date: string | null;
};

type NotificationRow = {
  created_at?: string;
  event?: string | null;
  section?: string | null;
  payload?: Record<string, unknown> | null;
};

type MarkPickedResult = {
  order_reference: string;
  status: "picked" | "already_picked" | "clickable" | "not_found" | "wrong_order_type" | "error";
  message: string;
  order_type?: string | null;
  scraped_at?: string | null;
  picked?: boolean | null;
  dry_run?: boolean;
  would_click?: boolean;
  bot_status?: string;
};

type PickupDoneBotResult = {
  order_reference?: string;
  status?: string;
  message?: string;
  dry_run?: boolean;
  would_click?: boolean;
  clicked?: boolean;
  button_visible?: boolean;
  button_enabled?: boolean;
};

type PickupDoneBotResponse = {
  ok?: boolean;
  error?: string;
  dry_run?: boolean;
  checked?: number;
  clickable?: number;
  clicked?: number;
  not_found?: number;
  errors?: number;
  current_url?: string;
  screenshot_path?: string | null;
  results?: PickupDoneBotResult[];
};

type PickupAttemptNotificationInput = {
  orderReferences: string[];
  attemptedOrders: PickupAttemptOrderSnapshot[];
  dryRun: boolean;
  pickedAt: string;
  botPayload: PickupDoneBotResponse | null;
  combinedResults: MarkPickedResult[];
  clickable: number;
  picked: number;
  alreadyPicked: number;
  errors: number;
  errorMessage?: string;
};

function requiredEnv(name: string) {
  const value = process.env[name];

  if (!value?.trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value.trim().replace(/^["']|["']$/g, "");
}

function supabaseUrl() {
  const value = requiredEnv("SUPABASE_URL").replace(/\/+$/, "");

  try {
    return new URL(value).href.replace(/\/+$/, "");
  } catch {
    throw new Error("Invalid SUPABASE_URL. Use a full URL like http://supabase-kong:8000 or https://supabase.intern-automation.de");
  }
}

function supabaseSchema() {
  return (process.env.SUPABASE_EOD_ORDERS_SCHEMA || process.env.SUPABASE_SCHEMA || "private").trim();
}

function supabaseTableName() {
  return (process.env.SUPABASE_EOD_ORDERS_TABLE || "doktorabc_eod_bot_orders").trim();
}

function operatorPassword() {
  return (process.env.NEXT_PUBLIC_SELF_PICKUP_PASSWORD || "").trim();
}

function sessionSignature(issuedAt: string) {
  return createHmac("sha256", operatorPassword())
    .update(`self-pickup-operator:${issuedAt}`)
    .digest("base64url");
}

function createSessionToken() {
  const issuedAt = String(Date.now());
  return `${issuedAt}.${sessionSignature(issuedAt)}`;
}

function cookieValue(request: Request, name: string) {
  const cookies = request.headers.get("cookie") || "";

  return cookies
    .split(";")
    .map((cookie) => cookie.trim())
    .map((cookie) => {
      const separatorIndex = cookie.indexOf("=");
      return separatorIndex === -1
        ? [cookie, ""]
        : [cookie.slice(0, separatorIndex), decodeURIComponent(cookie.slice(separatorIndex + 1))];
    })
    .find(([cookieName]) => cookieName === name)?.[1] || "";
}

function isValidSessionToken(value: string) {
  if (!value || !operatorPassword()) return false;

  const [issuedAt, receivedSignature] = value.split(".");
  const issuedAtMs = Number(issuedAt);

  if (!issuedAt || !receivedSignature || !Number.isFinite(issuedAtMs)) return false;
  if (Date.now() - issuedAtMs > SESSION_MAX_AGE_MS) return false;

  const expectedSignature = sessionSignature(issuedAt);
  const received = Buffer.from(receivedSignature);
  const expected = Buffer.from(expectedSignature);

  return received.length === expected.length && timingSafeEqual(received, expected);
}

function setSessionCookie(response: NextResponse, request: Request) {
  response.cookies.set(SESSION_COOKIE_NAME, createSessionToken(), {
    httpOnly: true,
    maxAge: Math.floor(SESSION_MAX_AGE_MS / 1000),
    path: "/",
    sameSite: "lax",
    secure: new URL(request.url).protocol === "https:",
  });
}

function pickupDoneBotEndpoint() {
  return (process.env.PICKUP_DONE_BOT_ENDPOINT || process.env.PICKUP_MARK_BOT_ENDPOINT || "").trim();
}

function booleanValue(value: unknown, fallback: boolean) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;

  return ["1", "true", "yes", "on"].includes(String(value).trim().toLowerCase());
}

function pickupDoneDryRunDefault() {
  return booleanValue(process.env.PICKUP_DONE_DRY_RUN ?? process.env.PICKUP_MARK_DRY_RUN, false);
}

function validateRequestAuth(request: Request, payload?: Record<string, unknown>) {
  const expectedPassword = operatorPassword();
  const receivedPassword = String(
    payload?.operator_password ||
      payload?.operatorPassword ||
      request.headers.get("x-operator-password") ||
      ""
  );

  if (!expectedPassword) {
    return { ok: false as const, error: "operator_password_not_configured" };
  }

  if (receivedPassword) {
    return receivedPassword === expectedPassword
      ? { ok: true as const, issueSession: true }
      : { ok: false as const, error: "operator_password_invalid" };
  }

  return isValidSessionToken(cookieValue(request, SESSION_COOKIE_NAME))
    ? { ok: true as const, issueSession: false }
    : { ok: false as const, error: "operator_session_invalid" };
}

function supabaseHeadersForSchema(schema: string) {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function supabaseHeaders() {
  return supabaseHeadersForSchema(supabaseSchema());
}

function tableUrl() {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(supabaseTableName())}`;
}

function notificationsSchema() {
  return (process.env.SUPABASE_NOTIFICATIONS_SCHEMA || "public").trim();
}

function notificationsTableName() {
  return (process.env.SUPABASE_NOTIFICATIONS_TABLE || "notifications").trim();
}

function notificationsTableUrl() {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(notificationsTableName())}`;
}

function notificationHeaders() {
  return supabaseHeadersForSchema(notificationsSchema());
}

function postgrestInValues(values: string[]) {
  return `in.(${values.map((value) => `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`).join(",")})`;
}

function uniqueOrderReferences(values: unknown[]) {
  const seen = new Set<string>();
  const references: string[] = [];

  values.forEach((value) => {
    const reference = String(value || "").trim();
    const key = reference.toUpperCase();
    if (!reference || seen.has(key)) return;

    seen.add(key);
    references.push(reference);
  });

  return references;
}

function recordValue(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function arrayRecordValue(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.map(recordValue).filter(Boolean) as Record<string, unknown>[] : [];
}

function normalizeOrderReferences(payload: Record<string, unknown>) {
  const raw =
    payload.order_references ??
    payload.orderReferences ??
    payload.order_ids ??
    payload.orderIds ??
    payload.orders ??
    payload.text;

  const values = Array.isArray(raw) ? raw : typeof raw === "string" ? raw.split(/[\s,;]+/) : [];
  const seen = new Set<string>();

  return values
    .map((value) => String(value).trim())
    .filter(Boolean)
    .filter((value) => {
      const key = value.toUpperCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

async function fetchOrderRows(orderReference: string) {
  const url = new URL(tableUrl());
  url.searchParams.set("select", "id,order_reference,order_type,scraped_at,picked");
  url.searchParams.set("order_reference", `eq.${orderReference}`);

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase lookup failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as SupabaseOrderRow[];
}

function orderReferencesFromNotificationPayload(payload: Record<string, unknown>) {
  const pickupReadyList = pickupReadyListFromNotificationPayload(payload);
  const directOrders = arrayRecordValue(payload.orders);
  const listOrders = arrayRecordValue(pickupReadyList?.orders);
  const orders = listOrders.length ? listOrders : directOrders;

  return uniqueOrderReferences(
    orders.map((order) => order.order_reference || order.order_id || order.orderReference || order.id)
  );
}

function pickupReadyListFromNotificationPayload(payload: Record<string, unknown>) {
  const orderLists = recordValue(payload.order_lists);

  return (
    recordValue(orderLists?.pickup_ready) ||
    recordValue(orderLists?.self_pickup) ||
    recordValue(orderLists?.["self pickup"])
  );
}

function hasPickupReadySnapshot(payload: Record<string, unknown>) {
  const event = String(payload.event || "");
  const source = String(payload.service || payload.source || "").toLowerCase();
  const orderListType = String(payload.order_list_type || "").toLowerCase();

  return Boolean(
    pickupReadyListFromNotificationPayload(payload) ||
      source.includes("pickup-ready") ||
      orderListType === "pickup_ready" ||
      event === "doktorabc_pickup_ready_orders_success"
  );
}

async function fetchPickupReadyNotificationRows(section?: string) {
  const url = new URL(notificationsTableUrl());
  url.searchParams.set("select", "created_at,event,section,payload");
  url.searchParams.set("status", "eq.success");
  url.searchParams.set("order", "created_at.desc");
  url.searchParams.set("limit", "50");

  if (section) {
    url.searchParams.set("section", `eq.${section}`);
  }

  const response = await fetch(url, {
    headers: supabaseHeadersForSchema(notificationsSchema()),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase notification lookup failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as NotificationRow[];
}

async function fetchLatestPickupReadyOrderReferences() {
  const rowGroups = [
    await fetchPickupReadyNotificationRows("realtime_bot"),
    await fetchPickupReadyNotificationRows(),
  ];

  for (const rows of rowGroups) {
    for (const row of rows) {
      const payload = recordValue(row.payload);
      if (!payload || !hasPickupReadySnapshot(payload)) continue;

      return {
        references: orderReferencesFromNotificationPayload(payload),
        notification_created_at: row.created_at || null,
      };
    }
  }

  return null;
}

function dedupeAndSortPendingOrders(rows: PendingPickupOrderRow[], orderReferences?: string[] | null) {
  const byReference = new Map<string, PendingPickupOrderRow>();

  rows.forEach((row) => {
    const key = row.order_reference.toUpperCase();
    if (!byReference.has(key)) {
      byReference.set(key, row);
    }
  });

  if (!orderReferences) {
    return [...byReference.values()];
  }

  return orderReferences
    .map((reference) => byReference.get(reference.toUpperCase()))
    .filter(Boolean) as PendingPickupOrderRow[];
}

async function fetchPendingPickupOrders(orderReferences?: string[] | null) {
  if (orderReferences && orderReferences.length === 0) {
    return [];
  }

  const url = new URL(tableUrl());
  url.searchParams.set(
    "select",
    "id,order_reference,order_type,scraped_at,picked,patient_name,billing_date,products"
  );
  url.searchParams.set("order_type", `eq.${SELF_PICKUP_ORDER_TYPE}`);
  url.searchParams.set("scraped_at", "is.null");
  url.searchParams.set("or", "(picked.is.false,picked.is.null)");
  url.searchParams.set("order", "billing_date.asc.nullslast");

  if (orderReferences) {
    url.searchParams.set("order_reference", postgrestInValues(orderReferences));
  }

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase pending lookup failed (${response.status}): ${await response.text()}`);
  }

  return dedupeAndSortPendingOrders((await response.json()) as PendingPickupOrderRow[], orderReferences);
}

async function fetchPickupAttemptOrderSnapshots(orderReferences: string[]): Promise<PickupAttemptOrderSnapshot[]> {
  if (!orderReferences.length) return [];

  const url = new URL(tableUrl());
  url.searchParams.set("select", "order_reference,patient_name,billing_date");
  url.searchParams.set("order_type", `eq.${SELF_PICKUP_ORDER_TYPE}`);
  url.searchParams.set("order_reference", postgrestInValues(orderReferences));

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase attempt lookup failed (${response.status}): ${await response.text()}`);
  }

  const rows = (await response.json()) as PickupAttemptOrderSnapshot[];
  const byReference = new Map(rows.map((row) => [row.order_reference.toUpperCase(), row]));

  return orderReferences.map((orderReference) => {
    const row = byReference.get(orderReference.toUpperCase());

    return {
      order_reference: orderReference,
      patient_name: row?.patient_name || null,
      billing_date: row?.billing_date || null,
    };
  });
}

function fallbackPickupAttemptOrderSnapshots(orderReferences: string[]): PickupAttemptOrderSnapshot[] {
  return orderReferences.map((orderReference) => ({
    order_reference: orderReference,
    patient_name: null,
    billing_date: null,
  }));
}

async function markRowPicked(row: SupabaseOrderRow, pickedAt: string) {
  const url = new URL(tableUrl());
  url.searchParams.set("id", `eq.${row.id}`);

  const response = await fetch(url, {
    method: "PATCH",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify({
      scraped_at: pickedAt,
      picked: true,
    }),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase update failed (${response.status}): ${await response.text()}`);
  }

  const updatedRows = (await response.json()) as SupabaseOrderRow[];
  return updatedRows[0] || { ...row, scraped_at: pickedAt, picked: true };
}

async function processOrderReference(orderReference: string, pickedAt: string): Promise<MarkPickedResult> {
  try {
    const rows = await fetchOrderRows(orderReference);

    if (!rows.length) {
      return {
        order_reference: orderReference,
        status: "not_found",
        message: "Order-ID wurde nicht in Supabase gefunden.",
      };
    }

    const selfPickupRows = rows.filter((row) => row.order_type === SELF_PICKUP_ORDER_TYPE);

    if (!selfPickupRows.length) {
      return {
        order_reference: orderReference,
        status: "wrong_order_type",
        order_type: rows.map((row) => row.order_type || "leer").join(", "),
        message: "Order existiert, ist aber kein Self-Pickup Auftrag.",
      };
    }

    const pendingSelfPickupRow = selfPickupRows.find((row) => !row.scraped_at && row.picked !== true);

    if (!pendingSelfPickupRow) {
      const pickedRow = selfPickupRows.find((row) => row.scraped_at) || selfPickupRows[0];

      return {
        order_reference: orderReference,
        status: "already_picked",
        order_type: pickedRow.order_type,
        scraped_at: pickedRow.scraped_at,
        picked: pickedRow.picked,
        message: "Bereits abgeholt. Es wurde nichts geaendert.",
      };
    }

    const updatedRow = await markRowPicked(pendingSelfPickupRow, pickedAt);

    return {
      order_reference: orderReference,
      status: "picked",
      order_type: updatedRow.order_type,
      scraped_at: updatedRow.scraped_at,
      picked: updatedRow.picked,
      message: "Als abgeholt markiert.",
    };
  } catch (error) {
    return {
      order_reference: orderReference,
      status: "error",
      message: error instanceof Error ? error.message : "Unbekannter Fehler.",
    };
  }
}

async function callPickupDoneBot(orderReferences: string[], dryRun: boolean) {
  const endpoint = pickupDoneBotEndpoint();

  if (!endpoint) {
    throw new Error("PICKUP_DONE_BOT_ENDPOINT is required for the DoktorABC abgeholt click.");
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      order_references: orderReferences,
      dry_run: dryRun,
    }),
    cache: "no-store",
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? ((await response.json()) as PickupDoneBotResponse)
    : ({ ok: false, error: await response.text() } satisfies PickupDoneBotResponse);

  if (!response.ok || !payload.ok) {
    throw new Error(payload.error || `Pickup action bot failed (${response.status}): ${response.statusText || "no details"}`);
  }

  return payload;
}

function botResultToMarkResult(result: PickupDoneBotResult): MarkPickedResult {
  const status = result.status || "error";
  const orderReference = String(result.order_reference || "");

  if (status === "clickable") {
    return {
      order_reference: orderReference,
      status: "clickable",
      dry_run: true,
      would_click: result.would_click,
      bot_status: status,
      message: result.message || "Dry run passed. The DoktorABC button is clickable.",
    };
  }

  if (status === "not_found") {
    return {
      order_reference: orderReference,
      status: "not_found",
      dry_run: result.dry_run,
      would_click: result.would_click,
      bot_status: status,
      message: result.message || "Order was not found in DoktorABC.",
    };
  }

  return {
    order_reference: orderReference,
    status: "error",
    dry_run: result.dry_run,
    would_click: result.would_click,
    bot_status: status,
    message: result.message || `Pickup action bot returned status: ${status}`,
  };
}

function isClickedBotResult(result: PickupDoneBotResult) {
  return ["clicked", "clicked_still_visible"].includes(result.status || "");
}

function pickupAttemptModeLabel(dryRun: boolean) {
  return dryRun ? "Trockenlauf" : "Echter Klick";
}

function pickupAttemptOrderLabel(order: PickupAttemptOrderSnapshot) {
  return [order.order_reference, order.patient_name].filter(Boolean).join(" - ");
}

function pickupAttemptMessage(input: PickupAttemptNotificationInput) {
  const mode = pickupAttemptModeLabel(input.dryRun);
  const countText = `${input.orderReferences.length} versucht`;
  const resultText = input.dryRun
    ? `${input.clickable} klickbar, nichts markiert`
    : `${input.picked} markiert, ${input.alreadyPicked} bereits abgeholt`;
  const errorText = input.errors ? `${input.errors} Fehler` : "0 Fehler";
  const previewOrders = input.attemptedOrders.slice(0, 6).map(pickupAttemptOrderLabel).filter(Boolean);
  const moreText = input.attemptedOrders.length > previewOrders.length
    ? `; +${input.attemptedOrders.length - previewOrders.length} weitere`
    : "";
  const orderText = previewOrders.length ? ` IDs: ${previewOrders.join("; ")}${moreText}` : "";

  return `${mode}: ${countText}, ${resultText}, ${errorText}.${orderText}`;
}

function pickupAttemptOrdersPayload(input: PickupAttemptNotificationInput) {
  const resultByReference = new Map(
    input.combinedResults.map((result) => [result.order_reference.toUpperCase(), result])
  );

  return input.attemptedOrders.map((order) => {
    const result = resultByReference.get(order.order_reference.toUpperCase());

    return {
      order_reference: order.order_reference,
      patient_name: order.patient_name,
      billing_date: order.billing_date,
      status: result?.status || (input.errorMessage ? "error" : "unknown"),
      bot_status: result?.bot_status || null,
      message: result?.message || input.errorMessage || "",
    };
  });
}

async function insertPickupAttemptNotification(input: PickupAttemptNotificationInput) {
  const status = input.errorMessage || input.errors > 0 ? "failure" : "success";
  const event = `doktorabc_pickup_done_attempt_${status}`;
  const payload = {
    action: "pickup_done_attempt",
    section: "realtime_bot",
    event,
    status,
    source: "self-pickup-signal-ui",
    mode: input.dryRun ? "dry_run" : "real_click",
    dry_run: input.dryRun,
    attempted: input.orderReferences.length,
    checked: input.combinedResults.length || input.orderReferences.length,
    clickable: input.clickable,
    picked: input.picked,
    already_picked: input.alreadyPicked,
    errors: input.errors,
    picked_at: input.pickedAt,
    orders: pickupAttemptOrdersPayload(input),
    results: input.combinedResults,
    bot: input.botPayload,
    error: input.errorMessage || null,
  };

  const notification = {
    section: "realtime_bot",
    event,
    status,
    title: input.dryRun ? "Self Pickup Trockenlauf" : "Self Pickup Klickversuch",
    message: pickupAttemptMessage(input),
    filename: null,
    upload_type: "doktorabc_pickup_done_attempt",
    bucket: null,
    path: null,
    size_bytes: null,
    error: input.errorMessage || null,
    source: "self-pickup-signal-ui",
    payload,
    created_at: input.pickedAt,
  };

  const response = await fetch(notificationsTableUrl(), {
    method: "POST",
    headers: {
      ...notificationHeaders(),
      Prefer: "return=minimal",
    },
    body: JSON.stringify(notification),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase notification insert failed (${response.status}): ${await response.text()}`);
  }

  return { sent_to_notification_app: true, notification_status_code: response.status };
}

async function sendPickupAttemptNotification(input: PickupAttemptNotificationInput) {
  try {
    return await insertPickupAttemptNotification(input);
  } catch (error) {
    return {
      sent_to_notification_app: false,
      notification_error: error instanceof Error ? error.message : "Unknown notification error",
    };
  }
}

export async function GET(request: Request) {
  const auth = validateRequestAuth(request);

  if (!auth.ok) {
    return NextResponse.json(
      { ok: false, error: auth.error },
      { status: auth.error.includes("not_configured") ? 500 : 401 }
    );
  }

  try {
    supabaseHeaders();
    tableUrl();
    const latestReadySnapshot = await fetchLatestPickupReadyOrderReferences();
    const orders = latestReadySnapshot
      ? await fetchPendingPickupOrders(latestReadySnapshot.references)
      : [];

    const response = NextResponse.json({
      ok: true,
      count: orders.length,
      orders,
      source: latestReadySnapshot ? "latest_pickup_ready_run" : "no_pickup_ready_snapshot",
      latest_ready_notification_at: latestReadySnapshot?.notification_created_at || null,
      table: supabaseTableName(),
      schema: supabaseSchema(),
    });

    if (auth.issueSession) {
      setSessionCookie(response, request);
    }

    return response;
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "Supabase is not configured.",
      },
      { status: 500 }
    );
  }
}

export async function POST(request: Request) {
  let payload: Record<string, unknown>;

  try {
    payload = (await request.json()) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  const orderReferences = normalizeOrderReferences(payload);
  const auth = validateRequestAuth(request, payload);

  if (!auth.ok) {
    return NextResponse.json(
      { ok: false, error: auth.error },
      { status: auth.error.includes("not_configured") ? 500 : 401 }
    );
  }

  if (!orderReferences.length) {
    return NextResponse.json({ ok: false, error: "no_order_references" }, { status: 400 });
  }

  if (orderReferences.length > MAX_ORDER_REFERENCES) {
    return NextResponse.json(
      {
        ok: false,
        error: `too_many_order_references_max_${MAX_ORDER_REFERENCES}`,
      },
      { status: 400 }
    );
  }

  try {
    supabaseHeaders();
    tableUrl();
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "Supabase is not configured.",
      },
      { status: 500 }
    );
  }

  const dryRun = booleanValue(payload.dry_run ?? payload.dryRun, pickupDoneDryRunDefault());
  const pickedAt = new Date().toISOString();
  const attemptedOrders = await fetchPickupAttemptOrderSnapshots(orderReferences).catch(() =>
    fallbackPickupAttemptOrderSnapshots(orderReferences)
  );
  let botPayload: PickupDoneBotResponse | null = null;

  try {
    botPayload = await callPickupDoneBot(orderReferences, dryRun);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : "Pickup action bot failed.";
    const failedResults = orderReferences.map((orderReference) => ({
      order_reference: orderReference,
      status: "error" as const,
      dry_run: dryRun,
      message: errorMessage,
    }));
    const notificationResult = await sendPickupAttemptNotification({
      orderReferences,
      attemptedOrders,
      dryRun,
      pickedAt,
      botPayload,
      combinedResults: failedResults,
      clickable: 0,
      picked: 0,
      alreadyPicked: 0,
      errors: orderReferences.length,
      errorMessage,
    });

    return NextResponse.json(
      {
        ok: false,
        error: errorMessage,
        notification: notificationResult,
      },
      { status: 502 }
    );
  }

  const botResults = botPayload?.results || [];
  const referencesToMark = botPayload
    ? botPayload.dry_run
      ? []
      : botResults
          .filter(isClickedBotResult)
          .map((result) => String(result.order_reference || "").trim())
          .filter(Boolean)
    : orderReferences;
  const botFailures = botPayload
    ? botPayload.dry_run
      ? botResults.map(botResultToMarkResult)
      : botResults.filter((result) => !isClickedBotResult(result)).map(botResultToMarkResult)
    : [];

  const results = await Promise.all(
    referencesToMark.map((orderReference) => processOrderReference(orderReference, pickedAt))
  );
  const combinedResults = [...results, ...botFailures];
  const clickable = botPayload
    ? (botPayload.results || []).filter((result) => result.status === "clickable").length
    : 0;
  const picked = combinedResults.filter((result) => result.status === "picked").length;
  const alreadyPicked = combinedResults.filter((result) => result.status === "already_picked").length;
  const errors = combinedResults.filter((result) => ["not_found", "wrong_order_type", "error"].includes(result.status)).length;
  const notificationResult = await sendPickupAttemptNotification({
    orderReferences,
    attemptedOrders,
    dryRun: Boolean(botPayload?.dry_run),
    pickedAt,
    botPayload,
    combinedResults,
    clickable,
    picked,
    alreadyPicked,
    errors,
  });

  const response = NextResponse.json({
    ok: true,
    dry_run: Boolean(botPayload?.dry_run),
    checked: combinedResults.length,
    clickable,
    picked,
    already_picked: alreadyPicked,
    errors,
    picked_at: pickedAt,
    table: supabaseTableName(),
    schema: supabaseSchema(),
    bot: botPayload,
    results: combinedResults,
    notification: notificationResult,
  });

  if (auth.issueSession) {
    setSessionCookie(response, request);
  }

  return response;
}
