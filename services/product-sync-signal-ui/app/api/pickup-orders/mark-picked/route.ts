import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const SELF_PICKUP_ORDER_TYPE = "self pickup";
const MAX_ORDER_REFERENCES = 200;

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
  return (process.env.NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD || "").trim();
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
  return booleanValue(process.env.PICKUP_DONE_DRY_RUN ?? process.env.PICKUP_MARK_DRY_RUN, true);
}

function validateOperatorPassword(payload: Record<string, unknown>) {
  const expectedPassword = operatorPassword();
  const receivedPassword = String(payload.operator_password || payload.operatorPassword || "");

  if (!expectedPassword) {
    return "operator_password_not_configured";
  }

  if (receivedPassword !== expectedPassword) {
    return "operator_password_invalid";
  }

  return "";
}

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = supabaseSchema();

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function tableUrl() {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(supabaseTableName())}`;
}

function validateRequestPassword(request: Request) {
  const expectedPassword = operatorPassword();
  const receivedPassword = request.headers.get("x-operator-password") || "";

  if (!expectedPassword) {
    return "operator_password_not_configured";
  }

  if (receivedPassword !== expectedPassword) {
    return "operator_password_invalid";
  }

  return "";
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

async function fetchPendingPickupOrders() {
  const url = new URL(tableUrl());
  url.searchParams.set(
    "select",
    "id,order_reference,order_type,scraped_at,picked,patient_name,billing_date,products"
  );
  url.searchParams.set("order_type", `eq.${SELF_PICKUP_ORDER_TYPE}`);
  url.searchParams.set("scraped_at", "is.null");
  url.searchParams.set("or", "(picked.is.false,picked.is.null)");
  url.searchParams.set("order", "billing_date.asc.nullslast");

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase pending lookup failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as PendingPickupOrderRow[];
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
    return null;
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

function isSuccessfulBotResult(result: PickupDoneBotResult) {
  return ["clickable", "clicked", "clicked_still_visible"].includes(result.status || "");
}

export async function GET(request: Request) {
  const passwordError = validateRequestPassword(request);

  if (passwordError) {
    return NextResponse.json(
      { ok: false, error: passwordError },
      { status: passwordError.includes("not_configured") ? 500 : 401 }
    );
  }

  try {
    supabaseHeaders();
    tableUrl();
    const orders = await fetchPendingPickupOrders();

    return NextResponse.json({
      ok: true,
      count: orders.length,
      orders,
      table: supabaseTableName(),
      schema: supabaseSchema(),
    });
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
  const passwordError = validateOperatorPassword(payload);

  if (passwordError) {
    return NextResponse.json(
      { ok: false, error: passwordError },
      { status: passwordError.includes("not_configured") ? 500 : 401 }
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
  let botPayload: PickupDoneBotResponse | null = null;

  try {
    botPayload = await callPickupDoneBot(orderReferences, dryRun);
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "Pickup action bot failed.",
      },
      { status: 502 }
    );
  }

  const referencesToMark = botPayload
    ? (botPayload.results || [])
        .filter(isSuccessfulBotResult)
        .map((result) => String(result.order_reference || "").trim())
        .filter(Boolean)
    : orderReferences;
  const botFailures = botPayload
    ? (botPayload.results || [])
        .filter((result) => !isSuccessfulBotResult(result))
        .map(botResultToMarkResult)
    : [];

  const pickedAt = new Date().toISOString();
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

  return NextResponse.json({
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
  });
}
