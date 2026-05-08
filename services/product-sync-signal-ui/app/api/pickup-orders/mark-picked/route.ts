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

type MarkPickedResult = {
  order_reference: string;
  status: "picked" | "already_picked" | "not_found" | "wrong_order_type" | "error";
  message: string;
  order_type?: string | null;
  scraped_at?: string | null;
  picked?: boolean | null;
};

function requiredEnv(name: string) {
  const value = process.env[name];

  if (!value?.trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value.trim();
}

function supabaseUrl() {
  return requiredEnv("SUPABASE_URL").replace(/\/$/, "");
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

    const selfPickupRow = rows.find((row) => row.order_type === SELF_PICKUP_ORDER_TYPE);

    if (!selfPickupRow) {
      return {
        order_reference: orderReference,
        status: "wrong_order_type",
        order_type: rows.map((row) => row.order_type || "leer").join(", "),
        message: "Order existiert, ist aber kein Self-Pickup Auftrag.",
      };
    }

    if (selfPickupRow.scraped_at) {
      return {
        order_reference: orderReference,
        status: "already_picked",
        order_type: selfPickupRow.order_type,
        scraped_at: selfPickupRow.scraped_at,
        picked: selfPickupRow.picked,
        message: "Bereits abgeholt. Es wurde nichts geaendert.",
      };
    }

    const updatedRow = await markRowPicked(selfPickupRow, pickedAt);

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

  const pickedAt = new Date().toISOString();
  const results = await Promise.all(
    orderReferences.map((orderReference) => processOrderReference(orderReference, pickedAt))
  );
  const picked = results.filter((result) => result.status === "picked").length;
  const alreadyPicked = results.filter((result) => result.status === "already_picked").length;
  const errors = results.filter((result) => ["not_found", "wrong_order_type", "error"].includes(result.status)).length;

  return NextResponse.json({
    ok: true,
    checked: results.length,
    picked,
    already_picked: alreadyPicked,
    errors,
    picked_at: pickedAt,
    table: supabaseTableName(),
    schema: supabaseSchema(),
    results,
  });
}
