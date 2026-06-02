import { NextResponse } from "next/server";
import { createHmac, timingSafeEqual } from "crypto";

export const dynamic = "force-dynamic";

const SELF_PICKUP_ORDER_TYPE = "self pickup";
const MAX_REPAIR_ORDER_REFERENCES = 20;
const SESSION_COOKIE_NAME = "self_pickup_operator_session";
const SESSION_MAX_AGE_MS = 12 * 60 * 60 * 1000;

type SupabaseOrderRow = {
  id: string;
  order_reference: string;
  order_type: string | null;
  scraped_at: string | null;
  picked: boolean | null;
  patient_name: string | null;
  billing_date: string | null;
  products: string | null;
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

function operatorPassword() {
  return (process.env.NEXT_PUBLIC_SELF_PICKUP_PASSWORD || "").trim();
}

function sessionSignature(issuedAt: string) {
  return createHmac("sha256", operatorPassword())
    .update(`self-pickup-operator:${issuedAt}`)
    .digest("base64url");
}

function cookieValue(request: Request, name: string) {
  const cookies = request.headers.get("cookie") || "";

  return (
    cookies
      .split(";")
      .map((cookie) => cookie.trim())
      .map((cookie) => {
        const separatorIndex = cookie.indexOf("=");

        return separatorIndex === -1
          ? [cookie, ""]
          : [cookie.slice(0, separatorIndex), decodeURIComponent(cookie.slice(separatorIndex + 1))];
      })
      .find(([cookieName]) => cookieName === name)?.[1] || ""
  );
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

function validateRequestAuth(request: Request) {
  if (!operatorPassword()) {
    return { ok: false as const, error: "operator_password_not_configured" };
  }

  return isValidSessionToken(cookieValue(request, SESSION_COOKIE_NAME))
    ? { ok: true as const }
    : { ok: false as const, error: "operator_session_invalid" };
}

function postgrestInValues(values: string[]) {
  return `in.(${values.map((value) => `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`).join(",")})`;
}

function uniqueTokens(values: unknown[]) {
  const seen = new Set<string>();
  const tokens: string[] = [];

  values.forEach((value) => {
    const token = String(value || "").trim();
    const key = token.toUpperCase();
    if (!token || seen.has(key)) return;

    seen.add(key);
    tokens.push(token);
  });

  return tokens;
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

  return uniqueTokens(values);
}

function uuidLike(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

async function fetchRowsByColumn(column: "id" | "order_reference", values: string[]) {
  if (!values.length) return [];

  const url = new URL(tableUrl());
  url.searchParams.set("select", "id,order_reference,order_type,scraped_at,picked,patient_name,billing_date,products");
  url.searchParams.set("order_type", `eq.${SELF_PICKUP_ORDER_TYPE}`);
  url.searchParams.set(column, postgrestInValues(values));

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase lookup failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as SupabaseOrderRow[];
}

async function fetchRepairRows(tokens: string[]) {
  const rowIds = tokens.filter(uuidLike);
  const orderReferences = tokens.filter((token) => !uuidLike(token));
  const rows = [
    ...(await fetchRowsByColumn("order_reference", orderReferences)),
    ...(await fetchRowsByColumn("id", rowIds)),
  ];
  const byId = new Map<string, SupabaseOrderRow>();

  rows.forEach((row) => byId.set(row.id, row));

  return [...byId.values()];
}

function isPending(row: SupabaseOrderRow) {
  return row.order_type === SELF_PICKUP_ORDER_TYPE && !row.scraped_at && row.picked !== true;
}

async function patchRepairRow(row: SupabaseOrderRow, pickedAt: string) {
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
    throw new Error(`Supabase repair update failed (${response.status}): ${await response.text()}`);
  }

  const updatedRows = (await response.json()) as SupabaseOrderRow[];
  return updatedRows[0] || { ...row, scraped_at: pickedAt, picked: true };
}

function tokenMatchesRow(token: string, row: SupabaseOrderRow) {
  const key = token.toUpperCase();

  return row.order_reference.toUpperCase() === key || row.id.toUpperCase() === key;
}

function logRepairEvent(event: string, fields: Record<string, unknown>) {
  console.log(
    JSON.stringify({
      event,
      service: "self-pickup-signal-ui",
      route: "/api/pickup-orders/manual-repair",
      ...fields,
    })
  );
}

export async function POST(request: Request) {
  const auth = validateRequestAuth(request);

  if (!auth.ok) {
    return NextResponse.json(
      { ok: false, error: auth.error },
      { status: auth.error.includes("not_configured") ? 500 : 401 }
    );
  }

  let payload: Record<string, unknown>;

  try {
    payload = (await request.json()) as Record<string, unknown>;
  } catch {
    return NextResponse.json({ ok: false, error: "invalid_json" }, { status: 400 });
  }

  const action = String(payload.action || "lookup");
  const tokens = normalizeOrderReferences(payload);

  if (!tokens.length) {
    return NextResponse.json({ ok: false, error: "no_order_references" }, { status: 400 });
  }

  if (tokens.length > MAX_REPAIR_ORDER_REFERENCES) {
    return NextResponse.json(
      { ok: false, error: `too_many_order_references_max_${MAX_REPAIR_ORDER_REFERENCES}` },
      { status: 400 }
    );
  }

  try {
    const rows = await fetchRepairRows(tokens);
    const pendingRows = rows.filter(isPending);

    if (action === "lookup") {
      const missing = tokens.filter((token) => !rows.some((row) => tokenMatchesRow(token, row)));
      const hidden = rows.length - pendingRows.length;

      return NextResponse.json({
        ok: true,
        orders: pendingRows,
        count: pendingRows.length,
        hidden,
        missing,
        table: supabaseTableName(),
        schema: supabaseSchema(),
      });
    }

    if (action === "mark") {
      if (tokens.length !== 1) {
        return NextResponse.json({ ok: false, error: "mark_requires_one_order_reference" }, { status: 400 });
      }

      const pendingRow = pendingRows.find((row) => tokenMatchesRow(tokens[0], row));

      if (!pendingRow) {
        return NextResponse.json({
          ok: true,
          status: rows.length ? "already_picked" : "not_found",
          order_reference: tokens[0],
          message: rows.length
            ? "Diese Bestellung ist schon markiert oder nicht mehr offen."
            : "Diese Bestellung wurde nicht gefunden.",
        });
      }

      const pickedAt = new Date().toISOString();
      logRepairEvent("manual_repair_pickup_mark_about_to_update", {
        order_reference: pendingRow.order_reference,
        patient_name: pendingRow.patient_name,
        row_id: pendingRow.id,
        picked_at: pickedAt,
      });
      const updatedRow = await patchRepairRow(pendingRow, pickedAt);
      logRepairEvent("manual_repair_pickup_mark_updated", {
        order_reference: updatedRow.order_reference,
        patient_name: updatedRow.patient_name,
        row_id: updatedRow.id,
        picked_at: updatedRow.scraped_at || pickedAt,
      });

      return NextResponse.json({
        ok: true,
        status: "picked",
        picked_at: pickedAt,
        order: updatedRow,
        table: supabaseTableName(),
        schema: supabaseSchema(),
      });
    }

    return NextResponse.json({ ok: false, error: "unknown_action" }, { status: 400 });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Manual repair failed." },
      { status: 500 }
    );
  }
}
