import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type InventoryProductRow = {
  id: string;
  canonical_id: string;
  status: string | null;
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
    throw new Error("Invalid SUPABASE_URL. Use a full URL such as http://supabase-kong:8000.");
  }
}

function writeSchema() {
  return (process.env.SUPABASE_INVENTORY_WRITE_SCHEMA || "private").trim() || "private";
}

function productTableName() {
  return (process.env.SUPABASE_INVENTORY_PRODUCTS_TABLE || "inventory_products").trim();
}

function platformNamesTableName() {
  return (process.env.SUPABASE_INVENTORY_PLATFORM_NAMES_TABLE || "inventory_product_platform_names").trim();
}

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = writeSchema();

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function tableUrl(tableName: string) {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(tableName)}`;
}

function stringFromPayload(payload: Record<string, unknown>, key: string) {
  const value = payload[key];

  return typeof value === "string" ? value.trim() : "";
}

async function fetchProduct(canonicalId: string) {
  const url = new URL(tableUrl(productTableName()));
  url.searchParams.set("select", "id,canonical_id,status");
  url.searchParams.set("canonical_id", `eq.${canonicalId}`);
  url.searchParams.set("limit", "2");

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase product lookup failed (${response.status}): ${await response.text()}`);
  }

  const rows = (await response.json()) as InventoryProductRow[];

  if (rows.length !== 1) {
    throw new Error(rows.length === 0 ? `Product ${canonicalId} was not found.` : `Product ${canonicalId} is ambiguous.`);
  }

  return rows[0];
}

async function markProductVerified(canonicalId: string) {
  const url = new URL(tableUrl(productTableName()));
  url.searchParams.set("canonical_id", `eq.${canonicalId}`);

  const response = await fetch(url, {
    method: "PATCH",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify({
      status: "verified",
      review_reason: null,
      updated_at: new Date().toISOString(),
    }),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase product update failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as InventoryProductRow[];
}

async function markPlatformNamesVerified(productId: string) {
  const url = new URL(tableUrl(platformNamesTableName()));
  url.searchParams.set("product_id", `eq.${productId}`);
  url.searchParams.set("mapping_status", "neq.archived");

  const response = await fetch(url, {
    method: "PATCH",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify({
      mapping_status: "verified",
      updated_at: new Date().toISOString(),
    }),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase platform-name update failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as unknown[];
}

export async function POST(request: Request) {
  try {
    const payload = (await request.json()) as Record<string, unknown>;
    const canonicalId = stringFromPayload(payload, "canonicalId");

    if (!canonicalId) {
      return NextResponse.json(
        {
          ok: false,
          error: "canonicalId is required.",
        },
        { status: 400 }
      );
    }

    const product = await fetchProduct(canonicalId);
    const platformRows = await markPlatformNamesVerified(product.id);
    const productRows = await markProductVerified(canonicalId);

    return NextResponse.json({
      ok: true,
      canonicalId,
      productRows: productRows.length,
      platformRows: platformRows.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown verification error.";

    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status: 500 }
    );
  }
}
