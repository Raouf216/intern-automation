import { randomUUID } from "crypto";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type InventoryProductRow = {
  id: string;
  canonical_id: string;
};

type PlatformInput = {
  platform: "wawican" | "doktorabc";
  name: string;
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

function textValue(value: unknown) {
  return typeof value === "string" ? value.trim() : "";
}

function nullableText(value: string) {
  return value.trim() || null;
}

function normalizeWhitespace(value: string) {
  return value.replace(/\s+/g, " ").trim();
}

function platformSearchKey(value: string) {
  return normalizeWhitespace(value.normalize("NFKC"));
}

function canonicalId() {
  return `manual_${Date.now().toString(36)}_${randomUUID().slice(0, 8)}`;
}

function payloadPlatforms(payload: Record<string, unknown>) {
  const platforms = payload.platforms;

  if (!platforms || typeof platforms !== "object" || Array.isArray(platforms)) {
    return [];
  }

  const platformRecord = platforms as Record<string, unknown>;
  const rows: PlatformInput[] = [];
  const wawicanName = textValue(platformRecord.wawicanName);
  const doktorabcName = textValue(platformRecord.doktorabcName);

  if (wawicanName) {
    rows.push({ platform: "wawican", name: normalizeWhitespace(wawicanName) });
  }

  if (doktorabcName) {
    rows.push({ platform: "doktorabc", name: normalizeWhitespace(doktorabcName) });
  }

  return rows;
}

function platformPayload(payload: Record<string, unknown>) {
  const platforms = payload.platforms;

  return platforms && typeof platforms === "object" && !Array.isArray(platforms)
    ? (platforms as Record<string, unknown>)
    : null;
}

function platformValidationError(payload: Record<string, unknown>) {
  const platforms = platformPayload(payload);

  if (!platforms) {
    return "Choose at least one platform and enter its exact name.";
  }

  const wawicanName = textValue(platforms.wawicanName);
  const doktorabcName = textValue(platforms.doktorabcName);

  if (!wawicanName && !doktorabcName) {
    return "Enter at least one platform name.";
  }

  if (wawicanName && !textValue(payload.kultivar)) {
    return "Kultivar is required when Wawican name is set.";
  }

  return "";
}

async function deleteProduct(productId: string) {
  const url = new URL(tableUrl(productTableName()));
  url.searchParams.set("id", `eq.${productId}`);

  await fetch(url, {
    method: "DELETE",
    headers: supabaseHeaders(),
    cache: "no-store",
  });
}

async function insertProduct(payload: Record<string, unknown>, platforms: PlatformInput[]) {
  const now = new Date().toISOString();
  const status = payload.verified === true ? "verified" : "needs_review";
  const productKind = platforms.length === 1 && platforms[0].platform === "doktorabc" ? "deal" : "stock";
  const url = new URL(tableUrl(productTableName()));

  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify({
      canonical_id: canonicalId(),
      kultivar: nullableText(textValue(payload.kultivar)),
      product_kind: productKind,
      status,
      review_reason: status === "verified" ? null : "manual_ui_needs_review",
      source: "inventory_mapping_ui",
      created_at: now,
      updated_at: now,
    }),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase product insert failed (${response.status}): ${await response.text()}`);
  }

  const rows = (await response.json()) as InventoryProductRow[];
  const product = rows[0];

  if (!product?.id) {
    throw new Error("Supabase product insert returned no product id.");
  }

  return {
    product,
    status,
    now,
  };
}

async function insertPlatformNames(productId: string, status: string, now: string, platforms: PlatformInput[]) {
  const url = new URL(tableUrl(platformNamesTableName()));
  const response = await fetch(url, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify(
      platforms.map((platform) => ({
        product_id: productId,
        platform: platform.platform,
        platform_name: platform.name,
        platform_search_key: platformSearchKey(platform.name),
        mapping_status: status,
        source: "inventory_mapping_ui",
        created_at: now,
        updated_at: now,
      }))
    ),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase platform-name insert failed (${response.status}): ${await response.text()}`);
  }

  return (await response.json()) as unknown[];
}

export async function POST(request: Request) {
  let createdProductId = "";

  try {
    const payload = (await request.json()) as Record<string, unknown>;
    const validationError = platformValidationError(payload);

    if (validationError) {
      return NextResponse.json(
        {
          ok: false,
          error: validationError,
        },
        { status: 400 }
      );
    }

    const platforms = payloadPlatforms(payload);

    if (!platforms.length) {
      return NextResponse.json(
        {
          ok: false,
          error: "Choose at least one platform and enter its exact name.",
        },
        { status: 400 }
      );
    }

    const { product, status, now } = await insertProduct(payload, platforms);
    createdProductId = product.id;

    const platformRows = await insertPlatformNames(product.id, status, now, platforms);

    return NextResponse.json({
      ok: true,
      canonicalId: product.canonical_id,
      platformRows: platformRows.length,
    });
  } catch (error) {
    if (createdProductId) {
      await deleteProduct(createdProductId);
    }

    const message = error instanceof Error ? error.message : "Unknown product creation error.";

    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status: 500 }
    );
  }
}
