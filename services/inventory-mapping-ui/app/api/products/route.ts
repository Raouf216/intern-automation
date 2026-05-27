import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type MappingViewRow = {
  product_id: string;
  canonical_id: string;
  our_name: string | null;
  kultivar: string | null;
  status: "verified" | "needs_review" | "archived" | string;
  product_kind: "standard" | "deal" | string;
  review_reason: string | null;
  wawican_name: string | null;
  doktorabc_name: string | null;
  wawican_search_key: string | null;
  doktorabc_search_key: string | null;
  wawican_mapping_status: string | null;
  doktorabc_mapping_status: string | null;
  search_text: string | null;
};

type ProductRow = {
  id: string;
  canonicalId: string;
  ourName: string;
  kultivar: string;
  status: string;
  productKind: string;
  reviewReason: string;
  wawicanName: string;
  doktorabcName: string;
  wawicanSearchKey: string;
  doktorabcSearchKey: string;
  wawicanStatus: string;
  doktorabcStatus: string;
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

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = (process.env.SUPABASE_INVENTORY_MAPPING_SCHEMA || "public").trim() || "public";

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function mappingViewName() {
  return (process.env.SUPABASE_INVENTORY_MAPPING_VIEW || "inventory_product_mapping").trim();
}

function mappingViewUrl() {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(mappingViewName())}`;
}

function normalize(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9/]+/g, " ")
    .trim();
}

function splitTerms(query: string) {
  return normalize(query)
    .split(/\s+/)
    .map((term) => term.trim())
    .filter(Boolean);
}

function textValue(value: string | null | undefined) {
  return value?.trim() || "";
}

function productFromViewRow(row: MappingViewRow): ProductRow {
  return {
    id: row.product_id,
    canonicalId: textValue(row.canonical_id),
    ourName: textValue(row.our_name),
    kultivar: textValue(row.kultivar),
    status: textValue(row.status),
    productKind: textValue(row.product_kind) || "standard",
    reviewReason: textValue(row.review_reason),
    wawicanName: textValue(row.wawican_name),
    doktorabcName: textValue(row.doktorabc_name),
    wawicanSearchKey: textValue(row.wawican_search_key),
    doktorabcSearchKey: textValue(row.doktorabc_search_key),
    wawicanStatus: textValue(row.wawican_mapping_status),
    doktorabcStatus: textValue(row.doktorabc_mapping_status),
  };
}

function matchesQuery(row: MappingViewRow, terms: string[]) {
  if (!terms.length) {
    return true;
  }

  const haystack = normalize(
    [
      row.search_text,
      row.canonical_id,
      row.our_name,
      row.kultivar,
      row.status,
      row.product_kind,
      row.review_reason,
      row.wawican_name,
      row.doktorabc_name,
      row.wawican_search_key,
      row.doktorabc_search_key,
    ]
      .filter(Boolean)
      .join(" ")
  );

  return terms.every((term) => haystack.includes(term));
}

function matchesKind(row: MappingViewRow, kind: string) {
  if (kind === "matched") {
    return Boolean(row.wawican_name && row.doktorabc_name && row.product_kind !== "deal");
  }

  if (kind === "missing-doktorabc") {
    return Boolean(row.wawican_name && !row.doktorabc_name);
  }

  if (kind === "deal") {
    return row.product_kind === "deal";
  }

  if (kind === "needs-review") {
    return row.status === "needs_review" || row.wawican_mapping_status?.includes("needs_review") || row.doktorabc_mapping_status?.includes("needs_review");
  }

  return true;
}

function buildStats(rows: MappingViewRow[]) {
  const wawicanNames = new Set(rows.map((row) => row.wawican_name).filter(Boolean));
  const doktorabcNames = new Set(rows.map((row) => row.doktorabc_name).filter(Boolean));

  return {
    totalProducts: rows.length,
    wawicanProducts: rows.filter((row) => row.wawican_name).length,
    doktorabcProducts: doktorabcNames.size,
    intersectionProducts: rows.filter((row) => row.wawican_name && row.doktorabc_name).length,
    deals: rows.filter((row) => row.product_kind === "deal").length,
    needsReview: rows.filter((row) => row.status === "needs_review").length,
    wawicanUniqueNames: wawicanNames.size,
  };
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get("q") || "";
    const kind = searchParams.get("kind") || "all";
    const terms = splitTerms(query);
    const url = new URL(mappingViewUrl());

    url.searchParams.set(
      "select",
      "product_id,canonical_id,our_name,kultivar,status,product_kind,review_reason,wawican_name,doktorabc_name,wawican_search_key,doktorabc_search_key,wawican_mapping_status,doktorabc_mapping_status,search_text"
    );
    url.searchParams.set("order", "product_kind.asc,canonical_id.asc");
    url.searchParams.set("limit", "1000");

    const response = await fetch(url, {
      headers: supabaseHeaders(),
      cache: "no-store",
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(`Supabase product mapping lookup failed (${response.status}): ${detail}`);
    }

    const rawRows = (await response.json()) as MappingViewRow[];
    const filteredRows = rawRows
      .filter((row) => matchesKind(row, kind))
      .filter((row) => matchesQuery(row, terms))
      .map(productFromViewRow);

    return NextResponse.json({
      ok: true,
      query,
      kind,
      stats: buildStats(rawRows),
      filteredCount: filteredRows.length,
      products: filteredRows,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown product mapping lookup error.";

    return NextResponse.json(
      {
        ok: false,
        error: message,
        products: [],
      },
      { status: 500 }
    );
  }
}
