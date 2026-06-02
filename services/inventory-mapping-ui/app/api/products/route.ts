import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type WawicanCatalogRow = {
  product_name?: string | null;
  cultivar?: string | null;
  kultivar?: string | null;
  availability_status?: string | null;
  available?: boolean | null;
  scraped_at?: string | null;
  raw_data?: {
    cultivar?: string | null;
    kultivar?: string | null;
    availability_status?: string | null;
    available?: boolean | null;
  } | null;
};

type DoktorabcCatalogRow = {
  product_name?: string | null;
  pzn?: string | null;
  strain?: string | null;
  availability?: boolean | null;
};

type ProductRow = {
  id: string;
  canonicalId: string;
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

type ProductStats = {
  totalProducts: number;
  wawicanProducts: number;
  wawicanAvailableProducts: number;
  wawicanUnavailableProducts: number;
  doktorabcProducts: number;
  intersectionProducts: number;
  deals: number;
  needsReview: number;
  wawicanUniqueNames: number;
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

function productCatalogSchema() {
  return (
    process.env.SUPABASE_PRODUCT_CATALOG_SCHEMA ||
    process.env.SUPABASE_ABRECHNUNG_SCHEMA ||
    "private"
  ).trim() || "private";
}

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = productCatalogSchema();

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

function doktorabcProductsTable() {
  return (process.env.DOKTORABC_PRODUCTS_TABLE || "doktorabc_products").trim() || "doktorabc_products";
}

function wawicanAvailableProductsTable() {
  return (process.env.WAWICAN_PRODUCTS_TABLE || "wawican_products").trim() || "wawican_products";
}

function wawicanUnavailableProductsTable() {
  return (
    process.env.WAWICAN_UNAVAILABLE_PRODUCTS_TABLE ||
    "wawican_unavailable_products"
  ).trim() || "wawican_unavailable_products";
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

function availabilityKey(value: string) {
  const folded = normalize(value).replace(/\s+/g, " ");

  if (folded === "verfugbar") return "available";
  if (folded === "nicht verfugbar") return "unavailable";
  return folded || "unknown";
}

function rowIdentity(parts: Array<string | null | undefined>) {
  return parts.map((part) => textValue(part).toLowerCase()).join("::");
}

async function fetchCatalogRows<T>(tableName: string, select: string, fallbackSelect?: string): Promise<T[]> {
  const url = new URL(tableUrl(tableName));
  url.searchParams.set("select", select);
  url.searchParams.set("limit", "5000");

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (response.ok) {
    return (await response.json()) as T[];
  }

  if (!fallbackSelect) {
    const detail = await response.text();
    throw new Error(`Supabase ${tableName} lookup failed (${response.status}): ${detail}`);
  }

  const fallbackUrl = new URL(tableUrl(tableName));
  fallbackUrl.searchParams.set("select", fallbackSelect);
  fallbackUrl.searchParams.set("limit", "5000");

  const fallbackResponse = await fetch(fallbackUrl, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!fallbackResponse.ok) {
    const detail = await fallbackResponse.text();
    throw new Error(`Supabase ${tableName} lookup failed (${fallbackResponse.status}): ${detail}`);
  }

  return (await fallbackResponse.json()) as T[];
}

function wawicanProductFromRow(row: WawicanCatalogRow, source: "available" | "unavailable"): ProductRow | null {
  const wawicanName = textValue(row.product_name);

  if (!wawicanName) {
    return null;
  }

  const rawData = row.raw_data || {};
  const kultivar = textValue(row.cultivar || row.kultivar || rawData.cultivar || rawData.kultivar);
  const status =
    textValue(row.availability_status || rawData.availability_status) ||
    (source === "available" ? "verfügbar" : "nicht verfügbar");
  const available = typeof row.available === "boolean" ? row.available : rawData.available;
  const normalizedStatus = available === false ? "nicht verfügbar" : available === true ? "verfügbar" : status;
  const sourceLabel = source === "available" ? "wawican_available" : "wawican_unavailable";
  const identity = rowIdentity([sourceLabel, wawicanName, kultivar]);

  return {
    id: identity,
    canonicalId: identity,
    kultivar,
    status: availabilityKey(normalizedStatus),
    productKind: "wawican",
    reviewReason: "",
    wawicanName,
    doktorabcName: "",
    wawicanSearchKey: [wawicanName, kultivar].filter(Boolean).join(" "),
    doktorabcSearchKey: "",
    wawicanStatus: normalizedStatus,
    doktorabcStatus: "",
  };
}

function doktorabcProductFromRow(row: DoktorabcCatalogRow): ProductRow | null {
  const doktorabcName = textValue(row.product_name);

  if (!doktorabcName) {
    return null;
  }

  const pzn = textValue(row.pzn);
  const strain = textValue(row.strain);
  const identity = rowIdentity(["doktorabc", pzn || doktorabcName, doktorabcName]);
  const status = row.availability === false ? "unavailable" : row.availability === true ? "available" : "unknown";

  return {
    id: identity,
    canonicalId: identity,
    kultivar: "",
    status,
    productKind: "doktorabc",
    reviewReason: strain,
    wawicanName: "",
    doktorabcName,
    wawicanSearchKey: "",
    doktorabcSearchKey: [doktorabcName, strain, pzn].filter(Boolean).join(" "),
    wawicanStatus: "",
    doktorabcStatus: status,
  };
}

function uniqueProducts(products: ProductRow[]) {
  const seen = new Set<string>();
  const rows: ProductRow[] = [];

  for (const product of products) {
    const key = product.productKind === "wawican"
      ? rowIdentity([product.productKind, product.wawicanName, product.kultivar, product.status])
      : rowIdentity([product.productKind, product.doktorabcName]);

    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    rows.push(product);
  }

  return rows;
}

function matchesQuery(row: ProductRow, terms: string[]) {
  if (!terms.length) {
    return true;
  }

  const haystack = normalize(
    [
      row.canonicalId,
      row.kultivar,
      row.status,
      row.productKind,
      row.reviewReason,
      row.wawicanName,
      row.doktorabcName,
      row.wawicanSearchKey,
      row.doktorabcSearchKey,
      row.wawicanStatus,
      row.doktorabcStatus,
    ]
      .filter(Boolean)
      .join(" ")
  );

  return terms.every((term) => haystack.includes(term));
}

function matchesPlatform(row: ProductRow, platform: string) {
  if (platform === "wawican") return row.productKind === "wawican";
  if (platform === "doktorabc") return row.productKind === "doktorabc";
  return true;
}

function matchesWawicanAvailability(row: ProductRow, selected: Set<string>) {
  if (row.productKind !== "wawican") {
    return true;
  }

  return selected.has(row.status);
}

function buildStats(products: ProductRow[]): ProductStats {
  const wawicanRows = products.filter((row) => row.productKind === "wawican");
  const doktorabcRows = products.filter((row) => row.productKind === "doktorabc");
  const wawicanNames = new Set(wawicanRows.map((row) => rowIdentity([row.wawicanName, row.kultivar])).filter(Boolean));

  return {
    totalProducts: products.length,
    wawicanProducts: wawicanRows.length,
    wawicanAvailableProducts: wawicanRows.filter((row) => row.status === "available").length,
    wawicanUnavailableProducts: wawicanRows.filter((row) => row.status === "unavailable").length,
    doktorabcProducts: doktorabcRows.length,
    intersectionProducts: 0,
    deals: 0,
    needsReview: 0,
    wawicanUniqueNames: wawicanNames.size,
  };
}

function sortProducts(products: ProductRow[]) {
  return [...products].sort((left, right) => {
    const leftName = left.productKind === "wawican" ? `${left.wawicanName} ${left.kultivar}` : left.doktorabcName;
    const rightName = right.productKind === "wawican" ? `${right.wawicanName} ${right.kultivar}` : right.doktorabcName;

    return leftName.localeCompare(rightName, "de", { sensitivity: "base" });
  });
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get("q") || "";
    const platform = searchParams.get("platform") || searchParams.get("kind") || "all";
    const availability = searchParams.get("availability") || "available,unavailable";
    const terms = splitTerms(query);
    const selectedAvailability = new Set(
      availability
        .split(",")
        .map((item) => availabilityKey(item))
        .filter((item) => item === "available" || item === "unavailable")
    );

    if (selectedAvailability.size === 0) {
      selectedAvailability.add("available");
      selectedAvailability.add("unavailable");
    }

    const [wawicanAvailableRows, wawicanUnavailableRows, doktorabcRows] = await Promise.all([
      fetchCatalogRows<WawicanCatalogRow>(
        wawicanAvailableProductsTable(),
        "product_name,cultivar,availability_status,available,raw_data,scraped_at",
        "product_name,availability_status,available,raw_data,scraped_at"
      ),
      fetchCatalogRows<WawicanCatalogRow>(
        wawicanUnavailableProductsTable(),
        "product_name,cultivar,availability_status,available,raw_data,scraped_at",
        "product_name,availability_status,available,raw_data,scraped_at"
      ),
      fetchCatalogRows<DoktorabcCatalogRow>(
        doktorabcProductsTable(),
        "product_name,pzn,strain,availability",
        "product_name,pzn"
      ),
    ]);

    const allProducts = uniqueProducts([
      ...wawicanAvailableRows
        .map((row) => wawicanProductFromRow(row, "available"))
        .filter((row): row is ProductRow => Boolean(row)),
      ...wawicanUnavailableRows
        .map((row) => wawicanProductFromRow(row, "unavailable"))
        .filter((row): row is ProductRow => Boolean(row)),
      ...doktorabcRows
        .map(doktorabcProductFromRow)
        .filter((row): row is ProductRow => Boolean(row)),
    ]);

    const filteredRows = sortProducts(
      allProducts
        .filter((row) => matchesPlatform(row, platform))
        .filter((row) => matchesWawicanAvailability(row, selectedAvailability))
        .filter((row) => matchesQuery(row, terms))
    );

    return NextResponse.json({
      ok: true,
      query,
      kind: platform,
      platform,
      availability: Array.from(selectedAvailability),
      stats: buildStats(allProducts),
      filteredCount: filteredRows.length,
      products: filteredRows,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown product catalog lookup error.";

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
