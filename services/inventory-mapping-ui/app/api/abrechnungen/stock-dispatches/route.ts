import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type JsonRecord = Record<string, unknown>;

type StockDispatchRow = {
  id: string;
  abrechnung_id: string;
  product_line_id: string;
  batch_id: string;
  platform: string;
  platform_product_name: string;
  wawican_kultivar: string | null;
  rechnungsnummer: string | null;
  source_product_name: string | null;
  chargennummer: string | null;
  expiry_date: string | null;
  quantity_g: number | string | null;
  netto_per_g: number | string | null;
  brutto_per_g: number | string | null;
  total_netto: number | string | null;
  total_brutto: number | string | null;
  created_at: string | null;
};

type BatchRow = {
  id: string;
  quantity: number | string | null;
  raw_batch: unknown;
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

function schemaName() {
  return (process.env.SUPABASE_ABRECHNUNG_SCHEMA || "private").trim() || "private";
}

function supabaseHeaders(prefer?: string) {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = schemaName();

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
    ...(prefer ? { Prefer: prefer } : {}),
  };
}

function tableName(envName: string, fallback: string) {
  return (process.env[envName] || fallback).trim() || fallback;
}

function restUrl(table: string) {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(table)}`;
}

function textValue(value: unknown) {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function numberValue(value: unknown) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  const text = String(value).trim().replace(/\s/g, "");
  const normalized = text.includes(",") && text.includes(".") ? text.replace(/\./g, "").replace(",", ".") : text.replace(",", ".");
  const parsed = Number(normalized);

  return Number.isFinite(parsed) ? parsed : null;
}

function jsonRecord(value: unknown): JsonRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as JsonRecord;
}

function pad2(value: number) {
  return String(value).padStart(2, "0");
}

function parseDateValue(value: unknown) {
  const text = textValue(value);
  if (!text) return null;

  const isoMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (isoMatch) return text;

  const numericMatch = /^(\d{1,2})[./-](\d{1,2})[./-](\d{2}|\d{4})$/.exec(text);
  if (!numericMatch) return null;

  const day = Number(numericMatch[1]);
  const month = Number(numericMatch[2]);
  let year = Number(numericMatch[3]);
  if (numericMatch[3].length === 2) year += 2000;

  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) return null;

  return `${String(year).padStart(4, "0")}-${pad2(month)}-${pad2(day)}`;
}

function stockDispatchesTable() {
  return tableName("SUPABASE_ABRECHNUNG_STOCK_DISPATCHES_TABLE", "abrechnung_stock_dispatches");
}

function batchesTable() {
  return tableName("SUPABASE_ABRECHNUNG_PRODUCT_BATCHES_TABLE", "abrechnung_product_batches");
}

function dispatchResponse(row: StockDispatchRow) {
  return {
    id: row.id,
    abrechnungId: row.abrechnung_id,
    productLineId: row.product_line_id,
    batchId: row.batch_id,
    platform: row.platform,
    platformProductName: textValue(row.platform_product_name),
    wawicanKultivar: textValue(row.wawican_kultivar),
    rechnungsnummer: textValue(row.rechnungsnummer),
    sourceProductName: textValue(row.source_product_name),
    chargennummer: textValue(row.chargennummer),
    expiryDate: textValue(row.expiry_date),
    quantityG: numberValue(row.quantity_g),
    nettoPerGram: numberValue(row.netto_per_g),
    bruttoPerGram: numberValue(row.brutto_per_g),
    totalNetto: numberValue(row.total_netto),
    totalBrutto: numberValue(row.total_brutto),
    createdAt: textValue(row.created_at),
  };
}

async function fetchJson<T>(url: URL) {
  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Supabase lookup failed (${response.status}): ${detail}`);
  }

  return (await response.json()) as T;
}

async function fetchBatchQuantity(batchId: string) {
  const url = new URL(restUrl(batchesTable()));
  url.searchParams.set("select", "id,quantity,raw_batch");
  url.searchParams.set("id", `eq.${batchId}`);
  url.searchParams.set("limit", "1");

  const rows = await fetchJson<BatchRow[]>(url);
  const row = rows[0];
  const rawBatch = jsonRecord(row?.raw_batch);
  const quantity = numberValue(rawBatch.total_quantity_g) ?? numberValue(row?.quantity);

  return quantity;
}

async function fetchAlreadySent(batchId: string) {
  const url = new URL(restUrl(stockDispatchesTable()));
  url.searchParams.set("select", "quantity_g");
  url.searchParams.set("batch_id", `eq.${batchId}`);
  url.searchParams.set("limit", "5000");

  const rows = await fetchJson<Array<{ quantity_g: number | string | null }>>(url);
  return rows.reduce((sum, row) => sum + (numberValue(row.quantity_g) || 0), 0);
}

export async function POST(request: Request) {
  try {
    const payload = (await request.json()) as JsonRecord;
    const platform = textValue(payload.platform).toLowerCase();
    const batchId = textValue(payload.batchId);
    const abrechnungId = textValue(payload.abrechnungId);
    const productLineId = textValue(payload.productLineId);
    const platformProductName = textValue(payload.platformProductName);
    const wawicanKultivar = textValue(payload.wawicanKultivar);
    const quantityG = numberValue(payload.quantityG);

    if (platform !== "doktorabc" && platform !== "wawican") {
      return NextResponse.json({ ok: false, error: "invalid_platform" }, { status: 400 });
    }

    if (!batchId || !abrechnungId || !productLineId) {
      return NextResponse.json({ ok: false, error: "missing_abrechnung_line_or_batch_id" }, { status: 400 });
    }

    if (!platformProductName) {
      return NextResponse.json({ ok: false, error: "missing_platform_product_name" }, { status: 400 });
    }

    if (platform === "wawican" && !wawicanKultivar) {
      return NextResponse.json({ ok: false, error: "missing_wawican_kultivar" }, { status: 400 });
    }

    if (quantityG === null || quantityG <= 0) {
      return NextResponse.json({ ok: false, error: "invalid_quantity_g" }, { status: 400 });
    }

    const available = await fetchBatchQuantity(batchId);

    if (available === null || available <= 0) {
      return NextResponse.json({ ok: false, error: "batch_quantity_missing" }, { status: 400 });
    }

    const alreadySent = await fetchAlreadySent(batchId);
    const remaining = available - alreadySent;

    if (quantityG > remaining + 0.001) {
      return NextResponse.json(
        {
          ok: false,
          error: "quantity_exceeds_remaining",
          available,
          alreadySent,
          remaining: Math.max(0, remaining),
        },
        { status: 409 }
      );
    }

    const insertPayload = {
      abrechnung_id: abrechnungId,
      product_line_id: productLineId,
      batch_id: batchId,
      platform,
      platform_product_name: platformProductName,
      wawican_kultivar: platform === "wawican" ? wawicanKultivar : null,
      rechnungsnummer: textValue(payload.rechnungsnummer) || null,
      source_product_name: textValue(payload.sourceProductName) || platformProductName,
      chargennummer: textValue(payload.chargennummer) || null,
      expiry_date: parseDateValue(payload.expiryDate),
      quantity_g: quantityG,
      netto_per_g: numberValue(payload.nettoPerGram),
      brutto_per_g: numberValue(payload.bruttoPerGram),
      total_netto: numberValue(payload.totalNetto),
      total_brutto: numberValue(payload.totalBrutto),
    };

    const response = await fetch(restUrl(stockDispatchesTable()), {
      method: "POST",
      headers: supabaseHeaders("return=representation"),
      body: JSON.stringify(insertPayload),
      cache: "no-store",
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(`Supabase stock dispatch insert failed (${response.status}): ${detail}`);
    }

    const rows = (await response.json()) as StockDispatchRow[];
    const dispatch = dispatchResponse(rows[0]);

    return NextResponse.json({
      ok: true,
      dispatch,
      available,
      alreadySent: alreadySent + quantityG,
      remaining: Math.max(0, available - alreadySent - quantityG),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown stock dispatch error.";

    return NextResponse.json(
      {
        ok: false,
        error: message,
      },
      { status: 500 }
    );
  }
}
