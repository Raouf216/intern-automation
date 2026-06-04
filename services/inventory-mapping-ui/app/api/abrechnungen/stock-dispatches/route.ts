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
  bot_status?: string | null;
  bot_screenshot_url?: string | null;
  bot_response?: unknown;
  bot_error?: string | null;
  created_at: string | null;
};

type BatchRow = {
  id: string;
  quantity: number | string | null;
  raw_batch: unknown;
};

type SendDoktorabcBotResponse = {
  ok?: boolean;
  error?: string;
  dry_run?: boolean;
  clicked?: boolean;
  add_button_text?: string;
  screenshot_url?: string | null;
  screenshots?: Array<{
    filename?: string;
    path?: string;
    url?: string;
  }>;
};

type PersistScreenshotInput = {
  screenshotUrl: string;
  abrechnungId: string;
  batchId: string;
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

function supabaseStorageHeaders(extra?: Record<string, string>) {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    ...(extra || {}),
  };
}

function tableName(envName: string, fallback: string) {
  return (process.env[envName] || fallback).trim() || fallback;
}

function restUrl(table: string) {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(table)}`;
}

function storageBucket() {
  return (process.env.SUPABASE_STOCK_SCREENSHOTS_BUCKET || "abrechnung-stock-screenshots").trim() || "abrechnung-stock-screenshots";
}

function storageObjectUrl(bucket: string, objectPath: string) {
  const encodedPath = objectPath.split("/").map(encodeURIComponent).join("/");
  return `${supabaseUrl()}/storage/v1/object/${encodeURIComponent(bucket)}/${encodedPath}`;
}

function stockScreenshotRoute(bucket: string, objectPath: string) {
  const encodedPath = [bucket, ...objectPath.split("/")].map(encodeURIComponent).join("/");
  return `/api/abrechnungen/stock-screenshots/${encodedPath}`;
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
    botStatus: textValue(row.bot_status),
    botScreenshotUrl: textValue(row.bot_screenshot_url),
    botError: textValue(row.bot_error),
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

function sendDoktorabcEndpoint() {
  return (
    process.env.SEND_DOKTORABC_ADD_DECREASE_ENDPOINT ||
    process.env.SEND_DOKTORABC_ENDPOINT ||
    process.env.DOKTORABC_SEND_BOT_ENDPOINT ||
    ""
  )
    .trim()
    .replace(/^["']|["']$/g, "");
}

function bestScreenshotUrl(payload: SendDoktorabcBotResponse) {
  return textValue(payload.screenshot_url) || textValue(payload.screenshots?.at(-1)?.url) || textValue(payload.screenshots?.[0]?.url);
}

async function ensureStockScreenshotBucket(bucket: string) {
  const detailsResponse = await fetch(`${supabaseUrl()}/storage/v1/bucket/${encodeURIComponent(bucket)}`, {
    method: "HEAD",
    headers: supabaseStorageHeaders(),
    cache: "no-store",
  });

  if (detailsResponse.ok) return;

  if (detailsResponse.status !== 404 && detailsResponse.status !== 400) {
    const detail = await detailsResponse.text();
    throw new Error(`Supabase screenshot bucket lookup failed (${detailsResponse.status}): ${detail}`);
  }

  const createResponse = await fetch(`${supabaseUrl()}/storage/v1/bucket`, {
    method: "POST",
    headers: supabaseStorageHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      id: bucket,
      name: bucket,
      public: false,
      file_size_limit: 10_000_000,
      allowed_mime_types: ["image/png", "image/jpeg", "image/webp"],
    }),
    cache: "no-store",
  });

  if (createResponse.ok) return;

  const detail = await createResponse.text();
  const normalized = detail.toLowerCase();
  if (createResponse.status === 400 && (normalized.includes("already") || normalized.includes("duplicate"))) return;

  throw new Error(`Supabase screenshot bucket create failed (${createResponse.status}): ${detail}`);
}

async function persistScreenshotProof({ screenshotUrl, abrechnungId, batchId }: PersistScreenshotInput) {
  const sourceResponse = await fetch(screenshotUrl, { cache: "no-store" });

  if (!sourceResponse.ok) {
    const detail = await sourceResponse.text().catch(() => "");
    throw new Error(`DoktorABC screenshot download failed (${sourceResponse.status}): ${detail}`);
  }

  const contentType = sourceResponse.headers.get("content-type") || "image/png";
  if (!contentType.toLowerCase().startsWith("image/")) {
    throw new Error(`DoktorABC screenshot has invalid content type: ${contentType}`);
  }

  const imageBytes = await sourceResponse.arrayBuffer();
  if (!imageBytes.byteLength) {
    throw new Error("DoktorABC screenshot download was empty.");
  }

  if (imageBytes.byteLength > 10_000_000) {
    throw new Error("DoktorABC screenshot is larger than 10 MB.");
  }

  const bucket = storageBucket();
  await ensureStockScreenshotBucket(bucket);

  const extension = contentType.includes("jpeg") || contentType.includes("jpg") ? "jpg" : contentType.includes("webp") ? "webp" : "png";
  const day = new Date().toISOString().slice(0, 10);
  const objectPath = `doktorabc/${day}/${abrechnungId}/${batchId}/${crypto.randomUUID()}.${extension}`;
  const uploadResponse = await fetch(storageObjectUrl(bucket, objectPath), {
    method: "POST",
    headers: supabaseStorageHeaders({
      "Content-Type": contentType,
      "x-upsert": "false",
    }),
    body: imageBytes,
    cache: "no-store",
  });

  if (!uploadResponse.ok) {
    const detail = await uploadResponse.text();
    throw new Error(`Supabase screenshot upload failed (${uploadResponse.status}): ${detail}`);
  }

  return stockScreenshotRoute(bucket, objectPath);
}

async function callSendDoktorabcBot(productName: string, quantityG: number) {
  const endpoint = sendDoktorabcEndpoint();

  if (!endpoint) {
    throw new Error("Missing SEND_DOKTORABC_ADD_DECREASE_ENDPOINT for DoktorABC stock preview.");
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000);

  try {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        product_name: productName,
        quantity_grams: String(quantityG),
      }),
      signal: controller.signal,
      cache: "no-store",
    });

    const text = await response.text();
    let payload: SendDoktorabcBotResponse = {};

    if (text) {
      try {
        payload = JSON.parse(text) as SendDoktorabcBotResponse;
      } catch {
        payload = { ok: false, error: text };
      }
    }

    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || `DoktorABC bot failed (${response.status}).`);
    }

    const screenshotUrl = bestScreenshotUrl(payload);

    if (!screenshotUrl) {
      throw new Error("DoktorABC bot prepared the modal but did not return a screenshot URL.");
    }

    if (payload.dry_run) {
      throw new Error(
        `DoktorABC dry run only: '${textValue(payload.add_button_text) || "Add grams"}' was clickable, but no stock was added. Set SEND_DOKTORABC_DRY_RUN=false for real sends.`
      );
    }

    if (payload.clicked !== true) {
      throw new Error("DoktorABC bot did not confirm the final Add grams click.");
    }

    return {
      status: "prepared",
      screenshotUrl,
      response: payload,
    };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("DoktorABC bot timed out before preparing the screenshot.");
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function insertStockDispatch(insertPayload: JsonRecord, botFields: JsonRecord) {
  const withBotFields = Object.keys(botFields).length ? { ...insertPayload, ...botFields } : insertPayload;

  const response = await fetch(restUrl(stockDispatchesTable()), {
    method: "POST",
    headers: supabaseHeaders("return=representation"),
    body: JSON.stringify(withBotFields),
    cache: "no-store",
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Supabase stock dispatch insert failed (${response.status}): ${detail}`);
  }

  const rows = (await response.json()) as StockDispatchRow[];
  return rows[0];
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

    const botResult = platform === "doktorabc" ? await callSendDoktorabcBot(platformProductName, quantityG) : null;
    const persistedScreenshotUrl = botResult
      ? await persistScreenshotProof({
          screenshotUrl: botResult.screenshotUrl,
          abrechnungId,
          batchId,
        })
      : "";
    const insertPayload: JsonRecord = {
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
    const botFields: JsonRecord = botResult
      ? {
          bot_status: botResult.status,
          bot_screenshot_url: persistedScreenshotUrl,
          bot_response: botResult.response,
          bot_error: null,
        }
      : {};
    const row = await insertStockDispatch(insertPayload, botFields);
    const dispatch = {
      ...dispatchResponse(row),
      ...(botResult
        ? {
            botStatus: botResult.status,
            botScreenshotUrl: persistedScreenshotUrl,
            botError: "",
          }
        : {}),
    };

    return NextResponse.json({
      ok: true,
      dispatch,
      botColumnsPersisted: true,
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
