import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

type JsonRecord = Record<string, unknown>;

type AbrechnungRow = {
  id: string;
  status: string | null;
  supplier_name: string | null;
  sender_email: string | null;
  email_subject: string | null;
  received_at: string | null;
  rechnungsnummer: string | null;
  rechnungsdatum: string | null;
  total_netto: number | string | null;
  total_brutto: number | string | null;
  currency: string | null;
  ai_confidence: number | string | null;
  ai_reason: string | null;
  raw_ai_output: unknown;
  review_note: string | null;
  created_at: string | null;
};

type DocumentRow = {
  id: string;
  abrechnung_id: string;
  file_name: string | null;
  mime_type: string | null;
  file_kind: string | null;
  created_at: string | null;
};

type ProductLineRow = {
  id: string;
  abrechnung_id: string;
  line_number: number | string | null;
  product_name_raw: string | null;
  quantity: number | string | null;
  quantity_unit: string | null;
  unit_price_netto: number | string | null;
  unit_price_brutto: number | string | null;
  line_netto: number | string | null;
  line_brutto: number | string | null;
  vat_rate: number | string | null;
  currency: string | null;
  match_status: string | null;
  ai_confidence: number | string | null;
  raw_line: unknown;
  created_at: string | null;
};

type PlatformSuggestions = {
  doktorabcName: string;
  wawicanName: string;
  wawicanKultivar: string;
  status: string;
};

type BatchRow = {
  id: string;
  product_line_id: string;
  chargennummer: string | null;
  expiry_date: string | null;
  quantity: number | string | null;
  quantity_unit: string | null;
  ai_confidence: number | string | null;
  raw_batch: unknown;
  created_at: string | null;
};

type StockDispatchRow = {
  id: string;
  abrechnung_id: string;
  product_line_id: string;
  batch_id: string;
  platform: string | null;
  platform_product_name: string | null;
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
  bot_error?: string | null;
  created_at: string | null;
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

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = schemaName();

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function tableName(envName: string, fallback: string) {
  return (process.env[envName] || fallback).trim() || fallback;
}

function restUrl(table: string) {
  return `${supabaseUrl()}/rest/v1/${encodeURIComponent(table)}`;
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

function stringValue(value: unknown) {
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

function roundMoney(value: number) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

function calculatedGross(netto: number | null, vatRate: number | null) {
  if (netto === null || vatRate === null) return null;
  return roundMoney(netto * (1 + vatRate / 100));
}

function pad2(value: number) {
  return String(value).padStart(2, "0");
}

function parseLooseDate(value: unknown) {
  const text = stringValue(value);
  if (!text) return "";

  const isoMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (isoMatch) return text;

  const numericMatch = /^(\d{1,2})[./-](\d{1,2})[./-](\d{2}|\d{4})$/.exec(text);
  if (!numericMatch) return "";

  const day = Number(numericMatch[1]);
  const month = Number(numericMatch[2]);
  let year = Number(numericMatch[3]);
  if (numericMatch[3].length === 2) year += 2000;

  const date = new Date(Date.UTC(year, month - 1, day));
  if (date.getUTCFullYear() !== year || date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day) return "";

  return `${String(year).padStart(4, "0")}-${pad2(month)}-${pad2(day)}`;
}

function jsonRecord(value: unknown): JsonRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as JsonRecord;
}

function jsonArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.map(jsonRecord) : [];
}

function platformSuggestions(value: unknown): PlatformSuggestions {
  const suggestion = jsonRecord(value);

  return {
    doktorabcName: stringValue(suggestion.doktorabc_name),
    wawicanName: stringValue(suggestion.wawican_name),
    wawicanKultivar: stringValue(suggestion.wawican_kultivar),
    status: stringValue(suggestion.status),
  };
}

function uniqueValues(values: Array<string | null | undefined>) {
  return Array.from(new Set(values.map((value) => value?.trim()).filter(Boolean) as string[]));
}

async function fetchJson<T>(url: URL) {
  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`Supabase Abrechnung lookup failed (${response.status}): ${detail}`);
  }

  return (await response.json()) as T;
}

function inFilter(values: string[]) {
  return `in.(${values.join(",")})`;
}

function stockDispatchesTable() {
  return tableName("SUPABASE_ABRECHNUNG_STOCK_DISPATCHES_TABLE", "abrechnung_stock_dispatches");
}

async function fetchStockDispatches(batchIds: string[]) {
  if (!batchIds.length) return [];

  const url = new URL(restUrl(stockDispatchesTable()));
  url.searchParams.set("select", "*");
  url.searchParams.set("batch_id", inFilter(batchIds));
  url.searchParams.set("order", "created_at.asc");
  url.searchParams.set("limit", "5000");

  try {
    return await fetchJson<StockDispatchRow[]>(url);
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    if (message.includes("42P01") || message.toLowerCase().includes("does not exist")) {
      return [];
    }

    throw error;
  }
}

function matchesQuery(abrechnung: ReturnType<typeof buildAbrechnung>, terms: string[]) {
  if (!terms.length) return true;

  const haystack = normalize(
    [
      abrechnung.supplierName,
      abrechnung.sellerName,
      abrechnung.senderEmail,
      abrechnung.emailSubject,
      abrechnung.rechnungsnummer,
      abrechnung.debitorNumber,
      abrechnung.rechnungsdatum,
      abrechnung.receivedAt,
      abrechnung.status,
      ...abrechnung.products.flatMap((product) => [
        product.productName,
        product.productCode,
        product.matchStatus,
        ...product.batches.flatMap((batch) => [batch.chargennummer, batch.expiryDate]),
      ]),
    ].join(" ")
  );

  return terms.every((term) => haystack.includes(term));
}

function buildAbrechnung(
  row: AbrechnungRow,
  documents: DocumentRow[],
  lines: ProductLineRow[],
  batchesByLineId: Map<string, BatchRow[]>,
  stockDispatchesByBatchId: Map<string, StockDispatchRow[]>
) {
  const rawAi = jsonRecord(row.raw_ai_output);
  const rawInvoice = jsonRecord(rawAi.invoice);

  const products = lines
    .sort((a, b) => Number(a.line_number || 0) - Number(b.line_number || 0))
    .map((line) => {
      const rawLine = jsonRecord(line.raw_line);
      const lineNetto = numberValue(line.line_netto);
      const vatRate = numberValue(line.vat_rate);
      const unitPriceNetto = numberValue(line.unit_price_netto);
      const unitPriceBrutto = numberValue(line.unit_price_brutto) ?? calculatedGross(unitPriceNetto, vatRate);
      const lineBrutto = numberValue(line.line_brutto) ?? calculatedGross(lineNetto, vatRate) ?? (lines.length === 1 ? numberValue(row.total_brutto) : null);
      const lineBatches = (batchesByLineId.get(line.id) || []).map((batch) => {
        const rawBatch = jsonRecord(batch.raw_batch);
        const chargennummer = stringValue(batch.chargennummer);

        return {
          id: batch.id,
          chargennummer,
          expiryDate: stringValue(batch.expiry_date) || parseLooseDate(rawBatch.expiry_date),
          quantity: numberValue(batch.quantity),
          quantityUnit: stringValue(batch.quantity_unit) || "g",
          quantityPieces: numberValue(rawBatch.quantity_pieces),
          unitWeightG: numberValue(rawBatch.unit_weight_g),
          totalQuantityG: numberValue(rawBatch.total_quantity_g) ?? numberValue(batch.quantity),
          aiConfidence: numberValue(batch.ai_confidence),
          stockDispatches: (stockDispatchesByBatchId.get(batch.id) || []).map((dispatch) => ({
            id: dispatch.id,
            abrechnungId: dispatch.abrechnung_id,
            productLineId: dispatch.product_line_id,
            batchId: dispatch.batch_id,
            platform: stringValue(dispatch.platform),
            platformProductName: stringValue(dispatch.platform_product_name),
            wawicanKultivar: stringValue(dispatch.wawican_kultivar),
            rechnungsnummer: stringValue(dispatch.rechnungsnummer),
            sourceProductName: stringValue(dispatch.source_product_name),
            chargennummer: stringValue(dispatch.chargennummer),
            expiryDate: stringValue(dispatch.expiry_date),
            quantityG: numberValue(dispatch.quantity_g),
            nettoPerGram: numberValue(dispatch.netto_per_g),
            bruttoPerGram: numberValue(dispatch.brutto_per_g),
            totalNetto: numberValue(dispatch.total_netto),
            totalBrutto: numberValue(dispatch.total_brutto),
            botStatus: stringValue(dispatch.bot_status),
            botScreenshotUrl: stringValue(dispatch.bot_screenshot_url),
            botError: stringValue(dispatch.bot_error),
            createdAt: stringValue(dispatch.created_at),
          })),
        };
      });

      return {
        id: line.id,
        lineNumber: numberValue(line.line_number),
        productName: stringValue(line.product_name_raw),
        productCode: stringValue(rawLine.product_code_raw),
        quantity: numberValue(line.quantity),
        quantityUnit: stringValue(line.quantity_unit) || "g",
        quantityPieces: numberValue(rawLine.quantity_pieces),
        unitWeightG: numberValue(rawLine.unit_weight_g),
        totalQuantityG: numberValue(rawLine.total_quantity_g) ?? numberValue(line.quantity),
        unitPriceNetto,
        unitPriceBrutto,
        lineNetto,
        lineBrutto,
        vatRate,
        currency: stringValue(line.currency) || stringValue(row.currency) || "EUR",
        matchStatus: stringValue(line.match_status),
        aiConfidence: numberValue(line.ai_confidence),
        platformSuggestions: platformSuggestions(rawLine.platform_suggestions),
        batches: lineBatches,
      };
    });

  const rawProducts = jsonArray(rawAi.products);
  const totalVatFromRaw = numberValue(rawInvoice.total_vat);

  return {
    id: row.id,
    status: stringValue(row.status) || "needs_review",
    supplierName: stringValue(row.supplier_name) || stringValue(rawInvoice.supplier_name),
    sellerName: stringValue(rawInvoice.seller_name),
    customerName: stringValue(rawInvoice.customer_name),
    senderEmail: stringValue(row.sender_email),
    emailSubject: stringValue(row.email_subject),
    receivedAt: stringValue(row.received_at),
    rechnungsnummer: stringValue(row.rechnungsnummer) || stringValue(rawInvoice.rechnungsnummer),
    debitorNumber: stringValue(rawInvoice.debitor_number),
    rechnungsdatum: stringValue(row.rechnungsdatum) || stringValue(rawInvoice.rechnungsdatum),
    faelligkeitsdatum: stringValue(rawInvoice.faelligkeitsdatum),
    totalNetto: numberValue(row.total_netto),
    totalVat: totalVatFromRaw,
    totalBrutto: numberValue(row.total_brutto),
    currency: stringValue(row.currency) || stringValue(rawInvoice.currency) || "EUR",
    aiConfidence: numberValue(row.ai_confidence),
    aiReason: stringValue(row.ai_reason),
    reviewNote: stringValue(row.review_note),
    createdAt: stringValue(row.created_at),
    documents: documents.map((document) => ({
      id: document.id,
      fileName: stringValue(document.file_name),
      mimeType: stringValue(document.mime_type),
      fileKind: stringValue(document.file_kind),
      createdAt: stringValue(document.created_at),
    })),
    products: products.length ? products : rawProducts.map((product, index) => ({
      id: `${row.id}-raw-${index}`,
      lineNumber: numberValue(product.line_number) ?? index + 1,
      productName: stringValue(product.product_name_raw),
      productCode: stringValue(product.product_code_raw),
      quantity: numberValue(product.total_quantity_g),
      quantityUnit: "g",
      quantityPieces: numberValue(product.quantity_pieces),
      unitWeightG: numberValue(product.unit_weight_g),
      totalQuantityG: numberValue(product.total_quantity_g),
      unitPriceNetto: numberValue(product.unit_price_netto),
      unitPriceBrutto: numberValue(product.unit_price_brutto) ?? calculatedGross(numberValue(product.unit_price_netto), numberValue(product.vat_rate)),
      lineNetto: numberValue(product.line_netto),
      lineBrutto: numberValue(product.line_brutto) ?? calculatedGross(numberValue(product.line_netto), numberValue(product.vat_rate)) ?? (rawProducts.length === 1 ? numberValue(row.total_brutto) : null),
      vatRate: numberValue(product.vat_rate),
      currency: stringValue(product.currency) || stringValue(row.currency) || "EUR",
      matchStatus: "",
      aiConfidence: null,
      platformSuggestions: platformSuggestions(product.platform_suggestions),
      batches: jsonArray(product.batches).map((batch, batchIndex) => ({
        id: `${row.id}-raw-${index}-${batchIndex}`,
        chargennummer: stringValue(batch.chargennummer),
        expiryDate: stringValue(batch.expiry_date) || parseLooseDate(batch.expiry_date),
        quantity: numberValue(batch.total_quantity_g),
        quantityUnit: "g",
        quantityPieces: numberValue(batch.quantity_pieces),
        unitWeightG: numberValue(batch.unit_weight_g),
        totalQuantityG: numberValue(batch.total_quantity_g),
        aiConfidence: null,
        stockDispatches: [],
      })),
    })),
  };
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get("q") || "";
    const limit = Math.min(Math.max(Number(searchParams.get("limit")) || 100, 1), 200);
    const terms = splitTerms(query);

    const abrechnungenTable = tableName("SUPABASE_ABRECHNUNGEN_TABLE", "abrechnungen");
    const documentsTable = tableName("SUPABASE_ABRECHNUNG_DOCUMENTS_TABLE", "abrechnung_documents");
    const linesTable = tableName("SUPABASE_ABRECHNUNG_PRODUCT_LINES_TABLE", "abrechnung_product_lines");
    const batchesTable = tableName("SUPABASE_ABRECHNUNG_PRODUCT_BATCHES_TABLE", "abrechnung_product_batches");

    const abrechnungenUrl = new URL(restUrl(abrechnungenTable));
    abrechnungenUrl.searchParams.set(
      "select",
      "id,status,supplier_name,sender_email,email_subject,received_at,rechnungsnummer,rechnungsdatum,total_netto,total_brutto,currency,ai_confidence,ai_reason,raw_ai_output,review_note,created_at"
    );
    abrechnungenUrl.searchParams.set("order", "received_at.desc.nullslast,rechnungsdatum.desc.nullslast,created_at.desc");
    abrechnungenUrl.searchParams.set("limit", String(limit));

    const rows = await fetchJson<AbrechnungRow[]>(abrechnungenUrl);
    const abrechnungIds = uniqueValues(rows.map((row) => row.id));

    if (!abrechnungIds.length) {
      return NextResponse.json({ ok: true, query, count: 0, abrechnungen: [] });
    }

    const documentsUrl = new URL(restUrl(documentsTable));
    documentsUrl.searchParams.set("select", "id,abrechnung_id,file_name,mime_type,file_kind,created_at");
    documentsUrl.searchParams.set("abrechnung_id", inFilter(abrechnungIds));
    documentsUrl.searchParams.set("order", "created_at.asc");

    const linesUrl = new URL(restUrl(linesTable));
    linesUrl.searchParams.set(
      "select",
      "id,abrechnung_id,line_number,product_name_raw,quantity,quantity_unit,unit_price_netto,unit_price_brutto,line_netto,line_brutto,vat_rate,currency,match_status,ai_confidence,raw_line,created_at"
    );
    linesUrl.searchParams.set("abrechnung_id", inFilter(abrechnungIds));
    linesUrl.searchParams.set("order", "line_number.asc.nullslast,created_at.asc");

    const [documents, lines] = await Promise.all([fetchJson<DocumentRow[]>(documentsUrl), fetchJson<ProductLineRow[]>(linesUrl)]);
    const lineIds = uniqueValues(lines.map((line) => line.id));
    let batches: BatchRow[] = [];

    if (lineIds.length) {
      const batchesUrl = new URL(restUrl(batchesTable));
      batchesUrl.searchParams.set("select", "id,product_line_id,chargennummer,expiry_date,quantity,quantity_unit,ai_confidence,raw_batch,created_at");
      batchesUrl.searchParams.set("product_line_id", inFilter(lineIds));
      batchesUrl.searchParams.set("order", "expiry_date.asc.nullslast,created_at.asc");
      batches = await fetchJson<BatchRow[]>(batchesUrl);
    }

    const stockDispatches = await fetchStockDispatches(uniqueValues(batches.map((batch) => batch.id)));

    const documentsByAbrechnungId = new Map<string, DocumentRow[]>();
    const linesByAbrechnungId = new Map<string, ProductLineRow[]>();
    const batchesByLineId = new Map<string, BatchRow[]>();
    const stockDispatchesByBatchId = new Map<string, StockDispatchRow[]>();

    for (const document of documents) {
      const current = documentsByAbrechnungId.get(document.abrechnung_id) || [];
      current.push(document);
      documentsByAbrechnungId.set(document.abrechnung_id, current);
    }

    for (const line of lines) {
      const current = linesByAbrechnungId.get(line.abrechnung_id) || [];
      current.push(line);
      linesByAbrechnungId.set(line.abrechnung_id, current);
    }

    for (const batch of batches) {
      const current = batchesByLineId.get(batch.product_line_id) || [];
      current.push(batch);
      batchesByLineId.set(batch.product_line_id, current);
    }

    for (const dispatch of stockDispatches) {
      const current = stockDispatchesByBatchId.get(dispatch.batch_id) || [];
      current.push(dispatch);
      stockDispatchesByBatchId.set(dispatch.batch_id, current);
    }

    const abrechnungen = rows
      .map((row) => buildAbrechnung(row, documentsByAbrechnungId.get(row.id) || [], linesByAbrechnungId.get(row.id) || [], batchesByLineId, stockDispatchesByBatchId))
      .filter((abrechnung) => matchesQuery(abrechnung, terms));

    return NextResponse.json({
      ok: true,
      query,
      count: abrechnungen.length,
      abrechnungen,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown Abrechnung lookup error.";

    return NextResponse.json(
      {
        ok: false,
        error: message,
        abrechnungen: [],
      },
      { status: 500 }
    );
  }
}
