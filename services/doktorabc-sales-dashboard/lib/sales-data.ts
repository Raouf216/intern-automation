export type SalesPeriodType = "day" | "week" | "month";

export type SalesMetricKey = "sold_products" | "returned_products" | "net_products" | "returned_grams" | "net_grams";

export type SalesRow = {
  period_type: SalesPeriodType;
  period_start: string;
  period_end: string;
  product_key: string;
  product_name: string;
  sold_products: number;
  returned_products: number;
  net_products: number;
  sold_grams: number;
  returned_grams: number;
  net_grams: number;
  billing_lines: number;
  billing_rows: number;
  orders: number;
};

function requiredEnv(name: string) {
  const value = process.env[name];

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

function supabaseUrl() {
  return requiredEnv("SUPABASE_URL").replace(/\/$/, "");
}

function supabaseHeaders() {
  const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  const schema = process.env.SALES_DASHBOARD_SCHEMA || process.env.SUPABASE_SCHEMA || "private";

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Type": "application/json",
  };
}

function viewName() {
  return encodeURIComponent(process.env.SALES_DASHBOARD_VIEW || "doktorabc_sales_by_product_period");
}

function numberValue(value: unknown) {
  const number = typeof value === "number" ? value : Number(value);
  return Number.isFinite(number) ? number : 0;
}

function stringValue(value: unknown) {
  return typeof value === "string" ? value : "";
}

function normalizeRow(row: Record<string, unknown>): SalesRow | null {
  const periodType = stringValue(row.period_type);

  if (!["day", "week", "month"].includes(periodType)) {
    return null;
  }

  const periodStart = stringValue(row.period_start);
  const productName = stringValue(row.product_name);

  if (!periodStart || !productName) {
    return null;
  }

  return {
    period_type: periodType as SalesPeriodType,
    period_start: periodStart,
    period_end: stringValue(row.period_end),
    product_key: stringValue(row.product_key) || productName.toLowerCase(),
    product_name: productName,
    sold_products: numberValue(row.sold_products),
    returned_products: numberValue(row.returned_products),
    net_products: numberValue(row.net_products),
    sold_grams: numberValue(row.sold_grams),
    returned_grams: numberValue(row.returned_grams),
    net_grams: numberValue(row.net_grams),
    billing_lines: numberValue(row.billing_lines),
    billing_rows: numberValue(row.billing_rows),
    orders: numberValue(row.orders),
  };
}

export async function listSalesRows() {
  const columns = [
    "period_type",
    "period_start",
    "period_end",
    "product_key",
    "product_name",
    "sold_products",
    "returned_products",
    "net_products",
    "sold_grams",
    "returned_grams",
    "net_grams",
    "billing_lines",
    "billing_rows",
    "orders",
  ].join(",");
  const url = `${supabaseUrl()}/rest/v1/${viewName()}?select=${columns}&order=period_start.asc&limit=50000`;
  const response = await fetch(url, {
    cache: "no-store",
    headers: supabaseHeaders(),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Supabase sales view request failed: ${response.status} ${body.slice(0, 500)}`);
  }

  const payload = (await response.json()) as unknown;
  const rows = Array.isArray(payload) ? payload : [];

  return rows
    .map((row) => (row && typeof row === "object" ? normalizeRow(row as Record<string, unknown>) : null))
    .filter((row): row is SalesRow => Boolean(row));
}
