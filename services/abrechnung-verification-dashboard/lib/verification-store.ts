import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

export type VerificationStatus = "success" | "failure" | "warning";
export type ProblemSeverity = "critical" | "high" | "medium";

export type VerificationProblem = {
  id: string;
  problem_type: string;
  order_reference: string;
  billing_id: string | null;
  line_no: string | null;
  order_type: string | null;
  billing_date: string | null;
  billing_type: string | null;
  pzn: string | null;
  product_name: string | null;
  expected_value: unknown;
  actual_value: unknown;
  problem: string;
  severity: ProblemSeverity;
  raw: Record<string, unknown>;
};

export type StoredVerificationRun = {
  id: string;
  status: VerificationStatus;
  source: string;
  bot_name: string;
  received_at: string;
  finished_at: string | null;
  billing_period_from: string | null;
  billing_period_to: string | null;
  invoice_file: string | null;
  success_count: number;
  success_ids: string[];
  problem_count: number;
  problems: VerificationProblem[];
  raw: Record<string, unknown>;
};

type NotificationStatus = "success" | "failure" | "warning";

type SupabaseNotificationRow = {
  id: string;
  section: string;
  event: string;
  status: NotificationStatus | string;
  title: string;
  message: string;
  filename: string | null;
  upload_type: string | null;
  bucket: string | null;
  path: string | null;
  size_bytes: number | null;
  error: string | null;
  source: string;
  payload: Record<string, unknown>;
  created_at: string;
};

const maxStoredRuns = 500;
const notificationSection = "abrechnung_verification";
const notificationUploadType = "doktorabc_abrechnung_verification";

function supabaseConfigured() {
  return Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);
}

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
  const schema = process.env.SUPABASE_NOTIFICATIONS_SCHEMA || process.env.SUPABASE_SCHEMA || "public";

  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    "Content-Type": "application/json",
  };
}

function tableName() {
  return encodeURIComponent(process.env.SUPABASE_NOTIFICATIONS_TABLE || "notifications");
}

function tableUrl() {
  return `${supabaseUrl()}/rest/v1/${tableName()}`;
}

function storePath() {
  return path.join(process.cwd(), "data", "verification-runs.json");
}

async function readStore() {
  try {
    const data = await readFile(storePath(), "utf8");
    const parsed = JSON.parse(data) as unknown;
    return Array.isArray(parsed) ? parsed.filter(isStoredRun) : [];
  } catch (error) {
    if (isNodeError(error) && error.code === "ENOENT") {
      return [];
    }

    throw error;
  }
}

async function writeStore(runs: StoredVerificationRun[]) {
  const filePath = storePath();
  await mkdir(path.dirname(filePath), { recursive: true });
  const temporaryPath = `${filePath}.${randomUUID()}.tmp`;
  await writeFile(temporaryPath, `${JSON.stringify(runs, null, 2)}\n`, "utf8");
  await rename(temporaryPath, filePath);
}

export async function listVerificationRuns(limit = 160) {
  if (supabaseConfigured()) {
    return listVerificationRunsFromSupabase(limit);
  }

  const runs = await readStore();
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.min(limit, maxStoredRuns)) : 160;

  return runs
    .sort((left, right) => Date.parse(right.received_at) - Date.parse(left.received_at))
    .slice(0, normalizedLimit);
}

export async function storeVerificationRun(payload: unknown) {
  const normalizedRun = normalizeRun(payload);

  if (supabaseConfigured()) {
    return storeVerificationRunInSupabase(normalizedRun);
  }

  const storedRun = withDocumentIdentity(normalizedRun, `local-${Date.now()}-${randomUUID().slice(0, 8)}`);
  const runs = await readStore();
  const nextRuns = [storedRun, ...runs]
    .sort((left, right) => Date.parse(right.received_at) - Date.parse(left.received_at))
    .slice(0, maxStoredRuns);

  await writeStore(nextRuns);
  return storedRun;
}

async function listVerificationRunsFromSupabase(limit: number) {
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.min(limit, maxStoredRuns)) : 160;
  const url = new URL(tableUrl());
  url.searchParams.set("select", "*");
  url.searchParams.set("section", `eq.${notificationSection}`);
  url.searchParams.set("order", "created_at.desc");
  url.searchParams.set("limit", String(normalizedLimit));

  const response = await fetch(url, {
    headers: supabaseHeaders(),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase read failed (${response.status}): ${await response.text()}`);
  }

  const rows = (await response.json()) as SupabaseNotificationRow[];
  return rows.map(notificationRowToRun).filter(Boolean) as StoredVerificationRun[];
}

async function storeVerificationRunInSupabase(run: StoredVerificationRun) {
  const notification = verificationRunToNotification(run);
  const response = await fetch(tableUrl(), {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "return=representation",
    },
    body: JSON.stringify(notification),
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`Supabase insert failed (${response.status}): ${await response.text()}`);
  }

  const rows = (await response.json()) as SupabaseNotificationRow[];
  return notificationRowToRun(rows[0]) || run;
}

function verificationRunToNotification(run: StoredVerificationRun) {
  return {
    section: notificationSection,
    event: `abrechnung_verification_${run.status}`,
    status: run.status,
    title: verificationTitle(run),
    message: verificationMessage(run),
    filename: run.invoice_file,
    upload_type: notificationUploadType,
    bucket: null,
    path: null,
    size_bytes: null,
    error: run.status === "failure" ? verificationMessage(run) : null,
    source: run.source,
    payload: {
      ...run.raw,
      document_kind: "abrechnung_verification_run",
      original_run_id: run.id,
      verification_run: run,
    },
    created_at: run.received_at,
  };
}

function notificationRowToRun(row: SupabaseNotificationRow | undefined): StoredVerificationRun | null {
  if (!row) {
    return null;
  }

  try {
    const payload = recordValue(row.payload) || {};
    const storedRun = recordValue(payload.verification_run);
    const run = storedRun && isStoredRun(storedRun) ? storedRun : normalizeRun(payload);

    return withDocumentIdentity(
      {
        ...run,
        status: normalizeStatus(row.status, run.problem_count),
        source: stringValue(row.source) || run.source,
        invoice_file: stringValue(row.filename) || run.invoice_file,
        received_at: validDateString(row.created_at) || run.received_at,
        raw: {
          ...run.raw,
          notification_id: row.id,
          notification_event: row.event,
          notification_created_at: row.created_at,
          original_run_id: stringValue(payload.original_run_id) || run.id,
        },
      },
      row.id
    );
  } catch {
    return null;
  }
}

function withDocumentIdentity(run: StoredVerificationRun, documentId: string): StoredVerificationRun {
  return {
    ...run,
    id: documentId,
    raw: {
      ...run.raw,
      original_run_id: stringValue(run.raw.original_run_id) || run.id,
      document_id: documentId,
    },
  };
}

function verificationTitle(run: StoredVerificationRun) {
  if (run.status === "success") {
    return "Abrechnung Verification erfolgreich";
  }

  if (run.status === "warning") {
    return "Abrechnung Verification pruefen";
  }

  return "Abrechnung Verification fehlgeschlagen";
}

function verificationMessage(run: StoredVerificationRun) {
  const file = run.invoice_file ? ` fuer ${run.invoice_file}` : "";

  if (run.status === "success") {
    return `${run.success_count} Orders${file} erfolgreich geprueft.`;
  }

  return `${run.problem_count} Problem(e) bei ${run.success_count + run.problem_count} geprueften Orders${file}.`;
}

function normalizeRun(rawPayload: unknown): StoredVerificationRun {
  const payload = extractVerificationPayload(rawPayload);
  const problems = recordArray(payload.problems).map(normalizeProblem);
  const successIds = stringArray(payload.success_ids);
  const problemCount = numberValue(payload.problem_count, problems.length);
  const successCount = numberValue(payload.success_count, successIds.length);
  const timestamp =
    validDateString(payload.timestamp) ||
    validDateString(payload.finished_at) ||
    validDateString(payload.checked_at) ||
    validDateString(payload.created_at);

  return {
    id:
      stringValue(payload.run_id) ||
      stringValue(payload.id) ||
      stringValue(payload.verification_id) ||
      `abrechnung-${Date.now()}-${randomUUID().slice(0, 8)}`,
    status: normalizeStatus(payload.status, problemCount),
    source: stringValue(payload.source) || stringValue(payload.service) || "abrechnung-bot",
    bot_name: stringValue(payload.bot_name) || stringValue(payload.bot) || "Abrechnung Bot",
    received_at: new Date().toISOString(),
    finished_at: timestamp,
    billing_period_from: stringValue(payload.billing_period_from) || null,
    billing_period_to: stringValue(payload.billing_period_to) || null,
    invoice_file:
      stringValue(payload.invoice_file) ||
      stringValue(payload.abrechnung_file) ||
      stringValue(payload.filename) ||
      null,
    success_count: successCount,
    success_ids: successIds,
    problem_count: problemCount,
    problems,
    raw: payload,
  };
}

function normalizeProblem(row: Record<string, unknown>, index: number): VerificationProblem {
  const problemType = stringValue(row.problem_type) || stringValue(row.type) || "unknown_problem";
  const orderReference =
    stringValue(row.order_reference) ||
    stringValue(row.order_id) ||
    stringValue(row.reference) ||
    `Problem ${index + 1}`;

  return {
    id: stringValue(row.id) || `${orderReference}-${problemType}-${index}`,
    problem_type: problemType,
    order_reference: orderReference,
    billing_id: stringValue(row.billing_id) || null,
    line_no: stringValue(row.line_no) || null,
    order_type: stringValue(row.order_type) || null,
    billing_date: validDateString(row.billing_date) || stringValue(row.billing_date) || null,
    billing_type: stringValue(row.billing_type) || null,
    pzn: stringValue(row.pzn) || null,
    product_name: stringValue(row.product_name) || null,
    expected_value: row.expected_value ?? row.expected ?? row.excel_value ?? null,
    actual_value: row.actual_value ?? row.actual ?? row.bot_value ?? row.db_value ?? null,
    problem: stringValue(row.problem) || stringValue(row.message) || "Abweichung in der Abrechnung gefunden.",
    severity: severityForProblem(problemType),
    raw: row,
  };
}

function normalizeStatus(value: unknown, problemCount: number): VerificationStatus {
  const status = stringValue(value).toLowerCase();

  if (status === "success" && problemCount === 0) {
    return "success";
  }

  if (status === "warning") {
    return "warning";
  }

  if (status === "success" && problemCount > 0) {
    return "warning";
  }

  return problemCount > 0 ? "failure" : "success";
}

function severityForProblem(problemType: string): ProblemSeverity {
  if (["missing_order", "unexpected_order", "duplicate_order", "billing_missing"].includes(problemType)) {
    return "critical";
  }

  if (
    [
      "billing_total_mismatch",
      "pzn_mismatch",
      "product_pzn_mismatch",
      "quantity_mismatch",
      "price_mismatch",
      "return_mismatch",
      "total_mismatch",
    ].includes(problemType)
  ) {
    return "high";
  }

  return "medium";
}

function extractVerificationPayload(value: unknown): Record<string, unknown> {
  const extracted = extractVerificationPayloadCandidate(value, 0);

  if (!extracted) {
    throw new Error("verification_payload_not_found");
  }

  return extracted;
}

function extractVerificationPayloadCandidate(value: unknown, depth: number): Record<string, unknown> | null {
  if (depth > 6) {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const extracted = extractVerificationPayloadCandidate(item, depth + 1);
      if (extracted) {
        return extracted;
      }
    }

    return null;
  }

  const record = recordValue(value);

  if (!record) {
    return null;
  }

  if (looksLikeVerificationPayload(record)) {
    const nestedResult = recordValue(record.result);
    if (nestedResult && looksLikeVerificationPayload(nestedResult)) {
      return withParentMetadata(nestedResult, record);
    }

    return record;
  }

  for (const key of ["body", "json", "data", "payload", "result"]) {
    const nested = record[key];
    const extracted = extractVerificationPayloadCandidate(nested, depth + 1);

    if (extracted) {
      return withParentMetadata(extracted, record);
    }
  }

  return null;
}

function looksLikeVerificationPayload(record: Record<string, unknown>) {
  return (
    Array.isArray(record.problems) ||
    Array.isArray(record.success_ids) ||
    record.problem_count !== undefined ||
    record.success_count !== undefined ||
    (record.status !== undefined && record.checked_at !== undefined)
  );
}

function withParentMetadata(payload: Record<string, unknown>, parent: Record<string, unknown>) {
  return {
    ...payload,
    checked_at: payload.checked_at ?? parent.checked_at,
    timestamp: payload.timestamp ?? parent.timestamp ?? parent.checked_at,
    source: payload.source ?? parent.source,
    service: payload.service ?? parent.service,
    bot_name: payload.bot_name ?? parent.bot_name,
    invoice_file: payload.invoice_file ?? parent.invoice_file ?? parent.filename,
    filename: payload.filename ?? parent.filename,
    execution_mode: payload.execution_mode ?? parent.executionMode,
  };
}

function isStoredRun(value: unknown): value is StoredVerificationRun {
  const row = recordValue(value);
  return Boolean(row && stringValue(row.id) && stringValue(row.received_at) && Array.isArray(row.problems));
}

function recordValue(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function recordArray(value: unknown) {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => Boolean(recordValue(item))) : [];
}

function stringArray(value: unknown) {
  return Array.isArray(value)
    ? value
        .map((item) => stringValue(item))
        .filter(Boolean)
    : [];
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return "";
}

function numberValue(value: unknown, fallback = 0) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  return fallback;
}

function validDateString(value: unknown) {
  const text = stringValue(value);
  if (!text) {
    return null;
  }

  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function isNodeError(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error && "code" in error;
}
