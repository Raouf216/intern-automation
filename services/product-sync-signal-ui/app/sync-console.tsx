"use client";

import {
  AlertTriangle,
  ArrowRight,
  CalendarCheck,
  CheckCircle2,
  Clock3,
  DatabaseZap,
  FileSpreadsheet,
  KeyRound,
  Loader2,
  LockKeyhole,
  Moon,
  RadioTower,
  RefreshCw,
  ShieldCheck,
  Sparkles,
  Sun,
} from "lucide-react";
import { useEffect, useMemo, useState, type FormEvent } from "react";

type Product = {
  product_name?: string;
  pzn?: string;
  strain?: string;
  quantity?: number;
  price_per_g_incl_vat?: number;
  additional_cost?: number;
  site_price?: number;
  availability?: boolean;
};

type ChangedProduct = {
  pzn?: string;
  product_name?: string;
  changes?: Record<string, { old: unknown; new: unknown }>;
  before?: Product;
  after?: Product;
};

type SyncResponse = {
  ok?: boolean;
  error?: string;
  scraped?: number;
  inserted?: number;
  updated?: number;
  unchanged?: number;
  sent_to_supabase?: number;
  reused_session?: boolean;
  new_products?: Product[];
  changed_products?: ChangedProduct[];
};

type ProductChangeExport = {
  filename: string;
  rowCount: number;
  generatedAt: string;
};

type EndOfDayResponse = {
  ok?: boolean;
  error?: string;
  current_url?: string;
  page_title?: string;
  reused_session?: boolean;
  session_state_path?: string;
  scraped?: number;
  saved?: number;
  sent_to_supabase?: number;
  targets?: Array<{
    order_type?: string;
    scraped?: number;
    valid_rows?: number;
    ready_for_customer_clicked?: boolean;
  }>;
  export?: {
    downloaded?: boolean;
    skipped?: boolean;
    sent_to_n8n?: boolean;
    download_filename?: string;
    excel_row_count?: number | null;
  };
  wait_result?: {
    stable?: boolean;
    final_snapshot?: {
      textLength?: number;
      tableRows?: number;
      buttons?: number;
      visibleLoaderCount?: number;
    };
  };
};

type EndOfDayHealthResponse = {
  ok?: boolean;
  service?: string;
  eod_sync_running?: boolean;
  eod_sync_started_at?: string | null;
};

type SyncNotification = {
  event: "doktorabc_sync_success" | "doktorabc_sync_failure";
  status: "success" | "failure";
  section: "doktorabc_sync";
  sync_type: "doktorabc_products";
  service: "product-sync-signal-ui";
  timestamp: string;
  started_at: string | null;
  finished_at: string;
  duration_ms: number | null;
  endpoint: string;
  summary: {
    scraped: number;
    inserted: number;
    updated: number;
    unchanged: number;
    sent_to_supabase: number;
  };
  logs: SyncResponse | null;
  error?: string;
};

type OperatorSessionResponse = {
  ok?: boolean;
  error?: string;
};

function legacyEndOfDayOrdersEndpoint(value: string) {
  return value
    .trim()
    .replace(/\/jobs\/end-of-day\/(?:login|session-check)\/?$/i, "/jobs/end-of-day/orders/sync");
}

const configuredEndpoint = process.env.NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT || "";
const configuredEndOfDayEndpoint =
  process.env.NEXT_PUBLIC_EOD_ORDERS_ENDPOINT ||
  legacyEndOfDayOrdersEndpoint(process.env.NEXT_PUBLIC_EOD_LOGIN_ENDPOINT || "");
const fallbackEndpoint = "http://178.104.144.30:8020/jobs/product-prices";
const fallbackEndOfDayEndpoint = "http://178.104.144.30:8021/jobs/end-of-day/orders/sync";
const syncEndpoint = configuredEndpoint || fallbackEndpoint;
const endOfDayEndpoint = configuredEndOfDayEndpoint || fallbackEndOfDayEndpoint;
const endOfDayHealthEndpoint = healthEndpointFor(endOfDayEndpoint);
const staffSteps = [
  {
    before: "Alle Produkte, bei denen Informationen geändert werden, zuerst in DoktorABC auf ",
    emphasis: "NICHT VERFÜGBAR (unavailable)",
    after: " setzen.",
  },
  {
    before: "Die Seite aktualisieren und sicherstellen, dass diese Produkte weiterhin ",
    emphasis: "nicht verfügbar (unavailable)",
    after: " sind.",
  },
  {
    before: "Auf „Produkte synchronisieren (DoktorABC)“ klicken.",
  },
  {
    emphasis: "Warten",
    after: ". Der Vorgang kann bis zu 5 Minuten dauern. Wenn er abstürzt, Raouf kontaktieren.",
  },
  {
    before: "Nach Abschluss die Änderungen unten lesen. Auf dem Handy ggf. nach unten scrollen.",
  },
  {
    before: "Wenn alles übereinstimmt, die Produkte in DoktorABC wieder ",
    emphasis: "verfügbar (available)",
    after: " machen.",
  },
  {
    before: "Fertig.",
  },
];
const endOfDaySteps = [
  "Starten Sie zuerst den End-of-Day-Lauf hier im Dashboard.",
  "Warten Sie auf die Antwort des Bots. Richtwert: ca. 1 Minute je 100 Orders.",
];

function healthEndpointFor(value: string) {
  try {
    const url = new URL(value);
    return `${url.origin}/health`;
  } catch {
    return "";
  }
}

function isFetchFailureMessage(value: string) {
  const normalized = value.toLowerCase();
  return normalized.includes("failed to fetch") || normalized.includes("networkerror");
}

function wait(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function fetchEndOfDayHealth() {
  if (!endOfDayHealthEndpoint) return null;

  const response = await fetch(endOfDayHealthEndpoint, {
    method: "GET",
    cache: "no-store",
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? ((await response.json()) as EndOfDayHealthResponse)
    : ({ ok: false } satisfies EndOfDayHealthResponse);

  return response.ok && payload.ok ? payload : null;
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function productQuantity(product: Product | undefined) {
  return product?.quantity ?? "leer";
}

function formatChangeValue(value: unknown) {
  if (value === null || value === undefined || value === "") return "leer";
  if (value === true) return "true";
  if (value === false) return "false";
  return String(value);
}

function germanyFilenameTimestamp(value: Date) {
  const parts = new Intl.DateTimeFormat("de-DE", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(value);
  const part = (type: string) => parts.find((item) => item.type === type)?.value || "00";

  return `${part("year")}-${part("month")}-${part("day")}_${part("hour")}-${part("minute")}-${part("second")}`;
}

function exportDisplayTime(value: string) {
  return new Intl.DateTimeFormat("de-DE", {
    timeZone: "Europe/Berlin",
    dateStyle: "short",
    timeStyle: "medium",
  }).format(new Date(value));
}

function excelCell(value: unknown) {
  return formatChangeValue(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function productValue(product: Product | undefined, field: keyof Product) {
  return product?.[field];
}

const productFieldLabels: Record<string, string> = {
  product_name: "Produktname",
  pzn: "PZN",
  strain: "Sorte",
  quantity: "Menge",
  price_per_g_incl_vat: "Preis/g inkl. MwSt.",
  additional_cost: "Zusatzkosten",
  site_price: "Seitenpreis",
  availability: "Verfügbarkeit",
};

const productExportColumns = [
  "Änderung",
  "Produktname",
  "PZN",
  "Feld",
  "Alter Wert",
  "Neuer Wert",
  "Sorte vorher",
  "Sorte nachher",
  "Menge vorher",
  "Menge nachher",
  "Preis/g vorher",
  "Preis/g nachher",
  "Zusatzkosten vorher",
  "Zusatzkosten nachher",
  "Seitenpreis vorher",
  "Seitenpreis nachher",
  "Verfügbarkeit vorher",
  "Verfügbarkeit nachher",
];

function productExportRow(kind: string, productName: unknown, pzn: unknown, field: string, oldValue: unknown, newValue: unknown, before?: Product, after?: Product) {
  return [
    kind,
    productName,
    pzn,
    field,
    oldValue,
    newValue,
    productValue(before, "strain"),
    productValue(after, "strain"),
    productValue(before, "quantity"),
    productValue(after, "quantity"),
    productValue(before, "price_per_g_incl_vat"),
    productValue(after, "price_per_g_incl_vat"),
    productValue(before, "additional_cost"),
    productValue(after, "additional_cost"),
    productValue(before, "site_price"),
    productValue(after, "site_price"),
    productValue(before, "availability"),
    productValue(after, "availability"),
  ];
}

function productChangeRows(payload: SyncResponse) {
  const rows: unknown[][] = [];

  for (const product of payload.new_products || []) {
    rows.push(productExportRow("Neu", product.product_name, product.pzn, "Neues Produkt", "", "neu", undefined, product));
  }

  for (const product of payload.changed_products || []) {
    const changes = Object.entries(product.changes || {});
    const before = product.before;
    const after = product.after;
    const productName = product.product_name || after?.product_name || before?.product_name;
    const pzn = product.pzn || after?.pzn || before?.pzn;

    if (!changes.length) {
      rows.push(productExportRow("Geändert", productName, pzn, "Geändert", "", "", before, after));
      continue;
    }

    for (const [field, change] of changes) {
      rows.push(
        productExportRow(
          "Geändert",
          productName,
          pzn,
          productFieldLabels[field] || field,
          change.old,
          change.new,
          before,
          after
        )
      );
    }
  }

  return rows;
}

function productChangesExcelHtml(payload: SyncResponse, rows: unknown[][], generatedAt: Date) {
  const summary = syncSummary(payload);
  const generatedAtLabel = new Intl.DateTimeFormat("de-DE", {
    timeZone: "Europe/Berlin",
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(generatedAt);

  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body { font-family: Arial, sans-serif; }
    h1 { font-size: 18px; }
    table { border-collapse: collapse; }
    th, td { border: 1px solid #999; padding: 6px 8px; mso-number-format:"\\@"; }
    th { background: #e8eef8; font-weight: bold; }
  </style>
</head>
<body>
  <h1>DoktorABC Produktänderungen</h1>
  <table>
    <tbody>
      <tr><th>Erstellt</th><td>${excelCell(generatedAtLabel)}</td></tr>
      <tr><th>Gescrapt</th><td>${excelCell(summary.scraped)}</td></tr>
      <tr><th>Neu</th><td>${excelCell(summary.inserted)}</td></tr>
      <tr><th>Geändert</th><td>${excelCell(summary.updated)}</td></tr>
      <tr><th>Unverändert</th><td>${excelCell(summary.unchanged)}</td></tr>
      <tr><th>An Supabase gesendet</th><td>${excelCell(summary.sent_to_supabase)}</td></tr>
    </tbody>
  </table>
  <br />
  <table>
    <thead>
      <tr>${productExportColumns.map((column) => `<th>${excelCell(column)}</th>`).join("")}</tr>
    </thead>
    <tbody>
      ${rows.map((row) => `<tr>${row.map((cell) => `<td>${excelCell(cell)}</td>`).join("")}</tr>`).join("")}
    </tbody>
  </table>
</body>
</html>`;
}

function downloadProductChangesExcel(payload: SyncResponse, generatedAt = new Date()): ProductChangeExport | null {
  const rows = productChangeRows(payload);

  if (!rows.length) return null;

  const filename = `doktorabc-produkt-aenderungen_${germanyFilenameTimestamp(generatedAt)}.xls`;
  const html = productChangesExcelHtml(payload, rows, generatedAt);
  const blob = new Blob(["\ufeff", html], { type: "application/vnd.ms-excel;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 30_000);

  return {
    filename,
    rowCount: rows.length,
    generatedAt: generatedAt.toISOString(),
  };
}

function syncSummary(payload: SyncResponse | null) {
  return {
    scraped: numberValue(payload?.scraped),
    inserted: numberValue(payload?.inserted),
    updated: numberValue(payload?.updated),
    unchanged: numberValue(payload?.unchanged),
    sent_to_supabase: numberValue(payload?.sent_to_supabase),
  };
}

function botSavedCount(payload: EndOfDayResponse | null) {
  return numberValue(payload?.sent_to_supabase ?? payload?.saved ?? payload?.scraped);
}

function authErrorMessage(value?: string) {
  if (value === "operator_password_invalid") return "Passwort ist falsch.";
  if (value === "operator_password_not_configured") return "Das Bedienerpasswort ist nicht konfiguriert.";
  if (value === "operator_session_invalid") return "Die Sitzung ist abgelaufen. Bitte erneut freischalten.";

  return value || "Zugang konnte nicht geprüft werden.";
}

function exportState(payload: EndOfDayResponse | null) {
  if (!payload?.export) return "noch nicht";
  if (payload.export.skipped) return "übersprungen";
  if (payload.export.sent_to_n8n) return "gesendet";
  if (payload.export.downloaded) return "geladen";
  return "noch nicht";
}

function DoktorabcLogo() {
  return (
    <h1 className="doktorabc-logo-card" aria-label="DoktorABC Pharmacies">
      <img className="doktorabc-logo-image doktorabc-logo-light" src="/pharmacies-logo-light.png" alt="" aria-hidden="true" width={198} height={66} />
      <img className="doktorabc-logo-image doktorabc-logo-night" src="/pharmacies-logo-night.png" alt="" aria-hidden="true" width={198} height={66} />
    </h1>
  );
}

async function sendFinalSyncNotification(notification: SyncNotification) {
  try {
    const response = await fetch("/api/sync-notification", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(notification),
    });

    if (!response.ok) {
      console.warn("Product sync notification endpoint failed", await response.text());
    }
  } catch (error) {
    console.warn("Could not send product sync notification", error);
  }
}

async function requestOperatorSession(passwordForUnlock?: string) {
  const response = await fetch("/api/operator-session", {
    method: passwordForUnlock ? "POST" : "GET",
    headers: passwordForUnlock ? { "Content-Type": "application/json" } : undefined,
    body: passwordForUnlock ? JSON.stringify({ operator_password: passwordForUnlock }) : undefined,
    cache: "no-store",
    credentials: "same-origin",
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? ((await response.json()) as OperatorSessionResponse)
    : ({ ok: false, error: await response.text() } satisfies OperatorSessionResponse);

  if (!response.ok || !payload.ok) {
    throw new Error(payload.error || `HTTP-Fehler ${response.status}: ${response.statusText || "keine Details"}`);
  }

  return payload;
}

export function SyncConsole() {
  const [passwordInput, setPasswordInput] = useState("");
  const [isUnlocked, setIsUnlocked] = useState(false);
  const [isSessionChecking, setIsSessionChecking] = useState(true);
  const [isUnlocking, setIsUnlocking] = useState(false);
  const [unlockStatus, setUnlockStatus] = useState<"idle" | "success" | "error">("idle");
  const [unlockMessage, setUnlockMessage] = useState("Bedienerpasswort eingeben, um EOD und Produktsync freizuschalten.");
  const [isRunning, setIsRunning] = useState(false);
  const [isEndOfDayRunning, setIsEndOfDayRunning] = useState(false);
  const [status, setStatus] = useState<"idle" | "success" | "error">("idle");
  const [endOfDayStatus, setEndOfDayStatus] = useState<"idle" | "success" | "error">("idle");
  const [message, setMessage] = useState("Bereit für eine kontrollierte Produktsynchronisierung.");
  const [endOfDayMessage, setEndOfDayMessage] = useState("Bereit für End-of-Day Bestellungen und Excel-Export.");
  const [result, setResult] = useState<SyncResponse | null>(null);
  const [endOfDayResult, setEndOfDayResult] = useState<EndOfDayResponse | null>(null);
  const [productExport, setProductExport] = useState<ProductChangeExport | null>(null);
  const [startedAt, setStartedAt] = useState<Date | null>(null);
  const [finishedAt, setFinishedAt] = useState<Date | null>(null);
  const [endOfDayStartedAt, setEndOfDayStartedAt] = useState<Date | null>(null);
  const [endOfDayFinishedAt, setEndOfDayFinishedAt] = useState<Date | null>(null);
  const [theme, setTheme] = useState<"light" | "night">("light");

  useEffect(() => {
    const storedTheme = window.localStorage.getItem("product-sync-signal-theme");
    const nextTheme = storedTheme === "night" ? "night" : "light";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;

    void restoreSession();
  }, []);

  function toggleTheme() {
    const nextTheme = theme === "night" ? "light" : "night";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    window.localStorage.setItem("product-sync-signal-theme", nextTheme);
  }

  const metrics = useMemo(
    () => [
      { label: "Geprüfte Produkte", value: numberValue(result?.scraped) },
      { label: "Neue Produkte", value: numberValue(result?.inserted) },
      { label: "Geänderte Zeilen", value: numberValue(result?.updated) },
      { label: "Unverändert", value: numberValue(result?.unchanged) },
    ],
    [result]
  );

  async function restoreSession() {
    try {
      await requestOperatorSession();
      setIsUnlocked(true);
      setUnlockStatus("success");
      setUnlockMessage("Bestehender Zugang wurde geladen.");
    } catch {
      // No valid browser session yet; keep the password gate visible.
    } finally {
      setIsSessionChecking(false);
    }
  }

  function requireUnlocked(target: "products" | "eod") {
    if (isUnlocked) return true;

    const messageText = "Bitte zuerst den Zugang freischalten.";
    setUnlockStatus("error");
    setUnlockMessage(messageText);
    if (target === "products") {
      setStatus("error");
      setMessage(messageText);
    } else {
      setEndOfDayStatus("error");
      setEndOfDayMessage(messageText);
    }

    return false;
  }

  function exportProductChanges(payload: SyncResponse, generatedAt = new Date()) {
    try {
      setProductExport(downloadProductChangesExcel(payload, generatedAt));
    } catch (error) {
      console.warn("Could not create product change Excel export", error);
      setProductExport(null);
    }
  }

  async function unlockConsole(event?: FormEvent<HTMLFormElement>) {
    event?.preventDefault();
    const nextPassword = passwordInput.trim();

    if (!nextPassword) {
      setUnlockStatus("error");
      setUnlockMessage("Bitte das Bedienerpasswort eingeben.");
      return;
    }

    setIsUnlocking(true);
    setUnlockStatus("idle");
    setUnlockMessage("Zugang wird geprüft.");

    try {
      await requestOperatorSession(nextPassword);
      setPasswordInput("");
      setIsUnlocked(true);
      setUnlockStatus("success");
      setUnlockMessage("Zugang freigeschaltet.");
      setStatus("idle");
      setEndOfDayStatus("idle");
      setMessage("Bereit für eine kontrollierte Produktsynchronisierung.");
      setEndOfDayMessage("Bereit für End-of-Day Bestellungen und Excel-Export.");
    } catch (error) {
      setIsUnlocked(false);
      setUnlockStatus("error");
      setUnlockMessage(authErrorMessage(error instanceof Error ? error.message : ""));
    } finally {
      setIsUnlocking(false);
    }
  }

  async function triggerSync() {
    if (!requireUnlocked("products")) return;

    if (!syncEndpoint.trim()) {
      setStatus("error");
      setMessage("Der feste DoktorABC Sync-Endpunkt ist nicht konfiguriert.");
      return;
    }

    setIsRunning(true);
    setStatus("idle");
    setMessage("Synchronisierung läuft. Bitte warten, der Vorgang kann bis zu 5 Minuten dauern.");
    setResult(null);
    setProductExport(null);
    const runStartedAt = new Date();
    setStartedAt(runStartedAt);
    setFinishedAt(null);
    let finalPayload: SyncResponse | null = null;

    try {
      const response = await fetch(syncEndpoint.trim(), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: "{}",
      });
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json")
        ? ((await response.json()) as SyncResponse)
        : ({ ok: false, error: await response.text() } satisfies SyncResponse);

      finalPayload = payload;
      setResult(payload);

      if (!response.ok || !payload.ok) {
        throw new Error(
          payload.error
            ? `Bot-Fehler: ${payload.error}`
            : `HTTP-Fehler ${response.status}: ${response.statusText || "keine Details"}`
        );
      }

      setStatus("success");
      setMessage(
        `Erfolgreich abgeschlossen: ${payload.inserted || 0} neu, ${payload.updated || 0} geändert, ${
          payload.unchanged || 0
        } unverändert.`
      );
      const runFinishedAt = new Date();
      setFinishedAt(runFinishedAt);
      exportProductChanges(payload, runFinishedAt);
      await sendFinalSyncNotification({
        event: "doktorabc_sync_success",
        status: "success",
        section: "doktorabc_sync",
        sync_type: "doktorabc_products",
        service: "product-sync-signal-ui",
        timestamp: runFinishedAt.toISOString(),
        started_at: runStartedAt.toISOString(),
        finished_at: runFinishedAt.toISOString(),
        duration_ms: runFinishedAt.getTime() - runStartedAt.getTime(),
        endpoint: syncEndpoint.trim(),
        summary: syncSummary(payload),
        logs: payload,
      });
    } catch (error) {
      setStatus("error");
      const errorMessage = error instanceof Error ? error.message : "Unbekannter Fehler";
      const isFetchFailure =
        errorMessage.toLowerCase().includes("failed to fetch") ||
        errorMessage.toLowerCase().includes("networkerror");

      setMessage(
        isFetchFailure
          ? `Netzwerk- oder CORS-Fehler: Der Browser konnte den DoktorABC Sync-Bot unter ${syncEndpoint} nicht erreichen oder die Antwort nicht lesen.`
          : `Sync-Anfrage fehlgeschlagen: ${errorMessage}`
      );
      const runFinishedAt = new Date();
      setFinishedAt(runFinishedAt);
      await sendFinalSyncNotification({
        event: "doktorabc_sync_failure",
        status: "failure",
        section: "doktorabc_sync",
        sync_type: "doktorabc_products",
        service: "product-sync-signal-ui",
        timestamp: runFinishedAt.toISOString(),
        started_at: runStartedAt.toISOString(),
        finished_at: runFinishedAt.toISOString(),
        duration_ms: runFinishedAt.getTime() - runStartedAt.getTime(),
        endpoint: syncEndpoint.trim(),
        summary: syncSummary(finalPayload),
        logs: finalPayload,
        error: isFetchFailure
          ? `Netzwerk- oder CORS-Fehler: Der Browser konnte den DoktorABC Sync-Bot unter ${syncEndpoint} nicht erreichen oder die Antwort nicht lesen.`
          : `Sync-Anfrage fehlgeschlagen: ${errorMessage}`,
      });
    } finally {
      setIsRunning(false);
    }
  }

  async function triggerEndOfDayOrders() {
    if (!requireUnlocked("eod")) return;

    if (!endOfDayEndpoint.trim()) {
      setEndOfDayStatus("error");
      setEndOfDayMessage("Der feste End-of-Day Bot-Endpunkt ist nicht konfiguriert.");
      return;
    }

    setIsEndOfDayRunning(true);
    setEndOfDayStatus("idle");
    setEndOfDayMessage("End-of-Day läuft. Der Bot synchronisiert die EOD-Bestellungen und exportiert die Excel-Datei.");
    setEndOfDayResult(null);
    const runStartedAt = new Date();
    setEndOfDayStartedAt(runStartedAt);
    setEndOfDayFinishedAt(null);
    let keepEndOfDayRunning = false;

    try {
      const response = await fetch(endOfDayEndpoint.trim(), {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: "{}",
      });
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json")
        ? ((await response.json()) as EndOfDayResponse)
        : ({ ok: false, error: await response.text() } satisfies EndOfDayResponse);

      setEndOfDayResult(payload);

      if (!response.ok || !payload.ok) {
        throw new Error(
          payload.error
            ? `Bot-Fehler: ${payload.error}`
            : `HTTP-Fehler ${response.status}: ${response.statusText || "keine Details"}`
        );
      }

      setEndOfDayStatus("success");
      setEndOfDayMessage(`End-of-Day abgeschlossen: ${botSavedCount(payload)} Bestellung(en) gespeichert, Export ${exportState(payload)}.`);
      setEndOfDayFinishedAt(new Date());
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unbekannter Fehler";
      const isFetchFailure = isFetchFailureMessage(errorMessage);

      if (isFetchFailure) {
        try {
          const health = await fetchEndOfDayHealth();

          if (health?.eod_sync_running) {
            setEndOfDayStatus("idle");
            setEndOfDayMessage(
              "Die Browser-Verbindung zur Antwort wurde unterbrochen, aber der End-of-Day Bot läuft weiter. Bitte nicht erneut klicken; ich warte auf den Abschluss."
            );
            keepEndOfDayRunning = true;

            for (let attempt = 1; attempt <= 540; attempt += 1) {
              await wait(5_000);
              let nextHealth: EndOfDayHealthResponse | null = null;

              try {
                nextHealth = await fetchEndOfDayHealth();
              } catch {
                nextHealth = null;
              }

              if (nextHealth && !nextHealth.eod_sync_running) {
                keepEndOfDayRunning = false;
                setEndOfDayStatus("success");
                setEndOfDayMessage(
                  "End-of-Day Bot ist fertig. Die Browser-Verbindung zur direkten Antwort war unterbrochen; bitte Ergebnis in der Notification App prüfen."
                );
                setEndOfDayFinishedAt(new Date());
                return;
              }

              if (attempt % 12 === 0) {
                setEndOfDayMessage(
                  "End-of-Day Bot läuft weiter. Browser-Verbindung zur direkten Antwort war unterbrochen; ich prüfe den Abschluss weiter."
                );
              }
            }

            keepEndOfDayRunning = false;
            setEndOfDayStatus("error");
            setEndOfDayMessage(
              "Die Browser-Verbindung war unterbrochen und der Bot meldet nach langer Wartezeit noch keinen Abschluss. Bitte /health und die Notification App prüfen."
            );
            setEndOfDayFinishedAt(new Date());
            return;
          }
        } catch {
          // If the health probe is also blocked, show the original network/CORS message below.
        }
      }

      setEndOfDayStatus("error");
      setEndOfDayMessage(
        isFetchFailure
          ? `Netzwerk- oder CORS-Fehler: Der Browser konnte den End-of-Day Bot unter ${endOfDayEndpoint} nicht erreichen oder die Antwort nicht lesen.`
          : `End-of-Day Anfrage fehlgeschlagen: ${errorMessage}`
      );
      setEndOfDayFinishedAt(new Date());
    } finally {
      setIsEndOfDayRunning(keepEndOfDayRunning);
    }
  }

  const anyBotRunning = isRunning || isEndOfDayRunning;
  const anyBotError =
    status === "error" || endOfDayStatus === "error";
  const anyBotSuccess =
    status === "success" || endOfDayStatus === "success";

  if (isSessionChecking) {
    return (
      <main className="page auth-page">
        <section className="auth-gate" aria-label="EOD und Produktsync Sitzung">
          <div className="auth-card">
            <DoktorabcLogo />
            <div>
              <p className="section-kicker">EOD & Produktsync</p>
              <h2>Sitzung wird geprüft</h2>
            </div>
            <div className="auth-message">
              <Loader2 size={18} className="spin" />
              <p>Bestehender Zugang wird geladen.</p>
            </div>
          </div>
        </section>
      </main>
    );
  }

  if (!isUnlocked) {
    return (
      <main className="page auth-page">
        <section className="auth-gate" aria-label="EOD und Produktsync Zugang">
          <div className="auth-card">
            <DoktorabcLogo />
            <div>
              <p className="section-kicker">EOD & Produktsync</p>
              <h2>Zugang freischalten</h2>
            </div>
            <form className="auth-form" onSubmit={unlockConsole}>
              <label className="field">
                <span>Bedienerpasswort</span>
                <div className="password-row">
                  <KeyRound size={18} />
                  <input
                    value={passwordInput}
                    onChange={(event) => setPasswordInput(event.target.value)}
                    type="password"
                    placeholder="Passwort eingeben"
                    autoComplete="current-password"
                    autoFocus
                  />
                </div>
              </label>
              <button className="unlock-button" type="submit" disabled={isUnlocking}>
                {isUnlocking ? <Loader2 size={18} className="spin" /> : <ShieldCheck size={18} />}
                <span>{isUnlocking ? "Prüfe Zugang" : "Zugang prüfen"}</span>
              </button>
            </form>
            <div className={`auth-message auth-message-${unlockStatus}`}>
              {unlockStatus === "success" ? <CheckCircle2 size={18} /> : unlockStatus === "error" ? <AlertTriangle size={18} /> : <LockKeyhole size={18} />}
              <p>{unlockMessage}</p>
            </div>
            <button className="theme-button auth-theme-button" type="button" onClick={toggleTheme} aria-label="Darstellung wechseln">
              {theme === "night" ? <Sun size={17} /> : <Moon size={17} />}
              <span>{theme === "night" ? "Hell" : "Nacht"}</span>
            </button>
          </div>
        </section>
      </main>
    );
  }

  const productStatusPanel = (
    <aside className="status-surface action-status-panel" aria-label="Produktsync Ergebnis">
      <div className="surface-heading compact">
        <div>
          <p className="section-kicker">Live-Ergebnis</p>
          <h2>Laufübersicht</h2>
        </div>
        {status === "success" ? <CheckCircle2 size={25} /> : status === "error" ? <AlertTriangle size={25} /> : <Sparkles size={25} />}
      </div>

      <div className={`message message-${isRunning ? "running" : status}`}>
        {isRunning ? <Loader2 size={18} className="spin" /> : status === "success" ? <CheckCircle2 size={18} /> : status === "error" ? <AlertTriangle size={18} /> : <ShieldCheck size={18} />}
        <p>{message}</p>
      </div>

      <div className="metric-grid">
        {metrics.map((metric) => (
          <div className="metric" key={metric.label}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
          </div>
        ))}
      </div>

      <div className="timeline">
        <div>
          <Clock3 size={15} />
          <span>Gestartet</span>
          <strong>{startedAt ? startedAt.toLocaleTimeString() : "noch nicht"}</strong>
        </div>
        <div>
          <Clock3 size={15} />
          <span>Beendet</span>
          <strong>{finishedAt ? finishedAt.toLocaleTimeString() : "noch nicht"}</strong>
        </div>
      </div>

      {productExport ? (
        <div className="product-export-note">
          <FileSpreadsheet size={18} />
          <div>
            <span>Excel-Datei erstellt</span>
            <strong>{productExport.filename}</strong>
            <small>
              {productExport.rowCount} Änderungszeilen · {exportDisplayTime(productExport.generatedAt)}
            </small>
          </div>
          <button type="button" onClick={() => result && exportProductChanges(result)} aria-label="Produktänderungen erneut als Excel exportieren">
            <FileSpreadsheet size={16} />
          </button>
        </div>
      ) : null}
    </aside>
  );

  const endOfDayStatusPanel = (
    <aside className="status-surface action-status-panel" aria-label="End-of-Day Ergebnis">
      <div className="surface-heading compact mini">
        <div>
          <p className="section-kicker">End-of-Day</p>
          <h3>Bestellungen & Export</h3>
        </div>
        {isEndOfDayRunning ? <Loader2 size={20} className="spin" /> : endOfDayStatus === "success" ? <CheckCircle2 size={20} /> : endOfDayStatus === "error" ? <AlertTriangle size={20} /> : <CalendarCheck size={20} />}
      </div>
      <div className={`message message-${isEndOfDayRunning ? "running" : endOfDayStatus}`}>
        {isEndOfDayRunning ? <Loader2 size={18} className="spin" /> : endOfDayStatus === "success" ? <CheckCircle2 size={18} /> : endOfDayStatus === "error" ? <AlertTriangle size={18} /> : <ShieldCheck size={18} />}
        <p>{endOfDayMessage}</p>
      </div>
      <dl className="eod-facts">
        <div>
          <dt>Sitzung</dt>
          <dd>
            {endOfDayStatus === "error"
              ? "fehlgeschlagen"
              : endOfDayResult
                ? endOfDayResult.reused_session
                  ? "wiederverwendet"
                  : "neu"
                : "noch nicht"}
          </dd>
        </div>
        <div>
          <dt>Gespeichert</dt>
          <dd>{endOfDayResult ? botSavedCount(endOfDayResult) : "noch nicht"}</dd>
        </div>
        <div>
          <dt>Export</dt>
          <dd>{exportState(endOfDayResult)}</dd>
        </div>
        <div>
          <dt>Gestartet</dt>
          <dd>{endOfDayStartedAt ? endOfDayStartedAt.toLocaleTimeString() : "noch nicht"}</dd>
        </div>
        <div>
          <dt>Beendet</dt>
          <dd>{endOfDayFinishedAt ? endOfDayFinishedAt.toLocaleTimeString() : "noch nicht"}</dd>
        </div>
      </dl>
      {endOfDayResult?.current_url ? <code className="eod-url">{endOfDayResult.current_url}</code> : null}
    </aside>
  );

  return (
    <main className="page">
      <section className="workspace" aria-label="Konsole für die DoktorABC Produktsynchronisierung">
        <header className="masthead">
          <div className="identity">
            <div className="mark" aria-hidden="true">
              <RadioTower size={30} />
            </div>
            <div>
              <p className="eyebrow">Rats-Apotheke Betrieb</p>
              <h1>Produktsynchronisierung</h1>
            </div>
          </div>
          <div className="masthead-actions">
            <button className="theme-button" type="button" onClick={toggleTheme} aria-label="Darstellung wechseln">
              {theme === "night" ? <Sun size={17} /> : <Moon size={17} />}
              <span>{theme === "night" ? "Hell" : "Nacht"}</span>
            </button>
            <div className={`state-pill state-${anyBotError ? "error" : anyBotSuccess ? "success" : status}`}>
              {anyBotRunning ? <Loader2 size={16} className="spin" /> : <ShieldCheck size={16} />}
              <span>{anyBotRunning ? "Läuft" : anyBotError ? "Prüfen" : anyBotSuccess ? "Erfolgreich" : "Bereit"}</span>
            </div>
          </div>
        </header>

        <div className="command-layout">
          <section className="command-surface">
            <div className="surface-heading">
              <div>
                <p className="section-kicker">Kontrollierter Auslöser</p>
                <h2>Produktsync-Bot auslösen</h2>
              </div>
              <DatabaseZap size={26} />
            </div>

            <div className="bot-action-list">
              <section className="bot-action-row">
                <section className="secondary-bot-card" aria-label="End-of-Day Bot">
                  <div>
                    <CalendarCheck size={22} />
                    <span>
                      <b>End-of-Day</b>
                      <small>Bestellungen und Excel-Export</small>
                    </span>
                  </div>
                  <section className="staff-note eod-staff-note" aria-label="Hinweis für End-of-Day">
                    <h3>Hinweis für End-of-Day</h3>
                    <ol>
                      {endOfDaySteps.map((step) => (
                        <li key={step}>{step}</li>
                      ))}
                      <li>
                        Erst wenn der Lauf abgeschlossen ist, in DoktorABC unter End-of-Day Orders auf{" "}
                        <span className="confirm-pickup-preview">
                          <CheckCircle2 size={17} />
                          Confirm pickup for all
                        </span>{" "}
                        klicken.
                      </li>
                    </ol>
                  </section>
                  <button className="trigger-button eod-button" type="button" onClick={triggerEndOfDayOrders} disabled={isEndOfDayRunning}>
                    {isEndOfDayRunning ? <Loader2 size={21} className="spin" /> : <CalendarCheck size={21} />}
                    <span>{isEndOfDayRunning ? "End-of-Day läuft" : "End-of-Day starten"}</span>
                    <ArrowRight size={20} />
                  </button>
                </section>
                {endOfDayStatusPanel}
              </section>

              <section className="bot-action-row">
                <div className="primary-bot-card">
                  <section className="staff-note product-staff-note" aria-label="Hinweis für Mitarbeitende">
                    <h3>Hinweis für Mitarbeitende</h3>
                    <ol>
                      {staffSteps.map((step, index) => (
                        <li key={`${index}-${step.before || step.emphasis}`}>
                          {step.before}
                          {step.emphasis ? <strong className="attention-word">{step.emphasis}</strong> : null}
                          {step.after}
                        </li>
                      ))}
                    </ol>
                  </section>
                  <button className="trigger-button" type="button" onClick={triggerSync} disabled={isRunning}>
                    {isRunning ? <Loader2 size={21} className="spin" /> : <RefreshCw size={21} />}
                    <span>{isRunning ? "Synchronisierung läuft" : "Produkte synchronisieren (DoktorABC)"}</span>
                    <ArrowRight size={20} />
                  </button>
                </div>
                {productStatusPanel}
              </section>
            </div>

            <p className="security-note">
              <LockKeyhole size={15} />
              Diese Schaltflächen lösen ausschließlich fest hinterlegte Aktionen aus.
            </p>
          </section>
        </div>

        {(result?.new_products?.length || result?.changed_products?.length) ? (
          <section className="details-area">
            {Boolean(result?.new_products?.length) && (
              <div className="detail-table">
                <h3>Neue Produkte</h3>
                <div className="line-list">
                  {result?.new_products?.map((product) => (
                    <div className="product-line" key={product.pzn || product.product_name}>
                      <strong>{product.product_name}</strong>
                      <span>PZN {product.pzn}</span>
                      <span>Menge {productQuantity(product)}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {Boolean(result?.changed_products?.length) && (
              <div className="detail-table">
                <h3>Geänderte Produkte</h3>
                <div className="line-list">
                  {result?.changed_products?.map((product) => (
                    <article className="product-change" key={product.pzn || product.product_name}>
                      <div className="product-change-head">
                        <strong>{product.product_name}</strong>
                        <span>PZN {product.pzn}</span>
                      </div>
                      <div className="change-diff-list">
                        {Object.entries(product.changes || {}).map(([field, change]) => (
                          <div className="change-diff" key={field}>
                            <span>{field}</span>
                            <code>{formatChangeValue(change.old)}</code>
                            <ArrowRight size={14} />
                            <code>{formatChangeValue(change.new)}</code>
                          </div>
                        ))}
                      </div>
                    </article>
                  ))}
                </div>
              </div>
            )}
          </section>
        ) : null}
      </section>
    </main>
  );
}
