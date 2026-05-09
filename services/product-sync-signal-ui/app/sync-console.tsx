"use client";

import {
  AlertTriangle,
  ArrowRight,
  CalendarCheck,
  CheckCircle2,
  Clock3,
  DatabaseZap,
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
import { useEffect, useMemo, useState } from "react";

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

function legacyEndOfDayOrdersEndpoint(value: string) {
  return value
    .trim()
    .replace(/\/jobs\/end-of-day\/(?:login|session-check)\/?$/i, "/jobs/end-of-day/orders/sync");
}

const configuredEndpoint = process.env.NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT || "";
const configuredEndOfDayEndpoint =
  process.env.NEXT_PUBLIC_EOD_ORDERS_ENDPOINT ||
  legacyEndOfDayOrdersEndpoint(process.env.NEXT_PUBLIC_EOD_LOGIN_ENDPOINT || "");
const expectedPassword = process.env.NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD || "";
const fallbackEndpoint = "http://178.104.144.30:8020/jobs/product-prices";
const fallbackEndOfDayEndpoint = "http://178.104.144.30:8021/jobs/end-of-day/orders/sync";
const syncEndpoint = configuredEndpoint || fallbackEndpoint;
const endOfDayEndpoint = configuredEndOfDayEndpoint || fallbackEndOfDayEndpoint;
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

function exportState(payload: EndOfDayResponse | null) {
  if (!payload?.export) return "noch nicht";
  if (payload.export.skipped) return "übersprungen";
  if (payload.export.sent_to_n8n) return "gesendet";
  if (payload.export.downloaded) return "geladen";
  return "noch nicht";
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

export function SyncConsole() {
  const [password, setPassword] = useState("");
  const [isRunning, setIsRunning] = useState(false);
  const [isEndOfDayRunning, setIsEndOfDayRunning] = useState(false);
  const [status, setStatus] = useState<"idle" | "success" | "error">("idle");
  const [endOfDayStatus, setEndOfDayStatus] = useState<"idle" | "success" | "error">("idle");
  const [message, setMessage] = useState("Bereit für eine kontrollierte Produktsynchronisierung.");
  const [endOfDayMessage, setEndOfDayMessage] = useState("Bereit für End-of-Day Bestellungen und Excel-Export.");
  const [result, setResult] = useState<SyncResponse | null>(null);
  const [endOfDayResult, setEndOfDayResult] = useState<EndOfDayResponse | null>(null);
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

  function validateOperatorPassword() {
    if (!password.trim()) {
      return "Bitte das Bedienerpasswort eingeben.";
    }

    if (!expectedPassword) {
      return "Das Bedienerpasswort ist nicht konfiguriert.";
    }

    if (expectedPassword && password !== expectedPassword) {
      return "Passwort ist falsch.";
    }

    return "";
  }

  async function triggerSync() {
    if (!syncEndpoint.trim()) {
      setStatus("error");
      setMessage("Der feste DoktorABC Sync-Endpunkt ist nicht konfiguriert.");
      return;
    }

    const passwordError = validateOperatorPassword();
    if (passwordError) {
      setStatus("error");
      setMessage(passwordError);
      return;
    }

    setIsRunning(true);
    setStatus("idle");
    setMessage("Synchronisierung läuft. Bitte warten, der Vorgang kann bis zu 5 Minuten dauern.");
    setResult(null);
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
    if (!endOfDayEndpoint.trim()) {
      setEndOfDayStatus("error");
      setEndOfDayMessage("Der feste End-of-Day Bot-Endpunkt ist nicht konfiguriert.");
      return;
    }

    const passwordError = validateOperatorPassword();
    if (passwordError) {
      setEndOfDayStatus("error");
      setEndOfDayMessage(passwordError);
      return;
    }

    setIsEndOfDayRunning(true);
    setEndOfDayStatus("idle");
    setEndOfDayMessage("End-of-Day läuft. Der Bot synchronisiert die EOD-Bestellungen und exportiert die Excel-Datei.");
    setEndOfDayResult(null);
    const runStartedAt = new Date();
    setEndOfDayStartedAt(runStartedAt);
    setEndOfDayFinishedAt(null);

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
      setEndOfDayStatus("error");
      const errorMessage = error instanceof Error ? error.message : "Unbekannter Fehler";
      const isFetchFailure =
        errorMessage.toLowerCase().includes("failed to fetch") ||
        errorMessage.toLowerCase().includes("networkerror");

      setEndOfDayMessage(
        isFetchFailure
          ? `Netzwerk- oder CORS-Fehler: Der Browser konnte den End-of-Day Bot unter ${endOfDayEndpoint} nicht erreichen oder die Antwort nicht lesen.`
          : `End-of-Day Anfrage fehlgeschlagen: ${errorMessage}`
      );
      setEndOfDayFinishedAt(new Date());
    } finally {
      setIsEndOfDayRunning(false);
    }
  }

  const anyBotRunning = isRunning || isEndOfDayRunning;
  const anyBotError =
    status === "error" || endOfDayStatus === "error";
  const anyBotSuccess =
    status === "success" || endOfDayStatus === "success";

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

            <label className="field">
              <span>Bedienerpasswort</span>
              <div className="password-row">
                <KeyRound size={18} />
                <input
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  type="password"
                  placeholder="Passwort eingeben"
                  autoComplete="current-password"
                />
              </div>
            </label>

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
                  <button className="trigger-button eod-button" type="button" onClick={triggerEndOfDayOrders} disabled={anyBotRunning}>
                    {isEndOfDayRunning ? <Loader2 size={21} className="spin" /> : <CalendarCheck size={21} />}
                    <span>{isEndOfDayRunning ? "End-of-Day läuft" : "End-of-Day starten"}</span>
                    <ArrowRight size={20} />
                  </button>
                </section>
                {endOfDayStatusPanel}
              </section>

              <section className="bot-action-row">
                <div className="primary-bot-card">
                  <section className="staff-note" aria-label="Hinweis für Mitarbeitende">
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
                  <button className="trigger-button" type="button" onClick={triggerSync} disabled={anyBotRunning}>
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
