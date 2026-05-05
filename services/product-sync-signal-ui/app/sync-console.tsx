"use client";

import {
  AlertTriangle,
  ArrowRight,
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

const configuredEndpoint = process.env.NEXT_PUBLIC_PRODUCT_SYNC_ENDPOINT || "";
const expectedPassword = process.env.NEXT_PUBLIC_PRODUCT_SYNC_PASSWORD || "";
const fallbackEndpoint = "http://178.104.144.30:8020/jobs/product-prices";
const syncEndpoint = configuredEndpoint || fallbackEndpoint;
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

export function SyncConsole() {
  const [password, setPassword] = useState("");
  const [isRunning, setIsRunning] = useState(false);
  const [status, setStatus] = useState<"idle" | "success" | "error">("idle");
  const [message, setMessage] = useState("Bereit für eine kontrollierte Produktsynchronisierung.");
  const [result, setResult] = useState<SyncResponse | null>(null);
  const [startedAt, setStartedAt] = useState<Date | null>(null);
  const [finishedAt, setFinishedAt] = useState<Date | null>(null);
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

  async function triggerSync() {
    if (!syncEndpoint.trim()) {
      setStatus("error");
      setMessage("Der feste DoktorABC Sync-Endpunkt ist nicht konfiguriert.");
      return;
    }

    if (!password.trim()) {
      setStatus("error");
      setMessage("Bitte das Bedienerpasswort eingeben.");
      return;
    }

    if (!expectedPassword) {
      setStatus("error");
      setMessage("Das Bedienerpasswort ist nicht konfiguriert.");
      return;
    }

    if (expectedPassword && password !== expectedPassword) {
      setStatus("error");
      setMessage("Passwort ist falsch.");
      return;
    }

    setIsRunning(true);
    setStatus("idle");
    setMessage("Synchronisierung läuft. Bitte warten, der Vorgang kann bis zu 5 Minuten dauern.");
    setResult(null);
    setStartedAt(new Date());
    setFinishedAt(null);

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
      setFinishedAt(new Date());
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
      setFinishedAt(new Date());
    } finally {
      setIsRunning(false);
    }
  }

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
            <div className={`state-pill state-${status}`}>
              {isRunning ? <Loader2 size={16} className="spin" /> : <ShieldCheck size={16} />}
              <span>{isRunning ? "Läuft" : status === "success" ? "Erfolgreich" : status === "error" ? "Prüfen" : "Bereit"}</span>
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

            <button className="trigger-button" type="button" onClick={triggerSync} disabled={isRunning}>
              {isRunning ? <Loader2 size={21} className="spin" /> : <RefreshCw size={21} />}
              <span>{isRunning ? "Synchronisierung läuft" : "Produkte synchronisieren (DoktorABC)"}</span>
              <ArrowRight size={20} />
            </button>

            <p className="security-note">
              <LockKeyhole size={15} />
              Diese Schaltfläche löst ausschließlich die fest hinterlegte DoktorABC Produktsynchronisierung aus.
            </p>
          </section>

          <aside className="status-surface">
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
