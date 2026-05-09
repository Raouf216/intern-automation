"use client";

import {
  AlertTriangle,
  ArrowRight,
  BadgeCheck,
  Bot,
  CalendarRange,
  CheckCircle2,
  Clipboard,
  FileWarning,
  Gauge,
  Moon,
  RefreshCw,
  Search,
  ShieldCheck,
  Sun,
  Target,
  XCircle,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import type { StoredVerificationRun, VerificationProblem, VerificationStatus } from "../lib/verification-store";

type Props = {
  initialRuns: StoredVerificationRun[];
  initialError: string | null;
};

type ProblemWithRun = {
  problem: VerificationProblem;
  run: StoredVerificationRun;
};

const demoRuns: StoredVerificationRun[] = [
  {
    id: "DEMO-ABR-2026-05-09-001",
    status: "failure",
    source: "abrechnung-verification-bot",
    bot_name: "Abrechnung Bot",
    received_at: "2026-05-09T15:52:00.000Z",
    finished_at: "2026-05-09T15:51:42.000Z",
    billing_period_from: "2026-05-01",
    billing_period_to: "2026-05-09",
    invoice_file: "doktorabc-abrechnung-2026-05-09.xlsx",
    success_count: 3,
    success_ids: ["TEST-PASS-001", "TEST-PASS-002", "TEST-PASS-RETURN-001"],
    problem_count: 3,
    problems: [
      {
        id: "TEST-FAIL-QTY-001-quantity_mismatch-0",
        problem_type: "quantity_mismatch",
        order_reference: "TEST-FAIL-QTY-001",
        billing_id: "23387",
        line_no: "1",
        order_type: "eod",
        billing_date: "2026-02-21T10:00:00.000Z",
        billing_type: "shipping",
        pzn: "20343593",
        product_name: "Blueten Canopy KMI 30/1 - Strain Kush Mints",
        expected_value: 20,
        actual_value: 15,
        problem: "Billing quantity in grams does not match bot quantity.",
        severity: "high",
        raw: {},
      },
      {
        id: "TEST-FAIL-PRICE-002-price_mismatch-1",
        problem_type: "price_mismatch",
        order_reference: "TEST-FAIL-PRICE-002",
        billing_id: "23388",
        line_no: "1",
        order_type: "eod",
        billing_date: "2026-02-21T10:00:00.000Z",
        billing_type: "shipping",
        pzn: "19474172",
        product_name: "Blueten Nimbus Health easy 26/1 - Strain French Cookies",
        expected_value: "EUR 147.50",
        actual_value: "EUR 132.50",
        problem: "Abrechnungspreis weicht von der Bot-Kontrolle ab.",
        severity: "high",
        raw: {},
      },
      {
        id: "TEST-MISSING-003-missing_order-2",
        problem_type: "missing_order",
        order_reference: "TEST-MISSING-003",
        billing_id: null,
        line_no: null,
        order_type: "eod",
        billing_date: null,
        billing_type: null,
        pzn: null,
        product_name: null,
        expected_value: "Order in Bot",
        actual_value: "Nicht in Abrechnung",
        problem: "Order wurde vom Bot gefunden, fehlt aber in der Abrechnung.",
        severity: "critical",
        raw: {},
      },
    ],
    raw: {},
  },
  {
    id: "DEMO-ABR-2026-05-08-002",
    status: "success",
    source: "abrechnung-verification-bot",
    bot_name: "Abrechnung Bot",
    received_at: "2026-05-08T18:20:00.000Z",
    finished_at: "2026-05-08T18:19:31.000Z",
    billing_period_from: "2026-05-01",
    billing_period_to: "2026-05-08",
    invoice_file: "doktorabc-abrechnung-2026-05-08.xlsx",
    success_count: 42,
    success_ids: ["A-2026-001", "A-2026-002", "A-2026-003", "A-2026-004"],
    problem_count: 0,
    problems: [],
    raw: {},
  },
];

const problemLabels: Record<string, string> = {
  billing_total_mismatch: "Abrechnungssumme",
  quantity_mismatch: "Mengenabweichung",
  pzn_mismatch: "PZN-Abweichung",
  price_mismatch: "Preisabweichung",
  product_pzn_mismatch: "Produkt/PZN Abweichung",
  total_mismatch: "Summenabweichung",
  return_mismatch: "Retoure-Abweichung",
  product_mismatch: "Produktabweichung",
  missing_order: "Order fehlt",
  unexpected_order: "Unerwartete Order",
  duplicate_order: "Doppelte Order",
  billing_missing: "Abrechnung fehlt",
  unknown_problem: "Unbekanntes Problem",
};

export function AbrechnungVerificationDashboard({ initialError, initialRuns }: Props) {
  const [runs, setRuns] = useState(initialRuns);
  const [loadError, setLoadError] = useState<string | null>(initialError);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [theme, setTheme] = useState<"light" | "night">("light");
  const [selectedType, setSelectedType] = useState("all");
  const [query, setQuery] = useState("");
  const [expandedRunId, setExpandedRunId] = useState(initialRuns[0]?.id || demoRuns[0].id);
  const [showDocumentList, setShowDocumentList] = useState(false);
  const [origin, setOrigin] = useState("");
  const [copiedEndpoint, setCopiedEndpoint] = useState(false);

  useEffect(() => {
    const savedTheme = window.localStorage.getItem("abrechnung-verification-theme");
    const nextTheme = savedTheme === "night" ? "night" : "light";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    setOrigin(window.location.origin);
  }, []);

  useEffect(() => {
    let isMounted = true;
    let refreshInFlight = false;

    async function refreshRuns() {
      if (refreshInFlight) {
        return;
      }

      refreshInFlight = true;

      try {
        const response = await fetch("/api/verification-runs?limit=160", { cache: "no-store" });
        const payload = (await response.json()) as {
          ok?: boolean;
          error?: string;
          runs?: StoredVerificationRun[];
        };

        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || `HTTP ${response.status}`);
        }

        if (!isMounted) {
          return;
        }

        const nextRuns = payload.runs || [];
        setRuns(nextRuns);
        setLoadError(null);
        setLastRefresh(new Date());
        if (nextRuns.length && !nextRuns.some((run) => run.id === expandedRunId)) {
          setExpandedRunId(nextRuns[0].id);
        }
      } catch (error) {
        if (!isMounted) {
          return;
        }

        setLoadError(error instanceof Error ? error.message : "Aktualisierung fehlgeschlagen");
      } finally {
        refreshInFlight = false;
      }
    }

    function refreshWhenVisible() {
      if (document.visibilityState === "visible") {
        void refreshRuns();
      }
    }

    void refreshRuns();
    const interval = window.setInterval(refreshRuns, 5000);
    window.addEventListener("focus", refreshWhenVisible);
    document.addEventListener("visibilitychange", refreshWhenVisible);

    return () => {
      isMounted = false;
      window.clearInterval(interval);
      window.removeEventListener("focus", refreshWhenVisible);
      document.removeEventListener("visibilitychange", refreshWhenVisible);
    };
  }, [expandedRunId]);

  function toggleTheme() {
    const nextTheme = theme === "night" ? "light" : "night";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    window.localStorage.setItem("abrechnung-verification-theme", nextTheme);
  }

  async function copyEndpoint() {
    const endpoint = `${origin || "http://localhost:8060"}/api/verification-runs`;
    await navigator.clipboard.writeText(endpoint);
    setCopiedEndpoint(true);
    window.setTimeout(() => setCopiedEndpoint(false), 1400);
  }

  const displayRuns = runs.length ? runs : demoRuns;
  const isPreview = runs.length === 0;
  const latestRun = displayRuns[0];
  const endpoint = `${origin || "http://localhost:8060"}/api/verification-runs`;
  const selectedRun = displayRuns.find((run) => run.id === expandedRunId) || latestRun;
  const focusedRuns = [selectedRun];

  const allProblems = useMemo(
    () =>
      focusedRuns.flatMap((run) =>
        run.problems.map((problem) => ({
          problem,
          run,
        }))
      ),
    [focusedRuns]
  );

  const summary = useMemo(() => {
    const successCount = selectedRun.success_count;
    const problemCount = selectedRun.problem_count;
    const criticalCount = allProblems.filter((item) => item.problem.severity === "critical").length;
    const affectedOrders = new Set(allProblems.map((item) => item.problem.order_reference)).size;
    const totalChecked = successCount + problemCount;
    const healthScore = totalChecked ? Math.max(0, Math.round((successCount / totalChecked) * 100)) : 100;

    return {
      affectedOrders,
      criticalCount,
      healthScore,
      problemCount,
      successCount,
      totalChecked,
    };
  }, [allProblems, selectedRun]);

  const problemTypeCounts = useMemo(() => {
    const counts = new Map<string, number>();
    allProblems.forEach(({ problem }) => {
      counts.set(problem.problem_type, (counts.get(problem.problem_type) || 0) + 1);
    });

    return Array.from(counts.entries())
      .map(([type, count]) => ({ type, count }))
      .sort((left, right) => right.count - left.count || typeLabel(left.type).localeCompare(typeLabel(right.type)));
  }, [allProblems]);

  const filteredProblems = useMemo(() => {
    const normalizedQuery = query.trim().toLowerCase();

    return allProblems.filter(({ problem, run }) => {
      const matchesType = selectedType === "all" || problem.problem_type === selectedType;
      const searchable = [
        problem.order_reference,
        problem.problem_type,
        problem.problem,
        formatValue(problem.expected_value),
        formatValue(problem.actual_value),
        run.id,
        run.invoice_file || "",
      ]
        .join(" ")
        .toLowerCase();
      const matchesQuery = normalizedQuery ? searchable.includes(normalizedQuery) : true;

      return matchesType && matchesQuery;
    });
  }, [allProblems, query, selectedType]);

  const maxTypeCount = Math.max(1, ...problemTypeCounts.map((item) => item.count));

  return (
    <main className="dashboard-page">
      <header className="app-topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <ShieldCheck size={30} />
            <span />
          </div>
          <div>
            <p className="eyebrow">Rats-Apotheke</p>
            <h1>Abrechnung Verification</h1>
            <p className="subtitle">Boss-Dashboard fuer Bot-Pruefungen, Abweichungen und betroffene Orders</p>
          </div>
        </div>
        <div className="topbar-actions">
          <button className="icon-button" type="button" onClick={toggleTheme} aria-label="Darstellung wechseln">
            {theme === "night" ? <Sun size={18} /> : <Moon size={18} />}
            <span>{theme === "night" ? "Hell" : "Nacht"}</span>
          </button>
          <div className={`state-badge state-${isPreview ? "preview" : latestRun.status}`}>
            {isPreview ? <Target size={17} /> : statusIcon(latestRun.status)}
            <span>{isPreview ? "Vorschau" : statusLabel(latestRun.status)}</span>
          </div>
        </div>
      </header>

      <section className="summary-strip" aria-label="Abrechnung Kennzahlen">
        <MetricCard label="Health Score" value={`${summary.healthScore}%`} tone={summary.healthScore >= 90 ? "good" : "danger"} icon={<Gauge size={22} />} />
        <MetricCard label="Probleme offen" value={summary.problemCount} tone={summary.problemCount ? "danger" : "good"} icon={<AlertTriangle size={22} />} />
        <MetricCard label="Kritisch" value={summary.criticalCount} tone={summary.criticalCount ? "danger" : "neutral"} icon={<XCircle size={22} />} />
        <MetricCard label="Geprueft OK" value={summary.successCount} tone="good" icon={<BadgeCheck size={22} />} />
        <MetricCard label="Betroffene Orders" value={summary.affectedOrders} tone={summary.affectedOrders ? "warn" : "neutral"} icon={<Target size={22} />} />
      </section>

      <section className="signal-band" aria-label="Letzter Verification Lauf">
        <div className="signal-main">
          <div className={`signal-icon signal-${latestRun.status}`}>
            {statusIcon(latestRun.status)}
          </div>
          <div>
            <p className="section-kicker">Letzter Lauf</p>
            <h2>{latestRun.invoice_file || latestRun.id}</h2>
            <p>
              {formatDateTime(latestRun.finished_at || latestRun.received_at)} · {latestRun.bot_name} · {latestRun.problem_count} Problem(e)
            </p>
          </div>
        </div>
        <div className="endpoint-box">
          <span>Ingest Endpoint</span>
          <code>{endpoint}</code>
          <button className="copy-button" type="button" onClick={copyEndpoint} aria-label="Endpoint kopieren">
            <Clipboard size={16} />
            <span>{copiedEndpoint ? "Kopiert" : "Kopieren"}</span>
          </button>
        </div>
      </section>

      {loadError ? (
        <section className="load-error" aria-label="Ladefehler">
          <AlertTriangle size={20} />
          <span>{loadError}</span>
        </section>
      ) : null}

      <section className="content-grid">
        <section className="problem-workspace" aria-label="Problemuebersicht">
          <div className="section-heading">
            <div>
              <p className="section-kicker">Problems im ausgewählten Dokument</p>
              <h2>Abweichungen in der Abrechnung</h2>
            </div>
            <div className="refresh-note">
              <RefreshCw size={15} />
              <span>{lastRefresh ? `Aktualisiert ${formatRelativeTime(lastRefresh.toISOString())}` : "Auto-Refresh aktiv"}</span>
            </div>
          </div>

          <div className="filter-bar">
            <label className="search-box">
              <Search size={17} />
              <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Order, Problem oder Datei suchen" />
            </label>
            <div className="type-tabs" role="tablist" aria-label="Problemtyp Filter">
              <button className={selectedType === "all" ? "active" : ""} type="button" onClick={() => setSelectedType("all")}>
                Alle
                <strong>{allProblems.length}</strong>
              </button>
              {problemTypeCounts.map((item) => (
                <button className={selectedType === item.type ? "active" : ""} type="button" onClick={() => setSelectedType(item.type)} key={item.type}>
                  {typeLabel(item.type)}
                  <strong>{item.count}</strong>
                </button>
              ))}
            </div>
          </div>

          <div className="problem-list">
            {filteredProblems.length ? (
              filteredProblems.map(({ problem, run }) => <ProblemCard problem={problem} run={run} key={`${run.id}-${problem.id}`} />)
            ) : (
              <div className="empty-state">
                <CheckCircle2 size={30} />
                <h3>Keine passenden Abweichungen</h3>
                <p>Der aktuelle Filter findet keine Probleme in den geladenen Verification Runs.</p>
              </div>
            )}
          </div>
        </section>

        <aside className="side-rail" aria-label="Analyse">
          <section className="analysis-panel">
            <div className="panel-heading">
              <FileWarning size={18} />
              <span>Problemtypen</span>
            </div>
            <div className="category-list">
              {problemTypeCounts.length ? (
                problemTypeCounts.map((item) => (
                  <button
                    className={selectedType === item.type ? "category-row active" : "category-row"}
                    type="button"
                    onClick={() => setSelectedType(item.type)}
                    key={item.type}
                  >
                    <span>
                      <b>{typeLabel(item.type)}</b>
                      <small>{item.count} Treffer</small>
                    </span>
                    <i style={{ width: `${Math.max(12, (item.count / maxTypeCount) * 100)}%` }} />
                  </button>
                ))
              ) : (
                <div className="quiet-line">Keine Problemtypen</div>
              )}
            </div>
          </section>

          <section className="analysis-panel">
            <div className="panel-heading">
              <CalendarRange size={18} />
              <span>Dokumente</span>
            </div>
            <button className="document-toggle-button" type="button" onClick={() => setShowDocumentList((visible) => !visible)}>
              <Clipboard size={16} />
              <span>{showDocumentList ? "Dokumente verbergen" : `Alle Dokumente (${displayRuns.length})`}</span>
            </button>
            {showDocumentList ? (
              <div className="run-list">
                {displayRuns.map((run) => (
                  <button
                    className={selectedRun.id === run.id ? `run-row active status-${run.status}` : `run-row status-${run.status}`}
                    type="button"
                    onClick={() => setExpandedRunId(run.id)}
                    key={run.id}
                  >
                    <span>
                      <b>{run.invoice_file || run.id}</b>
                      <small>{formatDateTime(run.finished_at || run.received_at)}</small>
                    </span>
                    <strong>{run.problem_count}</strong>
                  </button>
                ))}
              </div>
            ) : (
              <div className="quiet-line">Der neueste empfangene JSON-Request ist ausgewählt. Öffnen, um ältere Dokumente zu sehen.</div>
            )}
          </section>

          <section className="analysis-panel run-detail">
            <div className="panel-heading">
              <Bot size={18} />
              <span>Ausgewaehlter Run</span>
            </div>
            <dl className="fact-grid">
              <div>
                <dt>Status</dt>
                <dd>{statusLabel(selectedRun.status)}</dd>
              </div>
              <div>
                <dt>OK IDs</dt>
                <dd>{selectedRun.success_count}</dd>
              </div>
              <div>
                <dt>Probleme</dt>
                <dd>{selectedRun.problem_count}</dd>
              </div>
              <div>
                <dt>Quelle</dt>
                <dd>{selectedRun.source}</dd>
              </div>
            </dl>
            <div className="success-id-list">
              {selectedRun.success_ids.slice(0, 8).map((id) => (
                <code key={id}>{id}</code>
              ))}
              {selectedRun.success_ids.length > 8 ? <span>+{selectedRun.success_ids.length - 8} weitere</span> : null}
            </div>
          </section>
        </aside>
      </section>
    </main>
  );
}

function MetricCard({
  icon,
  label,
  tone,
  value,
}: {
  icon: React.ReactNode;
  label: string;
  tone: "danger" | "good" | "neutral" | "warn";
  value: number | string;
}) {
  return (
    <article className={`metric-card metric-${tone}`}>
      <div>{icon}</div>
      <span>{label}</span>
      <strong>{typeof value === "number" ? formatNumber(value) : value}</strong>
    </article>
  );
}

function ProblemCard({ problem, run }: ProblemWithRun) {
  return (
    <article className={`problem-card severity-${problem.severity}`}>
      <div className="problem-head">
        <div className="problem-title">
          <FileWarning size={19} />
          <div>
            <h3>{problem.order_reference}</h3>
            <p>{typeLabel(problem.problem_type)}</p>
          </div>
        </div>
        <span className={`severity-chip severity-${problem.severity}`}>{severityLabel(problem.severity)}</span>
      </div>

      <p className="problem-copy">{problem.problem}</p>

      {problem.product_name || problem.pzn || problem.billing_id || problem.order_type || problem.billing_date ? (
        <div className="problem-context">
          {problem.product_name ? (
            <span>
              <b>Produkt</b>
              <strong>{problem.product_name}</strong>
            </span>
          ) : null}
          {problem.pzn ? (
            <span>
              <b>PZN</b>
              <strong>{problem.pzn}</strong>
            </span>
          ) : null}
          {problem.billing_id ? (
            <span>
              <b>Billing ID</b>
              <strong>{problem.billing_id}</strong>
            </span>
          ) : null}
          {problem.line_no ? (
            <span>
              <b>Zeile</b>
              <strong>{problem.line_no}</strong>
            </span>
          ) : null}
          {problem.order_type ? (
            <span>
              <b>Order Typ</b>
              <strong>{problem.order_type}</strong>
            </span>
          ) : null}
          {problem.billing_date ? (
            <span>
              <b>Datum</b>
              <strong>{formatDateTime(problem.billing_date)}</strong>
            </span>
          ) : null}
        </div>
      ) : null}

      <div className="value-compare">
        <div>
          <span>Erwartet</span>
          <strong>{formatValue(problem.expected_value)}</strong>
        </div>
        <ArrowRight size={17} />
        <div>
          <span>Ist</span>
          <strong>{formatValue(problem.actual_value)}</strong>
        </div>
      </div>

      <div className="problem-foot">
        <span>{run.invoice_file || run.id}</span>
        <time dateTime={run.finished_at || run.received_at}>{formatDateTime(run.finished_at || run.received_at)}</time>
      </div>
    </article>
  );
}

function statusIcon(status: VerificationStatus) {
  if (status === "success") {
    return <CheckCircle2 size={17} />;
  }

  if (status === "warning") {
    return <AlertTriangle size={17} />;
  }

  return <XCircle size={17} />;
}

function statusLabel(status: VerificationStatus) {
  if (status === "success") {
    return "Alles OK";
  }

  if (status === "warning") {
    return "Pruefen";
  }

  return "Probleme";
}

function severityLabel(severity: VerificationProblem["severity"]) {
  if (severity === "critical") {
    return "Kritisch";
  }

  if (severity === "high") {
    return "Hoch";
  }

  return "Mittel";
}

function typeLabel(type: string) {
  return problemLabels[type] || type.replaceAll("_", " ");
}

function formatValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "leer";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3)));
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value);
}

function formatNumber(value: number) {
  return new Intl.NumberFormat("de-DE").format(value);
}

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Datum unbekannt";
  }

  return new Intl.DateTimeFormat("de-DE", {
    timeZone: "Europe/Berlin",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatRelativeTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Zeit unbekannt";
  }

  const diffSeconds = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));

  if (diffSeconds < 60) {
    return `vor ${diffSeconds} Sek`;
  }

  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) {
    return `vor ${diffMinutes} Min`;
  }

  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return `vor ${diffHours} Std`;
  }

  return `vor ${Math.floor(diffHours / 24)} Tg`;
}
