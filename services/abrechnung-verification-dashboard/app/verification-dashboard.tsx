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
  RotateCcw,
  XCircle,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import type { StoredVerificationRun, VerificationProblem, VerificationReturn, VerificationStatus, VerificationSuccess } from "../lib/verification-store";

type Props = {
  initialRuns: StoredVerificationRun[];
  initialError: string | null;
};

type ProblemWithRun = {
  problem: VerificationProblem;
  run: StoredVerificationRun;
};

type SuccessWithRun = {
  kind: "success";
  success: VerificationSuccess;
  run: StoredVerificationRun;
};

type ReturnWithRun = {
  kind: "return";
  returnItem: VerificationReturn;
  run: StoredVerificationRun;
};

type ProblemListItem =
  | {
      kind: "problem";
      problem: VerificationProblem;
      run: StoredVerificationRun;
    }
  | SuccessWithRun
  | ReturnWithRun;

const emptyRun: StoredVerificationRun = {
  id: "empty",
  status: "success",
  source: "abrechnung-bot",
  bot_name: "Abrechnung Bot",
  received_at: new Date(0).toISOString(),
  finished_at: null,
  billing_period_from: null,
  billing_period_to: null,
  invoice_file: null,
  success_count: 0,
  success_ids: [],
  returns: [],
  problem_count: 0,
  problems: [],
  raw: {},
};

const problemLabels: Record<string, string> = {
  billing_total_mismatch: "Abrechnungssumme",
  quantity_mismatch: "Mengenabweichung",
  pzn_mismatch: "PZN-Abweichung",
  price_mismatch: "Preisabweichung",
  product_pzn_mismatch: "Produkt/PZN Abweichung",
  total_mismatch: "Summenabweichung",
  return_mismatch: "Retoure-Abweichung",
  product_mismatch: "Produktabweichung",
  missing_in_billing: "Fehlt in der Abrechnung",
  missing_in_bot: "Fehlt im Bot",
  missing_eod_billing_date: "EOD-Abrechnungsdatum fehlt",
  missing_self_pickup_scraped_at: "Selbstabholung-Scrape fehlt",
  billing_product_name_not_found: "Produktname nicht gefunden",
  product_missing_in_bot: "Produkt fehlt im Bot",
  no_valid_price_at_billing_date: "Preis am Datum fehlt",
  wrong_return_sign: "RETURN-Vorzeichen falsch",
  wrong_positive_sign: "Vorzeichen falsch",
  missing_order: "Bestellung fehlt",
  unexpected_order: "Unerwartete Bestellung",
  duplicate_order: "Doppelte Bestellung",
  billing_missing: "Abrechnung fehlt",
  return_order_not_found: "RETURN ohne Versand",
  unknown_problem: "Unbekanntes Problem",
};

const problemMessages: Record<string, string> = {
  missing_in_billing: "Bot-Bestellung vorhanden, aber es wurde keine passende Zeile in doktorabc_billing ueber hash_id/order_reference gefunden.",
  missing_in_bot: "Abrechnungszeile vorhanden, aber es wurde keine passende Bot-Bestellung ueber order_reference/hash_id gefunden.",
  return_order_not_found: "RETURN ist in der Abrechnung vorhanden, aber die urspruengliche Versand-Bestellung wurde nicht in der Bot-Tabelle gefunden.",
  missing_eod_billing_date: "EOD-Bot-Bestellung hat kein billing_date. Die Preisgueltigkeit kann nicht geprueft werden.",
  missing_self_pickup_scraped_at: "Selbstabholung-Bot-Bestellung hat kein scraped_at. Sie kann keinem Abrechnungszeitraum zugeordnet werden.",
  billing_product_name_not_found: "Der Produktname aus billing.stock konnte nicht doktorabc_products.product_name zugeordnet werden. PZN und Preis koennen nicht geprueft werden.",
  pzn_mismatch: "Das Produkt aus der Abrechnung ist einer anderen PZN zugeordnet als die Bot-PZN fuer dieselbe Bestellung.",
  quantity_mismatch: "Die Menge in Gramm stimmt nicht mit der Bot-Menge ueberein.",
  product_missing_in_bot: "Die Abrechnung enthaelt diese Produktzeile, aber im Bot wurde kein passendes Produkt bzw. keine passende PZN gefunden.",
  no_valid_price_at_billing_date: "Fuer diese PZN gibt es am Abrechnungsdatum keinen gueltigen products_price-Eintrag mit price_type price_per_g_incl_vat.",
  billing_total_mismatch: "doktorabc_billing.supply_price_base stimmt nicht mit der berechneten Produktsumme ueberein: Anzahl x Gramm x price_per_g_incl_vat.",
  wrong_return_sign: "RETURN-Zeile muss einen negativen supply_price_base haben.",
  wrong_positive_sign: "Nicht-RETURN-Zeile muss einen positiven supply_price_base haben.",
};

const resultPreviewLimit = 100;

export function AbrechnungVerificationDashboard({ initialError, initialRuns }: Props) {
  const [runs, setRuns] = useState(initialRuns);
  const [loadError, setLoadError] = useState<string | null>(initialError);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [theme, setTheme] = useState<"light" | "night">("light");
  const [selectedType, setSelectedType] = useState("all");
  const [query, setQuery] = useState("");
  const [expandedRunId, setExpandedRunId] = useState(initialRuns[0]?.id || "");
  const [expandedResultSections, setExpandedResultSections] = useState<Record<string, boolean>>({});
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

  const displayRuns = runs;
  const isPreview = runs.length === 0;
  const latestRun = displayRuns[0] || emptyRun;
  const endpoint = `${origin || "http://localhost:8060"}/api/verification-runs`;
  const selectedRun = displayRuns.find((run) => run.id === expandedRunId) || latestRun;
  const focusedRuns = [selectedRun];

  const allProblems = useMemo(
    () =>
      focusedRuns.flatMap((run) =>
        run.problems.map((problem) => ({
          kind: "problem" as const,
          problem,
          run,
        }))
      ),
    [focusedRuns]
  );

  const allSuccesses = useMemo(
    () =>
      focusedRuns.flatMap((run) =>
        run.success_ids.map((success) => ({
          kind: "success" as const,
          success,
          run,
        }))
      ),
    [focusedRuns]
  );

  const allReturns = useMemo(
    () =>
      focusedRuns.flatMap((run) =>
        run.returns.map((returnItem) => ({
          kind: "return" as const,
          returnItem,
          run,
        }))
      ),
    [focusedRuns]
  );

  const returnsWithShipping = useMemo(() => allReturns.filter((item) => item.returnItem.found_in_bot), [allReturns]);
  const returnsWithoutShipping = useMemo(() => allReturns.filter((item) => !item.returnItem.found_in_bot), [allReturns]);

  const allItems = useMemo<ProblemListItem[]>(() => [...allProblems, ...allSuccesses, ...allReturns], [allProblems, allSuccesses, allReturns]);

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
      returnsWithShippingCount: returnsWithShipping.length,
      returnsWithoutShippingCount: returnsWithoutShipping.length,
      successCount,
      totalChecked,
    };
  }, [allProblems, returnsWithShipping, returnsWithoutShipping, selectedRun]);

  const problemTypeCounts = useMemo(() => {
    const counts = new Map<string, number>();
    allProblems.forEach(({ problem }) => {
      counts.set(problem.problem_type, (counts.get(problem.problem_type) || 0) + 1);
    });

    return Array.from(counts.entries())
      .map(([type, count]) => ({ type, count }))
      .sort((left, right) => right.count - left.count || typeLabel(left.type).localeCompare(typeLabel(right.type)));
  }, [allProblems]);

  const filteredItems = useMemo(() => {
    const normalizedQuery = normalizeSearchText(query);
    const compactQuery = compactSearchText(query);

    return allItems.filter((item) => {
      const matchesType =
        selectedType === "all" ||
        (selectedType === "ok" && item.kind === "success") ||
        (selectedType === "returns_sent" && item.kind === "return" && item.returnItem.found_in_bot) ||
        (selectedType === "returns_missing" && item.kind === "return" && !item.returnItem.found_in_bot) ||
        (item.kind === "problem" && item.problem.problem_type === selectedType);
      const searchable =
        item.kind === "success"
          ? [
              item.success.id,
              item.success.order_reference,
              item.success.hash_id || "",
              item.success.order_type || "",
              orderTypeLabel(item.success.order_type),
              item.run.invoice_file || "",
              runDisplayName(item.run),
            ]
              .join(" ")
          : item.kind === "return"
            ? [
                item.returnItem.id,
                item.returnItem.order_reference,
                item.returnItem.hash_id || "",
                item.returnItem.billing_id || "",
                item.returnItem.billing_type || "",
                item.returnItem.order_type || "",
                orderTypeLabel(item.returnItem.order_type),
                item.returnItem.found_in_bot ? "gefunden" : "nicht gefunden",
                item.returnItem.problem || "",
                formatValue(item.returnItem.supply_price_base),
                item.returnItem.sent_at ? formatDateTime(item.returnItem.sent_at) : "",
                item.run.invoice_file || "",
                runDisplayName(item.run),
              ]
                .join(" ")
          : [
              problemDisplayId(item.problem),
              item.problem.order_reference,
              item.problem.hash_id || "",
              item.problem.billing_id || "",
              item.problem.line_no || "",
              item.problem.pzn || "",
              item.problem.product_name || "",
              item.problem.order_type || "",
              orderTypeLabel(item.problem.order_type),
              item.problem.problem_type,
              item.problem.problem,
              problemMessage(item.problem),
              formatValue(item.problem.expected_value),
              formatValue(item.problem.actual_value),
              item.run.invoice_file || "",
              runDisplayName(item.run),
            ]
              .join(" ");
      const matchesQuery = normalizedQuery
        ? normalizeSearchText(searchable).includes(normalizedQuery) || (compactQuery ? compactSearchText(searchable).includes(compactQuery) : false)
        : true;

      return matchesType && matchesQuery;
    });
  }, [allItems, query, selectedType]);

  const resultSectionKey = selectedType;
  const sectionExpanded = Boolean(expandedResultSections[resultSectionKey]);
  const visibleItems = sectionExpanded ? filteredItems : filteredItems.slice(0, resultPreviewLimit);
  const hiddenItemCount = Math.max(0, filteredItems.length - visibleItems.length);
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
            <h1>Abrechnungspruefung</h1>
            <p className="subtitle">Dashboard fuer Bot-Pruefungen, Abweichungen und betroffene Bestellungen</p>
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
        <MetricCard label="Pruefquote" value={`${summary.healthScore}%`} tone={summary.healthScore >= 90 ? "good" : "danger"} icon={<Gauge size={22} />} />
        <MetricCard label="Probleme offen" value={summary.problemCount} tone={summary.problemCount ? "danger" : "good"} icon={<AlertTriangle size={22} />} />
        <MetricCard label="Kritisch" value={summary.criticalCount} tone={summary.criticalCount ? "danger" : "neutral"} icon={<XCircle size={22} />} />
        <MetricCard label="Geprueft OK" value={summary.successCount} tone="good" icon={<BadgeCheck size={22} />} />
        <MetricCard label="RETURN mit Versand" value={summary.returnsWithShippingCount} tone="good" icon={<RotateCcw size={22} />} />
        <MetricCard label="RETURN ohne Versand" value={summary.returnsWithoutShippingCount} tone={summary.returnsWithoutShippingCount ? "danger" : "neutral"} icon={<RotateCcw size={22} />} />
        <MetricCard label="Betroffene Bestellungen" value={summary.affectedOrders} tone={summary.affectedOrders ? "warn" : "neutral"} icon={<Target size={22} />} />
      </section>

      <section className="signal-band" aria-label="Letzter Verifikationslauf">
        <div className="signal-main">
          <div className={`signal-icon signal-${latestRun.status}`}>
            {statusIcon(latestRun.status)}
          </div>
          <div>
            <p className="section-kicker">Letzter Lauf</p>
            <h2>{runDisplayName(latestRun)}</h2>
            <p>
              {latestRun.id === "empty" ? "Noch keine Verifikationsdaten" : formatDateTime(latestRun.finished_at || latestRun.received_at)} · {latestRun.bot_name} · {latestRun.problem_count} Problem(e)
            </p>
          </div>
        </div>
        <div className="endpoint-box">
          <span>Eingangsendpunkt</span>
          <code>{endpoint}</code>
          <button className="copy-button" type="button" onClick={copyEndpoint} aria-label="Endpunkt kopieren">
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
              <p className="section-kicker">Pruefergebnisse im ausgewählten Dokument</p>
              <h2>Abweichungen und OK-Pruefungen</h2>
            </div>
            <div className="refresh-note">
              <RefreshCw size={15} />
              <span>{lastRefresh ? `Aktualisiert ${formatRelativeTime(lastRefresh.toISOString())}` : "Automatische Aktualisierung aktiv"}</span>
            </div>
          </div>

          <div className="filter-bar">
            <label className="search-box">
              <Search size={17} />
              <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Bestellreferenz, Hash-ID, Problem oder Datei suchen" />
            </label>
            <div className="type-tabs" role="tablist" aria-label="Problemtyp Filter">
              <button className={selectedType === "all" ? "active" : ""} type="button" onClick={() => setSelectedType("all")}>
                Alle
                <strong>{allItems.length}</strong>
              </button>
              <button className={selectedType === "ok" ? "active ok" : "ok"} type="button" onClick={() => setSelectedType("ok")}>
                OK
                <strong>{allSuccesses.length}</strong>
              </button>
              <button className={selectedType === "returns_sent" ? "active returns-sent" : "returns-sent"} type="button" onClick={() => setSelectedType("returns_sent")}>
                RETURN mit Versand
                <strong>{returnsWithShipping.length}</strong>
              </button>
              <button className={selectedType === "returns_missing" ? "active returns-missing" : "returns-missing"} type="button" onClick={() => setSelectedType("returns_missing")}>
                RETURN ohne Versand
                <strong>{returnsWithoutShipping.length}</strong>
              </button>
              {problemTypeCounts.map((item) => (
                <button className={selectedType === item.type ? "active" : ""} type="button" onClick={() => setSelectedType(item.type)} key={item.type}>
                  {typeLabel(item.type)}
                  <strong>{item.count}</strong>
                </button>
              ))}
            </div>
          </div>

          <div className="result-limit-bar">
            <span>
              {filteredItems.length
                ? `${formatNumber(visibleItems.length)} von ${formatNumber(filteredItems.length)} Treffer angezeigt`
                : "Keine Treffer im aktuellen Filter"}
            </span>
            <small>Suche prueft alle geladenen IDs, auch wenn nur 100 Karten sichtbar sind.</small>
            {filteredItems.length > resultPreviewLimit ? (
              <button
                type="button"
                onClick={() =>
                  setExpandedResultSections((sections) => ({
                    ...sections,
                    [resultSectionKey]: !sectionExpanded,
                  }))
                }
              >
                {sectionExpanded ? "Nur 100 anzeigen" : `Alle anzeigen (+${formatNumber(hiddenItemCount)})`}
              </button>
            ) : null}
          </div>

          <div className="problem-list">
            {visibleItems.length ? (
              visibleItems.map((item) =>
                item.kind === "success" ? (
                  <SuccessCard success={item.success} run={item.run} key={`${item.run.id}-ok-${item.success.id}`} />
                ) : item.kind === "return" ? (
                  <ReturnCard returnItem={item.returnItem} run={item.run} key={`${item.run.id}-return-${item.returnItem.id}`} />
                ) : (
                  <ProblemCard problem={item.problem} run={item.run} key={`${item.run.id}-${item.problem.id}`} />
                )
              )
            ) : (
              <div className="empty-state">
                <CheckCircle2 size={30} />
                <h3>Keine passenden Abweichungen</h3>
                <p>Der aktuelle Filter findet keine Probleme in den geladenen Verifikationslaeufen.</p>
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
                      <b>{runDisplayName(run)}</b>
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

          <section className="analysis-panel return-panel">
            <div className="panel-heading">
              <RotateCcw size={18} />
              <span>Retouren</span>
            </div>
            {selectedRun.returns.length ? (
              <div className="return-groups">
                <div className="return-group">
                  <div className="return-group-title missing">
                    <span>RETURN ohne Versand</span>
                    <strong>{returnsWithoutShipping.length}</strong>
                  </div>
                  {returnsWithoutShipping.length ? (
                    <div className="return-list">
                      {returnsWithoutShipping.slice(0, 6).map(({ returnItem }) => (
                        <button
                          className="return-row missing"
                          type="button"
                          onClick={() => {
                            setSelectedType("returns_missing");
                            setQuery(returnItem.hash_id || returnItem.order_reference);
                          }}
                          key={returnItem.id}
                        >
                          <span>
                            <b>{returnItem.hash_id || returnItem.order_reference}</b>
                            <small>Nicht im Bot gefunden</small>
                          </span>
                          <strong>!</strong>
                        </button>
                      ))}
                      {returnsWithoutShipping.length > 6 ? <span className="more-line">+{returnsWithoutShipping.length - 6} weitere</span> : null}
                    </div>
                  ) : (
                    <div className="quiet-line">Keine RETURN ohne Versand.</div>
                  )}
                </div>

                <div className="return-group">
                  <div className="return-group-title found">
                    <span>RETURN mit Versand</span>
                    <strong>{returnsWithShipping.length}</strong>
                  </div>
                  {returnsWithShipping.length ? (
                    <div className="return-list">
                      {returnsWithShipping.slice(0, 6).map(({ returnItem }) => (
                        <button
                          className="return-row found"
                          type="button"
                          onClick={() => {
                            setSelectedType("returns_sent");
                            setQuery(returnItem.hash_id || returnItem.order_reference);
                          }}
                          key={returnItem.id}
                        >
                          <span>
                            <b>{returnItem.hash_id || returnItem.order_reference}</b>
                            <small>{returnItem.sent_at ? `Gesendet: ${formatDateTime(returnItem.sent_at)}` : "Datum fehlt"}</small>
                          </span>
                          <strong>OK</strong>
                        </button>
                      ))}
                      {returnsWithShipping.length > 6 ? <span className="more-line">+{returnsWithShipping.length - 6} weitere</span> : null}
                    </div>
                  ) : (
                    <div className="quiet-line">Keine RETURN mit Versand.</div>
                  )}
                </div>
              </div>
            ) : (
              <div className="quiet-line">Keine Retouren in diesem Lauf.</div>
            )}
          </section>

          <section className="analysis-panel run-detail">
            <div className="panel-heading">
              <Bot size={18} />
              <span>Ausgewaehlter Lauf</span>
            </div>
            <dl className="fact-grid">
              <div>
                <dt>Status</dt>
                <dd>{statusLabel(selectedRun.status)}</dd>
              </div>
              <div>
                <dt>OK-IDs</dt>
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
              {selectedRun.success_ids.slice(0, 8).map((success, index) => (
                <code key={`${success.id}-${index}`}>
                  <span>{success.id}</span>
                  <small>{orderTypeLabel(success.order_type)}</small>
                </code>
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
  const displayId = problemDisplayId(problem);
  const orderType = orderTypeLabel(problem.order_type);

  return (
    <article className={`problem-card severity-${problem.severity}`}>
      <div className="problem-head">
        <div className="problem-title">
          <FileWarning size={19} />
          <div>
            <h3>{displayId}</h3>
            <p>
              {typeLabel(problem.problem_type)} · {orderType}
            </p>
          </div>
        </div>
        <span className={`severity-chip severity-${problem.severity}`}>{severityLabel(problem.severity)}</span>
      </div>

      <p className="problem-copy">{problemMessage(problem)}</p>

      <div className="problem-context">
        <span>
          <b>Bestelltyp</b>
          <strong>{orderType}</strong>
        </span>
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
            <b>Abrechnungs-ID</b>
            <strong>{problem.billing_id}</strong>
          </span>
        ) : null}
        {problem.hash_id ? (
          <span>
            <b>Hash-ID</b>
            <strong>{problem.hash_id}</strong>
          </span>
        ) : null}
        {problem.line_no ? (
          <span>
            <b>Zeile</b>
            <strong>{problem.line_no}</strong>
          </span>
        ) : null}
        {problem.billing_date ? (
          <span>
            <b>Datum</b>
            <strong>{formatDateTime(problem.billing_date)}</strong>
          </span>
        ) : null}
      </div>

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
        <span>
          {displayId} · {orderType}
        </span>
        <time dateTime={run.finished_at || run.received_at}>{formatDateTime(run.finished_at || run.received_at)}</time>
      </div>
    </article>
  );
}

function SuccessCard({ success, run }: Pick<SuccessWithRun, "success" | "run">) {
  const orderType = orderTypeLabel(success.order_type);

  return (
    <article className="problem-card success-card">
      <div className="problem-head">
        <div className="problem-title">
          <CheckCircle2 size={19} />
          <div>
            <h3>{success.id}</h3>
            <p>Geprueft OK · {orderType}</p>
          </div>
        </div>
        <span className="severity-chip severity-ok">OK</span>
      </div>

      <p className="problem-copy">Diese Bestellung wurde vom Abrechnung-Bot ohne Abweichung bestaetigt.</p>

      <div className="problem-context">
        <span>
          <b>Bestelltyp</b>
          <strong>{orderType}</strong>
        </span>
        <span>
          <b>Hash / Bestellung</b>
          <strong>{success.id}</strong>
        </span>
        <span>
          <b>Status</b>
          <strong>OK</strong>
        </span>
      </div>

      <div className="problem-foot">
        <span>
          {success.id} · {orderType}
        </span>
        <time dateTime={run.finished_at || run.received_at}>{formatDateTime(run.finished_at || run.received_at)}</time>
      </div>
    </article>
  );
}

function ReturnCard({ returnItem, run }: Pick<ReturnWithRun, "returnItem" | "run">) {
  const displayId = returnItem.hash_id || returnItem.order_reference;
  const orderType = orderTypeLabel(returnItem.order_type);

  return (
    <article className={returnItem.found_in_bot ? "problem-card return-card found" : "problem-card return-card missing"}>
      <div className="problem-head">
        <div className="problem-title">
          <RotateCcw size={19} />
          <div>
            <h3>{displayId}</h3>
            <p>{returnItem.found_in_bot ? `RETURN mit Versand · ${orderType}` : "RETURN ohne Versand"}</p>
          </div>
        </div>
        <span className={returnItem.found_in_bot ? "severity-chip severity-ok" : "severity-chip severity-critical"}>
          {returnItem.found_in_bot ? "Mit Versand" : "Ohne Versand"}
        </span>
      </div>

      <p className="problem-copy">
        {returnItem.found_in_bot
          ? "Diese Retoure wurde im Bot gefunden. Das Versanddatum kommt aus dem Bot-Lauf."
          : returnProblemMessage(returnItem)}
      </p>

      <div className="problem-context">
        <span>
          <b>Bestelltyp</b>
          <strong>{orderType}</strong>
        </span>
        <span>
          <b>Gesendet laut Bot</b>
          <strong>{returnItem.sent_at ? formatDateTime(returnItem.sent_at) : "Nicht gefunden"}</strong>
        </span>
        <span>
          <b>Lieferpreis</b>
          <strong>{formatValue(returnItem.supply_price_base)}</strong>
        </span>
        {returnItem.billing_id ? (
          <span>
            <b>Abrechnungs-ID</b>
            <strong>{returnItem.billing_id}</strong>
          </span>
        ) : null}
        {returnItem.billing_type ? (
          <span>
            <b>Abrechnungstyp</b>
            <strong>{returnItem.billing_type}</strong>
          </span>
        ) : null}
        {returnItem.return_billing_date ? (
          <span>
            <b>Retouren-Datum</b>
            <strong>{formatDateTime(returnItem.return_billing_date)}</strong>
          </span>
        ) : null}
      </div>

      <div className="problem-foot">
        <span>
          {displayId} · {returnItem.found_in_bot ? "Bot gefunden" : "Bot fehlt"}
        </span>
        <time dateTime={run.finished_at || run.received_at}>{formatDateTime(run.finished_at || run.received_at)}</time>
      </div>
    </article>
  );
}

function runDisplayName(run: StoredVerificationRun) {
  if (run.id === "empty") {
    return "Noch kein Verifikationslauf";
  }

  return run.invoice_file || `Verifikation ${formatDateTime(run.finished_at || run.received_at)}`;
}

function problemDisplayId(problem: VerificationProblem) {
  return problem.hash_id || problem.order_reference;
}

function problemMessage(problem: VerificationProblem) {
  return problemMessages[problem.problem_type] || translateKnownProblemMessage(problem.problem) || problem.problem;
}

function returnProblemMessage(returnItem: VerificationReturn) {
  return (returnItem.problem ? translateKnownProblemMessage(returnItem.problem) : "") || returnItem.problem || problemMessages.return_order_not_found;
}

function translateKnownProblemMessage(message: string) {
  const normalized = normalizeSearchText(message);

  if (normalized.includes("billing row exists") && normalized.includes("no matching bot order")) {
    return problemMessages.missing_in_bot;
  }

  if (normalized.includes("bot order exists") && normalized.includes("no matching doktorabc_billing")) {
    return problemMessages.missing_in_billing;
  }

  if (normalized.includes("return exists in billing") && normalized.includes("no matching sent order")) {
    return problemMessages.return_order_not_found;
  }

  if (normalized.includes("eod bot order has no billing_date")) {
    return problemMessages.missing_eod_billing_date;
  }

  if (normalized.includes("self pickup bot order has no scraped_at")) {
    return problemMessages.missing_self_pickup_scraped_at;
  }

  if (normalized.includes("billing stock product name could not be mapped")) {
    return problemMessages.billing_product_name_not_found;
  }

  if (normalized.includes("different pzn than the bot pzn")) {
    return problemMessages.pzn_mismatch;
  }

  if (normalized.includes("billing quantity in grams does not match bot quantity")) {
    return problemMessages.quantity_mismatch;
  }

  if (normalized.includes("billing contains this product line")) {
    return problemMessages.product_missing_in_bot;
  }

  if (normalized.includes("no products_price row exists")) {
    return problemMessages.no_valid_price_at_billing_date;
  }

  if (normalized.includes("supply_price_base does not match") || normalized.includes("total_medication_cost_incl_vat does not match")) {
    return problemMessages.billing_total_mismatch;
  }

  if (normalized.includes("return row should have a negative")) {
    return problemMessages.wrong_return_sign;
  }

  if (normalized.includes("non-return row should have a positive")) {
    return problemMessages.wrong_positive_sign;
  }

  return "";
}

function orderTypeLabel(value: string | null | undefined) {
  const normalized = (value || "").trim().toLowerCase().replaceAll("_", " ");

  if (normalized === "eod") {
    return "EOD";
  }

  if (normalized === "self pickup" || normalized === "pickup" || normalized === "pickup ready") {
    return "Selbstabholung";
  }

  return value || "Unbekannt";
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

function normalizeSearchText(value: string) {
  return value.trim().toLowerCase();
}

function compactSearchText(value: string) {
  return normalizeSearchText(value).replace(/[^a-z0-9]/g, "");
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
