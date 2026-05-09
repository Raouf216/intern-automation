"use client";

import {
  AlertTriangle,
  ArrowRight,
  ChevronDown,
  CheckCircle2,
  Clock3,
  FileSpreadsheet,
  Inbox,
  Moon,
  RefreshCw,
  ShieldCheck,
  Sun,
  UploadCloud,
  Workflow,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import type { StoredNotification } from "@/lib/notifications";

type ConfigStatus = {
  configured: boolean;
  table: string;
  schema: string;
};

type SectionKey = "upload" | "doktorabc_sync" | "check_bot" | "abrechnung_verification";

type Props = {
  initialNotifications: StoredNotification[];
  initialError: string | null;
  config: ConfigStatus;
};

type SyncChangedProduct = {
  pzn: string;
  productName: string;
  changes: Array<{
    field: string;
    oldValue: string;
    newValue: string;
  }>;
};

type SyncNewProduct = {
  pzn: string;
  productName: string;
  values: Array<{
    label: string;
    value: string;
  }>;
};

type SyncInvalidExample = {
  title: string;
  message: string;
  values: Array<{
    label: string;
    value: string;
  }>;
};

type SyncDetails = {
  scraped: number;
  inserted: number;
  updated: number;
  unchanged: number;
  duration: string;
  error: string | null;
  changedProducts: SyncChangedProduct[];
  newProducts: SyncNewProduct[];
  invalidExamples: SyncInvalidExample[];
};

type OrderBotOrder = {
  orderReference: string;
  billingDate: string;
  createdDate: string;
  products: string;
  prices: string;
  quantities: string;
};

type OrderBotList = {
  id: string;
  label: string;
  orderCount: number;
  orders: OrderBotOrder[];
};

type OrderBotDetails = {
  kind: "orders" | "excel";
  label: string;
  orderCount: number;
  lists: OrderBotList[];
  filename: string;
  sizeBytes: number | null;
  sentToN8n: boolean;
  n8nStatusCode: number | null;
  excelRowCount: number | null;
  exportDate: string;
  failedStep: string;
  currentUrl: string;
  screenshotPath: string;
};

type BotCheckProblemSection = {
  type: string;
  label: string;
  count: number;
  problems: Array<Record<string, unknown>>;
};

type BotCheckDetails = {
  status: string;
  scrapedAt: string;
  checkedOrders: number;
  dbRowsChecked: number;
  excelRowsChecked: number;
  totalProblems: number;
  sections: BotCheckProblemSection[];
};

const sections: Array<{ label: string; value: SectionKey; description: string; caption: string; active: boolean }> = [
  {
    label: "Upload",
    value: "upload",
    description: "Upload-Meldungen",
    caption: "OED und DoktorABC Abrechnung",
    active: true,
  },
  {
    label: "DoktorABC Sync",
    value: "doktorabc_sync",
    description: "Produktsynchronisierung",
    caption: "Button-Auslöser für DoktorABC",
    active: true,
  },
  {
    label: "Bot Check",
    value: "check_bot",
    description: "EOD Abgleich",
    caption: "Excel gegen Supabase",
    active: true,
  },
  {
    label: "Abrechnung Verifikation",
    value: "abrechnung_verification",
    description: "Abrechnung-Verifikation",
    caption: "Prüfung und Ergebnisprotokoll",
    active: true,
  },
];

export function NotificationDashboard({ initialNotifications, initialError }: Props) {
  const [notifications, setNotifications] = useState(initialNotifications);
  const [activeSection, setActiveSection] = useState<SectionKey>("upload");
  const [loadError, setLoadError] = useState<string | null>(initialError);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [theme, setTheme] = useState<"light" | "night">("light");

  useEffect(() => {
    const savedTheme = window.localStorage.getItem("notification-app-theme");
    const nextTheme = savedTheme === "night" ? "night" : "light";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
  }, []);

  useEffect(() => {
    const savedSection = window.localStorage.getItem("notification-app-section");
    if (isSectionKey(savedSection)) {
      setActiveSection(savedSection);
    }
  }, []);

  useEffect(() => {
    let isMounted = true;
    let refreshInFlight = false;

    async function refreshNotifications() {
      if (refreshInFlight) {
        return;
      }

      refreshInFlight = true;

      try {
        const response = await fetch("/api/notifications", { cache: "no-store" });
        const payload = (await response.json()) as {
          ok?: boolean;
          error?: string;
          notifications?: StoredNotification[];
        };

        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || `HTTP ${response.status}`);
        }

        if (!isMounted) {
          return;
        }

        setNotifications(payload.notifications || []);
        setLoadError(null);
        setLastRefresh(new Date());
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
        void refreshNotifications();
      }
    }

    void refreshNotifications();
    const interval = window.setInterval(refreshNotifications, 5000);
    window.addEventListener("focus", refreshWhenVisible);
    document.addEventListener("visibilitychange", refreshWhenVisible);

    return () => {
      isMounted = false;
      window.clearInterval(interval);
      window.removeEventListener("focus", refreshWhenVisible);
      document.removeEventListener("visibilitychange", refreshWhenVisible);
    };
  }, []);

  function toggleTheme() {
    const nextTheme = theme === "night" ? "light" : "night";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    window.localStorage.setItem("notification-app-theme", nextTheme);
  }

  function selectSection(section: SectionKey) {
    setActiveSection(section);
    window.localStorage.setItem("notification-app-section", section);
  }

  const sectionCounts = useMemo(
    () => {
      const displayNotifications = notifications.filter((notification) => !isNoisyUploadPlaceholder(notification));

      return sections.reduce(
        (counts, section) => {
          counts[section.value] = displayNotifications.filter((notification) => notification.section === section.value).length;
          return counts;
        },
        { upload: 0, doktorabc_sync: 0, check_bot: 0, abrechnung_verification: 0 } as Record<SectionKey, number>
      );
    },
    [notifications]
  );

  const displayNotifications = notifications.filter((notification) => !isNoisyUploadPlaceholder(notification));
  const visibleNotifications = displayNotifications.filter((notification) => notification.section === activeSection);
  const uploadNotifications = displayNotifications.filter((notification) => notification.section === "upload");
  const successCount = uploadNotifications.filter((notification) => notification.status === "success").length;
  const failureCount = uploadNotifications.filter((notification) => notification.status === "failure").length;
  const pendingCount = uploadNotifications.filter((notification) => notification.status === "triggered").length;
  const activeSectionMeta = sections.find((section) => section.value === activeSection) || sections[0];

  return (
    <main className="page-shell">
      <header className="topbar">
        <div className="brand">
          <NotifyBrandMark />
          <div>
            <p className="eyebrow">Apothekenbetrieb</p>
            <h1>Benachrichtigungen</h1>
            <p className="subtitle">Zentrale Übersicht für operative Systemmeldungen</p>
          </div>
        </div>
        <div className="topbar-actions">
          <button className="theme-toggle" type="button" onClick={toggleTheme} aria-pressed={theme === "night"}>
            {theme === "night" ? <Sun size={16} /> : <Moon size={16} />}
            <span>{theme === "night" ? "Hell" : "Nacht"}</span>
          </button>
        </div>
      </header>

      <section className="overview-band" aria-label="Upload-Übersicht">
        <div className="metric-block success">
          <span>Erfolgreiche Uploads</span>
          <strong>{successCount}</strong>
        </div>
        <div className="metric-block">
          <span>Gestartete Uploads</span>
          <strong>{pendingCount}</strong>
        </div>
        <div className="metric-block danger">
          <span>Fehlgeschlagene Uploads</span>
          <strong>{failureCount}</strong>
        </div>
      </section>

      <div className="workspace">
        <aside className="section-nav" aria-label="Meldungsbereiche">
          <div className="section-nav-title">
            <RefreshCw size={17} />
            <span>Bereiche</span>
          </div>
          <nav>
            {sections.map((section) => (
              <button
                type="button"
                className={section.value === activeSection ? "nav-item active" : "nav-item"}
                onClick={() => selectSection(section.value)}
                key={section.value}
              >
                <span>
                  <b>{section.label}</b>
                  <small>{section.caption}</small>
                </span>
                <strong>{sectionCounts[section.value]}</strong>
              </button>
            ))}
          </nav>
          <div className="refresh-note">
            <Clock3 size={15} />
            <span>{lastRefresh ? `Aktualisiert ${formatRelativeTime(lastRefresh.toISOString())}` : "Auto-Refresh aktiv"}</span>
          </div>
        </aside>

        <section className="feed" aria-label={`${activeSectionMeta.label} Meldungen`}>
          <div className="feed-header">
            <div>
              <p className="section-kicker">{activeSectionMeta.label}</p>
              <h2>{activeSectionMeta.description}</h2>
              <p className="section-copy">{activeSectionMeta.caption}</p>
            </div>
            <div className={activeSectionMeta.active ? "section-status active" : "section-status planned"}>
              {activeSectionMeta.active ? <ShieldCheck size={15} /> : <Workflow size={15} />}
              <span>{activeSectionMeta.active ? "Aktiv" : "Vorbereitet"}</span>
            </div>
          </div>

          {loadError ? (
            <div className="empty-state error">
              <AlertTriangle size={24} />
              <h3>Benachrichtigungen konnten nicht geladen werden</h3>
              <p>{loadError}</p>
            </div>
          ) : visibleNotifications.length ? (
            <div className="notification-list">
              {visibleNotifications.map((notification) => (
                <NotificationRow notification={notification} key={notification.id} />
              ))}
            </div>
          ) : (
            <div className="empty-state compact">
              <Inbox size={28} />
              <h3>Noch keine Meldungen</h3>
              <p>{emptyCopy(activeSection)}</p>
            </div>
          )}
        </section>

        <aside className="operations-panel" aria-label="Systeminformationen">
          <div className="panel-heading">
            <FileSpreadsheet size={18} />
            <span>Arbeitsbereiche</span>
          </div>
          <p className="panel-copy">
            Uploads, DoktorABC Sync, Bot Check und Abrechnung-Verifikation bleiben getrennt. Der gewählte Bereich bleibt nach dem Aktualisieren geöffnet.
          </p>
        </aside>
      </div>
    </main>
  );
}

function NotifyBrandMark() {
  return (
    <div className="brand-mark pharmacy-logo" role="img" aria-label="Medizinische Cannabis-Apotheke">
      <svg className="notify-brand-svg" viewBox="0 0 64 64" aria-hidden="true" focusable="false">
        <g className="notify-molecule" aria-hidden="true">
          <path d="M15.6 22.4 23.7 17.7l8.1 4.7v9.3l-8.1 4.7-8.1-4.7zM31.8 22.4l8.2-4.7 8.1 4.7v9.3L40 36.4l-8.2-4.7" />
          <circle cx="15.6" cy="22.4" r="2.2" />
          <circle cx="23.7" cy="17.7" r="2.2" />
          <circle cx="40" cy="17.7" r="2.2" />
          <circle cx="48.1" cy="31.7" r="2.2" />
        </g>
        <g className="notify-tincture">
          <path className="notify-dropper" d="M27 8.8h12.2v7.4H27zM30.8 16.2h4.6v8.1" />
          <rect className="notify-bottle" x="18.6" y="21.2" width="28" height="34.4" rx="7.5" />
          <path className="notify-bottle-shine" d="M25 24.8h5.2c-3 5.2-3.8 14.1-2.5 26.7h-2.3c-2.2 0-4-1.9-4-4.4V29c0-2.3 1.6-4.2 3.6-4.2Z" />
          <path className="notify-oil-drop" d="M32.7 23.8c2.8 2.9 4.2 5.3 4.2 7.3a4.2 4.2 0 0 1-8.4 0c0-2 1.4-4.4 4.2-7.3Z" />
          <rect className="notify-cbd-label" x="20.6" y="30" width="24" height="16.2" rx="4.4" />
          <text className="notify-cbd-text" x="32.6" y="41" textAnchor="middle">CBD</text>
        </g>
        <rect className="notify-alert-badge" x="39" y="38.5" width="20" height="20" rx="6" />
        <path className="notify-alert-cross" d="M49 43.9v9.2M44.4 48.5h9.2" />
      </svg>
    </div>
  );
}

function isSectionKey(value: string | null): value is SectionKey {
  return value === "upload" || value === "doktorabc_sync" || value === "check_bot" || value === "abrechnung_verification";
}

function NotificationRow({ notification }: { notification: StoredNotification }) {
  const rowsInserted = rowsInsertedFromPayload(notification.payload);
  const botCheckDetails = botCheckDetailsFromPayload(notification.payload);
  const syncDetails = syncDetailsFromPayload(notification.payload);
  const orderBotDetails = orderBotDetailsFromPayload(notification.payload);
  const showUploadDetails = shouldShowUploadDetails(notification);
  const notificationMessage =
    orderBotDetails?.kind === "excel" && notification.status !== "failure"
      ? excelMessageFromDetails(orderBotDetails)
      : notification.message;

  return (
    <article className={`notification-row status-${notification.status}`}>
      <div className="status-icon" aria-hidden="true">
        {notification.status === "success" ? (
          <CheckCircle2 size={18} />
        ) : notification.status === "failure" || notification.status === "warning" ? (
          <AlertTriangle size={18} />
        ) : (
          <UploadCloud size={18} />
        )}
      </div>

      <div className="notification-main">
        <div className="notification-title-row">
          <div>
            <h3>{notification.title}</h3>
            <p>{notificationMessage}</p>
          </div>
          <div className="notification-meta">
            <span className={`status-chip chip-${notification.status}`}>{formatStatus(notification.status)}</span>
            <time className="notification-time" dateTime={notification.created_at}>
              <span>{formatRelativeTime(notification.created_at)}</span>
              <small>{formatExactDateTime(notification.created_at)}</small>
            </time>
          </div>
        </div>
        {botCheckDetails ? (
          <dl className="detail-grid bot-check-grid">
            <div>
              <dt>Geprüfte Orders</dt>
              <dd>{botCheckDetails.checkedOrders}</dd>
            </div>
            <div>
              <dt>Excel-Zeilen</dt>
              <dd>{botCheckDetails.excelRowsChecked}</dd>
            </div>
            <div>
              <dt>DB-Zeilen</dt>
              <dd>{botCheckDetails.dbRowsChecked}</dd>
            </div>
            <div>
              <dt>{notification.status === "success" ? "Ergebnis" : "Probleme"}</dt>
              <dd>{notification.status === "success" ? "Keine Abweichung" : botCheckDetails.totalProblems}</dd>
            </div>
          </dl>
        ) : orderBotDetails ? (
          <dl className="detail-grid order-grid">
            <div>
              <dt>Bereich</dt>
              <dd>{orderBotDetails.label}</dd>
            </div>
            <div>
              <dt>{orderBotDetails.kind === "excel" ? "Excel-Zeilen" : "Orders"}</dt>
              <dd>{orderBotDetails.kind === "excel" ? formatNumber(orderBotDetails.excelRowCount) : orderCountSummary(orderBotDetails.lists)}</dd>
            </div>
            <div>
              <dt>{orderBotDetails.kind === "excel" ? "Datum" : "Status"}</dt>
              <dd>
                {orderBotDetails.kind === "excel"
                  ? orderBotDetails.exportDate || "nicht angegeben"
                  : formatStatus(notification.status)}
              </dd>
            </div>
          </dl>
        ) : syncDetails ? (
          <dl className="detail-grid sync-grid">
            <div>
              <dt>Geprüft</dt>
              <dd>{syncDetails.scraped}</dd>
            </div>
            <div>
              <dt>Neu</dt>
              <dd>{syncDetails.inserted}</dd>
            </div>
            <div>
              <dt>Geändert</dt>
              <dd>{syncDetails.updated}</dd>
            </div>
            <div>
              <dt>Unverändert</dt>
              <dd>{syncDetails.unchanged}</dd>
            </div>
            <div>
              <dt>Dauer</dt>
              <dd>{syncDetails.duration}</dd>
            </div>
          </dl>
        ) : showUploadDetails ? (
          <dl className="detail-grid">
            <div>
              <dt>Datei</dt>
              <dd>{notification.filename || "nicht angegeben"}</dd>
            </div>
            <div>
              <dt>Typ</dt>
              <dd>{formatUploadType(notification.upload_type)}</dd>
            </div>
            <div>
              <dt>Größe</dt>
              <dd>{formatBytes(notification.size_bytes)}</dd>
            </div>
            {rowsInserted === null ? null : (
              <div>
                <dt>DB-Zeilen</dt>
                <dd>{rowsInserted}</dd>
              </div>
            )}
          </dl>
        ) : null}
        {botCheckDetails ? (
          <BotCheckLogDetails details={botCheckDetails} status={notification.status} />
        ) : orderBotDetails ? (
          orderBotDetails.kind === "orders" && notification.status !== "failure" ? (
            <div className="order-toggle-stack">
              {orderBotDetails.lists.map((list) => (
                <details className="sync-log-panel success" key={list.id}>
                  <summary>
                    <ChevronDown size={15} />
                    <span>
                      {list.label}: {list.orderCount} Order IDs anzeigen
                    </span>
                  </summary>
                  <OrderBotOrderList list={list} />
                </details>
              ))}
            </div>
          ) : (
            <details className={`sync-log-panel ${notification.status === "failure" ? "danger" : "success"}`}>
              <summary>
                <ChevronDown size={15} />
                <span>{notification.status === "failure" ? "Fehlerdetails anzeigen" : "Excel Export Details anzeigen"}</span>
              </summary>
              <OrderBotLogDetails details={orderBotDetails} status={notification.status} error={notification.error} />
            </details>
          )
        ) : syncDetails ? (
          <details className={`sync-log-panel ${notification.status === "failure" ? "danger" : "success"}`}>
            <summary>
              <ChevronDown size={15} />
              <span>{notification.status === "failure" ? "Fehlerdetails anzeigen" : "Änderungsprotokoll anzeigen"}</span>
            </summary>
            <SyncLogDetails details={syncDetails} status={notification.status} error={notification.error} />
          </details>
        ) : null}
        {notification.error && !syncDetails ? <p className="error-line">{notification.error}</p> : null}
      </div>
    </article>
  );
}

function BotCheckLogDetails({
  details,
  status,
}: {
  details: BotCheckDetails;
  status: StoredNotification["status"];
}) {
  if (status === "success" || details.totalProblems === 0) {
    return null;
  }

  return (
    <div className="order-toggle-stack bot-check-toggle-stack">
      {details.sections.map((section) => (
        <details className={`sync-log-panel ${section.count > 0 ? "danger" : "success"}`} key={section.type}>
          <summary>
            <ChevronDown size={15} />
            <span>
              {section.label}: {section.count} {section.count === 1 ? "Fall" : "Fälle"} anzeigen
            </span>
          </summary>
          <BotCheckProblemList section={section} />
        </details>
      ))}
    </div>
  );
}

function BotCheckProblemList({ section }: { section: BotCheckProblemSection }) {
  if (!section.problems.length) {
    return (
      <div className="sync-log-content">
        <p className="sync-empty-log">Keine Fälle in dieser Gruppe.</p>
      </div>
    );
  }

  return (
    <div className="sync-log-content">
      <section className="sync-product-section" aria-label={section.label}>
        <h4>
          {section.label}: {section.count} {section.count === 1 ? "Fall" : "Fälle"}
        </h4>
        <div className="bot-check-list">
          {section.problems.map((problem, index) => (
            <BotCheckProblemCard problem={problem} sectionType={section.type} key={`${section.type}-${stringValue(problem.order_reference)}-${index}`} />
          ))}
        </div>
      </section>
    </div>
  );
}

function BotCheckProblemCard({
  problem,
  sectionType,
}: {
  problem: Record<string, unknown>;
  sectionType: string;
}) {
  const orderReference = stringValue(problem.order_reference);
  const message = stringValue(problem.message) || botCheckDefaultProblemMessage(sectionType);
  const excelData = recordValue(problem.excel_data);
  const dbData = recordValue(problem.db_data);
  const issues = arrayRecordValue(problem.issues);
  const issueCount = nullableNumberValue(problem.issue_count) ?? issues.length;

  return (
    <article className="sync-product-card bot-check-card">
      <div className="sync-product-head">
        <strong>{orderReference || "Order ID fehlt"}</strong>
        <span>{botCheckSectionLabel(sectionType)}</span>
      </div>
      <p className="bot-check-message">{message}</p>
      <div className="sync-product-meta compact bot-check-meta">
        <span>
          <b>Order</b>
          <strong>{orderReference || "nicht angegeben"}</strong>
        </span>
        <span>
          <b>Typ</b>
          <strong>{botCheckSectionLabel(sectionType)}</strong>
        </span>
        <span>
          <b>Issues</b>
          <strong>{issueCount}</strong>
        </span>
      </div>
      <div className="bot-check-snapshots">
        <BotCheckDataSnapshot label="Excel" row={excelData} />
        <BotCheckDataSnapshot label="Supabase" row={dbData} />
      </div>
      {issues.length ? (
        <section className="sync-product-section" aria-label="Abweichungen">
          <h4>Abweichungen</h4>
          <div className="sync-change-list">
            {issues.map((issue, index) => {
              const formatted = formatBotCheckIssue(issue);

              return (
                <div className="sync-change-row bot-check-issue-row" key={`${formatted.label}-${index}`}>
                  <span>{formatted.label}</span>
                  <code>{formatted.excelValue}</code>
                  <ArrowRight size={14} />
                  <code>{formatted.dbValue}</code>
                </div>
              );
            })}
          </div>
        </section>
      ) : null}
    </article>
  );
}

function BotCheckDataSnapshot({ label, row }: { label: string; row: Record<string, unknown> | null }) {
  if (!row) {
    return null;
  }

  const values = botCheckDataValues(row);

  if (!values.length) {
    return null;
  }

  return (
    <div className="bot-check-snapshot">
      <strong>{label}</strong>
      <div className="sync-product-meta compact">
        {values.map((value) => (
          <span key={`${label}-${value.label}`}>
            <b>{value.label}</b>
            <strong>{value.value}</strong>
          </span>
        ))}
      </div>
    </div>
  );
}

function OrderBotLogDetails({
  details,
  status,
  error,
}: {
  details: OrderBotDetails;
  status: StoredNotification["status"];
  error: string | null;
}) {
  if (status === "failure") {
    return (
      <div className="sync-log-content">
        <div className="sync-error-card">
          <strong>Fehler</strong>
          <span>{error || "Der Orders Bot konnte nicht abgeschlossen werden."}</span>
        </div>
        {details.failedStep ? (
          <div className="sync-product-meta compact">
            <span>
              <b>Schritt</b>
              <strong>{details.failedStep}</strong>
            </span>
            {details.currentUrl ? (
              <span>
                <b>URL</b>
                <strong>{details.currentUrl}</strong>
              </span>
            ) : null}
            {details.screenshotPath ? (
              <span>
                <b>Screenshot</b>
                <strong>{details.screenshotPath}</strong>
              </span>
            ) : null}
          </div>
        ) : null}
      </div>
    );
  }

  if (details.kind === "excel") {
    return (
      <div className="sync-log-content">
        <section className="sync-product-section" aria-label="Excel Export">
          <h4>Excel Export</h4>
          <div className="sync-product-meta order-export-meta">
            <span>
              <b>Zeilen</b>
              <strong>{formatNumber(details.excelRowCount)}</strong>
            </span>
            <span>
              <b>Datum</b>
              <strong>{details.exportDate || "nicht angegeben"}</strong>
            </span>
            <span>
              <b>Datei</b>
              <strong>{details.filename || "nicht angegeben"}</strong>
            </span>
            <span>
              <b>Größe</b>
              <strong>{formatBytes(details.sizeBytes)}</strong>
            </span>
            <span>
              <b>n8n</b>
              <strong>{details.sentToN8n ? `gesendet (${details.n8nStatusCode || "OK"})` : "nicht gesendet"}</strong>
            </span>
          </div>
        </section>
      </div>
    );
  }

  const [firstList] = details.lists;
  return firstList ? <OrderBotOrderList list={firstList} /> : null;
}

function OrderBotOrderList({ list }: { list: OrderBotList }) {
  if (!list.orders.length) {
    return (
      <div className="sync-log-content">
        <p className="sync-empty-log">Keine Order IDs für diesen Bereich im Payload gespeichert.</p>
      </div>
    );
  }

  return (
    <div className="sync-log-content">
      <section className="sync-product-section" aria-label={`${list.label} Order IDs`}>
        <h4>
          {list.label}: {list.orderCount} Orders
        </h4>
        <div className="order-bot-list">
          {list.orders.map((order, index) => (
            <article className="order-bot-card" key={`${order.orderReference}-${index}`}>
              <div className="sync-product-head">
                <strong>{order.orderReference || "Order ID fehlt"}</strong>
                <span>{formatOrderBotDate(order)}</span>
              </div>
              <div className="order-bot-products">
                <span>{order.products || "Produkte fehlen"}</span>
                <strong>{order.prices || "Preis fehlt"}</strong>
                {order.quantities ? <small>{order.quantities}</small> : null}
              </div>
            </article>
          ))}
        </div>
      </section>
    </div>
  );
}

function SyncLogDetails({
  details,
  status,
  error,
}: {
  details: SyncDetails;
  status: StoredNotification["status"];
  error: string | null;
}) {
  const visibleError = error || details.error;
  const hasProductDetails =
    details.changedProducts.length > 0 || details.newProducts.length > 0 || details.invalidExamples.length > 0;

  return (
    <div className="sync-log-content">
      {status === "failure" && visibleError ? (
        <div className="sync-error-card">
          <strong>Fehler</strong>
          <span>{visibleError}</span>
        </div>
      ) : null}

      {details.changedProducts.length ? (
        <section className="sync-product-section" aria-label="Geänderte Produkte">
          <h4>Geänderte Produkte</h4>
          <div className="sync-product-list">
            {details.changedProducts.map((product) => (
              <article className="sync-product-card" key={`changed-${product.pzn}-${product.productName}`}>
                <div className="sync-product-head">
                  <strong>{product.productName}</strong>
                  <span>{product.pzn ? `PZN ${product.pzn}` : "PZN fehlt"}</span>
                </div>
                <div className="sync-change-list">
                  {product.changes.map((change) => (
                    <div className="sync-change-row" key={`${product.pzn}-${change.field}`}>
                      <span>{change.field}</span>
                      <code>{change.oldValue}</code>
                      <ArrowRight size={14} />
                      <code>{change.newValue}</code>
                    </div>
                  ))}
                </div>
              </article>
            ))}
          </div>
        </section>
      ) : null}

      {details.newProducts.length ? (
        <section className="sync-product-section" aria-label="Neue Produkte">
          <h4>Neue Produkte</h4>
          <div className="sync-product-list">
            {details.newProducts.map((product) => (
              <article className="sync-product-card new" key={`new-${product.pzn}-${product.productName}`}>
                <div className="sync-product-head">
                  <strong>{product.productName}</strong>
                  <span>{product.pzn ? `PZN ${product.pzn}` : "PZN fehlt"}</span>
                </div>
                {product.values.length ? (
                  <div className="sync-product-meta">
                    {product.values.map((value) => (
                      <span key={`${product.pzn}-${value.label}`}>
                        <b>{value.label}</b>
                        <strong>{value.value}</strong>
                      </span>
                    ))}
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        </section>
      ) : null}

      {details.invalidExamples.length ? (
        <section className="sync-product-section" aria-label="Problematische Zeilen">
          <h4>Problematische Zeilen</h4>
          <div className="sync-invalid-list">
            {details.invalidExamples.map((example, index) => (
              <div className="sync-invalid-row" key={`${example.title}-${index}`}>
                <strong>{example.title}</strong>
                <span>{example.message}</span>
                {example.values.length ? (
                  <div className="sync-product-meta compact">
                    {example.values.map((value) => (
                      <span key={`${example.title}-${value.label}`}>
                        <b>{value.label}</b>
                        <strong>{value.value}</strong>
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        </section>
      ) : null}

      {!hasProductDetails && status !== "failure" ? (
        <p className="sync-empty-log">Keine Einzelprodukte im Protokoll. Die Zusammenfassung oben ist gespeichert.</p>
      ) : null}
      {!hasProductDetails && status === "failure" && !visibleError ? (
        <p className="sync-empty-log">Der Sync ist fehlgeschlagen, aber der Bot hat keine Details mitgesendet.</p>
      ) : null}
    </div>
  );
}

function emptyCopy(section: SectionKey) {
  if (section === "doktorabc_sync") {
    return "Sobald Produktsync oder EOD/Self-Pickup Botmeldungen eintreffen, werden sie hier angezeigt.";
  }

  if (section === "check_bot") {
    return "Sobald der EOD Abgleich läuft, werden Erfolgsmeldungen und Abweichungen hier angezeigt.";
  }

  if (section === "abrechnung_verification") {
    return "Sobald die Abrechnung-Verifikation aktiv ist, werden Ergebnisse und Fehler hier dokumentiert.";
  }

  return "Sobald Upload-Ereignisse eintreffen, erscheinen sie hier mit der neuesten Meldung zuerst.";
}

function formatStatus(value: string) {
  if (value === "success") {
    return "Erfolgreich";
  }

  if (value === "failure") {
    return "Fehler";
  }

  if (value === "warning") {
    return "Prüfen";
  }

  if (value === "triggered") {
    return "Gestartet";
  }

  return "Info";
}

function formatUploadType(value: string | null) {
  if (value === "doktorabc_abrechnung") {
    return "DoktorABC Abrechnung";
  }

  if (value === "doktorabc_eod_excel_export") {
    return "DoktorABC Excel Export";
  }

  if (value === "oed") {
    return "OED";
  }

  return value || "Upload";
}

function formatBytes(value: number | null) {
  if (!value || value <= 0) {
    return "nicht angegeben";
  }

  if (value < 1024 * 1024) {
    return `${Math.round(value / 1024)} KB`;
  }

  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

function formatNumber(value: number | null) {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "nicht angegeben";
}

function adjustedExcelRowCount(value: number | null) {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(value - 1, 0) : null;
}

function excelMessageFromDetails(details: OrderBotDetails) {
  const rows = formatNumber(details.excelRowCount);
  const exportDate = details.exportDate || "heute";

  if (details.sentToN8n) {
    return `${rows} Excel-Zeilen am ${exportDate} exportiert und an n8n gesendet.`;
  }

  return `${rows} Excel-Zeilen am ${exportDate} exportiert.`;
}

function orderCountSummary(lists: OrderBotList[]) {
  if (!lists.length) {
    return "nicht angegeben";
  }

  return lists.map((list) => `${list.label}: ${list.orderCount}`).join(", ");
}

function rowsInsertedFromPayload(payload: Record<string, unknown>) {
  return typeof payload.rows_inserted === "number" ? payload.rows_inserted : null;
}

function shouldShowUploadDetails(notification: StoredNotification) {
  const event = stringValue(notification.payload.event) || notification.event;
  return !(
    notification.upload_type === "doktorabc_abrechnung" &&
    notification.status === "success" &&
    event === "upload_success"
  );
}

function isNoisyUploadPlaceholder(notification: StoredNotification) {
  const event = stringValue(notification.payload.event) || notification.event;
  const filename = notification.filename || stringValue(notification.payload.filename);
  const uploadType = notification.upload_type || stringValue(notification.payload.upload_type);

  return (
    notification.section === "upload" &&
    notification.status === "info" &&
    event === "upload_info" &&
    (uploadType === "upload" || !uploadType) &&
    (!filename || filename === "unknown-file")
  );
}

const botCheckSectionMeta = [
  {
    type: "missing_in_db",
    label: "Fehlt in Supabase",
  },
  {
    type: "missing_in_excel",
    label: "Fehlt in Excel",
  },
  {
    type: "data_mismatch",
    label: "Datenabweichung",
  },
];

function botCheckDetailsFromPayload(payload: Record<string, unknown>): BotCheckDetails | null {
  const source = botCheckPayloadFromPayload(payload);
  const event = stringValue(source.event);
  const isBotCheck =
    source.section === "check_bot" ||
    source.section === "bot_check" ||
    event === "check_success" ||
    event === "check_failure" ||
    Array.isArray(source.ordered_problem_sections);

  if (!isBotCheck) {
    return null;
  }

  const rawSections = arrayRecordValue(source.ordered_problem_sections);
  const sections = botCheckSectionsFromPayload(rawSections);
  const sectionProblemCount = sections.reduce((sum, section) => sum + section.count, 0);

  return {
    status: stringValue(source.status),
    scrapedAt: stringValue(source.scraped_at),
    checkedOrders: numberValue(source.checked_orders),
    dbRowsChecked: numberValue(source.db_rows_checked),
    excelRowsChecked: numberValue(source.excel_rows_checked),
    totalProblems: nullableNumberValue(source.total_problems) ?? sectionProblemCount,
    sections,
  };
}

function botCheckPayloadFromPayload(payload: Record<string, unknown>) {
  const nestedPayload = recordValue(payload.payload);

  if (!nestedPayload) {
    return payload;
  }

  const hasBotCheckWrapper =
    payload.section === "check_bot" ||
    payload.section === "bot_check" ||
    Array.isArray(nestedPayload.ordered_problem_sections) ||
    stringValue(payload.event).startsWith("check_");

  if (!hasBotCheckWrapper) {
    return payload;
  }

  return {
    ...nestedPayload,
    section: stringValue(payload.section) || stringValue(nestedPayload.section),
    event: stringValue(payload.event) || stringValue(nestedPayload.event),
    status: stringValue(payload.status) || stringValue(nestedPayload.status),
  };
}

function botCheckSectionsFromPayload(rawSections: Record<string, unknown>[]): BotCheckProblemSection[] {
  const mainSections = botCheckSectionMeta.map((meta) => {
    const rawSection = rawSections.find((section) => stringValue(section.type) === meta.type);
    const problems = arrayRecordValue(rawSection?.problems);

    return {
      ...meta,
      count: nullableNumberValue(rawSection?.count) ?? problems.length,
      problems,
    };
  });
  const mainTypes = new Set(botCheckSectionMeta.map((section) => section.type));
  const extraSections = rawSections
    .filter((section) => {
      const type = stringValue(section.type);
      return type && !mainTypes.has(type);
    })
    .map((section) => {
      const type = stringValue(section.type);
      const problems = arrayRecordValue(section.problems);

      return {
        type,
        label: botCheckSectionLabel(type),
        count: nullableNumberValue(section.count) ?? problems.length,
        problems,
      };
    })
    .filter((section) => section.count > 0 || section.problems.length > 0);

  return [...mainSections, ...extraSections];
}

function botCheckSectionLabel(type: string) {
  if (type === "missing_in_db") {
    return "Fehlt in Supabase";
  }

  if (type === "missing_in_excel") {
    return "Fehlt in Excel";
  }

  if (type === "data_mismatch") {
    return "Datenabweichung";
  }

  if (type === "invalid_excel_row") {
    return "Ungültige Excel-Zeile";
  }

  return type || "Problem";
}

function botCheckDefaultProblemMessage(type: string) {
  if (type === "missing_in_db") {
    return "Order existiert in Excel, aber nicht in Supabase.";
  }

  if (type === "missing_in_excel") {
    return "Order existiert in Supabase, aber nicht in Excel.";
  }

  if (type === "data_mismatch") {
    return "Order existiert in beiden Systemen, aber einzelne Werte unterscheiden sich.";
  }

  return "Problem im Bot Check.";
}

function botCheckDataValues(row: Record<string, unknown>) {
  return [
    ["Order", row.order_reference],
    ["Produkte", row.products],
    ["PZNs", row.pzns],
    ["Mengen", row.quantities],
    ["Scraped at", row.scraped_at],
  ]
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .map(([label, value]) => ({
      label: String(label),
      value: formatLogValue(value),
    }));
}

function formatBotCheckIssue(issue: Record<string, unknown>) {
  const problem = stringValue(issue.problem);
  const field = stringValue(issue.field);
  const position = nullableNumberValue(issue.position);
  const label = [botCheckIssueLabel(problem || field), position ? `Pos. ${position}` : ""].filter(Boolean).join(" - ");

  if (problem.endsWith("_count_mismatch")) {
    return {
      label,
      excelValue: formatCountMismatchSide(issue.excel_count, issue.excel_values),
      dbValue: formatCountMismatchSide(issue.db_count, issue.db_values),
    };
  }

  if (problem === "product_pzn_pair_mismatch") {
    return {
      label,
      excelValue: formatProductPznPair(issue.excel_pair),
      dbValue: formatProductPznPair(issue.db_pair),
    };
  }

  return {
    label,
    excelValue: formatLogValue(issue.excel_value ?? issue.excel_values ?? issue.excel_pair),
    dbValue: formatLogValue(issue.db_value ?? issue.db_values ?? issue.db_pair),
  };
}

function botCheckIssueLabel(problem: string) {
  if (problem === "products_count_mismatch") {
    return "Produktanzahl";
  }

  if (problem === "products_value_mismatch") {
    return "Produktwert";
  }

  if (problem === "pzns_count_mismatch") {
    return "PZN-Anzahl";
  }

  if (problem === "pzns_value_mismatch") {
    return "PZN-Wert";
  }

  if (problem === "product_pzn_pair_mismatch") {
    return "Produkt/PZN Paar";
  }

  return problem || "Abweichung";
}

function formatCountMismatchSide(count: unknown, values: unknown) {
  const countText = typeof count === "number" && Number.isFinite(count) ? String(count) : "nicht angegeben";
  const valueText = Array.isArray(values) ? values.map(formatLogValue).join(" | ") : "";

  return valueText ? `${countText}: ${valueText}` : countText;
}

function formatProductPznPair(value: unknown) {
  const pair = recordValue(value);

  if (!pair) {
    return formatLogValue(value);
  }

  return `${stringValue(pair.product) || "Produkt fehlt"} -> ${stringValue(pair.pzn) || "PZN fehlt"}`;
}

function syncDetailsFromPayload(payload: Record<string, unknown>) {
  const event = typeof payload.event === "string" ? payload.event : "";
  const isSync =
    payload.section === "doktorabc_sync" ||
    payload.sync_type === "doktorabc_products" ||
    event === "doktorabc_sync_success" ||
    event === "doktorabc_sync_failure";

  if (!isSync) {
    return null;
  }

  const summary = recordValue(payload.summary);
  const logs = recordValue(payload.logs) || payload;

  return {
    scraped: numberValue(summary?.scraped ?? logs.scraped),
    inserted: numberValue(summary?.inserted ?? logs.inserted),
    updated: numberValue(summary?.updated ?? logs.updated),
    unchanged: numberValue(summary?.unchanged ?? logs.unchanged),
    duration: formatDuration(payload.duration_ms),
    error: stringValue(payload.error) || stringValue(logs.error),
    changedProducts: changedProductsFromValue(logs.changed_products),
    newProducts: newProductsFromValue(logs.new_products),
    invalidExamples: invalidExamplesFromValue(logs.invalid_examples ?? logs.invalid_rows ?? logs.failed_rows),
  };
}

function orderBotDetailsFromPayload(payload: Record<string, unknown>): OrderBotDetails | null {
  const event = stringValue(payload.event);
  const isOrderBot =
    payload.section === "doktorabc_orders" ||
    (payload.section === "doktorabc_sync" && payload.sync_type === "doktorabc_eod_bot") ||
    (payload.section === "upload" && payload.sync_type === "doktorabc_eod_bot") ||
    payload.sync_type === "doktorabc_eod_bot" ||
    payload.upload_type === "doktorabc_eod_excel_export" ||
    event === "doktorabc_eod_pickup_orders_success" ||
    event === "doktorabc_eod_orders_success" ||
    event === "doktorabc_eod_orders_failure" ||
    event === "doktorabc_pickup_ready_orders_success" ||
    event === "doktorabc_pickup_ready_orders_failure" ||
    event === "doktorabc_eod_excel_export_success" ||
    event === "doktorabc_eod_excel_export_failure";

  if (!isOrderBot) {
    return null;
  }

  const orderListType = stringValue(payload.order_list_type);
  const isExcel =
    orderListType === "excel_export" ||
    payload.upload_type === "doktorabc_eod_excel_export" ||
    event === "doktorabc_eod_excel_export_success" ||
    event === "doktorabc_eod_excel_export_failure";
  const failedStep = stringValue(payload.failed_step);
  const currentUrl = stringValue(payload.current_url);
  const screenshotPath = stringValue(payload.screenshot_path);

  if (isExcel) {
    return {
      kind: "excel",
      label: "Excel Export",
      orderCount: 0,
      lists: [],
      filename: stringValue(payload.download_filename) || stringValue(payload.filename),
      sizeBytes: nullableNumberValue(payload.download_size_bytes ?? payload.size_bytes),
      sentToN8n: Boolean(payload.sent_to_n8n),
      n8nStatusCode: nullableNumberValue(payload.n8n_status_code),
      excelRowCount: adjustedExcelRowCount(nullableNumberValue(payload.excel_row_count ?? recordValue(payload.summary)?.excel_rows)),
      exportDate: stringValue(payload.export_date),
      failedStep,
      currentUrl,
      screenshotPath,
    };
  }

  const orderLists = orderListsFromPayload(payload);
  const orderCount = orderLists.reduce((sum, list) => sum + list.orderCount, 0);
  const label =
    orderLists.length > 1
      ? "EOD und Self Pickup"
      : orderLists[0]?.label || orderListLabel(orderListType || "eod");

  return {
    kind: "orders",
    label,
    orderCount: numberValue(payload.order_count) || orderCount,
    lists: orderLists,
    filename: "",
    sizeBytes: null,
    sentToN8n: false,
    n8nStatusCode: null,
    excelRowCount: null,
    exportDate: "",
    failedStep,
    currentUrl,
    screenshotPath,
  };
}

function orderListsFromPayload(payload: Record<string, unknown>): OrderBotList[] {
  const rawLists = recordValue(payload.order_lists);

  if (rawLists) {
    return Object.entries(rawLists)
      .map(([id, value]) => {
        const list = recordValue(value);
        const orderListType = stringValue(list?.order_list_type) || id;
        const orders = arrayRecordValue(list?.orders).map(orderFromPayload);
        const explicitCount = nullableNumberValue(list?.order_count);

        return {
          id,
          label: stringValue(list?.label) || orderListLabel(orderListType),
          orderCount: explicitCount ?? orders.length,
          orders,
        };
      })
      .filter((list) => list.id || list.orders.length || list.orderCount > 0);
  }

  const orders = arrayRecordValue(payload.orders).map(orderFromPayload);
  const orderListType = stringValue(payload.order_list_type) || "eod";

  if (!orders.length && nullableNumberValue(payload.order_count) === null) {
    return [];
  }

  return [
    {
      id: orderListType,
      label: orderListLabel(orderListType),
      orderCount: nullableNumberValue(payload.order_count) ?? orders.length,
      orders,
    },
  ];
}

function orderFromPayload(row: Record<string, unknown>): OrderBotOrder {
  const billingDate = stringValue(row.billing_date) || stringValue(row.billingDate);

  return {
    orderReference: stringValue(row.order_id) || stringValue(row.order_reference) || stringValue(row.orderReference),
    billingDate,
    createdDate:
      stringValue(row.created_date) ||
      stringValue(row.createdDate) ||
      stringValue(row.prescription_date),
    products: stringValue(row.products),
    prices: stringValue(row.prices) || stringValue(row.price),
    quantities: stringValue(row.quantities),
  };
}

function orderListLabel(value: string) {
  if (value === "pickup_ready" || value === "self pickup") {
    return "Self Pickup READY";
  }

  if (value === "eod_and_pickup") {
    return "EOD und Self Pickup";
  }

  return "EOD";
}

function recordValue(value: unknown) {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function arrayRecordValue(value: unknown) {
  return Array.isArray(value)
    ? value.filter((item): item is Record<string, unknown> => Boolean(recordValue(item)))
    : [];
}

function changedProductsFromValue(value: unknown): SyncChangedProduct[] {
  return arrayRecordValue(value)
    .map((row) => {
      const before = recordValue(row.before);
      const after = recordValue(row.after);
      const changes = recordValue(row.changes);
      const entries = Object.entries(changes || {}).map(([field, changeValue]) => {
        const change = recordValue(changeValue);
        return {
          field,
          oldValue: formatLogValue(change?.old ?? before?.[field]),
          newValue: formatLogValue(change?.new ?? after?.[field]),
        };
      });
      const fallbackEntries = entries.length ? entries : changesFromBeforeAfter(before, after);

      return {
        pzn: stringValue(row.pzn) || stringValue(after?.pzn) || stringValue(before?.pzn) || "",
        productName:
          stringValue(row.product_name) ||
          stringValue(after?.product_name) ||
          stringValue(before?.product_name) ||
          "Unbekanntes Produkt",
        changes: fallbackEntries,
      };
    })
    .filter((product) => product.changes.length > 0 || product.pzn || product.productName !== "Unbekanntes Produkt");
}

function changesFromBeforeAfter(
  before: Record<string, unknown> | null,
  after: Record<string, unknown> | null
): SyncChangedProduct["changes"] {
  if (!before || !after) {
    return [];
  }

  const fields = ["quantity", "price_per_g_incl_vat", "additional_cost", "site_price", "availability", "strain"];
  return fields
    .filter((field) => formatLogValue(before[field]) !== formatLogValue(after[field]))
    .map((field) => ({
      field,
      oldValue: formatLogValue(before[field]),
      newValue: formatLogValue(after[field]),
    }));
}

function newProductsFromValue(value: unknown): SyncNewProduct[] {
  const fields = ["quantity", "price_per_g_incl_vat", "additional_cost", "site_price", "availability", "strain"];

  return arrayRecordValue(value).map((row) => ({
    pzn: stringValue(row.pzn) || "",
    productName: stringValue(row.product_name) || "Unbekanntes Produkt",
    values: fields
      .filter((field) => row[field] !== null && row[field] !== undefined && row[field] !== "")
      .map((field) => ({
        label: field,
        value: formatLogValue(row[field]),
      })),
  }));
}

function invalidExamplesFromValue(value: unknown): SyncInvalidExample[] {
  return arrayRecordValue(value).map((row, index) => {
    const title =
      stringValue(row.title) ||
      stringValue(row.product_name) ||
      stringValue(row.pzn && `PZN ${row.pzn}`) ||
      `Zeile ${numberValue(row.row_number) || index + 1}`;
    const message =
      stringValue(row.error) ||
      stringValue(row.reason) ||
      stringValue(row.message) ||
      "Diese Zeile konnte nicht verarbeitet werden.";
    const values = Object.entries(row)
      .filter(([key]) => !["title", "error", "reason", "message"].includes(key))
      .slice(0, 6)
      .map(([label, rawValue]) => ({
        label,
        value: formatLogValue(rawValue),
      }));

    return { title, message, values };
  });
}

function stringValue(value: unknown) {
  if (typeof value === "string" && value.trim()) {
    return value;
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return "";
}

function formatLogValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "leer";
  }

  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }

  if (typeof value === "number") {
    return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3)));
  }

  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value);
}

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function nullableNumberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatDuration(value: unknown) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return "nicht angegeben";
  }

  if (value < 1000) {
    return `${Math.round(value)} ms`;
  }

  const seconds = Math.round(value / 1000);
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;

  if (!minutes) {
    return `${seconds} s`;
  }

  return `${minutes} min ${remainingSeconds} s`;
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

  const diffDays = Math.floor(diffHours / 24);
  return `vor ${diffDays} Tg`;
}

function formatExactDateTime(value: string) {
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
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatOrderBotDate(order: OrderBotOrder) {
  if (order.billingDate) {
    return formatExactDateTime(order.billingDate);
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(order.createdDate)) {
    const [year, month, day] = order.createdDate.split("-");
    return `${day}.${month}.${year}`;
  }

  return order.createdDate ? formatExactDateTime(order.createdDate) : "Datum fehlt";
}
