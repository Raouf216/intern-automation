"use client";

import {
  AlertTriangle,
  Bell,
  CheckCircle2,
  Clock3,
  Database,
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

type SectionKey = "upload" | "doktorabc_sync" | "abrechnung_verification";

type Props = {
  initialNotifications: StoredNotification[];
  initialError: string | null;
  config: ConfigStatus;
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
    caption: "Button-Ausloeser fuer DoktorABC",
    active: false,
  },
  {
    label: "Abrechnung Verifikation",
    value: "abrechnung_verification",
    description: "Abrechnung-Verifikation",
    caption: "Pruefung und Ergebnisprotokoll",
    active: false,
  },
];

export function NotificationDashboard({ initialNotifications, initialError, config }: Props) {
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
    let isMounted = true;

    async function refreshNotifications() {
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
      }
    }

    const interval = window.setInterval(refreshNotifications, 5000);
    return () => {
      isMounted = false;
      window.clearInterval(interval);
    };
  }, []);

  function toggleTheme() {
    const nextTheme = theme === "night" ? "light" : "night";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    window.localStorage.setItem("notification-app-theme", nextTheme);
  }

  const sectionCounts = useMemo(
    () =>
      sections.reduce(
        (counts, section) => {
          counts[section.value] = notifications.filter((notification) => notification.section === section.value).length;
          return counts;
        },
        { upload: 0, doktorabc_sync: 0, abrechnung_verification: 0 } as Record<SectionKey, number>
      ),
    [notifications]
  );

  const visibleNotifications = notifications.filter((notification) => notification.section === activeSection);
  const uploadNotifications = notifications.filter((notification) => notification.section === "upload");
  const successCount = uploadNotifications.filter((notification) => notification.status === "success").length;
  const failureCount = uploadNotifications.filter((notification) => notification.status === "failure").length;
  const pendingCount = uploadNotifications.filter((notification) => notification.status === "triggered").length;
  const activeSectionMeta = sections.find((section) => section.value === activeSection) || sections[0];

  return (
    <main className="page-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <Bell size={24} />
          </div>
          <div>
            <p className="eyebrow">Rats-Apotheke Betrieb</p>
            <h1>Benachrichtigungen</h1>
            <p className="subtitle">Zentrale Uebersicht fuer operative Systemmeldungen</p>
          </div>
        </div>
        <div className="topbar-actions">
          <button className="theme-toggle" type="button" onClick={toggleTheme} aria-pressed={theme === "night"}>
            {theme === "night" ? <Sun size={16} /> : <Moon size={16} />}
            <span>{theme === "night" ? "Hell" : "Nacht"}</span>
          </button>
          <div className={config.configured ? "system-state ok" : "system-state warn"}>
            {config.configured ? <ShieldCheck size={16} /> : <AlertTriangle size={16} />}
            <span>{config.configured ? "Supabase verbunden" : "Supabase nicht konfiguriert"}</span>
          </div>
        </div>
      </header>

      <section className="overview-band" aria-label="Upload-Uebersicht">
        <div className="metric-block">
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
                onClick={() => setActiveSection(section.value)}
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
            <Database size={18} />
            <span>Datenquelle</span>
          </div>
          <div className="info-line">
            <span>Schema</span>
            <strong>{config.schema}</strong>
          </div>
          <div className="info-line">
            <span>Tabelle</span>
            <strong>{config.table}</strong>
          </div>
          <div className="panel-heading second">
            <FileSpreadsheet size={18} />
            <span>Bereiche</span>
          </div>
          <p className="panel-copy">
            Upload-Meldungen werden gespeichert und automatisch aktualisiert. DoktorABC Sync und Abrechnung-Verifikation bleiben als getrennte Arbeitsbereiche vorgesehen.
          </p>
        </aside>
      </div>
    </main>
  );
}

function NotificationRow({ notification }: { notification: StoredNotification }) {
  const rowsInserted = rowsInsertedFromPayload(notification.payload);

  return (
    <article className={`notification-row status-${notification.status}`}>
      <div className="status-icon" aria-hidden="true">
        {notification.status === "success" ? (
          <CheckCircle2 size={18} />
        ) : notification.status === "failure" ? (
          <AlertTriangle size={18} />
        ) : (
          <UploadCloud size={18} />
        )}
      </div>

      <div className="notification-main">
        <div className="notification-title-row">
          <div>
            <h3>{notification.title}</h3>
            <p>{notification.message}</p>
          </div>
          <div className="notification-meta">
            <span className={`status-chip chip-${notification.status}`}>{formatStatus(notification.status)}</span>
            <time dateTime={notification.created_at}>{formatRelativeTime(notification.created_at)}</time>
          </div>
        </div>
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
            <dt>Groesse</dt>
            <dd>{formatBytes(notification.size_bytes)}</dd>
          </div>
          {rowsInserted === null ? null : (
            <div>
              <dt>DB-Zeilen</dt>
              <dd>{rowsInserted}</dd>
            </div>
          )}
        </dl>
        {notification.error ? <p className="error-line">{notification.error}</p> : null}
      </div>
    </article>
  );
}

function emptyCopy(section: SectionKey) {
  if (section === "doktorabc_sync") {
    return "Sobald die Produktsynchronisierung Meldungen sendet, werden sie hier als eigener Verlauf angezeigt.";
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

  if (value === "triggered") {
    return "Gestartet";
  }

  return "Info";
}

function formatUploadType(value: string | null) {
  if (value === "doktorabc_abrechnung") {
    return "DoktorABC Abrechnung";
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

function rowsInsertedFromPayload(payload: Record<string, unknown>) {
  return typeof payload.rows_inserted === "number" ? payload.rows_inserted : null;
}

function formatRelativeTime(value: string) {
  const diffSeconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));

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
