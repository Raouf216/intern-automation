import {
  AlertTriangle,
  Bell,
  CheckCircle2,
  Clock3,
  Database,
  FileSpreadsheet,
  Inbox,
  RadioTower,
  ShieldCheck,
  UploadCloud,
} from "lucide-react";
import { listNotifications, notificationConfigStatus, type StoredNotification } from "@/lib/notifications";

export const dynamic = "force-dynamic";

const sections = [
  { label: "Alle Meldungen", value: "all", countKey: "all" },
  { label: "Upload", value: "upload", countKey: "upload" },
  { label: "Button", value: "button", countKey: "button", disabled: true },
  { label: "Abrechnung Verifikation", value: "abrechnung", countKey: "abrechnung", disabled: true },
];

export default async function Home() {
  const config = notificationConfigStatus();
  let notifications: StoredNotification[] = [];
  let loadError: string | null = null;

  try {
    notifications = await listNotifications(120);
  } catch (error) {
    loadError = error instanceof Error ? error.message : "Unbekannter Fehler";
  }

  const uploadNotifications = notifications.filter((notification) => notification.section === "upload");
  const counts = {
    all: notifications.length,
    upload: uploadNotifications.length,
    button: 0,
    abrechnung: 0,
  };

  const latest = notifications[0];
  const successCount = uploadNotifications.filter((notification) => notification.status === "success").length;
  const failureCount = uploadNotifications.filter((notification) => notification.status === "failure").length;
  const pendingCount = uploadNotifications.filter((notification) => notification.status === "triggered").length;

  return (
    <main className="page-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <Bell size={26} />
          </div>
          <div>
            <p className="eyebrow">Rats-Apotheke Betrieb</p>
            <h1>Benachrichtigungen</h1>
          </div>
        </div>
        <div className={config.configured ? "system-state ok" : "system-state warn"}>
          {config.configured ? <ShieldCheck size={17} /> : <AlertTriangle size={17} />}
          <span>{config.configured ? "Supabase verbunden" : "Supabase nicht konfiguriert"}</span>
        </div>
      </header>

      <section className="overview-band" aria-label="Benachrichtigungsuebersicht">
        <div className="metric-block">
          <span>Gesamt</span>
          <strong>{counts.all}</strong>
        </div>
        <div className="metric-block">
          <span>Upload erfolgreich</span>
          <strong>{successCount}</strong>
        </div>
        <div className="metric-block">
          <span>Upload gestartet</span>
          <strong>{pendingCount}</strong>
        </div>
        <div className="metric-block danger">
          <span>Fehler</span>
          <strong>{failureCount}</strong>
        </div>
      </section>

      <div className="workspace">
        <aside className="section-nav" aria-label="Meldungsbereiche">
          <div className="section-nav-title">
            <RadioTower size={18} />
            <span>Bereiche</span>
          </div>
          <nav>
            {sections.map((section) => (
              <button
                type="button"
                className={section.value === "upload" ? "nav-item active" : "nav-item"}
                disabled={section.disabled}
                key={section.value}
              >
                <span>{section.label}</span>
                <strong>{counts[section.countKey as keyof typeof counts]}</strong>
              </button>
            ))}
          </nav>
          <div className="webhook-box">
            <span>n8n Zielroute</span>
            <code>/api/notifications/upload</code>
          </div>
        </aside>

        <section className="feed" aria-label="Upload Meldungen">
          <div className="feed-header">
            <div>
              <p className="section-kicker">Upload</p>
              <h2>Aktuelle Upload-Meldungen</h2>
            </div>
            <div className="last-update">
              <Clock3 size={15} />
              <span>{latest ? formatDateTime(latest.created_at) : "Noch keine Meldung"}</span>
            </div>
          </div>

          {loadError ? (
            <div className="empty-state error">
              <AlertTriangle size={26} />
              <h3>Benachrichtigungen konnten nicht geladen werden</h3>
              <p>{loadError}</p>
            </div>
          ) : uploadNotifications.length ? (
            <div className="notification-list">
              {uploadNotifications.map((notification) => (
                <article className={`notification-row status-${notification.status}`} key={notification.id}>
                  <div className="status-icon" aria-hidden="true">
                    {notification.status === "success" ? (
                      <CheckCircle2 size={21} />
                    ) : notification.status === "failure" ? (
                      <AlertTriangle size={21} />
                    ) : (
                      <UploadCloud size={21} />
                    )}
                  </div>

                  <div className="notification-main">
                    <div className="notification-title-row">
                      <h3>{notification.title}</h3>
                      <time>{formatDateTime(notification.created_at)}</time>
                    </div>
                    <p>{notification.message}</p>
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
                        <dt>Speicherort</dt>
                        <dd>{notification.path || "nicht angegeben"}</dd>
                      </div>
                      <div>
                        <dt>Groesse</dt>
                        <dd>{formatBytes(notification.size_bytes)}</dd>
                      </div>
                    </dl>
                    {notification.error ? <p className="error-line">{notification.error}</p> : null}
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="empty-state">
              <Inbox size={30} />
              <h3>Noch keine Upload-Meldungen</h3>
              <p>Sobald n8n Upload-Ereignisse an diese App sendet, erscheinen sie hier chronologisch mit der neuesten Meldung zuerst.</p>
            </div>
          )}
        </section>

        <aside className="operations-panel" aria-label="Systeminformationen">
          <div className="panel-heading">
            <Database size={19} />
            <span>Speicherung</span>
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
            <FileSpreadsheet size={19} />
            <span>Aktive Kategorie</span>
          </div>
          <p className="panel-copy">
            Upload ist jetzt aktiv. Weitere Bereiche fuer Button-Ausloeser und Abrechnung-Verifikation sind vorbereitet.
          </p>
        </aside>
      </div>
    </main>
  );
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

function formatDateTime(value: string) {
  return new Intl.DateTimeFormat("de-DE", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Europe/Berlin",
  }).format(new Date(value));
}
