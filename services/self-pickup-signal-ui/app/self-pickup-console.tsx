"use client";

import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  KeyRound,
  Loader2,
  LockKeyhole,
  Moon,
  PackageCheck,
  RefreshCw,
  Search,
  ShieldCheck,
  Sun,
} from "lucide-react";
import { useEffect, useMemo, useState, type FormEvent } from "react";

type PickupMarkResult = {
  order_reference: string;
  status: "picked" | "already_picked" | "clickable" | "not_found" | "wrong_order_type" | "error";
  message: string;
  order_type?: string | null;
  scraped_at?: string | null;
  picked?: boolean | null;
  dry_run?: boolean;
  would_click?: boolean;
  bot_status?: string;
};

type PickupMarkResponse = {
  ok?: boolean;
  error?: string;
  dry_run?: boolean;
  checked?: number;
  clickable?: number;
  picked?: number;
  already_picked?: number;
  errors?: number;
  picked_at?: string;
  results?: PickupMarkResult[];
};

type PendingPickupOrder = {
  id: string;
  order_reference: string;
  patient_name?: string | null;
  billing_date?: string | null;
  products?: string | null;
};

type PendingPickupResponse = {
  ok?: boolean;
  error?: string;
  count?: number;
  orders?: PendingPickupOrder[];
};

const doktorabcReadyUrl = "https://pharmacies.doktorabc.com/self-pickup?tab=ready-for-customer&page=1";

function numberValue(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function pickupMarkStatusLabel(status: PickupMarkResult["status"]) {
  if (status === "picked") return "markiert";
  if (status === "already_picked") return "bereits abgeholt";
  if (status === "clickable") return "klickbar";
  if (status === "not_found") return "nicht gefunden";
  if (status === "wrong_order_type") return "falscher Typ";
  return "Fehler";
}

function formatPickupDate(value?: string | null) {
  if (!value) return "Datum fehlt";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;

  return date.toLocaleString("de-DE", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function normalizeSearchValue(value?: string | null) {
  return (value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function authErrorMessage(value?: string) {
  if (value === "operator_password_invalid") return "Passwort ist falsch.";
  if (value === "operator_password_not_configured") return "Das Bedienerpasswort ist nicht konfiguriert.";
  if (value === "operator_session_invalid") return "Die Sitzung ist abgelaufen. Bitte erneut freischalten.";

  return value || "Zugang konnte nicht geprüft werden.";
}

function DoktorabcLogo() {
  return (
    <h1 className="doktorabc-logo-card">
      <img className="doktorabc-logo-image doktorabc-logo-light" src="/pharmacies-logo-light.png" srcSet="/pharmacies-logo-light.png 1x, /pharmacies-logo-light@2x.png 2x" alt="DOKTORABC Pharmacies" width={198} height={66} />
      <img className="doktorabc-logo-image doktorabc-logo-night" src="/pharmacies-logo-night.png" srcSet="/pharmacies-logo-night.png 1x, /pharmacies-logo-night@2x.png 2x" alt="" aria-hidden="true" width={198} height={66} />
    </h1>
  );
}

export function SelfPickupConsole() {
  const [passwordInput, setPasswordInput] = useState("");
  const [isUnlocked, setIsUnlocked] = useState(false);
  const [isSessionChecking, setIsSessionChecking] = useState(true);
  const [isUnlocking, setIsUnlocking] = useState(false);
  const [unlockStatus, setUnlockStatus] = useState<"idle" | "success" | "error">("idle");
  const [unlockMessage, setUnlockMessage] = useState("Bedienerpasswort eingeben, um die Konsole freizuschalten.");
  const [isPickupMarkRunning, setIsPickupMarkRunning] = useState(false);
  const [isPickupPendingLoading, setIsPickupPendingLoading] = useState(false);
  const [pickupMarkStatus, setPickupMarkStatus] = useState<"idle" | "success" | "error">("idle");
  const [pickupMarkMessage, setPickupMarkMessage] = useState("Bereit, offene Self-Pickup Bestellungen zu laden.");
  const [pickupMarkResult, setPickupMarkResult] = useState<PickupMarkResponse | null>(null);
  const [pendingPickupOrders, setPendingPickupOrders] = useState<PendingPickupOrder[]>([]);
  const [selectedPickupReferences, setSelectedPickupReferences] = useState<string[]>([]);
  const [pickupSearchTerm, setPickupSearchTerm] = useState("");
  const [theme, setTheme] = useState<"light" | "night">("light");

  useEffect(() => {
    const storedTheme = window.localStorage.getItem("self-pickup-signal-theme");
    const nextTheme = storedTheme === "night" ? "night" : "light";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    void restoreSession();
  }, []);

  function toggleTheme() {
    const nextTheme = theme === "night" ? "light" : "night";
    setTheme(nextTheme);
    document.body.dataset.theme = nextTheme;
    window.localStorage.setItem("self-pickup-signal-theme", nextTheme);
  }

  const normalizedPickupSearchTerm = normalizeSearchValue(pickupSearchTerm.trim());
  const visiblePendingPickupOrders = useMemo(
    () =>
      normalizedPickupSearchTerm
        ? pendingPickupOrders.filter((order) =>
            [order.order_reference, order.patient_name].some((value) =>
              normalizeSearchValue(value).includes(normalizedPickupSearchTerm)
            )
          )
        : pendingPickupOrders,
    [normalizedPickupSearchTerm, pendingPickupOrders]
  );
  const hasPickupSearchMiss = Boolean(
    normalizedPickupSearchTerm && pendingPickupOrders.length && visiblePendingPickupOrders.length === 0
  );

  async function requestPendingPickupOrders(passwordForUnlock?: string) {
    const headers = passwordForUnlock ? { "x-operator-password": passwordForUnlock } : undefined;
    const response = await fetch("/api/pickup-orders/mark-picked", {
      method: "GET",
      headers,
      cache: "no-store",
      credentials: "same-origin",
    });
    const contentType = response.headers.get("content-type") || "";
    const payload = contentType.includes("application/json")
      ? ((await response.json()) as PendingPickupResponse)
      : ({ ok: false, error: await response.text() } satisfies PendingPickupResponse);

    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || `HTTP-Fehler ${response.status}: ${response.statusText || "keine Details"}`);
    }

    return payload.orders || [];
  }

  function applyPendingPickupOrders(orders: PendingPickupOrder[]) {
    setPendingPickupOrders(orders);
    setSelectedPickupReferences((selected) =>
      selected.filter((orderReference) => orders.some((order) => order.order_reference === orderReference))
    );
  }

  async function restoreSession() {
    try {
      const orders = await requestPendingPickupOrders();
      applyPendingPickupOrders(orders);
      setIsUnlocked(true);
      setPickupMarkStatus("idle");
      setPickupMarkMessage(`${orders.length} offene Self-Pickup Bestellung(en) geladen.`);
    } catch {
      // No valid browser session yet; keep the password gate visible.
    } finally {
      setIsSessionChecking(false);
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
      const orders = await requestPendingPickupOrders(nextPassword);
      applyPendingPickupOrders(orders);
      setPasswordInput("");
      setIsUnlocked(true);
      setUnlockStatus("success");
      setUnlockMessage("Zugang freigeschaltet.");
      setPickupMarkStatus("idle");
      setPickupMarkMessage(`${orders.length} offene Self-Pickup Bestellung(en) geladen.`);
    } catch (error) {
      setIsUnlocked(false);
      setUnlockStatus("error");
      setUnlockMessage(authErrorMessage(error instanceof Error ? error.message : ""));
    } finally {
      setIsUnlocking(false);
    }
  }

  async function refreshPendingPickupOrders(options: { preserveMessage?: boolean } = {}) {
    if (!isUnlocked) {
      setUnlockStatus("error");
      setUnlockMessage("Bitte zuerst den Zugang freischalten.");
      return;
    }

    setIsPickupPendingLoading(true);
    if (!options.preserveMessage) {
      setPickupMarkStatus("idle");
      setPickupMarkMessage("Offene Self-Pickup Bestellungen werden geladen.");
    }

    try {
      const orders = await requestPendingPickupOrders();
      applyPendingPickupOrders(orders);
      if (!options.preserveMessage) {
        setPickupMarkMessage(`${orders.length} offene Self-Pickup Bestellung(en) geladen.`);
      }
    } catch (error) {
      const message = error instanceof Error ? authErrorMessage(error.message) : "Offene Self-Pickup Bestellungen konnten nicht geladen werden.";
      if (error instanceof Error && error.message === "operator_session_invalid") {
        setIsUnlocked(false);
        setUnlockStatus("error");
        setUnlockMessage(message);
      }
      setPickupMarkStatus("error");
      setPickupMarkMessage(message);
    } finally {
      setIsPickupPendingLoading(false);
    }
  }

  function togglePickupSelection(orderReference: string) {
    setSelectedPickupReferences((selected) =>
      selected.includes(orderReference)
        ? selected.filter((value) => value !== orderReference)
        : [...selected, orderReference]
    );
  }

  function toggleAllPickupSelections() {
    setSelectedPickupReferences((selected) =>
      selected.length === pendingPickupOrders.length ? [] : pendingPickupOrders.map((order) => order.order_reference)
    );
  }

  async function triggerPickupMarkOrders() {
    if (!isUnlocked) {
      setUnlockStatus("error");
      setUnlockMessage("Bitte zuerst den Zugang freischalten.");
      return;
    }

    const orderReferences = selectedPickupReferences;
    if (!orderReferences.length) {
      setPickupMarkStatus("error");
      setPickupMarkMessage("Bitte mindestens eine offene Self-Pickup Bestellung auswählen.");
      return;
    }

    setIsPickupMarkRunning(true);
    setPickupMarkStatus("idle");
    setPickupMarkMessage("Self-Pickup Bestellungen werden geprüft.");
    setPickupMarkResult(null);

    try {
      const response = await fetch("/api/pickup-orders/mark-picked", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ order_references: orderReferences }),
        credentials: "same-origin",
      });
      const contentType = response.headers.get("content-type") || "";
      const payload = contentType.includes("application/json")
        ? ((await response.json()) as PickupMarkResponse)
        : ({ ok: false, error: await response.text() } satisfies PickupMarkResponse);

      setPickupMarkResult(payload);

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `HTTP-Fehler ${response.status}: ${response.statusText || "keine Details"}`);
      }

      const errors = numberValue(payload.errors);
      const clickable = numberValue(payload.clickable);
      const picked = numberValue(payload.picked);
      const alreadyPicked = numberValue(payload.already_picked);
      setPickupMarkStatus(errors ? "error" : "success");
      setPickupMarkMessage(
        payload.dry_run
          ? errors
            ? `Trockenlauf abgeschlossen: ${clickable} klickbar, ${picked} markiert, ${errors} Fehler.`
            : `Trockenlauf erfolgreich: ${clickable} klickbar, ${picked} markiert.`
          : errors
          ? `Prüfung abgeschlossen: ${picked} markiert, ${alreadyPicked} bereits abgeholt, ${errors} Fehler.`
          : `Erfolgreich abgeschlossen: ${picked} markiert, ${alreadyPicked} bereits abgeholt.`
      );
      if (!payload.dry_run || picked || alreadyPicked) {
        setSelectedPickupReferences([]);
        await refreshPendingPickupOrders({ preserveMessage: true });
      }
    } catch (error) {
      if (error instanceof Error && error.message === "operator_session_invalid") {
        setIsUnlocked(false);
        setUnlockStatus("error");
        setUnlockMessage(authErrorMessage(error.message));
      }
      setPickupMarkStatus("error");
      setPickupMarkMessage(error instanceof Error ? authErrorMessage(error.message) : "Self-Pickup Markierung fehlgeschlagen.");
    } finally {
      setIsPickupMarkRunning(false);
    }
  }

  const anyBotRunning = isPickupMarkRunning || isPickupPendingLoading;
  const anyBotError = pickupMarkStatus === "error";
  const anyBotSuccess = pickupMarkStatus === "success";

  if (isSessionChecking) {
    return (
      <main className="page auth-page">
        <section className="auth-gate" aria-label="Self Pickup Sitzung">
          <div className="auth-card">
            <DoktorabcLogo />
            <div>
              <p className="section-kicker">Self Pickup</p>
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
        <section className="auth-gate" aria-label="Self Pickup Zugang">
          <div className="auth-card">
            <DoktorabcLogo />
            <div>
              <p className="section-kicker">Self Pickup</p>
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

  const pickupStatusPanel = (
    <aside className="status-surface action-status-panel" aria-label="Self Pickup Ergebnis">
      <div className="surface-heading compact mini">
        <div>
          <p className="section-kicker">Self Pickup</p>
          <h3>Abholstatus</h3>
        </div>
        {isPickupMarkRunning ? <Loader2 size={20} className="spin" /> : pickupMarkStatus === "success" ? <CheckCircle2 size={20} /> : pickupMarkStatus === "error" ? <AlertTriangle size={20} /> : <PackageCheck size={20} />}
      </div>
      <div className={`message message-${isPickupMarkRunning ? "running" : pickupMarkStatus}`}>
        {isPickupMarkRunning ? <Loader2 size={18} className="spin" /> : pickupMarkStatus === "success" ? <CheckCircle2 size={18} /> : pickupMarkStatus === "error" ? <AlertTriangle size={18} /> : <ShieldCheck size={18} />}
        <p>{pickupMarkMessage}</p>
      </div>
      <dl className="eod-facts">
        <div>
          <dt>Geprüft</dt>
          <dd>{pickupMarkResult ? numberValue(pickupMarkResult.checked) : "noch nicht"}</dd>
        </div>
        <div>
          <dt>Klickbar</dt>
          <dd>{pickupMarkResult ? numberValue(pickupMarkResult.clickable) : "noch nicht"}</dd>
        </div>
        <div>
          <dt>Markiert</dt>
          <dd>{pickupMarkResult ? numberValue(pickupMarkResult.picked) : "noch nicht"}</dd>
        </div>
        <div>
          <dt>Bereits abgeholt</dt>
          <dd>{pickupMarkResult ? numberValue(pickupMarkResult.already_picked) : "noch nicht"}</dd>
        </div>
        <div>
          <dt>Fehler</dt>
          <dd>{pickupMarkResult ? numberValue(pickupMarkResult.errors) : "noch nicht"}</dd>
        </div>
      </dl>
      {pickupMarkResult?.results?.length ? (
        <div className="pickup-result-list">
          {pickupMarkResult.results.map((row) => (
            <article className={`pickup-result pickup-result-${row.status}`} key={row.order_reference}>
              <div>
                <strong>{row.order_reference}</strong>
                <span>{pickupMarkStatusLabel(row.status)}</span>
              </div>
              <p>{row.message}</p>
              {row.scraped_at ? <code>{new Date(row.scraped_at).toLocaleString("de-DE")}</code> : null}
              {row.order_type ? <small>{row.order_type}</small> : null}
            </article>
          ))}
        </div>
      ) : null}
    </aside>
  );

  return (
    <main className="page">
      <section className="workspace" aria-label="Konsole für Self Pickup Abholungen">
        <header className="masthead">
          <div className="identity">
            <div className="mark" aria-hidden="true">
              <PackageCheck size={30} />
            </div>
            <div className="brand-copy">
              <p className="eyebrow">Rats-Apotheke Betrieb</p>
              <DoktorabcLogo />
              {isUnlocked ? (
                <a className="doktorabc-ready-link" href={doktorabcReadyUrl}>
                  DoktorABC self-pickup Ready for customer
                </a>
              ) : null}
            </div>
          </div>
          <div className="masthead-actions">
            <button className="theme-button" type="button" onClick={toggleTheme} aria-label="Darstellung wechseln">
              {theme === "night" ? <Sun size={17} /> : <Moon size={17} />}
              <span>{theme === "night" ? "Hell" : "Nacht"}</span>
            </button>
            <div className={`state-pill state-${anyBotError ? "error" : anyBotSuccess ? "success" : pickupMarkStatus}`}>
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
                <h2>Self Pickup Abholung</h2>
              </div>
              <PackageCheck size={26} />
            </div>

            <div className="bot-action-list">
              <section className="bot-action-row">
                <section className="secondary-bot-card manual-pickup-card" aria-label="Self Pickup Abholung">
                  <div>
                    <PackageCheck size={22} />
                    <span>
                      <b>Self Pickup abgeholt</b>
                      <small>Offene Abholungen aus Supabase</small>
                    </span>
                  </div>
                  <div className="pickup-list-actions">
                    <button className="inline-action-button" type="button" onClick={() => refreshPendingPickupOrders()} disabled={anyBotRunning}>
                      {isPickupPendingLoading ? <Loader2 size={17} className="spin" /> : <RefreshCw size={17} />}
                      <span>{isPickupPendingLoading ? "Lade Liste" : "Liste aktualisieren"}</span>
                    </button>
                    <button
                      className="inline-action-button"
                      type="button"
                      onClick={toggleAllPickupSelections}
                      disabled={anyBotRunning || pendingPickupOrders.length === 0}
                    >
                      <CheckCircle2 size={17} />
                      <span>{selectedPickupReferences.length === pendingPickupOrders.length ? "Auswahl leeren" : "Alle auswählen"}</span>
                    </button>
                    <label className="pickup-search-field">
                      <Search size={17} />
                      <input
                        value={pickupSearchTerm}
                        onChange={(event) => setPickupSearchTerm(event.target.value)}
                        placeholder="Bestell-ID oder Name suchen"
                        aria-label="Bestell-ID oder Name suchen"
                      />
                    </label>
                  </div>
                  <div className="pending-pickup-shell" aria-label="Offene Self Pickup Bestellungen">
                    <div className="pending-pickup-list">
                      {visiblePendingPickupOrders.length ? (
                        visiblePendingPickupOrders.map((order) => (
                          <label className="pending-pickup-row" key={order.order_reference}>
                            <input
                              type="checkbox"
                              checked={selectedPickupReferences.includes(order.order_reference)}
                              onChange={() => togglePickupSelection(order.order_reference)}
                              disabled={anyBotRunning}
                            />
                            <span>
                              <strong>{order.order_reference}</strong>
                              <small className="pickup-patient-name">{order.patient_name || "Name fehlt"}</small>
                              <small>{formatPickupDate(order.billing_date)}</small>
                            </span>
                          </label>
                        ))
                      ) : hasPickupSearchMiss ? (
                        <p className="empty-pickup-list">Diese Bestell-ID oder dieser Name existiert nicht in der geladenen Liste.</p>
                      ) : (
                        <p className="empty-pickup-list">Keine offene Self-Pickup Bestellung geladen.</p>
                      )}
                    </div>
                  </div>
                  <button
                    className="trigger-button pickup-mark-button"
                    type="button"
                    onClick={triggerPickupMarkOrders}
                    disabled={anyBotRunning || selectedPickupReferences.length === 0}
                  >
                    {isPickupMarkRunning ? <Loader2 size={21} className="spin" /> : <PackageCheck size={21} />}
                    <span>
                      {isPickupMarkRunning
                        ? "Wird geprüft"
                        : selectedPickupReferences.length
                          ? `${selectedPickupReferences.length} als abgeholt markieren`
                          : "Bestellung auswählen"}
                    </span>
                    <ArrowRight size={20} />
                  </button>
                </section>
                {pickupStatusPanel}
              </section>
            </div>

            <p className="security-note">
              <LockKeyhole size={15} />
              Diese Schaltflächen lösen ausschließlich fest hinterlegte Aktionen aus.
            </p>
          </section>
        </div>
      </section>
    </main>
  );
}
