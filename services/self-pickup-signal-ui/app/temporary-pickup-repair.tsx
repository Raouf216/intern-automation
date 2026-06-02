"use client";

import { AlertTriangle, CheckCircle2, Loader2, RefreshCw, ShieldCheck, Wrench } from "lucide-react";
import { useMemo, useState } from "react";

type RepairOrder = {
  id: string;
  order_reference: string;
  order_type: string | null;
  scraped_at: string | null;
  picked: boolean | null;
  patient_name: string | null;
  billing_date: string | null;
  products: string | null;
};

type RepairLookupResponse = {
  ok?: boolean;
  error?: string;
  orders?: RepairOrder[];
  count?: number;
  hidden?: number;
  missing?: string[];
};

type RepairMarkResponse = {
  ok?: boolean;
  error?: string;
  status?: "picked" | "already_picked" | "not_found";
  picked_at?: string;
  order_reference?: string;
  message?: string;
  order?: RepairOrder;
};

const initialRepairIds = process.env.NEXT_PUBLIC_SELF_PICKUP_REPAIR_ORDER_IDS || "";

function normalizeRepairInput(value: string) {
  const seen = new Set<string>();

  return value
    .split(/[\s,;]+/)
    .map((token) => token.trim())
    .filter(Boolean)
    .filter((token) => {
      const key = token.toUpperCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function formatRepairDate(value?: string | null) {
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

async function parseRepairResponse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") || "";

  return contentType.includes("application/json")
    ? ((await response.json()) as T)
    : ({ ok: false, error: await response.text() } as T);
}

export function TemporaryPickupRepair() {
  const [repairInput, setRepairInput] = useState(initialRepairIds);
  const [orders, setOrders] = useState<RepairOrder[]>([]);
  const [missingOrders, setMissingOrders] = useState<string[]>([]);
  const [hiddenCount, setHiddenCount] = useState(0);
  const [message, setMessage] = useState("Temporäre Liste leer. Trage die betroffenen Bestell-IDs ein.");
  const [status, setStatus] = useState<"idle" | "running" | "success" | "error">("idle");
  const [loading, setLoading] = useState(false);
  const [markingId, setMarkingId] = useState<string | null>(null);

  const repairTokens = useMemo(() => normalizeRepairInput(repairInput), [repairInput]);

  async function loadRepairOrders() {
    if (!repairTokens.length) {
      setStatus("error");
      setMessage("Bitte mindestens eine Bestell-ID eintragen.");
      setOrders([]);
      setMissingOrders([]);
      setHiddenCount(0);
      return;
    }

    setLoading(true);
    setStatus("running");
    setMessage("Technische Nachpflege wird geladen.");

    try {
      const response = await fetch("/api/pickup-orders/manual-repair", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "lookup",
          order_references: repairTokens,
        }),
        credentials: "same-origin",
      });
      const payload = await parseRepairResponse<RepairLookupResponse>(response);

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `HTTP-Fehler ${response.status}`);
      }

      setOrders(payload.orders || []);
      setMissingOrders(payload.missing || []);
      setHiddenCount(payload.hidden || 0);
      setStatus("success");
      setMessage(`${payload.count || 0} offene Reparatur-Bestellung(en) geladen.`);
    } catch (error) {
      setOrders([]);
      setMissingOrders([]);
      setHiddenCount(0);
      setStatus("error");
      setMessage(error instanceof Error ? error.message : "Technische Nachpflege konnte nicht geladen werden.");
    } finally {
      setLoading(false);
    }
  }

  async function markRepairOrder(order: RepairOrder) {
    if (markingId) return;

    setMarkingId(order.id);
    setStatus("running");
    setMessage(`${order.order_reference} wird nur in Supabase markiert.`);

    try {
      const response = await fetch("/api/pickup-orders/manual-repair", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "mark",
          order_references: [order.order_reference],
        }),
        credentials: "same-origin",
      });
      const payload = await parseRepairResponse<RepairMarkResponse>(response);

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `HTTP-Fehler ${response.status}`);
      }

      setOrders((currentOrders) => currentOrders.filter((currentOrder) => currentOrder.id !== order.id));
      setStatus("success");
      setMessage(
        payload.status === "picked"
          ? `${order.order_reference} wurde manuell als abgeholt gespeichert.`
          : payload.message || `${order.order_reference} ist nicht mehr offen.`
      );
    } catch (error) {
      setStatus("error");
      setMessage(error instanceof Error ? error.message : "Bestellung konnte nicht markiert werden.");
    } finally {
      setMarkingId(null);
    }
  }

  const isBusy = loading || Boolean(markingId);

  return (
    <section className="repair-surface" aria-label="Temporäre Self Pickup Nachpflege">
      <div className="repair-heading">
        <div>
          <p className="section-kicker">Technische Nachpflege</p>
          <h2>Self Pickup Reparatur</h2>
        </div>
        <Wrench size={24} />
      </div>

      <div className="repair-grid">
        <section className="repair-input-card" aria-label="Reparatur IDs">
          <label className="field repair-id-field">
            <span>Betroffene Bestell-IDs</span>
            <textarea
              value={repairInput}
              onChange={(event) => setRepairInput(event.target.value)}
              placeholder="JF02... eine ID pro Zeile oder mit Komma getrennt"
              disabled={isBusy}
            />
          </label>
          <button className="inline-action-button repair-load-button" type="button" onClick={loadRepairOrders} disabled={isBusy || repairTokens.length === 0}>
            {loading ? <Loader2 size={17} className="spin" /> : <RefreshCw size={17} />}
            <span>{loading ? "Lade" : "Reparatur-Liste laden"}</span>
          </button>
          <div className={`repair-message repair-message-${status}`}>
            {status === "running" ? <Loader2 size={17} className="spin" /> : status === "success" ? <CheckCircle2 size={17} /> : status === "error" ? <AlertTriangle size={17} /> : <ShieldCheck size={17} />}
            <p>{message}</p>
          </div>
          {hiddenCount || missingOrders.length ? (
            <div className="repair-meta">
              {hiddenCount ? <span>{hiddenCount} schon markiert oder nicht offen</span> : null}
              {missingOrders.length ? <span>Nicht gefunden: {missingOrders.join(", ")}</span> : null}
            </div>
          ) : null}
        </section>

        <section className="repair-orders-card" aria-label="Reparatur Bestellungen">
          {orders.length ? (
            <div className="repair-order-list">
              {orders.map((order) => (
                <article className="repair-order-row" key={order.id}>
                  <div className="repair-order-main">
                    <strong>{order.order_reference}</strong>
                    <span>{order.patient_name || "Name fehlt"}</span>
                    <small>{formatRepairDate(order.billing_date)}</small>
                  </div>
                  {order.products ? <p>{order.products}</p> : null}
                  <button
                    className="repair-mark-button"
                    type="button"
                    onClick={() => markRepairOrder(order)}
                    disabled={isBusy}
                  >
                    {markingId === order.id ? <Loader2 size={16} className="spin" /> : <CheckCircle2 size={16} />}
                    <span>{markingId === order.id ? "Speichert" : "Nur Supabase markieren"}</span>
                  </button>
                </article>
              ))}
            </div>
          ) : (
            <p className="empty-pickup-list">Keine offene Reparatur-Bestellung geladen.</p>
          )}
        </section>
      </div>
    </section>
  );
}
