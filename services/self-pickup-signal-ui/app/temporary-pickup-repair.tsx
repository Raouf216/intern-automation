"use client";

import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  Loader2,
  PackageCheck,
  RefreshCw,
  ShieldCheck,
  Wrench,
} from "lucide-react";
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
  status?: "completed";
  picked_at?: string;
  picked?: number;
  skipped?: number;
  errors?: number;
  results?: Array<{
    order_reference: string;
    status: "picked" | "already_picked" | "not_found" | "error";
    message: string;
    picked_at?: string;
    order?: RepairOrder;
  }>;
};

const temporaryRepairIds = "JE22NRPQA\nJE27KGRNG\nJF01PCYRQ\nJF01VSJJZ\nJF02FNYZZ";
const initialRepairIds = process.env.NEXT_PUBLIC_SELF_PICKUP_REPAIR_ORDER_IDS || temporaryRepairIds;

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
  const [isMarkingSelected, setIsMarkingSelected] = useState(false);
  const [selectedRepairReferences, setSelectedRepairReferences] = useState<string[]>([]);

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
      setSelectedRepairReferences((selected) =>
        selected.filter((orderReference) =>
          (payload.orders || []).some((order) => order.order_reference === orderReference)
        )
      );
      setMissingOrders(payload.missing || []);
      setHiddenCount(payload.hidden || 0);
      setStatus("success");
      setMessage(`${payload.count || 0} offene Reparatur-Bestellung(en) geladen.`);
    } catch (error) {
      setOrders([]);
      setSelectedRepairReferences([]);
      setMissingOrders([]);
      setHiddenCount(0);
      setStatus("error");
      setMessage(error instanceof Error ? error.message : "Technische Nachpflege konnte nicht geladen werden.");
    } finally {
      setLoading(false);
    }
  }

  function toggleRepairSelection(orderReference: string) {
    setSelectedRepairReferences((selected) =>
      selected.includes(orderReference)
        ? selected.filter((value) => value !== orderReference)
        : [...selected, orderReference]
    );
  }

  function toggleAllRepairSelections() {
    setSelectedRepairReferences((selected) =>
      selected.length === orders.length ? [] : orders.map((order) => order.order_reference)
    );
  }

  async function markSelectedRepairOrders() {
    if (isMarkingSelected) return;

    if (!selectedRepairReferences.length) {
      setStatus("error");
      setMessage("Bitte mindestens eine Reparatur-Bestellung auswählen.");
      return;
    }

    setIsMarkingSelected(true);
    setStatus("running");
    setMessage(`${selectedRepairReferences.length} Bestellung(en) werden nur in Supabase markiert.`);

    try {
      const response = await fetch("/api/pickup-orders/manual-repair", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "mark",
          order_references: selectedRepairReferences,
        }),
        credentials: "same-origin",
      });
      const payload = await parseRepairResponse<RepairMarkResponse>(response);

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `HTTP-Fehler ${response.status}`);
      }

      const pickedReferences = new Set(
        (payload.results || [])
          .filter((result) => result.status === "picked")
          .map((result) => result.order_reference)
      );
      setOrders((currentOrders) =>
        currentOrders.filter((currentOrder) => !pickedReferences.has(currentOrder.order_reference))
      );
      setSelectedRepairReferences((selected) =>
        selected.filter((orderReference) => !pickedReferences.has(orderReference))
      );
      setStatus("success");
      setMessage(`${payload.picked || 0} markiert, ${payload.skipped || 0} uebersprungen, ${payload.errors || 0} Fehler.`);
    } catch (error) {
      setStatus("error");
      setMessage(error instanceof Error ? error.message : "Bestellung konnte nicht markiert werden.");
    } finally {
      setIsMarkingSelected(false);
    }
  }

  const isBusy = loading || isMarkingSelected;

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
        <section className="secondary-bot-card repair-input-card" aria-label="Reparatur IDs">
          <div>
            <Wrench size={22} />
            <span>
              <b>Technische Liste</b>
              <small>Nur fuer diese betroffenen Bestellungen</small>
            </span>
          </div>
          <label className="field repair-id-field">
            <span>Betroffene Bestell-IDs</span>
            <textarea
              value={repairInput}
              onChange={(event) => setRepairInput(event.target.value)}
              placeholder="JF02... eine ID pro Zeile oder mit Komma getrennt"
              disabled={isBusy}
            />
          </label>
          <div className="pickup-list-actions repair-actions">
            <button className="inline-action-button repair-load-button" type="button" onClick={loadRepairOrders} disabled={isBusy || repairTokens.length === 0}>
              {loading ? <Loader2 size={17} className="spin" /> : <RefreshCw size={17} />}
              <span>{loading ? "Lade" : "Liste laden"}</span>
            </button>
            <button
              className="inline-action-button"
              type="button"
              onClick={toggleAllRepairSelections}
              disabled={isBusy || orders.length === 0}
            >
              <CheckCircle2 size={17} />
              <span>{selectedRepairReferences.length === orders.length ? "Auswahl leeren" : "Alle auswählen"}</span>
            </button>
          </div>
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

        <section className="secondary-bot-card repair-orders-card" aria-label="Reparatur Bestellungen">
          <div>
            <PackageCheck size={22} />
            <span>
              <b>Reparatur-Auswahl</b>
              <small>{orders.length ? `${orders.length} offene Bestellung(en)` : "Noch nichts geladen"}</small>
            </span>
          </div>
          <div className="pending-pickup-shell repair-pickup-shell">
            <div className="pending-pickup-list repair-order-list">
              {orders.length ? (
                orders.map((order) => (
                  <label className="pending-pickup-row repair-pickup-row" key={order.id}>
                    <input
                      type="checkbox"
                      checked={selectedRepairReferences.includes(order.order_reference)}
                      onChange={() => toggleRepairSelection(order.order_reference)}
                      disabled={isBusy}
                    />
                    <span>
                      <strong>{order.order_reference}</strong>
                      <small className="pickup-patient-name">{order.patient_name || "Name fehlt"}</small>
                      <small>{formatRepairDate(order.billing_date)}</small>
                      {order.products ? <small>{order.products}</small> : null}
                    </span>
                  </label>
                ))
              ) : (
                <p className="empty-pickup-list">Keine offene Reparatur-Bestellung geladen.</p>
              )}
            </div>
          </div>
          <button
            className="trigger-button pickup-mark-button repair-submit-button"
            type="button"
            onClick={markSelectedRepairOrders}
            disabled={isBusy || selectedRepairReferences.length === 0}
          >
            {isMarkingSelected ? <Loader2 size={21} className="spin" /> : <PackageCheck size={21} />}
            <span>
              {isMarkingSelected
                ? "Speichert"
                : selectedRepairReferences.length
                  ? `${selectedRepairReferences.length} manuell markieren`
                  : "Bestellung auswählen"}
            </span>
            <ArrowRight size={20} />
          </button>
        </section>
      </div>
    </section>
  );
}
