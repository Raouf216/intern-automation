"use client";

import {
  AlertTriangle,
  ArrowDownWideNarrow,
  Boxes,
  CalendarDays,
  FileText,
  Inbox,
  Loader2,
  Mail,
  ReceiptText,
  Search,
  X,
} from "lucide-react";
import Link from "next/link";
import { useEffect, useState } from "react";

type Batch = {
  id: string;
  chargennummer: string;
  expiryDate: string;
  quantity: number | null;
  quantityUnit: string;
  quantityPieces: number | null;
  unitWeightG: number | null;
  totalQuantityG: number | null;
  aiConfidence: number | null;
};

type ProductLine = {
  id: string;
  lineNumber: number | null;
  productName: string;
  productCode: string;
  quantity: number | null;
  quantityUnit: string;
  quantityPieces: number | null;
  unitWeightG: number | null;
  totalQuantityG: number | null;
  unitPriceNetto: number | null;
  unitPriceBrutto: number | null;
  lineNetto: number | null;
  lineBrutto: number | null;
  vatRate: number | null;
  currency: string;
  matchStatus: string;
  aiConfidence: number | null;
  batches: Batch[];
};

type Abrechnung = {
  id: string;
  status: string;
  supplierName: string;
  sellerName: string;
  customerName: string;
  senderEmail: string;
  emailSubject: string;
  receivedAt: string;
  rechnungsnummer: string;
  debitorNumber: string;
  rechnungsdatum: string;
  faelligkeitsdatum: string;
  totalNetto: number | null;
  totalVat: number | null;
  totalBrutto: number | null;
  currency: string;
  aiConfidence: number | null;
  aiReason: string;
  reviewNote: string;
  createdAt: string;
  documents: Array<{
    id: string;
    fileName: string;
    mimeType: string;
    fileKind: string;
    createdAt: string;
  }>;
  products: ProductLine[];
};

type AbrechnungenResponse = {
  ok?: boolean;
  error?: string;
  count?: number;
  abrechnungen?: Abrechnung[];
};

function displayValue(value: string | null | undefined, fallback = "—") {
  return value?.trim() || fallback;
}

function formatDate(value: string) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("de-DE", { dateStyle: "medium" }).format(date);
}

function formatDateTime(value: string) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("de-DE", { dateStyle: "medium", timeStyle: "short" }).format(date);
}

function formatMoney(value: number | null, currency: string) {
  if (value === null || value === undefined) return "—";
  return new Intl.NumberFormat("de-DE", { style: "currency", currency: currency || "EUR" }).format(value);
}

function formatNumber(value: number | null, suffix = "") {
  if (value === null || value === undefined) return "—";
  return `${new Intl.NumberFormat("de-DE", { maximumFractionDigits: 3 }).format(value)}${suffix}`;
}

function formatQuantity(line: ProductLine | Batch) {
  const pieceText = line.quantityPieces !== null && line.unitWeightG !== null ? `${formatNumber(line.quantityPieces)} x ${formatNumber(line.unitWeightG, " g")}` : "";
  const total = line.totalQuantityG ?? line.quantity;
  const totalText = total !== null ? formatNumber(total, ` ${line.quantityUnit || "g"}`) : "";

  if (pieceText && totalText) return `${pieceText} = ${totalText}`;
  return totalText || pieceText || "—";
}

function statusLabel(status: string) {
  if (status === "needs_review") return "Prüfen";
  if (status === "verified") return "OK";
  if (status === "archived") return "Archiv";
  return status || "Prüfen";
}

export function AbrechnungenApp() {
  const [query, setQuery] = useState("");
  const [abrechnungen, setAbrechnungen] = useState<Abrechnung[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    let intervalId: number | undefined;
    let requestInFlight = false;

    const loadAbrechnungen = async (showSpinner: boolean) => {
      if (requestInFlight) return;
      requestInFlight = true;
      if (showSpinner) setLoading(true);
      setError("");

      try {
        const params = new URLSearchParams();
        if (query.trim()) params.set("q", query.trim());
        params.set("limit", "120");

        const response = await fetch(`/api/abrechnungen?${params.toString()}`, {
          signal: controller.signal,
          cache: "no-store",
        });
        const payload = (await response.json()) as AbrechnungenResponse;

        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || `Abrechnung lookup failed (${response.status}).`);
        }

        setAbrechnungen(payload.abrechnungen || []);
      } catch (requestError) {
        if (requestError instanceof DOMException && requestError.name === "AbortError") return;
        setAbrechnungen([]);
        setError(requestError instanceof Error ? requestError.message : "Abrechnung lookup failed.");
      } finally {
        requestInFlight = false;
        if (!controller.signal.aborted && showSpinner) setLoading(false);
      }
    };

    const timer = window.setTimeout(() => {
      void loadAbrechnungen(true);
      intervalId = window.setInterval(() => {
        if (document.visibilityState === "visible") void loadAbrechnungen(false);
      }, 30000);
    }, 220);

    return () => {
      window.clearTimeout(timer);
      if (intervalId) window.clearInterval(intervalId);
      controller.abort();
    };
  }, [query]);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <ReceiptText size={28} />
          </div>
          <div>
            <p>Inventory</p>
            <h1>Abrechnungen</h1>
          </div>
        </div>
        <nav className="subnav" aria-label="Inventory">
          <Link href="/">
            <Boxes size={18} />
            Inventory
          </Link>
          <Link href="/products">
            <Boxes size={18} />
            Produkte
          </Link>
          <Link className="active" href="/abrechnungen">
            <ReceiptText size={18} />
            Abrechnungen
          </Link>
        </nav>
      </header>

      <section className="abrechnung-toolbar">
        <div className="search-box">
          <Search size={20} />
          <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Abrechnung, Großhändler oder Produkt" />
          {query ? (
            <button className="clear-button" type="button" onClick={() => setQuery("")} aria-label="Suche leeren">
              <X size={18} />
            </button>
          ) : null}
        </div>
        <div className="table-state">
          {loading ? <Loader2 className="spin" size={18} /> : <ArrowDownWideNarrow size={18} />}
          <span>Neueste zuerst</span>
        </div>
      </section>

      {error ? (
        <div className="error-box standalone">
          <AlertTriangle size={20} />
          <span>{error}</span>
        </div>
      ) : null}

      <section className="abrechnung-feed" aria-label="Abrechnungen">
        {!loading && !error && abrechnungen.length === 0 ? (
          <div className="abrechnung-empty-card">
            <Inbox size={22} />
            <div>
              <p>Abrechnungen</p>
              <h2>Keine Abrechnungen gefunden.</h2>
            </div>
          </div>
        ) : null}

        {abrechnungen.map((abrechnung, index) => (
          <article className="abrechnung-card" key={abrechnung.id}>
            <header className="abrechnung-card-head">
              <div className="abrechnung-rank">#{index + 1}</div>
              <div className="abrechnung-title">
                <p>{displayValue(abrechnung.emailSubject, "E-Mail")}</p>
                <h2>{displayValue(abrechnung.supplierName, "Unbekannter Großhändler")}</h2>
              </div>
              <span className={`status ${abrechnung.status}`}>{statusLabel(abrechnung.status)}</span>
            </header>

            <div className="abrechnung-meta-grid">
              <div>
                <span>Rechnung</span>
                <strong>{displayValue(abrechnung.rechnungsnummer)}</strong>
              </div>
              <div>
                <span>Rechnungsdatum</span>
                <strong>{formatDate(abrechnung.rechnungsdatum)}</strong>
              </div>
              <div>
                <span>Eingang</span>
                <strong>{formatDateTime(abrechnung.receivedAt || abrechnung.createdAt)}</strong>
              </div>
              <div>
                <span>Netto</span>
                <strong>{formatMoney(abrechnung.totalNetto, abrechnung.currency)}</strong>
              </div>
              <div>
                <span>MwSt.</span>
                <strong>{formatMoney(abrechnung.totalVat, abrechnung.currency)}</strong>
              </div>
              <div>
                <span>Brutto</span>
                <strong>{formatMoney(abrechnung.totalBrutto, abrechnung.currency)}</strong>
              </div>
            </div>

            <div className="abrechnung-details">
              <div>
                <Mail size={17} />
                <span>{displayValue(abrechnung.senderEmail)}</span>
              </div>
              <div>
                <CalendarDays size={17} />
                <span>Fällig {formatDate(abrechnung.faelligkeitsdatum)}</span>
              </div>
              <div>
                <FileText size={17} />
                <span>{abrechnung.documents.length ? abrechnung.documents.map((document) => document.fileName || document.fileKind).join(", ") : "E-Mail Text"}</span>
              </div>
            </div>

            <div className="abrechnung-products">
              {abrechnung.products.map((product) => (
                <section className="abrechnung-product" key={product.id}>
                  <div className="product-line-head">
                    <div>
                      <p>Position {product.lineNumber ?? "—"}</p>
                      <h3>{displayValue(product.productName, "Unbekanntes Produkt")}</h3>
                    </div>
                    {product.productCode ? <span className="product-code">{product.productCode}</span> : null}
                  </div>

                  <div className="product-line-grid">
                    <div>
                      <span>Menge</span>
                      <strong>{formatQuantity(product)}</strong>
                    </div>
                    <div>
                      <span>Einzelpreis netto</span>
                      <strong>{formatMoney(product.unitPriceNetto, product.currency)}</strong>
                    </div>
                    <div>
                      <span>Netto</span>
                      <strong>{formatMoney(product.lineNetto, product.currency)}</strong>
                    </div>
                    <div>
                      <span>Brutto</span>
                      <strong>{formatMoney(product.lineBrutto, product.currency)}</strong>
                    </div>
                    <div>
                      <span>MwSt.</span>
                      <strong>{product.vatRate === null ? "—" : `${formatNumber(product.vatRate)} %`}</strong>
                    </div>
                  </div>

                  <div className="batch-list">
                    {product.batches.length ? (
                      product.batches.map((batch) => (
                        <div className="batch-row" key={batch.id}>
                          <span>Charge {displayValue(batch.chargennummer)}</span>
                          <span>Ablauf {formatDate(batch.expiryDate)}</span>
                          <span>{formatQuantity(batch)}</span>
                        </div>
                      ))
                    ) : (
                      <div className="batch-row muted">
                        <span>Keine Charge gespeichert</span>
                      </div>
                    )}
                  </div>
                </section>
              ))}
            </div>

            {(abrechnung.sellerName || abrechnung.debitorNumber || abrechnung.aiReason || abrechnung.reviewNote) ? (
              <footer className="abrechnung-foot">
                {abrechnung.sellerName ? <span>Verkäufer: {abrechnung.sellerName}</span> : null}
                {abrechnung.debitorNumber ? <span>Debitor: {abrechnung.debitorNumber}</span> : null}
                {abrechnung.aiConfidence !== null ? <span>AI: {formatNumber(Math.round(abrechnung.aiConfidence * 100), " %")}</span> : null}
                {abrechnung.reviewNote ? <span>{abrechnung.reviewNote}</span> : null}
              </footer>
            ) : null}
          </article>
        ))}
      </section>
    </main>
  );
}
