"use client";

import {
  AlertTriangle,
  ArrowDownWideNarrow,
  Boxes,
  CalendarDays,
  CheckCircle2,
  FileText,
  Inbox,
  Loader2,
  Mail,
  ReceiptText,
  Search,
  X,
  XCircle,
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

type ReviewResponse = {
  ok?: boolean;
  error?: string;
  id?: string;
  status?: string;
  reviewNote?: string;
};

type ReviewIssue = "product" | "quantity" | "charge" | "expiry" | "price" | "other";

const reviewIssueOptions: Array<{ key: ReviewIssue; label: string }> = [
  { key: "product", label: "Produkt" },
  { key: "quantity", label: "Menge" },
  { key: "charge", label: "Charge" },
  { key: "expiry", label: "Ablaufdatum" },
  { key: "price", label: "Preis" },
  { key: "other", label: "Sonstiges" },
];

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

function parseReviewIssue(note: string) {
  const text = note.trim();
  if (!text.startsWith("Abweichung:")) return null;

  const sections = text
    .split("|")
    .map((section) => section.trim())
    .filter(Boolean);
  const position = sections[0]?.replace(/^Abweichung:\s*/i, "").trim() || "Abweichung";
  const area = sections.find((section) => section.toLowerCase().startsWith("bereich:"))?.replace(/^Bereich:\s*/i, "");
  const detail = sections.find((section) => section.toLowerCase().startsWith("hinweis:"))?.replace(/^Hinweis:\s*/i, "").trim() || "";
  const issueTypes = area
    ? area
        .split(",")
        .map((issueType) => issueType.trim())
        .filter(Boolean)
    : [];

  return {
    position,
    issueTypes,
    detail,
  };
}

function ReviewNoteDisplay({ note }: { note: string }) {
  const issue = parseReviewIssue(note);

  if (!issue) return <span>{note}</span>;

  return (
    <div className="review-note-issue">
      <strong>Abweichung</strong>
      <span className="issue-position">{issue.position}</span>
      {issue.issueTypes.length ? (
        <div className="issue-chip-row">
          {issue.issueTypes.map((issueType) => (
            <span className="issue-chip" key={issueType}>
              {issueType}
            </span>
          ))}
        </div>
      ) : null}
      {issue.detail ? <p>{issue.detail}</p> : null}
    </div>
  );
}

export function AbrechnungenApp() {
  const [query, setQuery] = useState("");
  const [abrechnungen, setAbrechnungen] = useState<Abrechnung[]>([]);
  const [reviewTarget, setReviewTarget] = useState<Abrechnung | null>(null);
  const [reviewIssueMode, setReviewIssueMode] = useState(false);
  const [reviewIssueTypes, setReviewIssueTypes] = useState<ReviewIssue[]>([]);
  const [reviewPositionId, setReviewPositionId] = useState("all");
  const [reviewDetail, setReviewDetail] = useState("");
  const [reviewing, setReviewing] = useState<"verified" | "needs_review" | "">("");
  const [reviewError, setReviewError] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const resetReviewForm = () => {
    setReviewIssueMode(false);
    setReviewIssueTypes([]);
    setReviewPositionId("all");
    setReviewDetail("");
    setReviewError("");
  };

  const openReviewDialog = (abrechnung: Abrechnung) => {
    setReviewTarget(abrechnung);
    resetReviewForm();
  };

  const closeReviewDialog = () => {
    if (reviewing) return;
    setReviewTarget(null);
    resetReviewForm();
  };

  const toggleReviewIssue = (issueType: ReviewIssue) => {
    setReviewIssueTypes((current) => (current.includes(issueType) ? current.filter((item) => item !== issueType) : [...current, issueType]));
    setReviewError("");
  };

  const reviewPositionLabel = () => {
    if (!reviewTarget || reviewPositionId === "all") return "Gesamte Abrechnung";

    const product = reviewTarget.products.find((item) => item.id === reviewPositionId);
    if (!product) return "Gesamte Abrechnung";

    return `Position ${product.lineNumber ?? "?"}: ${displayValue(product.productName, "Unbekanntes Produkt")}`;
  };

  const submitReview = async (decision: "verified" | "needs_review") => {
    if (!reviewTarget || reviewing) return;

    if (decision === "needs_review" && reviewIssueTypes.length === 0) {
      setReviewError("Bitte wähle mindestens einen Bereich aus.");
      return;
    }

    setReviewing(decision);
    setReviewError("");

    try {
      const response = await fetch("/api/abrechnungen/review", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          id: reviewTarget.id,
          decision,
          issueTypes: decision === "needs_review" ? reviewIssueTypes : [],
          positionLabel: decision === "needs_review" ? reviewPositionLabel() : "",
          detail: decision === "needs_review" ? reviewDetail : "",
        }),
      });
      const payload = (await response.json()) as ReviewResponse;

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `Abrechnung review failed (${response.status}).`);
      }

      setAbrechnungen((current) =>
        current.map((abrechnung) =>
          abrechnung.id === reviewTarget.id
            ? {
                ...abrechnung,
                status: payload.status || decision,
                reviewNote: payload.reviewNote || abrechnung.reviewNote,
              }
            : abrechnung
        )
      );
      setReviewTarget(null);
      resetReviewForm();
    } catch (requestError) {
      setReviewError(requestError instanceof Error ? requestError.message : "Abrechnung review failed.");
    } finally {
      setReviewing("");
    }
  };

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
              <button
                className={`review-status-button status ${abrechnung.status}`}
                type="button"
                onClick={() => {
                  openReviewDialog(abrechnung);
                }}
                title="Abrechnung mit der Ware vor Ort prüfen"
              >
                {statusLabel(abrechnung.status)}
              </button>
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
                {abrechnung.reviewNote ? <ReviewNoteDisplay note={abrechnung.reviewNote} /> : null}
              </footer>
            ) : null}
          </article>
        ))}
      </section>

      {reviewTarget ? (
        <div className="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="review-title">
          <section className="modal-card review-card">
            <div className="modal-head">
              <div>
                <p>Abrechnung prüfen</p>
                <h2 id="review-title">{displayValue(reviewTarget.supplierName, "Unbekannter Großhändler")}</h2>
              </div>
              <button className="modal-close" type="button" onClick={closeReviewDialog} aria-label="Schließen" disabled={Boolean(reviewing)}>
                <X size={20} />
              </button>
            </div>

            <div className="review-question">
              <p>Stimmen die Produkte, Mengen, Chargen und Ablaufdaten mit der Ware vor Ort überein?</p>
              <span>Rechnung {displayValue(reviewTarget.rechnungsnummer)} · {reviewTarget.products.length} Positionen</span>
            </div>

            {reviewIssueMode ? (
              <div className="review-issue-form">
                <label>
                  <span>Betroffene Position</span>
                  <select value={reviewPositionId} onChange={(event) => setReviewPositionId(event.target.value)} disabled={Boolean(reviewing)}>
                    <option value="all">Gesamte Abrechnung</option>
                    {reviewTarget.products.map((product) => (
                      <option key={product.id} value={product.id}>
                        Position {product.lineNumber ?? "?"}: {displayValue(product.productName, "Unbekanntes Produkt")}
                      </option>
                    ))}
                  </select>
                </label>

                <div>
                  <span>Was stimmt nicht?</span>
                  <div className="review-issue-grid">
                    {reviewIssueOptions.map((option) => (
                      <label className="review-check" key={option.key}>
                        <input
                          type="checkbox"
                          checked={reviewIssueTypes.includes(option.key)}
                          onChange={() => toggleReviewIssue(option.key)}
                          disabled={Boolean(reviewing)}
                        />
                        {option.label}
                      </label>
                    ))}
                  </div>
                </div>

                <label>
                  <span>Kurzer Hinweis</span>
                  <textarea
                    value={reviewDetail}
                    onChange={(event) => setReviewDetail(event.target.value)}
                    placeholder="Optional"
                    maxLength={500}
                    disabled={Boolean(reviewing)}
                  />
                </label>
              </div>
            ) : null}

            {reviewError ? <div className="modal-error">{reviewError}</div> : null}

            <div className="modal-actions">
              {reviewIssueMode ? (
                <button className="secondary-action" type="button" onClick={() => setReviewIssueMode(false)} disabled={Boolean(reviewing)}>
                  Zurück
                </button>
              ) : (
                <button className="review-no-action" type="button" onClick={() => setReviewIssueMode(true)} disabled={Boolean(reviewing)}>
                  <XCircle size={18} />
                  Nein, Abweichung
                </button>
              )}
              {reviewIssueMode ? (
                <button className="review-save-issue-action" type="button" onClick={() => void submitReview("needs_review")} disabled={Boolean(reviewing)}>
                  {reviewing === "needs_review" ? <Loader2 className="spin" size={18} /> : <XCircle size={18} />}
                  Abweichung speichern
                </button>
              ) : (
                <button className="review-yes-action" type="button" onClick={() => void submitReview("verified")} disabled={Boolean(reviewing)}>
                  {reviewing === "verified" ? <Loader2 className="spin" size={18} /> : <CheckCircle2 size={18} />}
                  Ja, stimmt
                </button>
              )}
            </div>
          </section>
        </div>
      ) : null}
    </main>
  );
}
