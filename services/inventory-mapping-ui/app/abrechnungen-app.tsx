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
  Send,
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
  platformSuggestions: {
    doktorabcName: string;
    wawicanName: string;
    wawicanKultivar: string;
    status: string;
  };
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

type ProductMapping = {
  id: string;
  canonicalId: string;
  kultivar: string;
  status: string;
  productKind: string;
  reviewReason: string;
  wawicanName: string;
  doktorabcName: string;
  wawicanSearchKey: string;
  doktorabcSearchKey: string;
  wawicanStatus: string;
  doktorabcStatus: string;
};

type ProductsResponse = {
  ok?: boolean;
  error?: string;
  products?: ProductMapping[];
};

type StockUploadTarget = {
  abrechnung: Abrechnung;
  product: ProductLine;
  batch: Batch;
};

type StockUploadForm = {
  rechnungsnummer: string;
  productName: string;
  chargeNumber: string;
  expiryDate: string;
  availableGrams: string;
  nettoPerGram: string;
  bruttoPerGram: string;
  totalNetto: string;
  totalBrutto: string;
  doktorabcName: string;
  wawicanName: string;
  wawicanKultivar: string;
  doktorabcGrams: string;
  doktorabcPercent: string;
  wawicanGrams: string;
  wawicanPercent: string;
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

const emptyStockUploadForm: StockUploadForm = {
  rechnungsnummer: "",
  productName: "",
  chargeNumber: "",
  expiryDate: "",
  availableGrams: "",
  nettoPerGram: "",
  bruttoPerGram: "",
  totalNetto: "",
  totalBrutto: "",
  doktorabcName: "",
  wawicanName: "",
  wawicanKultivar: "",
  doktorabcGrams: "",
  doktorabcPercent: "",
  wawicanGrams: "",
  wawicanPercent: "",
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

function formatInputNumber(value: number, fractionDigits = 2) {
  if (!Number.isFinite(value)) return "";
  const rounded = Number(value.toFixed(fractionDigits));
  return String(rounded).replace(".", ",");
}

function formatMoneyPerGram(value: number | null, currency: string) {
  if (value === null || value === undefined) return "—";
  const formatted = new Intl.NumberFormat("de-DE", {
    style: "currency",
    currency: currency || "EUR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 3,
  }).format(value);
  return `${formatted} / g`;
}

function formatQuantity(line: ProductLine | Batch) {
  const pieceText = line.quantityPieces !== null && line.unitWeightG !== null ? `${formatNumber(line.quantityPieces)} x ${formatNumber(line.unitWeightG, " g")}` : "";
  const total = line.totalQuantityG ?? line.quantity;
  const totalText = total !== null ? formatNumber(total, ` ${line.quantityUnit || "g"}`) : "";

  if (pieceText && totalText) return `${pieceText} = ${totalText}`;
  return totalText || pieceText || "—";
}

function parseDecimalInput(value: string) {
  const cleaned = value.trim().replace(/\s/g, "").replace(/[^\d,.-]/g, "");
  if (!cleaned) return null;
  const normalized =
    cleaned.includes(",") && cleaned.includes(".") ? cleaned.replace(/\./g, "").replace(",", ".") : cleaned.includes(",") ? cleaned.replace(",", ".") : cleaned;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

function normalizeText(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9/]+/g, " ")
    .trim();
}

function uniqueTerms(value: string) {
  return Array.from(new Set(normalizeText(value).split(/\s+/).filter((term) => term.length > 1)));
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

function lineTotalGrams(product: ProductLine) {
  if (product.totalQuantityG !== null) return product.totalQuantityG;
  if (product.quantityPieces !== null && product.unitWeightG !== null) return product.quantityPieces * product.unitWeightG;
  if (product.quantity !== null && (product.quantityUnit || "g").toLowerCase().includes("g")) return product.quantity;
  return null;
}

function batchTotalGrams(batch: Batch) {
  if (batch.totalQuantityG !== null) return batch.totalQuantityG;
  if (batch.quantity !== null) return batch.quantity;
  if (batch.quantityPieces !== null && batch.unitWeightG !== null) return batch.quantityPieces * batch.unitWeightG;
  return null;
}

function calculatePricePerGram(product: ProductLine, kind: "netto" | "brutto") {
  const totalGrams = lineTotalGrams(product);
  const lineAmount = kind === "netto" ? product.lineNetto : product.lineBrutto;
  if (lineAmount !== null && totalGrams !== null && totalGrams > 0) return lineAmount / totalGrams;

  const unitAmount = kind === "netto" ? product.unitPriceNetto : product.unitPriceBrutto;
  if (unitAmount !== null && product.unitWeightG !== null && product.unitWeightG > 0) return unitAmount / product.unitWeightG;

  return null;
}

function scoreProductMapping(product: ProductLine, mapping: ProductMapping, platform: "doktorabc" | "wawican") {
  const platformName = platform === "doktorabc" ? mapping.doktorabcName : mapping.wawicanName;
  if (!platformName) return -1;

  const terms = uniqueTerms([product.productName, product.productCode].filter(Boolean).join(" "));
  const haystack = normalizeText([platformName, mapping.kultivar, mapping.doktorabcSearchKey, mapping.wawicanSearchKey, mapping.canonicalId].filter(Boolean).join(" "));
  const directNeedle = normalizeText(product.productName);
  let score = 0;

  if (directNeedle && haystack.includes(directNeedle)) score += 8;
  for (const term of terms) {
    if (haystack.includes(term)) score += term.length > 3 ? 3 : 1;
  }
  if (mapping.status === "verified") score += 1;

  return score;
}

function bestMappingSuggestion(product: ProductLine, products: ProductMapping[], platform: "doktorabc" | "wawican") {
  let best: ProductMapping | null = null;
  let bestScore = -1;

  for (const mapping of products) {
    const score = scoreProductMapping(product, mapping, platform);
    if (score > bestScore) {
      best = mapping;
      bestScore = score;
    }
  }

  return bestScore > 0 ? best : null;
}

function uniqueProductOptions(products: ProductMapping[], platform: "doktorabc" | "wawican") {
  const seen = new Set<string>();
  const options: ProductMapping[] = [];

  for (const product of products) {
    const name = platform === "doktorabc" ? product.doktorabcName.trim() : product.wawicanName.trim();
    if (!name) continue;
    const key = platform === "doktorabc" ? name.toLowerCase() : `${name.toLowerCase()}::${product.kultivar.trim().toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    options.push(product);
  }

  return options.sort((a, b) => {
    const left = platform === "doktorabc" ? a.doktorabcName : `${a.wawicanName} ${a.kultivar}`;
    const right = platform === "doktorabc" ? b.doktorabcName : `${b.wawicanName} ${b.kultivar}`;
    return left.localeCompare(right, "de");
  });
}

function findDoktorabcOption(options: ProductMapping[], name: string) {
  const trimmed = name.trim();
  if (!trimmed) return null;
  return options.find((option) => option.doktorabcName.trim() === trimmed) || null;
}

function findWawicanOption(options: ProductMapping[], name: string, kultivar: string) {
  const trimmedName = name.trim();
  const trimmedKultivar = kultivar.trim();
  if (!trimmedName || !trimmedKultivar) return null;
  return options.find((option) => option.wawicanName.trim() === trimmedName && option.kultivar.trim() === trimmedKultivar) || null;
}

function suggestionValue(value: string) {
  const trimmed = value.trim();
  return trimmed && trimmed.toUpperCase() !== "UNKNOWN" ? trimmed : "";
}

function stockAmount(value: string) {
  const parsed = parseDecimalInput(value);
  return parsed !== null && parsed > 0 ? parsed : 0;
}

function formatStockInput(value: number | null, fractionDigits = 2) {
  if (value === null || value === undefined) return "";
  return formatInputNumber(value, fractionDigits);
}

function calculatedBatchTotal(batchGrams: number | null, pricePerGram: number | null) {
  if (batchGrams === null || pricePerGram === null) return null;
  return batchGrams * pricePerGram;
}

function initialStockUploadForm(target: StockUploadTarget, products: ProductMapping[]): StockUploadForm {
  const batchGrams = batchTotalGrams(target.batch);
  const nettoPerGram = calculatePricePerGram(target.product, "netto");
  const bruttoPerGram = calculatePricePerGram(target.product, "brutto");
  const doktorabcSuggestion = bestMappingSuggestion(target.product, products, "doktorabc");
  const wawicanSuggestion = bestMappingSuggestion(target.product, products, "wawican");
  const storedDoktorabcName = suggestionValue(target.product.platformSuggestions.doktorabcName);
  const storedWawicanName = suggestionValue(target.product.platformSuggestions.wawicanName);
  const storedWawicanKultivar = suggestionValue(target.product.platformSuggestions.wawicanKultivar);
  const storedDoktorabcOption = storedDoktorabcName ? findDoktorabcOption(uniqueProductOptions(products, "doktorabc"), storedDoktorabcName) : null;
  const storedWawicanOption =
    storedWawicanName && storedWawicanKultivar ? findWawicanOption(uniqueProductOptions(products, "wawican"), storedWawicanName, storedWawicanKultivar) : null;

  return {
    ...emptyStockUploadForm,
    rechnungsnummer: target.abrechnung.rechnungsnummer,
    productName: target.product.productName,
    chargeNumber: target.batch.chargennummer,
    expiryDate: target.batch.expiryDate,
    availableGrams: formatStockInput(batchGrams, 2),
    nettoPerGram: formatStockInput(nettoPerGram, 3),
    bruttoPerGram: formatStockInput(bruttoPerGram, 3),
    totalNetto: formatStockInput(calculatedBatchTotal(batchGrams, nettoPerGram), 2),
    totalBrutto: formatStockInput(calculatedBatchTotal(batchGrams, bruttoPerGram), 2),
    doktorabcName: storedDoktorabcOption?.doktorabcName || doktorabcSuggestion?.doktorabcName || "",
    wawicanName: storedWawicanOption?.wawicanName || wawicanSuggestion?.wawicanName || "",
    wawicanKultivar: storedWawicanOption?.kultivar || wawicanSuggestion?.kultivar || "",
  };
}

export function AbrechnungenApp() {
  const [query, setQuery] = useState("");
  const [abrechnungen, setAbrechnungen] = useState<Abrechnung[]>([]);
  const [productMappings, setProductMappings] = useState<ProductMapping[]>([]);
  const [productsLoading, setProductsLoading] = useState(true);
  const [productsError, setProductsError] = useState("");
  const [reviewTarget, setReviewTarget] = useState<Abrechnung | null>(null);
  const [reviewIssueMode, setReviewIssueMode] = useState(false);
  const [reviewIssueTypes, setReviewIssueTypes] = useState<ReviewIssue[]>([]);
  const [reviewPositionId, setReviewPositionId] = useState("all");
  const [reviewDetail, setReviewDetail] = useState("");
  const [reviewing, setReviewing] = useState<"verified" | "needs_review" | "">("");
  const [reviewError, setReviewError] = useState("");
  const [stockUploadTarget, setStockUploadTarget] = useState<StockUploadTarget | null>(null);
  const [stockUploadForm, setStockUploadForm] = useState<StockUploadForm>(emptyStockUploadForm);
  const [stockUploadNotice, setStockUploadNotice] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const doktorabcOptions = uniqueProductOptions(productMappings, "doktorabc");
  const wawicanOptions = uniqueProductOptions(productMappings, "wawican");
  const wawicanKultivarOptions = Array.from(
    new Set(
      wawicanOptions
        .filter((option) => !stockUploadForm.wawicanName.trim() || option.wawicanName.trim() === stockUploadForm.wawicanName.trim())
        .map((option) => option.kultivar.trim())
        .filter(Boolean)
    )
  ).sort((a, b) => a.localeCompare(b, "de"));
  const selectedDoktorabcOption = findDoktorabcOption(doktorabcOptions, stockUploadForm.doktorabcName);
  const selectedWawicanOption = findWawicanOption(wawicanOptions, stockUploadForm.wawicanName, stockUploadForm.wawicanKultivar);
  const stockAvailableGrams = stockAmount(stockUploadForm.availableGrams);
  const stockDoktorabcGrams = stockAmount(stockUploadForm.doktorabcGrams);
  const stockWawicanGrams = stockAmount(stockUploadForm.wawicanGrams);
  const stockAllocatedGrams = stockDoktorabcGrams + stockWawicanGrams;
  const stockAllocationDiff = stockAvailableGrams - stockAllocatedGrams;
  const stockDoktorabcNeedsProduct = stockDoktorabcGrams > 0;
  const stockWawicanNeedsProduct = stockWawicanGrams > 0;
  const stockOptionId = stockUploadTarget?.batch.id.replace(/[^a-zA-Z0-9_-]/g, "") || "stock-upload";
  const stockCanPrepare = Boolean(
    stockUploadTarget &&
      stockAvailableGrams > 0 &&
      stockAllocatedGrams > 0 &&
      Math.abs(stockAllocationDiff) <= 0.01 &&
      (!stockDoktorabcNeedsProduct || selectedDoktorabcOption) &&
      (!stockWawicanNeedsProduct || selectedWawicanOption)
  );

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

  const openStockUploadDialog = (abrechnung: Abrechnung, product: ProductLine, batch: Batch) => {
    const target = { abrechnung, product, batch };
    setStockUploadTarget(target);
    setStockUploadForm(initialStockUploadForm(target, productMappings));
    setStockUploadNotice("");
  };

  const closeStockUploadDialog = () => {
    setStockUploadTarget(null);
    setStockUploadForm(emptyStockUploadForm);
    setStockUploadNotice("");
  };

  const updateStockUploadField = (field: keyof StockUploadForm, value: string) => {
    setStockUploadNotice("");
    setStockUploadForm((current) => {
      const next = {
        ...current,
        [field]: value,
      };

      if (field === "availableGrams" || field === "nettoPerGram" || field === "bruttoPerGram") {
        const grams = field === "availableGrams" ? parseDecimalInput(value) : parseDecimalInput(current.availableGrams);
        const netto = field === "nettoPerGram" ? parseDecimalInput(value) : parseDecimalInput(current.nettoPerGram);
        const brutto = field === "bruttoPerGram" ? parseDecimalInput(value) : parseDecimalInput(current.bruttoPerGram);
        if (grams !== null && netto !== null) next.totalNetto = formatStockInput(grams * netto, 2);
        if (grams !== null && brutto !== null) next.totalBrutto = formatStockInput(grams * brutto, 2);
      }

      if (field === "wawicanName") {
        const match = wawicanOptions.find((option) => option.wawicanName.trim() === value.trim());
        if (match) next.wawicanKultivar = match.kultivar;
      }

      return next;
    });
  };

  const updateStockAllocation = (platform: "doktorabc" | "wawican", unit: "grams" | "percent", value: string) => {
    setStockUploadNotice("");
    setStockUploadForm((current) => {
      const available = stockAmount(current.availableGrams);
      const parsed = parseDecimalInput(value);
      const ownGrams = parsed === null ? null : unit === "percent" ? (available * clamp(parsed, 0, 100)) / 100 : clamp(parsed, 0, available);

      if (ownGrams === null) {
        return {
          ...current,
          [platform === "doktorabc" ? (unit === "grams" ? "doktorabcGrams" : "doktorabcPercent") : unit === "grams" ? "wawicanGrams" : "wawicanPercent"]: value,
        };
      }

      const otherGrams = Math.max(0, available - ownGrams);
      const ownPercent = available > 0 ? (ownGrams / available) * 100 : 0;
      const otherPercent = available > 0 ? (otherGrams / available) * 100 : 0;

      if (platform === "doktorabc") {
        return {
          ...current,
          doktorabcGrams: formatStockInput(ownGrams, 2),
          doktorabcPercent: formatStockInput(ownPercent, 2),
          wawicanGrams: formatStockInput(otherGrams, 2),
          wawicanPercent: formatStockInput(otherPercent, 2),
        };
      }

      return {
        ...current,
        wawicanGrams: formatStockInput(ownGrams, 2),
        wawicanPercent: formatStockInput(ownPercent, 2),
        doktorabcGrams: formatStockInput(otherGrams, 2),
        doktorabcPercent: formatStockInput(otherPercent, 2),
      };
    });
  };

  const prepareStockUpload = () => {
    if (!stockUploadTarget || !stockCanPrepare) return;

    setStockUploadNotice(
      `Bereit: ${formatNumber(stockDoktorabcGrams, " g")} DoktorABC, ${formatNumber(stockWawicanGrams, " g")} Wawican. Der Bot ist noch nicht verbunden.`
    );
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

    const loadProducts = async () => {
      setProductsLoading(true);
      setProductsError("");

      try {
        const response = await fetch("/api/products?kind=all", {
          signal: controller.signal,
          cache: "no-store",
        });
        const payload = (await response.json()) as ProductsResponse;

        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || `Product mapping lookup failed (${response.status}).`);
        }

        setProductMappings(payload.products || []);
      } catch (requestError) {
        if (requestError instanceof DOMException && requestError.name === "AbortError") return;
        setProductsError(requestError instanceof Error ? requestError.message : "Product mapping lookup failed.");
      } finally {
        if (!controller.signal.aborted) setProductsLoading(false);
      }
    };

    void loadProducts();

    return () => {
      controller.abort();
    };
  }, []);

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
              {abrechnung.products.map((product) => {
                const nettoPerGram = calculatePricePerGram(product, "netto");
                const bruttoPerGram = calculatePricePerGram(product, "brutto");

                return (
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
                        <span>Netto €/g</span>
                        <strong>{formatMoneyPerGram(nettoPerGram, product.currency)}</strong>
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
                        <span>Brutto €/g</span>
                        <strong>{formatMoneyPerGram(bruttoPerGram, product.currency)}</strong>
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
                            <button className="batch-send-button" type="button" onClick={() => openStockUploadDialog(abrechnung, product, batch)}>
                              <Send size={15} />
                              Bestand senden
                            </button>
                          </div>
                        ))
                      ) : (
                        <div className="batch-row muted">
                          <span>Keine Charge gespeichert</span>
                        </div>
                      )}
                    </div>
                  </section>
                );
              })}
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

      {stockUploadTarget ? (
        <div className="modal-backdrop" role="dialog" aria-modal="true" aria-labelledby="stock-upload-title">
          <section className="modal-card stock-upload-card">
            <div className="modal-head">
              <div>
                <p>Bestand senden</p>
                <h2 id="stock-upload-title">{displayValue(stockUploadForm.productName, "Produkt")}</h2>
              </div>
              <button className="modal-close" type="button" onClick={closeStockUploadDialog} aria-label="Schließen">
                <X size={20} />
              </button>
            </div>

            <div className="stock-source-strip">
              <span>Rechnung {displayValue(stockUploadForm.rechnungsnummer)}</span>
              <span>Position {stockUploadTarget.product.lineNumber ?? "—"}</span>
              <span>Charge {displayValue(stockUploadForm.chargeNumber)}</span>
            </div>

            {productsError ? <div className="modal-error">{productsError}</div> : null}
            {productsLoading ? (
              <div className="stock-loading">
                <Loader2 className="spin" size={17} />
                <span>Produkte werden geladen…</span>
              </div>
            ) : null}

            <div className="stock-edit-grid">
              <label>
                <span>Rechnungsnummer</span>
                <input value={stockUploadForm.rechnungsnummer} onChange={(event) => updateStockUploadField("rechnungsnummer", event.target.value)} />
              </label>
              <label>
                <span>Produkt</span>
                <input value={stockUploadForm.productName} onChange={(event) => updateStockUploadField("productName", event.target.value)} />
              </label>
              <label>
                <span>Charge</span>
                <input value={stockUploadForm.chargeNumber} onChange={(event) => updateStockUploadField("chargeNumber", event.target.value)} />
              </label>
              <label>
                <span>Ablaufdatum</span>
                <input value={stockUploadForm.expiryDate} onChange={(event) => updateStockUploadField("expiryDate", event.target.value)} placeholder="TT.MM.JJJJ" />
              </label>
              <label>
                <span>Menge gesamt (g)</span>
                <input inputMode="decimal" value={stockUploadForm.availableGrams} onChange={(event) => updateStockUploadField("availableGrams", event.target.value)} />
              </label>
              <label>
                <span>Netto €/g</span>
                <input inputMode="decimal" value={stockUploadForm.nettoPerGram} onChange={(event) => updateStockUploadField("nettoPerGram", event.target.value)} />
              </label>
              <label>
                <span>Brutto €/g</span>
                <input inputMode="decimal" value={stockUploadForm.bruttoPerGram} onChange={(event) => updateStockUploadField("bruttoPerGram", event.target.value)} />
              </label>
              <label>
                <span>Gesamt netto</span>
                <input inputMode="decimal" value={stockUploadForm.totalNetto} onChange={(event) => updateStockUploadField("totalNetto", event.target.value)} />
              </label>
              <label>
                <span>Gesamt brutto</span>
                <input inputMode="decimal" value={stockUploadForm.totalBrutto} onChange={(event) => updateStockUploadField("totalBrutto", event.target.value)} />
              </label>
            </div>

            <div className="stock-platform-grid">
              <section className="stock-platform-panel">
                <h3>DoktorABC</h3>
                <label>
                  <span>DoktorABC Produkt</span>
                  <input
                    list={`doktorabc-products-${stockOptionId}`}
                    value={stockUploadForm.doktorabcName}
                    onChange={(event) => updateStockUploadField("doktorabcName", event.target.value)}
                    placeholder="Exakter DoktorABC Name"
                  />
                </label>
                <div className="stock-split-grid">
                  <label>
                    <span>Gramm</span>
                    <input inputMode="decimal" value={stockUploadForm.doktorabcGrams} onChange={(event) => updateStockAllocation("doktorabc", "grams", event.target.value)} />
                  </label>
                  <label>
                    <span>Prozent</span>
                    <input inputMode="decimal" value={stockUploadForm.doktorabcPercent} onChange={(event) => updateStockAllocation("doktorabc", "percent", event.target.value)} />
                  </label>
                </div>
                {stockDoktorabcNeedsProduct && !selectedDoktorabcOption ? <p className="stock-field-warning">Bitte ein echtes DoktorABC Produkt auswählen.</p> : null}
              </section>

              <section className="stock-platform-panel">
                <h3>Wawican</h3>
                <label>
                  <span>Wawican Name</span>
                  <input
                    list={`wawican-products-${stockOptionId}`}
                    value={stockUploadForm.wawicanName}
                    onChange={(event) => updateStockUploadField("wawicanName", event.target.value)}
                    placeholder="Exakter Wawican Name"
                  />
                </label>
                <label>
                  <span>Kultivar</span>
                  <input
                    list={`wawican-kultivars-${stockOptionId}`}
                    value={stockUploadForm.wawicanKultivar}
                    onChange={(event) => updateStockUploadField("wawicanKultivar", event.target.value)}
                    placeholder="Exakter Kultivar"
                  />
                </label>
                <div className="stock-split-grid">
                  <label>
                    <span>Gramm</span>
                    <input inputMode="decimal" value={stockUploadForm.wawicanGrams} onChange={(event) => updateStockAllocation("wawican", "grams", event.target.value)} />
                  </label>
                  <label>
                    <span>Prozent</span>
                    <input inputMode="decimal" value={stockUploadForm.wawicanPercent} onChange={(event) => updateStockAllocation("wawican", "percent", event.target.value)} />
                  </label>
                </div>
                {stockWawicanNeedsProduct && !selectedWawicanOption ? <p className="stock-field-warning">Bitte echten Wawican Name und Kultivar auswählen.</p> : null}
              </section>
            </div>

            <datalist id={`doktorabc-products-${stockOptionId}`}>
              {doktorabcOptions.map((product) => (
                <option key={`doktorabc-${product.id}`} value={product.doktorabcName} />
              ))}
            </datalist>
            <datalist id={`wawican-products-${stockOptionId}`}>
              {wawicanOptions.map((product) => (
                <option key={`wawican-${product.id}`} value={product.wawicanName} />
              ))}
            </datalist>
            <datalist id={`wawican-kultivars-${stockOptionId}`}>
              {wawicanKultivarOptions.map((kultivar) => (
                <option key={kultivar} value={kultivar} />
              ))}
            </datalist>

            <div className="stock-payload-preview">
              <div>
                <span>Verfügbar</span>
                <strong>{formatNumber(stockAvailableGrams, " g")}</strong>
              </div>
              <div>
                <span>DoktorABC</span>
                <strong>{formatNumber(stockDoktorabcGrams, " g")}</strong>
              </div>
              <div>
                <span>Wawican</span>
                <strong>{formatNumber(stockWawicanGrams, " g")}</strong>
              </div>
              <div className={Math.abs(stockAllocationDiff) <= 0.01 ? "ok" : "warn"}>
                <span>Differenz</span>
                <strong>{formatNumber(stockAllocationDiff, " g")}</strong>
              </div>
            </div>

            {stockAllocatedGrams > 0 && Math.abs(stockAllocationDiff) > 0.01 ? (
              <div className="modal-error">Die Gramm-Aufteilung muss genau zur verfügbaren Menge passen.</div>
            ) : null}
            {stockUploadNotice ? <div className="create-note success">{stockUploadNotice}</div> : null}

            <div className="modal-actions">
              <button className="secondary-action" type="button" onClick={closeStockUploadDialog}>
                Abbrechen
              </button>
              <button className="save-action" type="button" onClick={prepareStockUpload} disabled={!stockCanPrepare}>
                <Send size={18} />
                Payload vorbereiten
              </button>
            </div>
          </section>
        </div>
      ) : null}

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
