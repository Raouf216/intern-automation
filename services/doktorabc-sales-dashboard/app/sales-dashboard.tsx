"use client";

import { BarChart3, CalendarDays, Check, ChevronDown, FlaskConical, LogOut, PackageCheck, Pill, RefreshCw, RotateCcw, Scale, Search, X } from "lucide-react";
import { useMemo, useState } from "react";
import { BrandMark } from "./brand-mark";
import type { SalesMetricKey, SalesRow } from "../lib/sales-data";
import { ThemeToggle } from "./theme-toggle";

type Props = {
  initialRows: SalesRow[];
  loadError: string | null;
};

type PeriodKey = "day" | "week" | "month" | "year";

type ChartPoint = {
  key: string;
  label: string;
  value: number;
  sold_products: number;
  returned_products: number;
  net_products: number;
  returned_grams: number;
  net_grams: number;
  orders: number;
};

type ProductOption = {
  label: string;
  value: string;
};

const periodOptions: Array<{ label: string; value: PeriodKey }> = [
  { label: "Tag", value: "day" },
  { label: "Woche", value: "week" },
  { label: "Monat", value: "month" },
  { label: "Jahr", value: "year" },
];

const metricOptions: Array<{ description: string; icon: React.ReactNode; label: string; value: SalesMetricKey }> = [
  { description: "Einheiten aus normalen Abrechnungen", icon: <PackageCheck size={18} />, label: "Verkaufte Einheiten", value: "sold_products" },
  { description: "Einheiten aus RETURN-Zeilen", icon: <RotateCcw size={18} />, label: "Retouren", value: "returned_products" },
  { description: "Verkauft minus retourniert", icon: <BarChart3 size={18} />, label: "Netto Einheiten", value: "net_products" },
  { description: "Gramm aus RETURN-Zeilen", icon: <Scale size={18} />, label: "Retouren Gramm", value: "returned_grams" },
  { description: "Verkaufte Gramm minus Retouren", icon: <FlaskConical size={18} />, label: "Netto Gramm", value: "net_grams" },
];

const emptyMetricTotals: Record<SalesMetricKey, number> = {
  sold_products: 0,
  returned_products: 0,
  net_products: 0,
  returned_grams: 0,
  net_grams: 0,
};

export function SalesDashboard({ initialRows, loadError }: Props) {
  const [period, setPeriod] = useState<PeriodKey>("day");
  const [metric, setMetric] = useState<SalesMetricKey>("net_products");
  const [selectedProduct, setSelectedProduct] = useState("__all__");

  const productOptions = useMemo(() => {
    const products = new Map<string, string>();

    initialRows.forEach((row) => {
      if (row.product_key && row.product_name) {
        products.set(row.product_key, row.product_name);
      }
    });

    return Array.from(products.entries())
      .map(([value, label]) => ({ label, value }))
      .sort((left, right) => left.label.localeCompare(right.label, "de"));
  }, [initialRows]);

  const chartPoints = useMemo(() => buildChartPoints(initialRows, period, metric, selectedProduct), [initialRows, metric, period, selectedProduct]);
  const totals = useMemo(() => sumTotals(chartPoints), [chartPoints]);
  const metricOption = metricOptions.find((option) => option.value === metric) || metricOptions[2];
  const selectedPeriodLabel = periodOptions.find((option) => option.value === period)?.label || "Tag";
  const selectedProductLabel =
    selectedProduct === "__all__" ? "Alle Produkte" : productOptions.find((option) => option.value === selectedProduct)?.label || "Produkt";
  const showProductSelector = productOptions.length > 1;

  async function logout() {
    await fetch("/api/auth/logout", { method: "POST" });
    window.location.href = "/login";
  }

  return (
    <main className="dashboard-shell">
      <header className="dashboard-header">
        <div className="brand-row">
          <BrandMark />
          <div>
            <p className="eyebrow">Rats-Apotheke Blieskastel</p>
            <h1>DoktorABC Sales</h1>
          </div>
        </div>
        <div className="header-actions">
          <ThemeToggle />
          <button className="ghost-button" onClick={() => window.location.reload()} type="button">
            <RefreshCw size={17} />
            <span>Aktualisieren</span>
          </button>
          <button className="icon-button" onClick={logout} type="button" aria-label="Abmelden">
            <LogOut size={18} />
          </button>
        </div>
      </header>

      <section className="control-band" aria-label="Filter">
        <div className="control-group period-control">
          <span>
            <CalendarDays size={17} />
            Zeitraum
          </span>
          <div className="segmented-control">
            {periodOptions.map((option) => (
              <button className={period === option.value ? "active" : ""} key={option.value} onClick={() => setPeriod(option.value)} type="button">
                {option.label}
              </button>
            ))}
          </div>
        </div>

        <div className="control-group metric-control">
          <span>
            {metricOption.icon}
            Kennzahl
          </span>
          <div className="metric-grid">
            {metricOptions.map((option) => (
              <button className={metric === option.value ? "metric-option active" : "metric-option"} key={option.value} onClick={() => setMetric(option.value)} type="button">
                {option.icon}
                <strong>{option.label}</strong>
                <small>{option.description}</small>
              </button>
            ))}
          </div>
        </div>

        {showProductSelector ? (
          <ProductCombobox onChange={setSelectedProduct} options={productOptions} selectedLabel={selectedProductLabel} value={selectedProduct} />
        ) : null}
      </section>

      {loadError ? (
        <section className="error-banner">
          <strong>Daten konnten nicht geladen werden.</strong>
          <span>{loadError}</span>
        </section>
      ) : null}

      <section className="summary-strip" aria-label="Zusammenfassung">
        <SummaryTile label="Verkauft" value={totals.sold_products} />
        <SummaryTile label="Retouren" value={totals.returned_products} tone="warn" />
        <SummaryTile label="Netto Einheiten" value={totals.net_products} tone={totals.net_products < 0 ? "danger" : "good"} />
        <SummaryTile label="Retouren Gramm" value={totals.returned_grams} unit="g" tone="warn" />
        <SummaryTile label="Netto Gramm" value={totals.net_grams} unit="g" tone={totals.net_grams < 0 ? "danger" : "good"} />
      </section>

      <section className="chart-panel">
        <div className="chart-heading">
          <div>
            <p className="eyebrow">Auswertung</p>
            <h2>{metricOption.label}</h2>
          </div>
          <div className="active-filter">
            <span>{selectedPeriodLabel}</span>
            <span>{selectedProductLabel}</span>
          </div>
        </div>
        <SalesLineChart metricLabel={metricOption.label} points={chartPoints} />
      </section>
    </main>
  );
}

function ProductCombobox({
  onChange,
  options,
  selectedLabel,
  value,
}: {
  onChange: (value: string) => void;
  options: ProductOption[];
  selectedLabel: string;
  value: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const filteredOptions = useMemo(() => {
    const search = normalizeText(query);

    if (!search) {
      return options;
    }

    return options.filter((option) => normalizeText(option.label).includes(search));
  }, [options, query]);
  const inputValue = open ? query : selectedLabel;
  const hasProductSelected = value !== "__all__";

  function openPicker() {
    setQuery(hasProductSelected ? selectedLabel : "");
    setOpen(true);
  }

  function chooseProduct(nextValue: string) {
    onChange(nextValue);
    setQuery("");
    setOpen(false);
  }

  function handleKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      setOpen(false);
      setQuery("");
      return;
    }

    if (event.key !== "Enter") {
      return;
    }

    event.preventDefault();

    if (!query.trim()) {
      chooseProduct("__all__");
      return;
    }

    if (filteredOptions[0]) {
      chooseProduct(filteredOptions[0].value);
    }
  }

  return (
    <label className="product-select product-search">
      <span>Produkt</span>
      <div className="product-combobox" onBlur={() => window.setTimeout(() => setOpen(false), 110)}>
        <div className="product-search-field">
          <Search className="product-search-icon" size={18} />
          <input
            aria-controls="product-results"
            aria-expanded={open}
            autoComplete="off"
            onChange={(event) => {
              setQuery(event.target.value);
              setOpen(true);
            }}
            onFocus={openPicker}
            onKeyDown={handleKeyDown}
            placeholder="Produkt suchen..."
            role="combobox"
            value={inputValue}
          />
          {hasProductSelected ? (
            <button
              className="product-clear"
              onClick={(event) => {
                event.preventDefault();
                chooseProduct("__all__");
              }}
              type="button"
              aria-label="Produktauswahl zuruecksetzen"
            >
              <X size={17} />
            </button>
          ) : (
            <ChevronDown className="product-chevron" size={18} />
          )}
        </div>

        {open ? (
          <div className="product-results" id="product-results" role="listbox">
            <button
              className={value === "__all__" ? "product-result active" : "product-result"}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => chooseProduct("__all__")}
              type="button"
              role="option"
              aria-selected={value === "__all__"}
            >
              <span>Alle Produkte</span>
              {value === "__all__" ? <Check size={17} /> : null}
            </button>

            {filteredOptions.length ? (
              filteredOptions.map((product) => (
                <button
                  className={value === product.value ? "product-result active" : "product-result"}
                  key={product.value}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => chooseProduct(product.value)}
                  type="button"
                  role="option"
                  aria-selected={value === product.value}
                >
                  <span>{product.label}</span>
                  {value === product.value ? <Check size={17} /> : null}
                </button>
              ))
            ) : (
              <div className="product-empty">Kein Produkt gefunden</div>
            )}
          </div>
        ) : null}
      </div>
    </label>
  );
}

function SummaryTile({ label, tone = "neutral", unit, value }: { label: string; tone?: "danger" | "good" | "neutral" | "warn"; unit?: string; value: number }) {
  return (
    <article className={`summary-tile tone-${tone}`}>
      <span>{label}</span>
      <strong>
        {formatNumber(value)}
        {unit ? <small>{unit}</small> : null}
      </strong>
    </article>
  );
}

function SalesLineChart({ metricLabel, points }: { metricLabel: string; points: ChartPoint[] }) {
  if (!points.length) {
    return (
      <div className="empty-chart">
        <Pill size={34} />
        <strong>Keine Werte gefunden</strong>
      </div>
    );
  }

  const width = 980;
  const height = 390;
  const margin = { bottom: 58, left: 74, right: 32, top: 26 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const values = points.map((point) => point.value);
  const minValue = Math.min(0, ...values);
  const maxValue = Math.max(0, ...values);
  const span = Math.max(1, maxValue - minValue);
  const paddedMin = minValue - span * 0.12;
  const paddedMax = maxValue + span * 0.12;
  const xFor = (index: number) => margin.left + (points.length === 1 ? innerWidth / 2 : (index / (points.length - 1)) * innerWidth);
  const yFor = (value: number) => margin.top + ((paddedMax - value) / (paddedMax - paddedMin)) * innerHeight;
  const linePath = points.map((point, index) => `${index === 0 ? "M" : "L"} ${xFor(index)} ${yFor(point.value)}`).join(" ");
  const zeroY = yFor(0);
  const ticks = makeTicks(paddedMin, paddedMax, 5);
  const xLabelEvery = Math.max(1, Math.ceil(points.length / 7));

  return (
    <div className="chart-wrap">
      <svg role="img" aria-label={metricLabel} viewBox={`0 0 ${width} ${height}`}>
        <defs>
          <linearGradient id="salesLine" x1="0" x2="1" y1="0" y2="0">
            <stop offset="0%" stopColor="#0d9488" />
            <stop offset="55%" stopColor="#22c55e" />
            <stop offset="100%" stopColor="#2563eb" />
          </linearGradient>
        </defs>
        {ticks.map((tick) => (
          <g key={tick}>
            <line className="grid-line" x1={margin.left} x2={width - margin.right} y1={yFor(tick)} y2={yFor(tick)} />
            <text className="axis-label" x={margin.left - 14} y={yFor(tick) + 5} textAnchor="end">
              {formatCompact(tick)}
            </text>
          </g>
        ))}
        <line className="zero-line" x1={margin.left} x2={width - margin.right} y1={zeroY} y2={zeroY} />
        <path className="chart-line" d={linePath} />
        {points.map((point, index) => (
          <g key={point.key}>
            <circle className="chart-dot" cx={xFor(index)} cy={yFor(point.value)} r={5} />
            {index % xLabelEvery === 0 || index === points.length - 1 ? (
              <text className="x-label" x={xFor(index)} y={height - 24} textAnchor="middle">
                {point.label}
              </text>
            ) : null}
            <title>{`${point.label}: ${formatNumber(point.value)}`}</title>
          </g>
        ))}
      </svg>
    </div>
  );
}

function buildChartPoints(rows: SalesRow[], period: PeriodKey, metric: SalesMetricKey, selectedProduct: string): ChartPoint[] {
  const sourceRows = period === "year" ? rows.filter((row) => row.period_type === "month") : rows.filter((row) => row.period_type === period);
  const filteredRows = sourceRows.filter((row) => selectedProduct === "__all__" || row.product_key === selectedProduct);
  const buckets = new Map<string, ChartPoint>();

  filteredRows.forEach((row) => {
    const key = period === "year" ? row.period_start.slice(0, 4) : row.period_start;
    const current =
      buckets.get(key) ||
      ({
        key,
        label: formatPeriodLabel(key, period),
        value: 0,
        ...emptyMetricTotals,
        orders: 0,
      } satisfies ChartPoint);

    current.sold_products += row.sold_products;
    current.returned_products += row.returned_products;
    current.net_products += row.net_products;
    current.returned_grams += row.returned_grams;
    current.net_grams += row.net_grams;
    current.orders += row.orders;
    current.value += row[metric];
    buckets.set(key, current);
  });

  return Array.from(buckets.values()).sort((left, right) => left.key.localeCompare(right.key));
}

function sumTotals(points: ChartPoint[]) {
  return points.reduce(
    (totals, point) => ({
      sold_products: totals.sold_products + point.sold_products,
      returned_products: totals.returned_products + point.returned_products,
      net_products: totals.net_products + point.net_products,
      returned_grams: totals.returned_grams + point.returned_grams,
      net_grams: totals.net_grams + point.net_grams,
    }),
    { ...emptyMetricTotals }
  );
}

function localDate(value: string) {
  return new Date(`${value}T12:00:00`);
}

function formatPeriodLabel(value: string, period: PeriodKey) {
  if (period === "year") {
    return value;
  }

  const date = localDate(value);

  if (period === "month") {
    return new Intl.DateTimeFormat("de-DE", { month: "short", year: "2-digit" }).format(date);
  }

  return new Intl.DateTimeFormat("de-DE", { day: "2-digit", month: "2-digit" }).format(date);
}

function formatNumber(value: number) {
  return new Intl.NumberFormat("de-DE", { maximumFractionDigits: value % 1 === 0 ? 0 : 2 }).format(value);
}

function formatCompact(value: number) {
  return new Intl.NumberFormat("de-DE", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function normalizeText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function makeTicks(min: number, max: number, count: number) {
  if (min === max) {
    return [min];
  }

  const step = (max - min) / Math.max(1, count - 1);
  return Array.from({ length: count }, (_, index) => min + step * index);
}
