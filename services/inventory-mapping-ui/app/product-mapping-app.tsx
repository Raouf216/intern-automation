"use client";

import {
  AlertTriangle,
  Boxes,
  CheckCircle2,
  Database,
  Filter,
  Leaf,
  Loader2,
  PackageSearch,
  Search,
  X,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";

type ProductRow = {
  id: string;
  canonicalId: string;
  ourName: string;
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

type ProductStats = {
  totalProducts: number;
  wawicanProducts: number;
  doktorabcProducts: number;
  intersectionProducts: number;
  deals: number;
  needsReview: number;
  wawicanUniqueNames: number;
};

type ProductsResponse = {
  ok?: boolean;
  error?: string;
  query?: string;
  kind?: FilterKind;
  stats?: ProductStats;
  filteredCount?: number;
  products?: ProductRow[];
};

type FilterKind = "all" | "matched" | "missing-doktorabc" | "deal" | "needs-review";

const filterOptions: Array<{
  key: FilterKind;
  label: string;
  icon: typeof Boxes;
}> = [
  { key: "all", label: "Alle", icon: Database },
  { key: "matched", label: "Beide", icon: CheckCircle2 },
  { key: "missing-doktorabc", label: "Wawican offen", icon: PackageSearch },
  { key: "deal", label: "Deals", icon: Leaf },
  { key: "needs-review", label: "Prüfen", icon: AlertTriangle },
];

const emptyStats: ProductStats = {
  totalProducts: 0,
  wawicanProducts: 0,
  doktorabcProducts: 0,
  intersectionProducts: 0,
  deals: 0,
  needsReview: 0,
  wawicanUniqueNames: 0,
};

function displayValue(value: string, fallback = "—") {
  return value.trim() || fallback;
}

function statusLabel(status: string) {
  if (status === "verified") return "verified";
  if (status === "needs_review") return "needs review";
  if (status === "archived") return "archived";
  return status || "unknown";
}

function productSignal(product: ProductRow) {
  if (product.productKind === "deal") {
    return "deal";
  }

  if (product.wawicanName && product.doktorabcName && product.status === "verified") {
    return "matched";
  }

  if (product.doktorabcName) {
    return "review";
  }

  return "missing";
}

function signalText(product: ProductRow) {
  const signal = productSignal(product);

  if (signal === "deal") return "Deal";
  if (signal === "matched") return "Beide";
  if (signal === "review") return "Prüfen";
  return "Kein DoktorABC";
}

export function ProductMappingApp() {
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<FilterKind>("all");
  const [products, setProducts] = useState<ProductRow[]>([]);
  const [stats, setStats] = useState<ProductStats>(emptyStats);
  const [filteredCount, setFilteredCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    const timer = window.setTimeout(async () => {
      setLoading(true);
      setError("");

      try {
        const params = new URLSearchParams();
        if (query.trim()) params.set("q", query.trim());
        params.set("kind", filter);

        const response = await fetch(`/api/products?${params.toString()}`, {
          signal: controller.signal,
          cache: "no-store",
        });
        const payload = (await response.json()) as ProductsResponse;

        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || `Product lookup failed (${response.status}).`);
        }

        setProducts(payload.products || []);
        setStats(payload.stats || emptyStats);
        setFilteredCount(payload.filteredCount || 0);
      } catch (requestError) {
        if (requestError instanceof DOMException && requestError.name === "AbortError") return;
        setProducts([]);
        setFilteredCount(0);
        setError(requestError instanceof Error ? requestError.message : "Product lookup failed.");
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }, 220);

    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [query, filter]);

  const summaryCards = useMemo(
    () => [
      { label: "Wawican", value: stats.wawicanProducts, tone: "green" },
      { label: "DoktorABC", value: stats.doktorabcProducts, tone: "blue" },
      { label: "Beide", value: stats.intersectionProducts, tone: "green" },
      { label: "Deals", value: stats.deals, tone: "amber" },
      { label: "Prüfen", value: stats.needsReview, tone: "red" },
    ],
    [stats]
  );

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <Boxes size={28} />
          </div>
          <div>
            <p>Inventory</p>
            <h1>Product Mapping</h1>
          </div>
        </div>
        <nav className="subnav" aria-label="Inventory">
          <a className="active" href="/products">
            <PackageSearch size={18} />
            Produkte
          </a>
        </nav>
      </header>

      <section className="summary-grid" aria-label="Summary">
        {summaryCards.map((card) => (
          <div className={`summary-card ${card.tone}`} key={card.label}>
            <span>{card.label}</span>
            <strong>{card.value}</strong>
          </div>
        ))}
      </section>

      <section className="search-panel">
        <div className="search-box">
          <Search size={20} />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Wawican, DoktorABC, Kultivar oder Name"
          />
          {query ? (
            <button className="clear-button" type="button" onClick={() => setQuery("")} aria-label="Suche leeren">
              <X size={18} />
            </button>
          ) : null}
        </div>

        <div className="filter-row" aria-label="Filter">
          {filterOptions.map((option) => {
            const Icon = option.icon;

            return (
              <button
                className={filter === option.key ? "active" : ""}
                key={option.key}
                type="button"
                onClick={() => setFilter(option.key)}
              >
                <Icon size={17} />
                {option.label}
              </button>
            );
          })}
        </div>
      </section>

      <section className="table-panel">
        <div className="table-head">
          <div>
            <p>Produkte</p>
            <h2>{loading ? "Lade..." : `${filteredCount} rows`}</h2>
          </div>
          <div className="table-state">
            {loading ? <Loader2 className="spin" size={18} /> : <Filter size={18} />}
            <span>{filterOptions.find((option) => option.key === filter)?.label || "Alle"}</span>
          </div>
        </div>

        {error ? (
          <div className="error-box">
            <AlertTriangle size={20} />
            <span>{error}</span>
          </div>
        ) : null}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Wawican</th>
                <th>Kultivar</th>
                <th>DoktorABC</th>
                <th>Search key</th>
                <th>Art</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {!loading && products.length === 0 ? (
                <tr>
                  <td className="empty-row" colSpan={7}>
                    Keine Produkte gefunden.
                  </td>
                </tr>
              ) : null}

              {products.map((product, index) => (
                <tr key={product.id}>
                  <td className="row-number">{index + 1}</td>
                  <td>
                    <strong>{displayValue(product.wawicanName, product.productKind === "deal" ? "—" : "Wawican fehlt")}</strong>
                    {product.wawicanSearchKey && product.wawicanSearchKey !== product.wawicanName ? (
                      <small>{product.wawicanSearchKey}</small>
                    ) : null}
                  </td>
                  <td>{displayValue(product.kultivar)}</td>
                  <td>
                    <strong>{displayValue(product.doktorabcName, "Nicht in DoktorABC")}</strong>
                  </td>
                  <td>{displayValue(product.doktorabcSearchKey || product.wawicanSearchKey)}</td>
                  <td>
                    <span className={`pill ${productSignal(product)}`}>{signalText(product)}</span>
                  </td>
                  <td>
                    <span className={`status ${product.status}`}>{statusLabel(product.status)}</span>
                    {product.reviewReason ? <small>{product.reviewReason}</small> : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
