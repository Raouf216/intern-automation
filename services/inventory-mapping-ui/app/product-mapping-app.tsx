"use client";

import { AlertTriangle, Boxes, CheckCircle2, Database, Filter, Loader2, PackageSearch, ReceiptText, Search, X } from "lucide-react";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

type ProductRow = {
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

type ProductStats = {
  totalProducts: number;
  wawicanProducts: number;
  wawicanAvailableProducts: number;
  wawicanUnavailableProducts: number;
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
  platform?: CatalogPlatform;
  stats?: ProductStats;
  filteredCount?: number;
  products?: ProductRow[];
};

type CatalogPlatform = "doktorabc" | "wawican";
type WawicanAvailability = "available" | "unavailable";

const emptyStats: ProductStats = {
  totalProducts: 0,
  wawicanProducts: 0,
  wawicanAvailableProducts: 0,
  wawicanUnavailableProducts: 0,
  doktorabcProducts: 0,
  intersectionProducts: 0,
  deals: 0,
  needsReview: 0,
  wawicanUniqueNames: 0,
};

const platformOptions: Array<{
  key: CatalogPlatform;
  label: string;
  icon: typeof Boxes;
}> = [
  { key: "doktorabc", label: "DoktorABC", icon: Database },
  { key: "wawican", label: "Wawican", icon: PackageSearch },
];

const availabilityOptions: Array<{
  key: WawicanAvailability;
  label: string;
  icon: typeof CheckCircle2;
}> = [
  { key: "available", label: "Verfügbar", icon: CheckCircle2 },
  { key: "unavailable", label: "Nicht verfügbar", icon: AlertTriangle },
];

function displayValue(value: string, fallback = "-") {
  return value.trim() || fallback;
}

function availabilityLabel(status: string) {
  if (status === "available") return "verfügbar";
  if (status === "unavailable") return "nicht verfügbar";
  return status || "-";
}

function availabilityTone(status: string) {
  if (status === "available") return "verified";
  if (status === "unavailable") return "needs_review";
  return "";
}

function productName(product: ProductRow, platform: CatalogPlatform) {
  return platform === "wawican" ? product.wawicanName : product.doktorabcName;
}

export function ProductMappingApp() {
  const [query, setQuery] = useState("");
  const [platform, setPlatform] = useState<CatalogPlatform>("wawican");
  const [availability, setAvailability] = useState<Record<WawicanAvailability, boolean>>({
    available: true,
    unavailable: true,
  });
  const [products, setProducts] = useState<ProductRow[]>([]);
  const [stats, setStats] = useState<ProductStats>(emptyStats);
  const [filteredCount, setFilteredCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const availabilityParam = useMemo(
    () => availabilityOptions.filter((option) => availability[option.key]).map((option) => option.key).join(","),
    [availability]
  );

  useEffect(() => {
    const controller = new AbortController();
    const timer = window.setTimeout(async () => {
      setLoading(true);
      setError("");

      try {
        const params = new URLSearchParams();
        params.set("platform", platform);
        if (query.trim()) params.set("q", query.trim());
        if (platform === "wawican") params.set("availability", availabilityParam);

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
  }, [query, platform, availabilityParam]);

  function toggleAvailability(key: WawicanAvailability) {
    setAvailability((current) => {
      const next = {
        ...current,
        [key]: !current[key],
      };

      if (!next.available && !next.unavailable) {
        next[key] = true;
      }

      return next;
    });
  }

  const summaryCards = useMemo(
    () => [
      { label: "Wawican verfügbar", value: stats.wawicanAvailableProducts, tone: "green" },
      { label: "Wawican nicht verfügbar", value: stats.wawicanUnavailableProducts, tone: "red" },
      { label: "DoktorABC", value: stats.doktorabcProducts, tone: "blue" },
    ],
    [stats]
  );

  const activePlatformLabel = platformOptions.find((option) => option.key === platform)?.label || "Produkte";
  const searchPlaceholder = platform === "wawican" ? "Wawican Name oder Kultivar" : "DoktorABC Name";

  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <Boxes size={28} />
          </div>
          <div>
            <p>Inventory</p>
            <h1>Produktnamen</h1>
          </div>
        </div>
        <nav className="subnav" aria-label="Inventory">
          <Link href="/">
            <Boxes size={18} />
            Inventory
          </Link>
          <Link href="/abrechnungen">
            <ReceiptText size={18} />
            Abrechnungen
          </Link>
          <Link className="active" href="/products">
            <PackageSearch size={18} />
            Produktnamen
          </Link>
        </nav>
      </header>

      <section className="summary-grid summary-grid-three" aria-label="Summary">
        {summaryCards.map((card) => (
          <div className={`summary-card ${card.tone}`} key={card.label}>
            <span>{card.label}</span>
            <strong>{card.value}</strong>
          </div>
        ))}
      </section>

      <section className="search-panel catalog-search-panel">
        <div className="search-box">
          <Search size={20} />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder={searchPlaceholder}
          />
          {query ? (
            <button className="clear-button" type="button" onClick={() => setQuery("")} aria-label="Suche leeren">
              <X size={18} />
            </button>
          ) : null}
        </div>

        <div className="filter-row" aria-label="Plattform">
          {platformOptions.map((option) => {
            const Icon = option.icon;

            return (
              <button
                className={platform === option.key ? "active" : ""}
                key={option.key}
                type="button"
                onClick={() => setPlatform(option.key)}
              >
                <Icon size={17} />
                {option.label}
              </button>
            );
          })}
        </div>

        {platform === "wawican" ? (
          <div className="filter-row availability-filter-row" aria-label="Wawican Verfügbarkeit">
            {availabilityOptions.map((option) => {
              const Icon = option.icon;

              return (
                <button
                  className={availability[option.key] ? "active" : ""}
                  key={option.key}
                  type="button"
                  onClick={() => toggleAvailability(option.key)}
                >
                  <Icon size={17} />
                  {option.label}
                </button>
              );
            })}
          </div>
        ) : null}
      </section>

      <section className="table-panel">
        <div className="table-head">
          <div>
            <p>{activePlatformLabel}</p>
            <h2>{loading ? "Lade..." : `${filteredCount} Produkte`}</h2>
          </div>
          <div className="table-state">
            {loading ? <Loader2 className="spin" size={18} /> : <Filter size={18} />}
            <span>{activePlatformLabel}</span>
          </div>
        </div>

        {error ? (
          <div className="error-box">
            <AlertTriangle size={20} />
            <span>{error}</span>
          </div>
        ) : null}

        <div className="table-wrap">
          <table className={platform === "wawican" ? "catalog-table catalog-table-wawican" : "catalog-table"}>
            <thead>
              <tr>
                <th>#</th>
                <th>{platform === "wawican" ? "Wawican Name" : "DoktorABC Name"}</th>
                {platform === "wawican" ? <th>Kultivar</th> : null}
                {platform === "wawican" ? <th>Verfügbarkeit</th> : null}
              </tr>
            </thead>
            <tbody>
              {!loading && products.length === 0 ? (
                <tr>
                  <td className="empty-row" colSpan={platform === "wawican" ? 4 : 2}>
                    Keine Produkte gefunden.
                  </td>
                </tr>
              ) : null}

              {products.map((product, index) => (
                <tr key={product.id}>
                  <td className="row-number">{index + 1}</td>
                  <td>
                    <strong>{displayValue(productName(product, platform))}</strong>
                  </td>
                  {platform === "wawican" ? <td>{displayValue(product.kultivar)}</td> : null}
                  {platform === "wawican" ? (
                    <td>
                      <span className={`status ${availabilityTone(product.status)}`}>{availabilityLabel(product.status)}</span>
                    </td>
                  ) : null}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
