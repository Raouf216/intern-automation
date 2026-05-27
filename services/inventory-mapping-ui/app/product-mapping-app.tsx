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
  Plus,
  Save,
  Search,
  X,
} from "lucide-react";
import type { FormEvent } from "react";
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

type VerifyResponse = {
  ok?: boolean;
  error?: string;
};

type CreateProductResponse = {
  ok?: boolean;
  error?: string;
};

type NewProductForm = {
  inWawican: boolean;
  inDoktorabc: boolean;
  wawicanName: string;
  doktorabcName: string;
  kultivar: string;
  ourName: string;
  verified: boolean;
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

const emptyNewProduct: NewProductForm = {
  inWawican: true,
  inDoktorabc: false,
  wawicanName: "",
  doktorabcName: "",
  kultivar: "",
  ourName: "",
  verified: false,
};

function displayValue(value: string, fallback = "—") {
  return value.trim() || fallback;
}

function statusLabel(status: string) {
  if (status === "verified") return "OK";
  if (status === "needs_review") return "Prüfen";
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
  const [refreshTick, setRefreshTick] = useState(0);
  const [verifyingProductId, setVerifyingProductId] = useState("");
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newProduct, setNewProduct] = useState<NewProductForm>(emptyNewProduct);
  const [creatingProduct, setCreatingProduct] = useState(false);
  const [createMessage, setCreateMessage] = useState("");

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
  }, [query, filter, refreshTick]);

  async function verifyProduct(product: ProductRow) {
    if (!product.canonicalId || verifyingProductId) return;

    const label = product.wawicanName || product.doktorabcName || product.canonicalId;
    const confirmed = window.confirm(`Dieses Produkt als OK markieren?\n\n${label}`);
    if (!confirmed) return;

    setVerifyingProductId(product.id);
    setError("");

    try {
      const response = await fetch("/api/products/verify", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          canonicalId: product.canonicalId,
        }),
      });
      const payload = (await response.json()) as VerifyResponse;

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `Verify failed (${response.status}).`);
      }

      setRefreshTick((value) => value + 1);
    } catch (verifyError) {
      setError(verifyError instanceof Error ? verifyError.message : "Verify failed.");
    } finally {
      setVerifyingProductId("");
    }
  }

  function updateNewProduct(patch: Partial<NewProductForm>) {
    setCreateMessage("");
    setNewProduct((value) => ({
      ...value,
      ...patch,
    }));
  }

  async function createProduct(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!newProduct.inWawican && !newProduct.inDoktorabc) {
      setCreateMessage("Mindestens eine Plattform wählen.");
      return;
    }

    if ((newProduct.inWawican && !newProduct.wawicanName.trim()) || (newProduct.inDoktorabc && !newProduct.doktorabcName.trim())) {
      setCreateMessage("Gewählte Plattformen brauchen exakte Namen.");
      return;
    }

    setCreatingProduct(true);
    setError("");
    setCreateMessage("");

    try {
      const response = await fetch("/api/products/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          ourName: newProduct.ourName,
          kultivar: newProduct.kultivar,
          verified: newProduct.verified,
          platforms: {
            wawican: newProduct.inWawican,
            wawicanName: newProduct.wawicanName,
            doktorabc: newProduct.inDoktorabc,
            doktorabcName: newProduct.doktorabcName,
          },
        }),
      });
      const payload = (await response.json()) as CreateProductResponse;

      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `Create product failed (${response.status}).`);
      }

      setNewProduct(emptyNewProduct);
      setShowCreateForm(false);
      setCreateMessage("Produkt gespeichert.");
      setRefreshTick((value) => value + 1);
    } catch (createError) {
      setCreateMessage(createError instanceof Error ? createError.message : "Produkt konnte nicht gespeichert werden.");
    } finally {
      setCreatingProduct(false);
    }
  }

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

  const canCreateProduct =
    (newProduct.inWawican || newProduct.inDoktorabc) &&
    (!newProduct.inWawican || Boolean(newProduct.wawicanName.trim())) &&
    (!newProduct.inDoktorabc || Boolean(newProduct.doktorabcName.trim()));

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
          <a href="/">
            <Boxes size={18} />
            Inventory
          </a>
          <a className="active" href="/products">
            <PackageSearch size={18} />
            Produktnamen
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

      <section className="create-strip">
        <button className="primary-action" type="button" onClick={() => setShowCreateForm((value) => !value)}>
          {showCreateForm ? <X size={18} /> : <Plus size={18} />}
          Produkt hinzufügen
        </button>
        {createMessage ? <span className={createMessage === "Produkt gespeichert." ? "create-note success" : "create-note"}>{createMessage}</span> : null}
      </section>

      {showCreateForm ? (
        <form className="create-card" onSubmit={createProduct}>
          <div className="platform-picker" aria-label="Plattformen">
            <button
              className={newProduct.inWawican ? "active" : ""}
              type="button"
              onClick={() => updateNewProduct({ inWawican: !newProduct.inWawican })}
            >
              Wawican
            </button>
            <button
              className={newProduct.inDoktorabc ? "active" : ""}
              type="button"
              onClick={() => updateNewProduct({ inDoktorabc: !newProduct.inDoktorabc })}
            >
              DoktorABC
            </button>
          </div>

          <div className="create-grid">
            {newProduct.inWawican ? (
              <label>
                <span>Wawican Name</span>
                <input
                  value={newProduct.wawicanName}
                  onChange={(event) => updateNewProduct({ wawicanName: event.target.value })}
                  placeholder="Exakter Name"
                />
              </label>
            ) : null}

            {newProduct.inDoktorabc ? (
              <label>
                <span>DoktorABC Name</span>
                <input
                  value={newProduct.doktorabcName}
                  onChange={(event) => updateNewProduct({ doktorabcName: event.target.value })}
                  placeholder="Exakter Name"
                />
              </label>
            ) : null}

            <label>
              <span>Kultivar</span>
              <input
                value={newProduct.kultivar}
                onChange={(event) => updateNewProduct({ kultivar: event.target.value })}
                placeholder="Optional"
              />
            </label>

            <label>
              <span>Our name</span>
              <input
                value={newProduct.ourName}
                onChange={(event) => updateNewProduct({ ourName: event.target.value })}
                placeholder="Optional"
              />
            </label>
          </div>

          <div className="create-actions">
            <label className="check-line">
              <input
                type="checkbox"
                checked={newProduct.verified}
                onChange={(event) => updateNewProduct({ verified: event.target.checked })}
              />
              <span>OK</span>
            </label>
            <button className="save-action" type="submit" disabled={!canCreateProduct || creatingProduct}>
              {creatingProduct ? <Loader2 className="spin" size={17} /> : <Save size={17} />}
              Speichern
            </button>
          </div>
        </form>
      ) : null}

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
                <th>Art</th>
                <th>Status</th>
                <th>Aktion</th>
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
                  <td>
                    <span className={`pill ${productSignal(product)}`}>{signalText(product)}</span>
                  </td>
                  <td>
                    <span className={`status ${product.status}`}>{statusLabel(product.status)}</span>
                    {product.reviewReason ? <small>{product.reviewReason}</small> : null}
                  </td>
                  <td>
                    {product.status === "verified" ? (
                      <span className="verified-action">
                        <CheckCircle2 size={16} />
                        OK
                      </span>
                    ) : (
                      <button
                        className="verify-button"
                        type="button"
                        onClick={() => verifyProduct(product)}
                        disabled={Boolean(verifyingProductId)}
                        title="Dieses Mapping als OK markieren"
                      >
                        {verifyingProductId === product.id ? <Loader2 className="spin" size={16} /> : <CheckCircle2 size={16} />}
                        OK setzen
                      </button>
                    )}
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
