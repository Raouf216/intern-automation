import { ArrowDownWideNarrow, Boxes, Inbox, ReceiptText, Search } from "lucide-react";
import Link from "next/link";

export default function AbrechnungenPage() {
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
          <Link className="active" href="/abrechnungen">
            <ReceiptText size={18} />
            Abrechnungen
          </Link>
        </nav>
      </header>

      <section className="abrechnung-toolbar">
        <div className="search-box">
          <Search size={20} />
          <input placeholder="Abrechnung, Großhändler oder Produkt" />
        </div>
        <div className="table-state">
          <ArrowDownWideNarrow size={18} />
          <span>Neueste zuerst</span>
        </div>
      </section>

      <section className="abrechnung-feed" aria-label="Abrechnungen">
        <div className="abrechnung-empty-card">
          <Inbox size={22} />
          <div>
            <p>Abrechnungen</p>
            <h2>Keine Abrechnungen gefunden.</h2>
          </div>
        </div>
      </section>
    </main>
  );
}
