import { ArrowRight, Boxes, ClipboardList, PackageSearch, ScanLine } from "lucide-react";
import Link from "next/link";

const inventoryOptions = [
  {
    title: "Produktnamen",
    eyebrow: "Mapping",
    href: "/products",
    icon: PackageSearch,
    active: true,
    meta: "Wawican · DoktorABC · Kultivar",
  },
  {
    title: "Wareneingang",
    eyebrow: "Scan",
    icon: ScanLine,
    active: false,
    meta: "Fotos · Mengen · Ablaufdaten",
  },
  {
    title: "Bestandsaktionen",
    eyebrow: "Automation",
    icon: ClipboardList,
    active: false,
    meta: "Bot-Schritte · Plattformen",
  },
];

export default function Home() {
  return (
    <main className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <Boxes size={28} />
          </div>
          <div>
            <p>Inventory</p>
            <h1>Inventory</h1>
          </div>
        </div>
      </header>

      <section className="option-grid" aria-label="Inventory Bereiche">
        {inventoryOptions.map((option) => {
          const Icon = option.icon;
          const content = (
            <>
              <div className="option-card-head">
                <span className="option-icon">
                  <Icon size={24} />
                </span>
                <span className={option.active ? "option-state active" : "option-state"}>{option.eyebrow}</span>
              </div>
              <div>
                <h2>{option.title}</h2>
                <p>{option.meta}</p>
              </div>
              <span className="option-action" aria-hidden="true">
                <ArrowRight size={19} />
              </span>
            </>
          );

          if (!option.active || !option.href) {
            return (
              <div className="option-card disabled" key={option.title} aria-disabled="true">
                {content}
              </div>
            );
          }

          return (
            <Link className="option-card" href={option.href} key={option.title}>
              {content}
            </Link>
          );
        })}
      </section>
    </main>
  );
}
