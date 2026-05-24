export function BrandMark() {
  return (
    <div className="brand-mark" aria-hidden="true">
      <svg className="brand-chart-svg" viewBox="0 0 64 64" role="img">
        <line className="brand-grid-line" x1="13" x2="54" y1="18" y2="18" />
        <line className="brand-grid-line" x1="13" x2="54" y1="31" y2="31" />
        <line className="brand-grid-line" x1="13" x2="54" y1="44" y2="44" />
        <path className="brand-axis" d="M13 12v38h43" />

        <line className="brand-candle-stem" x1="22" x2="22" y1="22" y2="43" />
        <rect className="brand-candle brand-candle-up" x="18" y="28" width="8" height="11" rx="2" />

        <line className="brand-candle-stem" x1="33" x2="33" y1="16" y2="38" />
        <rect className="brand-candle brand-candle-down" x="29" y="20" width="8" height="13" rx="2" />

        <line className="brand-candle-stem" x1="44" x2="44" y1="15" y2="34" />
        <rect className="brand-candle brand-candle-up" x="40" y="18" width="8" height="11" rx="2" />

        <path className="brand-trend" d="M16 45l10-12 9 5 8-15 9-5" />
        <circle className="brand-trend-dot" cx="16" cy="45" r="2" />
        <circle className="brand-trend-dot" cx="43" cy="23" r="2" />
        <circle className="brand-trend-dot" cx="52" cy="18" r="2" />
      </svg>
    </div>
  );
}
