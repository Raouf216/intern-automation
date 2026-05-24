export function BrandMark() {
  return (
    <div className="brand-mark" aria-hidden="true">
      <svg className="brand-chart-svg" viewBox="0 0 64 64" role="img">
        <rect className="brand-screen" x="9" y="10" width="46" height="40" rx="9" />
        <line className="brand-grid-line" x1="15" x2="50" y1="22" y2="22" />
        <line className="brand-grid-line" x1="15" x2="50" y1="34" y2="34" />
        <line className="brand-grid-line" x1="15" x2="50" y1="46" y2="46" />
        <rect className="brand-bar brand-bar-one" x="16" y="34" width="5" height="10" rx="2" />
        <rect className="brand-bar brand-bar-two" x="25" y="28" width="5" height="16" rx="2" />
        <rect className="brand-bar brand-bar-three" x="34" y="31" width="5" height="13" rx="2" />
        <rect className="brand-bar brand-bar-four" x="43" y="22" width="5" height="22" rx="2" />
        <path className="brand-trend" d="M16 39l10-9 9 5 10-15" />
        <circle className="brand-trend-dot" cx="16" cy="39" r="2.7" />
        <circle className="brand-trend-dot" cx="26" cy="30" r="2.7" />
        <circle className="brand-trend-dot" cx="35" cy="35" r="2.7" />
        <circle className="brand-trend-dot" cx="45" cy="20" r="2.7" />
      </svg>
    </div>
  );
}
