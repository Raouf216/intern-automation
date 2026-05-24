export function BrandMark() {
  return (
    <div className="brand-mark" aria-hidden="true">
      <svg className="brand-chart-svg" viewBox="0 0 48 48" role="img">
        <path className="brand-axis" d="M12 36h26" />
        <rect className="brand-bar brand-bar-one" x="14" y="27" width="5" height="9" rx="2" />
        <rect className="brand-bar brand-bar-two" x="23" y="21" width="5" height="15" rx="2" />
        <rect className="brand-bar brand-bar-three" x="32" y="15" width="5" height="21" rx="2" />
        <path className="brand-trend" d="M13 30l10-8 7 4 9-12" />
      </svg>
    </div>
  );
}
