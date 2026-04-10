import { Activity } from 'lucide-react';

function getConfidenceColor(score) {
  if (score >= 80) return 'var(--success)';
  if (score >= 60) return 'var(--warning)';
  return 'var(--danger)';
}

function getMissingColor(pct) {
  if (pct < 5)  return 'var(--success)';
  if (pct < 20) return 'var(--warning)';
  return 'var(--danger)';
}

export default function DataHealthPanel({ health }) {
  if (!health) return null;

  const { missing_pct, outliers, rows_used, confidence } = health;
  const confColor = getConfidenceColor(confidence);

  return (
    <div className="health-panel">
      <div className="health-title" style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
        <Activity size={11} />
        Data Health
      </div>

      <div className="health-metrics">
        {/* Missing */}
        <div className="health-metric">
          <div className="health-metric-value" style={{ color: getMissingColor(missing_pct) }}>
            {missing_pct.toFixed(1)}%
          </div>
          <div className="health-metric-label">Missing</div>
        </div>

        {/* Outliers */}
        <div className="health-metric">
          <div className="health-metric-value" style={{ color: outliers > 0 ? 'var(--warning)' : 'var(--success)' }}>
            {outliers}
          </div>
          <div className="health-metric-label">Outliers</div>
        </div>

        {/* Rows used */}
        <div className="health-metric">
          <div className="health-metric-value" style={{ color: 'var(--secondary)' }}>
            {rows_used >= 1000 ? `${(rows_used / 1000).toFixed(1)}k` : rows_used}
          </div>
          <div className="health-metric-label">Rows Used</div>
        </div>

        {/* Confidence */}
        <div className="health-metric">
          <div className="health-metric-value" style={{ color: confColor }}>
            {confidence}%
          </div>
          <div className="health-metric-label">Confidence</div>
        </div>
      </div>

      {/* Confidence bar */}
      <div className="health-conf-bar">
        <div
          className="health-conf-fill"
          style={{ width: `${confidence}%`, background: confColor }}
        />
      </div>
    </div>
  );
}
