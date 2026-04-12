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

export default function DataHealthPanel({ health, loading = false }) {
  if (!health) return null;

  const { missing_pct, outliers, rows_used, confidence } = health;
  const confColor = getConfidenceColor(confidence);

  return (
    <div className="health-panel" style={{ opacity: loading ? 0.6 : 1, transition: 'opacity 0.3s ease' }}>
      <div className="health-title" style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
        <Activity size={11} />
        Data Health
        {/* Pulsing live / loading indicator */}
        <span style={{
          marginLeft: 'auto',
          width: 7, height: 7,
          borderRadius: '50%',
          background: loading ? 'var(--warning)' : 'var(--success)',
          boxShadow: `0 0 6px ${loading ? 'var(--warning)' : 'var(--success)'}`,
          display: 'inline-block',
          animation: 'pulse-dot 1.4s ease-in-out infinite',
        }} />
      </div>

      {/* 2×2 grid that fits the narrow sidebar */}
      <div className="health-metrics" style={{ gridTemplateColumns: 'repeat(2, 1fr)' }}>
        <div className="health-metric">
          <div className="health-metric-value" style={{ color: getMissingColor(missing_pct) }}>
            {missing_pct.toFixed(1)}%
          </div>
          <div className="health-metric-label">Missing</div>
        </div>

        <div className="health-metric">
          <div className="health-metric-value" style={{ color: outliers > 0 ? 'var(--warning)' : 'var(--success)' }}>
            {outliers}
          </div>
          <div className="health-metric-label">Outliers</div>
        </div>

        <div className="health-metric">
          <div className="health-metric-value" style={{ color: 'var(--secondary)' }}>
            {rows_used >= 1000 ? `${(rows_used / 1000).toFixed(1)}k` : rows_used}
          </div>
          <div className="health-metric-label">Rows Used</div>
        </div>

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

      {/* Mode-aware caption */}
      {loading && (
        <div style={{ fontSize: 10, color: 'var(--text-faint)', textAlign: 'center', marginTop: 4 }}>
          Recalculating…
        </div>
      )}
    </div>
  );
}
