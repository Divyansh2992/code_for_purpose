import { useMemo, useState } from 'react';
import { Activity, ChevronDown, ChevronUp } from 'lucide-react';

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

  const [showDetails, setShowDetails] = useState(false);
  const [showAllColumns, setShowAllColumns] = useState(false);

  const {
    missing_pct = 0,
    outliers = 0,
    rows_used = 0,
    confidence = 0,
    confidence_level = 'Unknown',
    confidence_reason = [],
    penalty_breakdown = {},
    column_health = [],
    summary_text = '',
  } = health;

  const confColor = getConfidenceColor(confidence);

  const penalties = useMemo(() => {
    return Object.entries(penalty_breakdown || {})
      .map(([key, value]) => ({ key, value: Number(value) || 0 }))
      .filter((entry) => entry.value > 0)
      .sort((a, b) => b.value - a.value);
  }, [penalty_breakdown]);

  const reasons = Array.isArray(confidence_reason) ? confidence_reason : [];

  const flaggedColumns = useMemo(() => {
    return (Array.isArray(column_health) ? column_health : [])
      .filter((col) => Array.isArray(col.flags) && col.flags.length > 0)
      .sort((a, b) => (a.score ?? 100) - (b.score ?? 100));
  }, [column_health]);

  const visibleColumns = showAllColumns ? flaggedColumns : flaggedColumns.slice(0, 5);

  const formatPenaltyKey = (key) => {
    if (!key) return 'Unknown';
    return String(key)
      .replace(/_/g, ' ')
      .replace(/\b\w/g, (c) => c.toUpperCase());
  };

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

      <button
        type="button"
        className="health-drill-toggle"
        onClick={() => setShowDetails((prev) => !prev)}
      >
        <span>Drill Down</span>
        <span className="health-drill-toggle-right">
          <span className="health-drill-pill" style={{ color: confColor }}>
            {confidence_level}
          </span>
          {showDetails ? <ChevronUp size={13} /> : <ChevronDown size={13} />}
        </span>
      </button>

      {showDetails && (
        <div className="health-drill">
          {summary_text && (
            <div className="health-drill-section">
              <div className="health-drill-label">Summary</div>
              <p className="health-drill-summary">{summary_text}</p>
            </div>
          )}

          <div className="health-drill-section">
            <div className="health-drill-label">Penalty Breakdown</div>
            {penalties.length > 0 ? (
              <div className="health-penalties">
                {penalties.map((penalty) => (
                  <div key={penalty.key} className="health-penalty-row">
                    <div className="health-penalty-head">
                      <span>{formatPenaltyKey(penalty.key)}</span>
                      <span>-{penalty.value.toFixed(1)}</span>
                    </div>
                    <div className="health-penalty-track">
                      <div
                        className="health-penalty-fill"
                        style={{ width: `${Math.min((penalty.value / 40) * 100, 100)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="health-drill-empty">No active penalties.</div>
            )}
          </div>

          <div className="health-drill-section">
            <div className="health-drill-label">Reasons</div>
            {reasons.length > 0 ? (
              <ul className="health-reasons">
                {reasons.slice(0, 6).map((reason, idx) => (
                  <li key={`${idx}-${reason.slice(0, 20)}`}>{reason}</li>
                ))}
              </ul>
            ) : (
              <div className="health-drill-empty">No major issues detected.</div>
            )}
          </div>

          <div className="health-drill-section">
            <div className="health-drill-label">Flagged Columns</div>
            {visibleColumns.length > 0 ? (
              <div className="health-columns">
                {visibleColumns.map((col) => (
                  <div key={col.name} className="health-column-card">
                    <div className="health-column-head">
                      <span className="health-column-name" title={col.name}>{col.name}</span>
                      <span className="health-column-score">{(col.score ?? 100).toFixed(1)}</span>
                    </div>
                    <div className="health-column-flags">
                      {col.flags.slice(0, 3).map((flag) => (
                        <span key={`${col.name}-${flag}`} className="health-flag">{flag}</span>
                      ))}
                    </div>
                  </div>
                ))}
                {flaggedColumns.length > 5 && (
                  <button
                    type="button"
                    className="health-more-btn"
                    onClick={() => setShowAllColumns((prev) => !prev)}
                  >
                    {showAllColumns
                      ? 'Show fewer columns'
                      : `Show all (${flaggedColumns.length}) columns`}
                  </button>
                )}
              </div>
            ) : (
              <div className="health-drill-empty">No flagged columns.</div>
            )}
          </div>
        </div>
      )}

      {/* Mode-aware caption */}
      {loading && (
        <div style={{ fontSize: 10, color: 'var(--text-faint)', textAlign: 'center', marginTop: 4 }}>
          Recalculating…
        </div>
      )}
    </div>
  );
}
