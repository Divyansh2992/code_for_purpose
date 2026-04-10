import { useMemo } from 'react';
import { BarChart2, PieChart as PieIcon, TrendingUp, Sparkles, Zap, AlertTriangle } from 'lucide-react';
import ChartRenderer from './ChartRenderer';

/* ───────────────────── helpers ───────────────────── */
function avg(arr, key) {
  if (!arr.length) return 0;
  const vals = arr.map(r => parseFloat(r[key]) || 0).filter(v => !isNaN(v));
  return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
}
function maxRow(arr, key) {
  if (!arr.length) return null;
  return arr.reduce((best, r) => (parseFloat(r[key]) > parseFloat(best[key]) ? r : best), arr[0]);
}
function fmt(n) {
  if (n === null || n === undefined) return 'N/A';
  const num = parseFloat(n);
  if (isNaN(num)) return String(n);
  if (Math.abs(num) >= 1000) return `${(num / 1000).toFixed(1)}k`;
  return num.toFixed(2).replace(/\.?0+$/, '');
}

/* ───────────────────── stat mini-card ─────────────── */
function StatCard({ label, value, color = 'var(--secondary)', sub }) {
  return (
    <div style={{
      flex: 1, background: 'rgba(255,255,255,0.04)', padding: '12px 14px',
      borderRadius: 12, border: '1px solid rgba(255,255,255,0.08)',
    }}>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color, lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: 'var(--text-faint)', marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

/* ───────────────────── chart card ─────────────────── */
function ChartCard({ icon: Icon, title, accentColor = 'var(--primary-dim)', children }) {
  return (
    <div className="insight-card">
      <div className="insight-card-header">
        <div className="insight-card-icon" style={{ background: accentColor }}>
          <Icon size={16} />
        </div>
        <h3 className="insight-card-title">{title}</h3>
      </div>
      {children}
    </div>
  );
}

/* ───────────────────── main component ─────────────── */
export default function InsightsDashboard({ result, columns, chartX, chartY, question }) {
  const hasChart = chartX && chartY?.length && result?.length > 1;

  const stats = useMemo(() => {
    if (!result?.length || !chartY?.length) return null;
    const yKey = chartY[0];
    const top = maxRow(result, yKey);
    const average = avg(result, yKey);
    const values = result.map(r => parseFloat(r[yKey]) || 0);
    const max = Math.max(...values);
    const min = Math.min(...values);
    return { top, average, max, min, yKey };
  }, [result, chartY]);

  if (!result || result.length === 0) {
    return (
      <div className="dashboard-container">
        <div className="chat-empty">
          <div className="chat-empty-icon">📊</div>
          <h2>No data to visualize</h2>
          <p>Ask a question first, then click Visualize to see the dashboard.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="dashboard-container">
      {/* Header */}
      <div className="dashboard-header">
        <div>
          <h2 className="dashboard-title">✨ Visual Insights</h2>
          {question && (
            <p className="chat-header-sub" style={{ maxWidth: 520, marginTop: 2 }}>
              ← <em>"{question}"</em>
            </p>
          )}
        </div>
        <div style={{ display: 'flex', gap: 10 }}>
          <div className="stat-chip">Rows: <span>{result.length}</span></div>
          <div className="stat-chip">Fields: <span>{columns?.length || '—'}</span></div>
        </div>
      </div>

      <div className="dashboard-grid">
        {/* Card 1: Area / Trend */}
        <ChartCard icon={TrendingUp} title="Trend Analysis" accentColor="var(--primary-dim)">
          {hasChart ? (
            <ChartRenderer chartType="area" chartX={chartX} chartY={chartY}
              result={result} height={240} isDashboard />
          ) : (
            <NoChartFallback />
          )}
        </ChartCard>

        {/* Card 2: Pie / Composition */}
        <ChartCard icon={PieIcon} title="Composition Breakdown" accentColor="rgba(6,182,212,0.15)">
          {hasChart ? (
            <ChartRenderer chartType="pie" chartX={chartX} chartY={chartY}
              result={result} height={240} isDashboard />
          ) : (
            <NoChartFallback />
          )}
        </ChartCard>

        {/* Card 3: Bar / Comparison */}
        <ChartCard icon={BarChart2} title="Comparative Analysis" accentColor="rgba(16,185,129,0.15)">
          {hasChart ? (
            <ChartRenderer chartType="bar" chartX={chartX} chartY={chartY}
              result={result} height={240} isDashboard />
          ) : (
            <NoChartFallback />
          )}
        </ChartCard>

        {/* Card 4: Intelligent Summary */}
        <ChartCard icon={Sparkles} title="Intelligent Summary" accentColor="rgba(245,158,11,0.15)">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14, flex: 1 }}>
            {/* Stat row */}
            {stats ? (
              <div style={{ display: 'flex', gap: 10 }}>
                <StatCard
                  label="Top Category"
                  value={stats.top?.[chartX] || 'N/A'}
                  color="var(--secondary)"
                  sub={`${stats.yKey}: ${fmt(stats.top?.[stats.yKey])}`}
                />
                <StatCard
                  label="Average"
                  value={fmt(stats.average)}
                  color="var(--success)"
                  sub={stats.yKey}
                />
              </div>
            ) : (
              <div style={{ display: 'flex', gap: 10 }}>
                <StatCard label="Total Rows" value={result.length} color="var(--secondary)" />
                <StatCard label="Fields" value={columns?.length || '—'} color="var(--primary-light)" />
              </div>
            )}

            {/* Min / Max */}
            {stats && (
              <div style={{ display: 'flex', gap: 10 }}>
                <StatCard label="Maximum" value={fmt(stats.max)} color="var(--warning)" sub={stats.yKey} />
                <StatCard label="Minimum" value={fmt(stats.min)} color="var(--text-muted)" sub={stats.yKey} />
              </div>
            )}

            {/* Anomaly marker */}
            <div style={{
              display: 'flex', alignItems: 'center', gap: 10, padding: '10px 12px',
              borderRadius: 10, background: 'rgba(0,0,0,0.25)', border: '1px solid rgba(255,255,255,0.06)',
            }}>
              <Zap size={18} color="var(--warning)" />
              <div>
                <div style={{ fontSize: 12, fontWeight: 600, color: 'var(--text)' }}>Anomaly Check</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
                  {stats && (stats.max / (stats.average || 1) > 3)
                    ? '⚠ Possible outlier detected (max > 3× avg)'
                    : '✓ No significant outliers in this subset.'}
                </div>
              </div>
            </div>

            {/* Footer */}
            <div style={{
              marginTop: 'auto', display: 'flex', justifyContent: 'space-between',
              fontSize: 10, color: 'var(--text-faint)', borderTop: '1px solid var(--border)', paddingTop: 10,
            }}>
              <span>⚡ PROCESSED VIA DUCKDB</span>
              <span>🤖 GROQ AI INSIGHTS</span>
            </div>
          </div>
        </ChartCard>
      </div>
    </div>
  );
}

function NoChartFallback() {
  return (
    <div style={{
      flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center',
      justifyContent: 'center', gap: 8, color: 'var(--text-faint)', minHeight: 200,
    }}>
      <AlertTriangle size={28} opacity={0.4} />
      <p style={{ fontSize: 12, textAlign: 'center' }}>
        Query returned a single row or<br />no categorical/numeric column pair found.
      </p>
    </div>
  );
}
