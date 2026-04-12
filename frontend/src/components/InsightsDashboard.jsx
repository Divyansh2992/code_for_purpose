import { useMemo, useEffect, useState, useCallback } from 'react';
import { BarChart2, PieChart as PieIcon, TrendingUp, Sparkles, Zap, RefreshCw } from 'lucide-react';
import ChartRenderer from './ChartRenderer';
import { fetchAutoVisualize } from '../api/client';

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
  if (Math.abs(num) >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  if (Math.abs(num) >= 1000) return `${(num / 1000).toFixed(1)}k`;
  return num.toFixed(2).replace(/\.?0+$/, '');
}

function isChartable(result, chartX, chartY) {
  return !!(chartX && chartY?.length > 0 && result?.length > 1);
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

/* ───────────────────── source badge ────────────────── */
function SourceBadge({ isAuto }) {
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4,
      fontSize: 10, padding: '2px 8px', borderRadius: 99,
      background: isAuto ? 'rgba(6,182,212,0.12)' : 'rgba(124,58,237,0.12)',
      color: isAuto ? 'var(--secondary)' : 'var(--primary-light)',
      border: `1px solid ${isAuto ? 'rgba(6,182,212,0.25)' : 'rgba(124,58,237,0.25)'}`,
      fontWeight: 600, letterSpacing: '0.04em',
    }}>
      {isAuto ? '⚙ DATASET OVERVIEW' : '💬 FROM QUERY'}
    </span>
  );
}

/* ───────────────────── chart card ─────────────────── */
function ChartCard({ icon: Icon, title, accentColor = 'var(--primary-dim)', badge, children }) {
  return (
    <div className="insight-card">
      <div className="insight-card-header">
        <div className="insight-card-icon" style={{ background: accentColor }}>
          <Icon size={16} />
        </div>
        <h3 className="insight-card-title">{title}</h3>
        {badge && <div style={{ marginLeft: 'auto' }}>{badge}</div>}
      </div>
      {children}
    </div>
  );
}

/* ───────────────────── loading skeleton ────────────── */
function ChartSkeleton() {
  return (
    <div style={{
      flex: 1, minHeight: 200, borderRadius: 10,
      background: 'rgba(255,255,255,0.03)',
      animation: 'skeleton-pulse 1.5s ease-in-out infinite',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
    }}>
      <RefreshCw size={24} style={{ color: 'var(--text-faint)', opacity: 0.5, animation: 'spin 1.5s linear infinite' }} />
    </div>
  );
}

/* ───────────────────── main component ─────────────── */
export default function InsightsDashboard({ datasetId, mode, result, columns, chartX, chartY, question }) {
  const [autoData, setAutoData]       = useState(null);
  const [autoLoading, setAutoLoading] = useState(false);

  // Is the last query result chart-friendly?
  const queryChartable = isChartable(result, chartX, chartY);

  const loadAutoData = useCallback(() => {
    if (!datasetId) return;
    setAutoLoading(true);
    fetchAutoVisualize({ datasetId, mode })
      .then(setAutoData)
      .catch(() => {})
      .finally(() => setAutoLoading(false));
  }, [datasetId, mode]);

  // Only fetch auto-visualize when the query result can't produce charts itself,
  // OR when the dataset / mode changes and we don't have auto data yet.
  useEffect(() => {
    if (!datasetId) return;
    // Always refresh auto data when dataset/mode changes (for the overview cards)
    loadAutoData();
  }, [datasetId, mode]); // eslint-disable-line react-hooks/exhaustive-deps

  /* ── Decide chart sources ────────────────────────────────────────────────
     When the query IS chartable:
       - Trend     → query result as area chart  (shows what was asked)
       - Pie       → query result top 8 as pie   (composition of the answer)
       - Comparison→ query result as bar chart   (comparison view of the answer)
     When the query is NOT chartable:
       - All 3   → auto-generated dataset overview
  ──────────────────────────────────────────────────────────────────────── */

  const trendSrc = queryChartable
    ? { result, chartX, chartY, chartType: 'area',  title: `Trend — ${question || chartX}`, isAuto: false }
    : autoData?.trend
      ? { result: autoData.trend.result, chartX: autoData.trend.chart_x, chartY: autoData.trend.chart_y, chartType: autoData.trend.chart_type, title: autoData.trend.title, isAuto: true }
      : null;

  const compSrc = queryChartable
    ? { result, chartX, chartY, chartType: 'pie',   title: `Composition — ${chartX}`, isAuto: false }
    : autoData?.composition
      ? { result: autoData.composition.result, chartX: autoData.composition.chart_x, chartY: autoData.composition.chart_y, chartType: 'pie', title: autoData.composition.title, isAuto: true }
      : null;

  const barSrc = queryChartable
    ? { result, chartX, chartY, chartType: 'bar',   title: `Comparison — ${question || chartX}`, isAuto: false }
    : autoData?.comparison
      ? { result: autoData.comparison.result, chartX: autoData.comparison.chart_x, chartY: autoData.comparison.chart_y, chartType: 'bar', title: autoData.comparison.title, isAuto: true }
      : null;

  /* ── Stats (from query if chartable, else auto trend) ────────────────── */
  const statsSource = queryChartable ? result : (autoData?.trend?.result ?? []);
  const statsKey    = queryChartable ? chartY?.[0] : autoData?.trend?.chart_y?.[0];

  const stats = useMemo(() => {
    if (!statsSource?.length || !statsKey) return null;
    const top     = maxRow(statsSource, statsKey);
    const average = avg(statsSource, statsKey);
    const values  = statsSource.map(r => parseFloat(r[statsKey]) || 0);
    const max     = Math.max(...values);
    const min     = Math.min(...values);
    return { top, average, max, min, yKey: statsKey };
  }, [statsSource, statsKey]);

  const summaryStats = autoData?.summary_stats ?? {};

  if (!datasetId) {
    return (
      <div className="dashboard-container">
        <div className="chat-empty">
          <div className="chat-empty-icon">📊</div>
          <h2>Upload a CSV first</h2>
          <p>Load a dataset, then switch to Visualize to see charts.</p>
        </div>
      </div>
    );
  }

  const isAutoMode = !queryChartable;

  return (
    <div className="dashboard-container">
      {/* Header */}
      <div className="dashboard-header">
        <div>
          <h2 className="dashboard-title">✨ Visual Insights</h2>
          {queryChartable && question && (
            <p className="chat-header-sub" style={{ maxWidth: 520, marginTop: 2 }}>
              ← <em>"{question}"</em>
            </p>
          )}
          {isAutoMode && (
            <p className="chat-header-sub" style={{ marginTop: 4, color: 'var(--secondary)' }}>
              ⚙ Showing dataset overview — ask a question with multiple rows to see query-specific charts
            </p>
          )}
        </div>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <div className="stat-chip">
            Rows: <span>{queryChartable ? result.length.toLocaleString() : (summaryStats.total_rows ?? '—').toLocaleString?.()}</span>
          </div>
          <div className="stat-chip">
            Fields: <span>{queryChartable ? (columns?.length ?? '—') : (summaryStats.total_cols ?? '—')}</span>
          </div>
          <button
            onClick={loadAutoData}
            disabled={autoLoading}
            style={{
              background: 'rgba(255,255,255,0.06)', border: '1px solid var(--border)',
              borderRadius: 8, padding: '5px 10px', color: 'var(--text-muted)',
              cursor: autoLoading ? 'not-allowed' : 'pointer', fontSize: 11,
              display: 'flex', alignItems: 'center', gap: 5, transition: 'var(--transition)',
            }}
            title="Refresh auto-generated dataset overview"
          >
            <RefreshCw size={12} style={{ animation: autoLoading ? 'spin 1s linear infinite' : 'none' }} />
            Refresh
          </button>
        </div>
      </div>

      <div className="dashboard-grid">
        {/* Card 1: Trend / Area */}
        <ChartCard
          icon={TrendingUp}
          title={trendSrc?.title || 'Trend Analysis'}
          accentColor="var(--primary-dim)"
          badge={trendSrc && <SourceBadge isAuto={trendSrc.isAuto} />}
        >
          {autoLoading && !trendSrc ? <ChartSkeleton /> :
           trendSrc ? (
            <ChartRenderer
              chartType={trendSrc.chartType}
              chartX={trendSrc.chartX}
              chartY={trendSrc.chartY}
              result={trendSrc.result}
              height={240}
              isDashboard
            />
          ) : <NoChartFallback loading={autoLoading} />}
        </ChartCard>

        {/* Card 2: Composition / Pie */}
        <ChartCard
          icon={PieIcon}
          title={compSrc?.title || 'Composition Breakdown'}
          accentColor="rgba(6,182,212,0.15)"
          badge={compSrc && <SourceBadge isAuto={compSrc.isAuto} />}
        >
          {autoLoading && !compSrc ? <ChartSkeleton /> :
           compSrc ? (
            <ChartRenderer
              chartType="pie"
              chartX={compSrc.chartX}
              chartY={compSrc.chartY}
              result={compSrc.result}
              height={240}
              isDashboard
            />
          ) : <NoChartFallback loading={autoLoading} />}
        </ChartCard>

        {/* Card 3: Comparison / Bar */}
        <ChartCard
          icon={BarChart2}
          title={barSrc?.title || 'Comparative Analysis'}
          accentColor="rgba(16,185,129,0.15)"
          badge={barSrc && <SourceBadge isAuto={barSrc.isAuto} />}
        >
          {autoLoading && !barSrc ? <ChartSkeleton /> :
           barSrc ? (
            <ChartRenderer
              chartType="bar"
              chartX={barSrc.chartX}
              chartY={barSrc.chartY}
              result={barSrc.result}
              height={240}
              isDashboard
            />
          ) : <NoChartFallback loading={autoLoading} />}
        </ChartCard>

        {/* Card 4: Intelligent Summary */}
        <ChartCard icon={Sparkles} title="Intelligent Summary" accentColor="rgba(245,158,11,0.15)">
          <div style={{ display: 'flex', flexDirection: 'column', gap: 14, flex: 1 }}>
            {stats ? (
              <>
                <div style={{ display: 'flex', gap: 10 }}>
                  <StatCard
                    label="Top Entry"
                    value={String(stats.top?.[queryChartable ? chartX : autoData?.trend?.chart_x] ?? 'N/A').slice(0, 12)}
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
                <div style={{ display: 'flex', gap: 10 }}>
                  <StatCard label="Maximum" value={fmt(stats.max)} color="var(--warning)" sub={stats.yKey} />
                  <StatCard label="Minimum" value={fmt(stats.min)} color="var(--text-muted)" sub={stats.yKey} />
                </div>
              </>
            ) : (
              <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
                <StatCard label="Total Rows"   value={String(summaryStats.total_rows ?? result?.length ?? '—')} color="var(--secondary)" />
                <StatCard label="Fields"       value={String(summaryStats.total_cols ?? columns?.length ?? '—')} color="var(--primary-light)" />
                <StatCard label="Numeric Cols" value={String(summaryStats.numeric_cols ?? '—')} color="var(--success)" />
              </div>
            )}

            {/* Anomaly */}
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

function NoChartFallback({ loading }) {
  return (
    <div style={{
      flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center',
      justifyContent: 'center', gap: 8, color: 'var(--text-faint)', minHeight: 200,
    }}>
      {loading
        ? <ChartSkeleton />
        : <p style={{ fontSize: 12, textAlign: 'center', opacity: 0.5 }}>No data available for this chart.</p>
      }
    </div>
  );
}
