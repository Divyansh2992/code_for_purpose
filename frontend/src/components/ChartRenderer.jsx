import { useMemo, useState } from 'react';
import {
  BarChart, Bar,
  LineChart, Line,
  AreaChart, Area,
  ScatterChart, Scatter,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from 'recharts';

const COLORS = ['#7c3aed', '#06b6d4', '#10b981', '#f59e0b', '#ef4444'];

// ── Correlation Heatmap ────────────────────────────────────────────────────────
function correlationColor(value) {
  // -1 → red, 0 → dark, +1 → purple-cyan
  if (value === null || value === undefined || isNaN(value)) return 'rgba(255,255,255,0.05)';
  const v = Math.max(-1, Math.min(1, value));
  if (v >= 0) {
    // 0 → dark slate, 1 → vivid purple
    const r = Math.round(17 + v * (124 - 17));
    const g = Math.round(24 + v * (58 - 24));
    const b = Math.round(39 + v * (237 - 39));
    return `rgb(${r},${g},${b})`;
  } else {
    // 0 → dark slate, -1 → vivid red
    const abs = Math.abs(v);
    const r = Math.round(17 + abs * (239 - 17));
    const g = Math.round(24 + abs * (68 - 24));
    const b = Math.round(39 + abs * (68 - 39));
    return `rgb(${r},${g},${b})`;
  }
}

function CorrelationHeatmap({ result }) {
  const [tooltip, setTooltip] = useState(null);

  if (!result?.length) return null;

  // Build unique sorted label lists
  const cols = [...new Set(result.map(r => r.col_a))].sort();
  const n = cols.length;

  // Build lookup map { "col_a::col_b" -> correlation }
  const lookup = {};
  result.forEach(r => { lookup[`${r.col_a}::${r.col_b}`] = r.correlation; });

  const cellSize = Math.max(44, Math.min(80, Math.floor(480 / n)));
  const labelW = 110;
  const totalW = labelW + n * cellSize;
  const totalH = 24 + n * cellSize;

  return (
    <div style={{ overflowX: 'auto', overflowY: 'auto', position: 'relative' }}>
      <p style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8, letterSpacing: '0.05em' }}>
        🔥 CORRELATION MATRIX — {n} numeric columns
      </p>
      <div style={{ position: 'relative', display: 'inline-block', minWidth: totalW }}>
        {/* Column header labels */}
        <div style={{ display: 'flex', marginLeft: labelW, marginBottom: 2 }}>
          {cols.map(col => (
            <div key={col} style={{
              width: cellSize, fontSize: 9, color: 'var(--text-muted)', fontWeight: 600,
              textAlign: 'center', overflow: 'hidden', textOverflow: 'ellipsis',
              whiteSpace: 'nowrap', padding: '0 2px',
              transform: 'rotate(-30deg)', transformOrigin: 'bottom left',
              height: 40, display: 'flex', alignItems: 'flex-end', justifyContent: 'center',
            }}>
              {col.length > 10 ? col.slice(0, 9) + '…' : col}
            </div>
          ))}
        </div>
        {/* Rows */}
        {cols.map(rowCol => (
          <div key={rowCol} style={{ display: 'flex', alignItems: 'center' }}>
            {/* Row label */}
            <div style={{
              width: labelW, fontSize: 10, color: 'var(--text-muted)', fontWeight: 600,
              textAlign: 'right', paddingRight: 8, flex: '0 0 auto',
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
            }}>
              {rowCol.length > 14 ? rowCol.slice(0, 13) + '…' : rowCol}
            </div>
            {/* Cells */}
            {cols.map(colCol => {
              const val = lookup[`${rowCol}::${colCol}`];
              const displayVal = (val === null || val === undefined || isNaN(val))
                ? 'N/A' : Number(val).toFixed(2);
              const bg = correlationColor(val);
              const isDiag = rowCol === colCol;
              return (
                <div
                  key={colCol}
                  onMouseEnter={e => setTooltip({ x: e.clientX, y: e.clientY, rowCol, colCol, val })}
                  onMouseLeave={() => setTooltip(null)}
                  style={{
                    width: cellSize, height: cellSize, background: bg,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',

                    fontSize: n <= 6 ? 11 : 9, color: isDiag ? '#fff' : (Math.abs(val ?? 0) > 0.5 ? '#fff' : 'var(--text-muted)'),
                    fontWeight: isDiag ? 700 : 500, cursor: 'default',
                    border: isDiag ? '1px solid rgba(255,255,255,0.2)' : '1px solid rgba(255,255,255,0.03)',
                    transition: 'filter 0.15s', borderRadius: 2,
                    userSelect: 'none',
                  }}
                >
                  {n <= 8 ? displayVal : (isDiag ? '1.0' : '')}
                </div>
              );
            })}
          </div>
        ))}
        {/* Legend */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 12, marginLeft: labelW }}>
          <span style={{ fontSize: 10, color: '#ef4444' }}>-1.0</span>
          <div style={{
            flex: 1, height: 8, borderRadius: 4,
            background: 'linear-gradient(to right, rgb(239,68,68), rgb(17,24,39), rgb(124,58,237))',
            maxWidth: Math.min(n * cellSize, 240),
          }} />
          <span style={{ fontSize: 10, color: '#7c3aed' }}>+1.0</span>
        </div>
      </div>
      {/* Floating tooltip */}
      {tooltip && (
        <div style={{
          position: 'fixed', left: tooltip.x + 12, top: tooltip.y + 12,
          background: 'rgba(13,18,36,0.97)', border: '1px solid rgba(255,255,255,0.12)',
          borderRadius: 8, padding: '8px 12px', fontSize: 12, zIndex: 9999,
          boxShadow: '0 8px 30px rgba(0,0,0,0.6)', pointerEvents: 'none',
        }}>
          <div style={{ color: 'var(--text-muted)', marginBottom: 4 }}>
            {tooltip.rowCol} <span style={{ color: '#7c3aed' }}>↔</span> {tooltip.colCol}
          </div>
          <div style={{ color: '#fff', fontWeight: 700, fontSize: 16 }}>
            {(tooltip.val === null || tooltip.val === undefined || isNaN(tooltip.val))
              ? 'N/A' : Number(tooltip.val).toFixed(4)}
          </div>
          <div style={{ color: 'var(--text-muted)', fontSize: 10, marginTop: 2 }}>
            {Math.abs(tooltip.val ?? 0) >= 0.7 ? '🔴 Strong correlation' :
             Math.abs(tooltip.val ?? 0) >= 0.4 ? '🟡 Moderate correlation' :
             '🟢 Weak/no correlation'}
          </div>
        </div>
      )}
    </div>
  );
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: 'rgba(13,18,36,0.95)',
      border: '1px solid rgba(255,255,255,0.1)',
      borderRadius: 8,
      padding: '8px 12px',
      fontSize: 12,
      boxShadow: '0 4px 20px rgba(0,0,0,0.5)',
      backdropFilter: 'blur(10px)'
    }}>
      <p style={{ color: 'var(--text-muted)', marginBottom: 4, fontWeight: 500 }}>{label}</p>
      {payload.map((p, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 2 }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: p.color || p.fill }} />
          <span style={{ color: 'var(--text)', fontSize: 11 }}>{p.name}:</span>

          <span style={{ color: p.color || p.fill, fontWeight: 700 }}>
            {typeof p.value === 'number' ? p.value.toLocaleString() : p.value}
          </span>
        </div>
      ))}
    </div>
  );
};

function toNumber(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value === null || value === undefined) return null;
  const parsed = parseFloat(String(value).replace(/,/g, '').trim());
  return Number.isFinite(parsed) ? parsed : null;
}

function aggregateData(rows, xKey, yKey, aggregation, limit = 50) {
  if (!rows?.length || !xKey) return [];

  if (aggregation === 'none') {
    return rows.slice(0, limit).map((row, idx) => ({
      name: String(row[xKey] ?? `row_${idx + 1}`),
      xRaw: row[xKey],
      yVal: toNumber(row[yKey]),
    }));
  }

  const grouped = new Map();
  rows.forEach((row) => {
    const bucket = String(row[xKey] ?? 'Unknown');
    if (!grouped.has(bucket)) {
      grouped.set(bucket, { count: 0, values: [] });
    }
    const slot = grouped.get(bucket);
    slot.count += 1;
    const num = toNumber(row[yKey]);
    if (num !== null) slot.values.push(num);
  });

  const out = [];
  grouped.forEach((stats, bucket) => {
    const { count, values } = stats;
    let value = null;

    if (aggregation === 'count') {
      value = count;
    } else if (!values.length) {
      value = null;
    } else if (aggregation === 'sum') {
      value = values.reduce((a, b) => a + b, 0);
    } else if (aggregation === 'avg') {
      value = values.reduce((a, b) => a + b, 0) / values.length;
    } else if (aggregation === 'min') {
      value = Math.min(...values);
    } else if (aggregation === 'max') {
      value = Math.max(...values);
    }

    out.push({ name: bucket, xRaw: bucket, yVal: value });
  });

  return out.slice(0, limit);
}

export default function ChartRenderer({ chartType, chartX, chartY, result, height = 220, isDashboard = false }) {
  // ── Correlation matrix: special renderer, bypass Recharts entirely
  if (chartType === 'correlation_matrix') {
    return (
      <div className={`chart-container-full ${isDashboard ? 'dashboard-view' : ''}`}>
        {!isDashboard && <p className="chart-title">🔥 Correlation Matrix</p>}
        <CorrelationHeatmap result={result} />
      </div>
    );
  }

  if (!result?.length) return null;

  const columns = useMemo(() => {
    const seen = new Set();
    result.forEach((row) => Object.keys(row || {}).forEach((k) => seen.add(k)));
    return Array.from(seen);
  }, [result]);

  const numericColumns = useMemo(() => {
    return columns.filter((col) => {
      let numericHits = 0;
      let checked = 0;
      for (let i = 0; i < result.length && checked < 25; i += 1) {
        const val = result[i]?.[col];
        if (val === null || val === undefined || val === '') continue;
        checked += 1;
        if (toNumber(val) !== null) numericHits += 1;
      }
      return checked > 0 && numericHits / checked >= 0.7;
    });
  }, [columns, result]);

  const defaultType = chartType || 'bar';
  const defaultX = chartX || columns[0] || '';
  const defaultY = (Array.isArray(chartY) && chartY[0]) || numericColumns[0] || columns[0] || '';

  const [selectedType, setSelectedType] = useState(defaultType);
  const [selectedX, setSelectedX] = useState(defaultX);
  const [selectedY, setSelectedY] = useState(defaultY);
  const [aggregation, setAggregation] = useState('none');

  const dataLimit = isDashboard ? 30 : 50;

  const aggregated = useMemo(() => {
    return aggregateData(result, selectedX, selectedY, aggregation, dataLimit);
  }, [result, selectedX, selectedY, aggregation, dataLimit]);

  const data = useMemo(() => {
    if (selectedType === 'scatter') {
      if (aggregation === 'none') {
        return result
          .slice(0, dataLimit)
          .map((row, idx) => {
            const xNum = toNumber(row[selectedX]);
            const yNum = toNumber(row[selectedY]);
            if (yNum === null) return null;
            return {
              x: xNum === null ? idx + 1 : xNum,
              y: yNum,
              label: String(row[selectedX] ?? idx + 1),
            };
          })
          .filter(Boolean);
      }

      return aggregated
        .map((row, idx) => ({
          x: toNumber(row.xRaw) ?? idx + 1,
          y: toNumber(row.yVal),
          label: row.name,
        }))
        .filter((row) => row.y !== null);
    }

    return aggregated
      .map((row) => ({
        name: row.name,
        [selectedY]: toNumber(row.yVal),
      }))
      .filter((row) => row[selectedY] !== null);
  }, [selectedType, aggregation, result, selectedX, selectedY, aggregated, dataLimit]);

  if (!selectedX || !selectedY || !data.length) return null;

  const axisStyle = { fontSize: 10, fill: 'var(--text-muted)', fontWeight: 500 };

  const sharedProps = {
    data,
    margin: { top: 10, right: 10, left: 0, bottom: 10 },
  };

  const commonChildren = (
    <>
      <defs>
        <linearGradient id="colorPrimary" x1="0" y1="0" x2="0" y2="1">
          <stop offset="5%" stopColor="#7c3aed" stopOpacity={0.3}/>
          <stop offset="95%" stopColor="#7c3aed" stopOpacity={0}/>
        </linearGradient>
        <linearGradient id="colorSecondary" x1="0" y1="0" x2="0" y2="1">
          <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.3}/>
          <stop offset="95%" stopColor="#06b6d4" stopOpacity={0}/>
        </linearGradient>
      </defs>
      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" vertical={false} />
      <XAxis 
        dataKey="name" 
        tick={axisStyle} 
        axisLine={false} 
        tickLine={false} 
        minTickGap={20}
      />
      <YAxis 
        tick={axisStyle} 
        axisLine={false} 
        tickLine={false} 
        width={45}
        tickFormatter={(v) => v >= 1000 ? `${(v/1000).toFixed(1)}k` : v} 
      />
      <Tooltip content={<CustomTooltip />} cursor={{ stroke: 'rgba(255,255,255,0.1)', strokeWidth: 1 }} />
    </>
  );

  const renderChart = () => {
    switch (selectedType) {
      case 'area':
        return (
          <AreaChart {...sharedProps}>
            {commonChildren}
            {[selectedY].map((col, i) => (
              <Area
                key={col}
                type="monotone"
                dataKey={col}
                stroke={COLORS[i % COLORS.length]}
                fill={`url(#${i === 0 ? 'colorPrimary' : 'colorSecondary'})`}
                strokeWidth={2.5}
                activeDot={{ r: 5, strokeWidth: 0 }}
              />
            ))}
          </AreaChart>
        );
      case 'pie':
        const pieData = data.slice(0, 8); // Top 8 for pie
        return (
          <PieChart>
            <Pie
              data={pieData}
              innerRadius={isDashboard ? "60%" : "50%"}
              outerRadius={isDashboard ? "85%" : "80%"}
              paddingAngle={5}
              dataKey={selectedY}
            >
              {pieData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} stroke="none" />
              ))}
            </Pie>
            <Tooltip content={<CustomTooltip />} />
          </PieChart>
        );
      case 'bar':
        return (
          <BarChart {...sharedProps}>
            {commonChildren}
            {[selectedY].map((col, i) => (
              <Bar
                key={col}
                dataKey={col}
                fill={COLORS[i % COLORS.length]}
                radius={[6, 6, 0, 0]}
                maxBarSize={isDashboard ? 30 : 50}
                animationDuration={1500}
              />
            ))}
          </BarChart>
        );
      case 'scatter':
        return (
          <ScatterChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" />
            <XAxis dataKey="x" tick={axisStyle} axisLine={false} tickLine={false} name={selectedX} />
            <YAxis dataKey="y" tick={axisStyle} axisLine={false} tickLine={false} name={selectedY} />
            <Tooltip
              cursor={{ strokeDasharray: '3 3' }}
              formatter={(value, name) => [
                typeof value === 'number' ? value.toLocaleString() : value,
                name === 'y' ? selectedY : selectedX,
              ]}
              labelFormatter={(_, payload) => payload?.[0]?.payload?.label || ''}
            />
            <Scatter dataKey="y" fill={COLORS[0]} />
          </ScatterChart>
        );
      case 'line':
      default:
        return (
          <LineChart {...sharedProps}>
            {commonChildren}
            {[selectedY].map((col, i) => (
              <Line
                key={col}
                type="monotone"
                dataKey={col}
                stroke={COLORS[i % COLORS.length]}
                strokeWidth={2.5}
                dot={{ r: 3, fill: COLORS[i % COLORS.length], strokeWidth: 0 }}
                activeDot={{ r: 6, strokeWidth: 0 }}
              />
            ))}
          </LineChart>
        );
    }
  };

  return (
    <div className={`chart-container-full ${isDashboard ? 'dashboard-view' : ''}`}>
      {!isDashboard && (
        <div className="chart-builder-controls">
          <label className="chart-builder-field">
            <span>Type</span>
            <select value={selectedType} onChange={(e) => setSelectedType(e.target.value)}>
              <option value="bar">Bar</option>
              <option value="line">Line</option>
              <option value="area">Area</option>
              <option value="pie">Pie</option>
              <option value="scatter">Scatter</option>
            </select>
          </label>

          <label className="chart-builder-field">
            <span>X</span>
            <select value={selectedX} onChange={(e) => setSelectedX(e.target.value)}>
              {columns.map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </label>

          <label className="chart-builder-field">
            <span>Y</span>
            <select value={selectedY} onChange={(e) => setSelectedY(e.target.value)}>
              {(numericColumns.length ? numericColumns : columns).map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </label>

          <label className="chart-builder-field">
            <span>Agg</span>
            <select value={aggregation} onChange={(e) => setAggregation(e.target.value)}>
              <option value="none">None</option>
              <option value="sum">Sum</option>
              <option value="avg">Avg</option>
              <option value="min">Min</option>
              <option value="max">Max</option>
              <option value="count">Count</option>
            </select>
          </label>
        </div>
      )}

      {!isDashboard && (
        <p className="chart-title">
          {selectedType === 'area' ? '📈 Confidence Trend' : 
           selectedType === 'pie' ? '🥧 Composition' : 
           selectedType === 'scatter' ? '🟣 Relationship' :
           selectedType === 'bar' ? '📊 Comparison' : '📉 Trend'} — {selectedX}
        </p>
      )}
      <ResponsiveContainer width="100%" height={height}>
        {renderChart()}
      </ResponsiveContainer>
      {selectedType === 'pie' && isDashboard && (
        <div className="pie-center-label">
          <div className="pie-center-value">{result.length}</div>
          <div className="pie-center-text">Total Rows</div>
        </div>
      )}
    </div>
  );
}
