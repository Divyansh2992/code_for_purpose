import { useState } from 'react';
import {
  BarChart, Bar,
  LineChart, Line,
  AreaChart, Area,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
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

  if (!chartType || !chartX || !chartY?.length || !result?.length) return null;

  // Prepare Recharts data (cap at 30 for dashboard cleanliness, 50 for chat)
  const dataLimit = isDashboard ? 30 : 50;
  const data = result.slice(0, dataLimit).map((row) => {
    const point = { name: String(row[chartX] ?? '') };
    chartY.forEach((col) => {
      point[col] = typeof row[col] === 'number' ? row[col] : parseFloat(row[col]) || 0;
    });
    return point;
  });

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
      {!isDashboard && chartY.length > 1 && <Legend wrapperStyle={{ fontSize: 11, paddingTop: 10 }} />}
    </>
  );

  const renderChart = () => {
    switch (chartType) {
      case 'area':
        return (
          <AreaChart {...sharedProps}>
            {commonChildren}
            {chartY.map((col, i) => (
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
              dataKey={chartY[0]}
            >
              {pieData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} stroke="none" />
              ))}
            </Pie>
            <Tooltip content={<CustomTooltip />} />
            {isDashboard && (
              <Legend verticalAlign="bottom" height={36} iconType="circle" wrapperStyle={{ fontSize: 10 }} />
            )}
          </PieChart>
        );
      case 'bar':
        return (
          <BarChart {...sharedProps}>
            {commonChildren}
            {chartY.map((col, i) => (
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
      case 'line':
      default:
        return (
          <LineChart {...sharedProps}>
            {commonChildren}
            {chartY.map((col, i) => (
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
        <p className="chart-title">
          {chartType === 'area' ? '📈 Confidence Trend' : 
           chartType === 'pie' ? '🥧 Composition' : 
           chartType === 'bar' ? '📊 Comparison' : '📉 Trend'} — {chartX}
        </p>
      )}
      <ResponsiveContainer width="100%" height={height}>
        {renderChart()}
      </ResponsiveContainer>
      {chartType === 'pie' && isDashboard && (
        <div className="pie-center-label">
          <div className="pie-center-value">{result.length}</div>
          <div className="pie-center-text">Total Rows</div>
        </div>
      )}
    </div>
  );
}
