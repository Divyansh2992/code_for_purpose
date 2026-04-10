import {
  BarChart, Bar,
  LineChart, Line,
  AreaChart, Area,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts';

const COLORS = ['#7c3aed', '#06b6d4', '#10b981', '#f59e0b', '#ef4444'];

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
