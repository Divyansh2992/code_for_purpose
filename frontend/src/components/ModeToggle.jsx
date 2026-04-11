import { Zap, FlaskConical, Cpu } from 'lucide-react';

const MODE_DESCRIPTIONS = {
  raw: 'Direct query on original data, no preprocessing',
  smart: 'Auto-clean nulls & outliers before querying',
  scalable: 'PySpark preprocessing for larger datasets (local Spark)',
};

export default function ModeToggle({ mode, onChange }) {
  return (
    <div className="mode-toggle-wrap">
      <p className="section-label">Query Mode</p>
      <div className="mode-toggle">
        <button
          id="mode-raw"
          className={`mode-btn${mode === 'raw' ? ' active' : ''}`}
          onClick={() => onChange('raw')}
        >
          <Zap size={13} />
          Raw
        </button>
        <button
          id="mode-smart"
          className={`mode-btn${mode === 'smart' ? ' active' : ''}`}
          onClick={() => onChange('smart')}
        >
          <FlaskConical size={13} />
          Smart
        </button>
        <button
          id="mode-scalable"
          className={`mode-btn${mode === 'scalable' ? ' active' : ''}`}
          onClick={() => onChange('scalable')}
        >
          <Cpu size={13} />
          Scalable
        </button>
      </div>
      <p className="mode-desc">{MODE_DESCRIPTIONS[mode] || MODE_DESCRIPTIONS.raw}</p>
    </div>
  );
}
