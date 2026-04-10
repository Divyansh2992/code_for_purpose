import { Zap, FlaskConical } from 'lucide-react';

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
      </div>
      <p className="mode-desc">
        {mode === 'raw'
          ? 'Direct query on original data, no preprocessing'
          : 'Auto-clean nulls & outliers before querying'}
      </p>
    </div>
  );
}
