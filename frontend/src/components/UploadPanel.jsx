import { useState, useRef } from 'react';
import { uploadCSV } from '../api/client';
import { Upload, FileSpreadsheet, CheckCircle } from 'lucide-react';

export default function UploadPanel({ onUpload, dataset }) {
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [error, setError]       = useState('');
  const [showSchema, setShowSchema] = useState(false);
  const inputRef = useRef();

  const handleFile = async (file) => {
    if (!file || !file.name.toLowerCase().endsWith('.csv')) {
      setError('Please upload a valid .csv file.');
      return;
    }
    setError('');
    setUploading(true);
    try {
      const data = await uploadCSV(file);
      onUpload(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setUploading(false);
    }
  };

  const onDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    handleFile(e.dataTransfer.files[0]);
  };

  const getNullColor = (pct) => {
    if (pct < 5)  return 'var(--success)';
    if (pct < 20) return 'var(--warning)';
    return 'var(--danger)';
  };

  return (
    <div className="upload-panel">
      <p className="section-label">Dataset</p>

      {/* Drop zone */}
      <div
        className={`drop-zone${dragOver ? ' drag-over' : ''}`}
        onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
        onDragLeave={() => setDragOver(false)}
        onDrop={onDrop}
        onClick={() => inputRef.current?.click()}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".csv"
          onChange={(e) => handleFile(e.target.files[0])}
          onClick={(e) => e.stopPropagation()}
        />
        <div className="drop-zone-icon">
          {dataset ? '✅' : <Upload size={26} color="var(--primary-light)" />}
        </div>
        <div className="drop-zone-text">
          {dataset ? 'Click to replace CSV' : 'Drop CSV here or click to browse'}
        </div>
        <div className="drop-zone-sub">Only .csv files supported</div>
      </div>

      {/* Upload progress */}
      {uploading && (
        <div className="upload-progress">
          <div className="upload-progress-bar" style={{ width: '70%' }} />
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="error-card" style={{ marginTop: 8, fontSize: 12 }}>
          {error}
        </div>
      )}

      {/* Dataset info */}
      {dataset && (
        <div className="dataset-info">
          <div style={{ color: "#10b981", fontSize: 12 }}>
            ✅ Dataset loaded successfully
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
            <FileSpreadsheet size={14} color="var(--primary-light)" />
            <div className="dataset-name">{dataset.filename}</div>
          </div>
          <div className="dataset-stats">
            <div className="stat-chip">Rows: <span>{dataset.row_count.toLocaleString()}</span></div>
            <div className="stat-chip">Cols: <span>{dataset.columns.length}</span></div>
          </div>

          <button
            className="suggestion-btn"
            style={{ marginTop: 8, fontSize: 11 }}
            onClick={() => setShowSchema(!showSchema)}
          >
            {showSchema ? '▲ Hide schema' : '▼ View schema'}
          </button>

          {showSchema && (
            <div style={{ marginTop: 8, overflow: 'auto', maxHeight: 220 }}>
              <table className="schema-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Null%</th>
                  </tr>
                </thead>
                <tbody>
                  {dataset.columns.map((col) => (
                    <tr key={col.name}>
                      <td style={{ fontWeight: 500 }}>{col.name}</td>
                      <td><span className="type-badge">{col.type}</span></td>
                      <td>
                        <div className="null-bar">
                          <div className="null-bar-track">
                            <div
                              className="null-bar-fill"
                              style={{
                                width: `${Math.min(col.null_pct, 100)}%`,
                                background: getNullColor(col.null_pct),
                              }}
                            />
                          </div>
                          <span style={{ fontSize: 10, color: 'var(--text-muted)', minWidth: 30 }}>
                            {col.null_pct.toFixed(1)}%
                          </span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
