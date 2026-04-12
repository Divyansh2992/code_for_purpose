import { useState, useCallback, useEffect } from 'react';
import './index.css';
import UploadPanel from './components/UploadPanel';
import ModeToggle from './components/ModeToggle';
import SuggestedQuestions from './components/SuggestedQuestions';
import ChatWindow from './components/ChatWindow';
import InsightsDashboard from './components/InsightsDashboard';
import DataHealthPanel from './components/DataHealthPanel';
import { Database, MessageSquare, BarChart3 } from 'lucide-react';
import { fetchDataHealth } from './api/client';
import LandingPage from './components/landing';

export default function App() {
  const [dataset, setDataset] = useState(null);
  const [mode, setMode] = useState('raw');
  const [pendingQuestion, setPendingQuestion] = useState('');
  const [view, setView] = useState('chat');
  const [latestResult, setLatestResult] = useState(null);
  const [latestQuestion, setLatestQuestion] = useState('');
  const [dataHealth, setDataHealth] = useState(null);
  const [healthLoading, setHealthLoading] = useState(false);

  const handleUpload = useCallback((data) => {
    setDataset(data);
    setLatestResult(null);
    setLatestQuestion('');
    // Instantly show raw health from upload schema (no API needed)
    if (data?.columns?.length) {
      const avgMissing = data.columns.reduce((s, c) => s + (c.null_pct || 0), 0) / data.columns.length;
      const missingPenalty = Math.min(avgMissing * 1.5, 40);
      setDataHealth({
        missing_pct: parseFloat(avgMissing.toFixed(2)),
        outliers: 0,
        rows_used: data.row_count || 0,
        confidence: parseFloat(Math.max(100 - missingPenalty, 0).toFixed(1)),
      });
    } else {
      setDataHealth(null);
    }
  }, []);


  const handleSuggestion = useCallback((q) => {
    setPendingQuestion(q);
  }, []);

  // Capture query result from ChatWindow; update health panel persistently
  const handleQueryResult = useCallback((res) => {
    if (res && !res.error) {
      setLatestResult(res);
      setLatestQuestion(res._question || '');
      if (res.data_health) {
        setDataHealth(res.data_health);
      }
    }
  }, []);

  // Fetch health metrics whenever mode changes (or dataset is first loaded)
  useEffect(() => {
    if (!dataset?.dataset_id) return;
    let cancelled = false;
    setHealthLoading(true);
    fetchDataHealth({ datasetId: dataset.dataset_id, mode })
      .then((health) => { if (!cancelled) setDataHealth(health); })
      .catch(() => {}) // silently ignore; panel keeps old value
      .finally(() => { if (!cancelled) setHealthLoading(false); });
    return () => { cancelled = true; };
  }, [mode, dataset?.dataset_id]);
  if (!dataset) {
    return <LandingPage onUpload={handleUpload} />;
  }
  return (
    <div className="app-shell">
      {/* ── Sidebar ─────────────────────────────────── */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div className="app-logo">
            <div className="app-logo-icon">
              <Database size={18} color="#fff" />
            </div>
            <div>
              <h1>Talk to Data</h1>
              <p>AI-powered CSV Query Engine</p>
            </div>
          </div>
        </div>

        <div className="sidebar-body">
          {/* Upload */}
          <UploadPanel onUpload={handleUpload} dataset={dataset} />

          <div className="divider" />

          {/* Mode toggle */}
          <ModeToggle mode={mode} onChange={setMode} />

          <div className="divider" />

          {/* Suggested questions */}
          {dataset?.suggested_questions?.length > 0 && (
            <SuggestedQuestions
              questions={dataset.suggested_questions}
              onSelect={handleSuggestion}
            />
          )}

          {/* Persistent Data Health Panel — shown once dataset+mode are known */}
          {dataHealth && (
            <>
              <div className="divider" />
              <DataHealthPanel health={dataHealth} loading={healthLoading} />
            </>
          )}

          {/* Footer */}
          <div style={{ marginTop: 'auto', paddingTop: 8 }}>
            <p style={{ fontSize: 10, color: 'var(--text-faint)', textAlign: 'center', lineHeight: 1.6 }}>
              Powered by DuckDB · Groq · FastAPI
              <br />
              Schema + 5 rows sent to LLM — never full data
            </p>
          </div>
        </div>
      </aside>

      {/* ── Main Area ───────────────────────────────── */}
      <main className="chat-area">
        {/* Sub-header with View Toggle */}
        <div style={{ padding: '12px 24px', position: 'absolute', top: 0, right: 0, zIndex: 100, display: dataset ? 'flex' : 'none' }}>
           <div className="view-toggle">
              <button 
                className={`view-toggle-btn ${view === 'chat' ? 'active' : ''}`}
                onClick={() => setView('chat')}
              >
                <MessageSquare size={14} />
                Chat
              </button>
              <button 
                className={`view-toggle-btn ${view === 'dashboard' ? 'active' : ''}`}
                onClick={() => setView('dashboard')}
              >
                <BarChart3 size={14} />
                Visualize
              </button>
           </div>
        </div>

        {/* Keep ChatWindow mounted always to preserve chat state; hide with CSS when in dashboard view */}
        <div style={{ display: view === 'chat' ? 'contents' : 'none' }}>
          <ChatWindow
            datasetId={dataset?.dataset_id || null}
            mode={mode}
            pendingQuestion={pendingQuestion}
            onPendingConsumed={() => setPendingQuestion('')}
            onResult={handleQueryResult}
          />
        </div>
        {view === 'dashboard' && (
          <InsightsDashboard
            datasetId={dataset?.dataset_id || null}
            mode={mode}
            result={latestResult?.result || []}
            columns={latestResult?.columns || []}
            chartType={latestResult?.chart_type}
            chartX={latestResult?.chart_x}
            chartY={latestResult?.chart_y}
            question={latestQuestion}
          />
        )}
      </main>
    </div>
  );
}
