import { useState, useCallback, useEffect } from 'react';
import './index.css';
import UploadPanel from './components/UploadPanel';
import ModeToggle from './components/ModeToggle';
import SuggestedQuestions from './components/SuggestedQuestions';
import ChatWindow from './components/ChatWindow';
import InsightsDashboard from './components/InsightsDashboard';
import DataHealthPanel from './components/DataHealthPanel';
import ReportExporter from './components/ReportExporter';
import { Database, MessageSquare, BarChart3, Cpu } from 'lucide-react';
import { fetchDataHealth } from './api/client';
import LandingPage from './components/landing';

export default function App() {
  const [dataset, setDataset]               = useState(null);
  const [mode, setMode]                     = useState('raw');
  const [pendingQuestion, setPendingQuestion] = useState('');
  const [view, setView]                     = useState('chat');
  const [latestResult, setLatestResult]     = useState(null);
  const [latestQuestion, setLatestQuestion] = useState('');
  const [dataHealth, setDataHealth]         = useState(null);
  const [healthLoading, setHealthLoading]   = useState(false);
  const [chatMessages, setChatMessages]     = useState([]);

  const handleUpload = useCallback((data) => {
    setDataset(data);
    setLatestResult(null);
    setLatestQuestion('');
    setChatMessages([]);
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

  const handleSuggestion  = useCallback((q) => setPendingQuestion(q), []);
  const handleQueryResult = useCallback((res) => {
    if (res && !res.error) {
      setLatestResult(res);
      setLatestQuestion(res._question || '');
      if (res.data_health) setDataHealth(res.data_health);
    }
  }, []);
  const handleMessages = useCallback((msgs) => setChatMessages(msgs), []);

  useEffect(() => {
    if (!dataset?.dataset_id) return;
    let cancelled = false;
    setHealthLoading(true);
    fetchDataHealth({ datasetId: dataset.dataset_id, mode })
      .then((health) => { if (!cancelled) setDataHealth(health); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setHealthLoading(false); });
    return () => { cancelled = true; };
  }, [mode, dataset?.dataset_id]);

  if (!dataset) return <LandingPage onUpload={handleUpload} />;

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
              <h1>DataLens</h1>
              <p>AI CSV Query Engine</p>
            </div>
          </div>
        </div>

        <div className="sidebar-body">
          <UploadPanel onUpload={handleUpload} dataset={dataset} />
          <div className="divider" />
          <ModeToggle mode={mode} onChange={setMode} />
          <div className="divider" />
          {dataset?.suggested_questions?.length > 0 && (
            <SuggestedQuestions questions={dataset.suggested_questions} onSelect={handleSuggestion} />
          )}
          {dataHealth && (
            <>
              <div className="divider" />
              <DataHealthPanel health={dataHealth} loading={healthLoading} />
            </>
          )}

          {/* Footer */}
          <div style={{ marginTop: 'auto', paddingTop: 10 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6, opacity: 0.5 }}>
              <Cpu size={10} color="var(--text-faint)" />
              <p style={{ fontSize: 10, color: 'var(--text-faint)', fontFamily: 'var(--font-mono)', textAlign: 'center', lineHeight: 1.7, letterSpacing: '0.04em' }}>
                DuckDB · Groq · FastAPI<br />
                Schema only — data stays local
              </p>
            </div>
          </div>
        </div>
      </aside>

      {/* ── Main Area ───────────────────────────────── */}
      <main className="chat-area">
        {/* Top action bar */}
        {dataset && (
          <div style={{
            position: 'absolute',
            top: 14, right: 20,
            zIndex: 100,
            display: 'flex',
            alignItems: 'center',
            gap: 10,
          }}>
            <ReportExporter
              messages={chatMessages}
              dataset={dataset}
              mode={mode}
              dataHealth={dataHealth}
            />
            <div className="view-toggle">
              <button className={`view-toggle-btn${view === 'chat' ? ' active' : ''}`} onClick={() => setView('chat')}>
                <MessageSquare size={13} />Chat
              </button>
              <button className={`view-toggle-btn${view === 'dashboard' ? ' active' : ''}`} onClick={() => setView('dashboard')}>
                <BarChart3 size={13} />Visualize
              </button>
            </div>
          </div>
        )}

        <div style={{ display: view === 'chat' ? 'contents' : 'none' }}>
          <ChatWindow
            datasetId={dataset?.dataset_id || null}
            mode={mode}
            pendingQuestion={pendingQuestion}
            onPendingConsumed={() => setPendingQuestion('')}
            onResult={handleQueryResult}
            onMessages={handleMessages}
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
