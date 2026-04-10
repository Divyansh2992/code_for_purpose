import { useState, useCallback } from 'react';
import './index.css';
import UploadPanel from './components/UploadPanel';
import ModeToggle from './components/ModeToggle';
import SuggestedQuestions from './components/SuggestedQuestions';
import ChatWindow from './components/ChatWindow';
import InsightsDashboard from './components/InsightsDashboard';
import { Database, MessageSquare, BarChart3 } from 'lucide-react';

export default function App() {
  const [dataset, setDataset]               = useState(null);   // full upload response
  const [mode, setMode]                     = useState('raw');
  const [pendingQuestion, setPendingQuestion] = useState('');
  const [view, setView]                     = useState('chat'); // 'chat' | 'dashboard'
  const [latestResult, setLatestResult]     = useState(null);
  const [latestQuestion, setLatestQuestion] = useState('');

  const handleUpload = useCallback((data) => {
    setDataset(data);
  }, []);

  const handleSuggestion = useCallback((q) => {
    setPendingQuestion(q);
  }, []);

  // Callback to capture when a query finishes in ChatWindow
  const onQueryResult = useCallback((res) => {
    if (res && !res.error) {
      setLatestResult(res);
      // Also save the question text from the result object (set by ChatWindow via message)
    }
  }, []);

  // We grab the question from the last user message via ref via onResult from the hook itself
  const handleQueryResult = useCallback((res) => {
    if (res && !res.error) {
      setLatestResult(res);
      setLatestQuestion(res._question || '');
    }
  }, []);

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
          <UploadPanel onUpload={handleUpload} />

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
                disabled={!latestResult}
                title={!latestResult ? "Ask a question first to enable dashboard" : ""}
              >
                <BarChart3 size={14} />
                Visualize
              </button>
           </div>
        </div>

        {view === 'chat' ? (
          <ChatWindow
            datasetId={dataset?.dataset_id || null}
            mode={mode}
            pendingQuestion={pendingQuestion}
            onPendingConsumed={() => setPendingQuestion('')}
          onResult={handleQueryResult}
          />
        ) : (
          <InsightsDashboard 
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
