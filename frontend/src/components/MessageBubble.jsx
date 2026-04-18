import { useState } from 'react';
import { ChevronDown, ChevronUp, Code2 } from 'lucide-react';
import ChartRenderer from './ChartRenderer';
import ResultTable from './ResultTable';

function UserBubble({ text }) {
  return (
    <div className="message-row user" id={`msg-user-${Date.now()}`}>
      <div className="msg-avatar user">👤</div>
      <div className="msg-content">
        <div className="user-bubble">{text}</div>
      </div>
    </div>
  );
}

function AIBubble({ msg }) {
  const [showSQL, setShowSQL] = useState(false);
  const [showLog, setShowLog] = useState(false);

  const modeLabel =
    msg.mode === 'smart'
      ? '⚡ Smart Mode'
      : msg.mode === 'scalable'
        ? '🧠 Scalable Mode'
        : '⚪ Raw Mode';

  if (msg.error) {
    return (
      <div className="message-row">
        <div className="msg-avatar ai">🤖</div>
        <div className="msg-content" style={{ flex: 1 }}>
          <div className="error-card">⚠️ {msg.error}</div>
        </div>
      </div>
    );
  }

  const hasLog = msg.preprocessing_log?.length > 0;
  const hasSql = msg.sql?.trim().length > 0;

  return (
    <div className="message-row">
      <div className="msg-avatar ai">🤖</div>
      <div className="msg-content" style={{ flex: 1 }}>
        <div className="ai-card">

          {/* Mode badge */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span className={`mode-badge ${msg.mode}`}>
              {modeLabel}
            </span>
            {msg.sql && (
              <button
                style={{ background: 'none', border: 'none', color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: 3, fontSize: 11, cursor: 'pointer' }}
                onClick={() => setShowSQL(!showSQL)}
              >
                <Code2 size={12} />
                {showSQL ? 'Hide SQL' : 'Show SQL'}
                {showSQL ? <ChevronUp size={11} /> : <ChevronDown size={11} />}
              </button>
            )}
          </div>

          {/* SQL block */}
          {showSQL && hasSql && (
            <div>
              <p className="sql-label">Generated SQL</p>
              <div className="sql-block">{msg.sql}</div>
            </div>
          )}

          {/* Explanation */}
          {msg.explanation && (
            <p className="explanation-text">{msg.explanation}</p>
          )}

          {/* Insights */}
          {msg.insights?.length > 0 && (
            <ul className="insight-list">
              {msg.insights.map((ins, i) => (
                <li key={i} className="insight-item">
                  <span className="insight-dot" />
                  {ins}
                </li>
              ))}
            </ul>
          )}

          {/* Why analysis */}
          {msg.why_analysis && (
            <div className="why-block">
              <div className="why-block-label">🔍 Why did this happen?</div>
              <p>{msg.why_analysis}</p>
            </div>
          )}

          {/* Chart */}
          <ChartRenderer
            chartType={msg.chart_type}
            chartX={msg.chart_x}
            chartY={msg.chart_y}
            result={msg.result}
            msgId={msg.id}
          />

          {/* Result table */}
          <ResultTable result={msg.result} columns={msg.columns} />

          {/* Preprocessing log */}
          {hasLog && (
            <div className="preproc-log">
              <div className="preproc-log-header">
                <span>🔬 Preprocessing</span>
                <button
                  style={{ background: 'none', border: 'none', color: 'var(--text-muted)', cursor: 'pointer', fontSize: 10, marginLeft: 4 }}
                  onClick={() => setShowLog(!showLog)}
                >
                  {showLog ? '▲ collapse' : '▼ expand'}
                </button>
              </div>
              {showLog && msg.preprocessing_log.map((line, i) => (
                <div key={i} className="preproc-log-item">{line}</div>
              ))}
              {!showLog && (
                <div className="preproc-log-item" style={{ fontStyle: 'italic' }}>
                  {msg.preprocessing_log.length} step{msg.preprocessing_log.length !== 1 ? 's' : ''} applied
                </div>
              )}
            </div>
          )}

        </div>
      </div>
    </div>
  );
}

export default function MessageBubble({ msg }) {
  if (msg.role === 'user') return <UserBubble text={msg.text} />;
  return <AIBubble msg={msg} />;
}
