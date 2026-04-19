import { useMemo, useState } from 'react';
import { ChevronDown, ChevronUp, Code2 } from 'lucide-react';
import ChartRenderer from './ChartRenderer';
import ResultTable from './ResultTable';

const STAGE_ORDER = [
  { key: 'validator', label: 'Validator' },
  { key: 'semantic_review', label: 'Semantic Review' },
  { key: 'dry_run', label: 'Dry-run' },
  { key: 'repair', label: 'Repair' },
];

function normaliseGuardianSteps(msg) {
  if (Array.isArray(msg.guardian_steps) && msg.guardian_steps.length > 0) {
    return msg.guardian_steps;
  }

  const rawLog = Array.isArray(msg.guardian_log) ? msg.guardian_log : [];
  if (!rawLog.length) return [];

  const attempts = new Map();
  let lastAttemptNo = 1;

  const getAttempt = (attemptNo) => {
    if (!attempts.has(attemptNo)) {
      attempts.set(attemptNo, { attempt: attemptNo, stages: [] });
    }
    return attempts.get(attemptNo);
  };

  for (const line of rawLog) {
    let m = line.match(/^Attempt\s+(\d+):\s+static SQL safety check\s+(passed|failed)(?::\s*(.*))?$/i);
    if (m) {
      const attemptNo = Number(m[1]);
      lastAttemptNo = attemptNo;
      const attempt = getAttempt(attemptNo);
      attempt.stages.push({
        stage: 'validator',
        status: m[2].toLowerCase() === 'passed' ? 'pass' : 'fail',
        message: line,
      });
      continue;
    }

    m = line.match(/^Attempt\s+(\d+):\s+semantic review\s+(PASS|FAIL)\s+-\s+(.*)$/i);
    if (m) {
      const attemptNo = Number(m[1]);
      lastAttemptNo = attemptNo;
      const attempt = getAttempt(attemptNo);
      attempt.stages.push({
        stage: 'semantic_review',
        status: m[2].toUpperCase() === 'PASS' ? 'pass' : 'fail',
        message: line,
      });
      continue;
    }

    m = line.match(/^Attempt\s+(\d+):\s+dry-run\s+(passed|failed)(.*)$/i);
    if (m) {
      const attemptNo = Number(m[1]);
      lastAttemptNo = attemptNo;
      const attempt = getAttempt(attemptNo);
      attempt.stages.push({
        stage: 'dry_run',
        status: m[2].toLowerCase() === 'passed' ? 'pass' : 'fail',
        message: line,
      });
      continue;
    }

    if (line.includes('repaired SQL candidate')) {
      const attempt = getAttempt(lastAttemptNo);
      attempt.stages.push({
        stage: 'repair',
        status: 'pass',
        message: line,
      });
      continue;
    }

    if (line.toLowerCase().includes('verifier supplied sql hint')) {
      const attempt = getAttempt(lastAttemptNo);
      attempt.stages.push({
        stage: 'repair',
        status: 'info',
        message: line,
      });
    }
  }

  return Array.from(attempts.values()).sort((a, b) => a.attempt - b.attempt);
}

function attemptStatus(attempt) {
  const stages = Array.isArray(attempt?.stages) ? attempt.stages : [];
  if (stages.some((s) => s.status === 'fail')) return 'fail';
  if (stages.some((s) => s.status === 'pass')) return 'pass';
  return 'info';
}

function GuardianPanel({ msg }) {
  const [openPanel, setOpenPanel] = useState(false);
  const [openAttempts, setOpenAttempts] = useState({});
  const [openStages, setOpenStages] = useState({});

  const steps = useMemo(() => normaliseGuardianSteps(msg), [msg.guardian_steps, msg.guardian_log]);
  const hasGuardian = Boolean(msg.guardian_enabled);

  if (!hasGuardian || steps.length === 0) return null;

  const attemptsCount = Number(msg.guardian_retries || 0) + 1;

  const toggleAttempt = (attemptNo) => {
    setOpenAttempts((prev) => ({ ...prev, [attemptNo]: !prev[attemptNo] }));
  };

  const toggleStage = (attemptNo, stageKey) => {
    const key = `${attemptNo}-${stageKey}`;
    setOpenStages((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <div className="guardian-panel">
      <div className="guardian-panel-header">
        <span>🛡️ SQL Guardian</span>
        <button
          type="button"
          className="guardian-panel-toggle"
          onClick={() => setOpenPanel((prev) => !prev)}
        >
          {openPanel ? 'Collapse' : 'Expand'}
        </button>
      </div>

      {!openPanel && (
        <div className="guardian-panel-summary">
          {msg.guardian_passed
            ? `Verified in ${attemptsCount} attempt${attemptsCount !== 1 ? 's' : ''}`
            : `Blocked after ${attemptsCount} attempt${attemptsCount !== 1 ? 's' : ''}`}
        </div>
      )}

      {openPanel && (
        <div className="guardian-attempt-list">
          {steps.map((attempt) => {
            const status = attemptStatus(attempt);
            const isOpen = Boolean(openAttempts[attempt.attempt]);
            const stageMap = new Map((attempt.stages || []).map((stage) => [stage.stage, stage]));

            return (
              <div key={attempt.attempt} className="guardian-attempt">
                <button
                  type="button"
                  className="guardian-attempt-head"
                  onClick={() => toggleAttempt(attempt.attempt)}
                >
                  <span>Attempt {attempt.attempt}</span>
                  <span className={`guardian-attempt-badge ${status}`}>{status.toUpperCase()}</span>
                </button>

                {isOpen && (
                  <div className="guardian-stage-list">
                    {STAGE_ORDER.map(({ key, label }) => {
                      const stage = stageMap.get(key) || {
                        stage: key,
                        status: 'skipped',
                        message: 'Not reached in this attempt.',
                      };
                      const openKey = `${attempt.attempt}-${key}`;
                      const stageOpen = Boolean(openStages[openKey]);

                      return (
                        <div key={openKey} className="guardian-stage">
                          <button
                            type="button"
                            className="guardian-stage-head"
                            onClick={() => toggleStage(attempt.attempt, key)}
                          >
                            <span className="guardian-stage-label">{label}</span>
                            <span className={`guardian-stage-status ${stage.status}`}>
                              {String(stage.status || 'info').toUpperCase()}
                            </span>
                          </button>

                          {stageOpen && (
                            <div className="guardian-stage-details">
                              <div>{stage.message}</div>
                              {stage.details && typeof stage.details === 'object' && (
                                <div className="guardian-stage-kv">
                                  {Object.entries(stage.details).map(([k, v]) => (
                                    <div key={k}>
                                      <strong>{k}:</strong> {String(v)}
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

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
          <div style={{ marginTop: 10 }}>
            <GuardianPanel msg={msg} />
          </div>
        </div>
      </div>
    );
  }

  const hasLog = msg.preprocessing_log?.length > 0;
  const hasSql = msg.sql?.trim().length > 0;
  const hasGuardian = Boolean(msg.guardian_enabled);
  const guardianConfidence = Number.isFinite(msg.guardian_confidence)
    ? Number(msg.guardian_confidence).toFixed(1)
    : '0.0';

  return (
    <div className="message-row">
      <div className="msg-avatar ai">🤖</div>
      <div className="msg-content" style={{ flex: 1 }}>
        <div className="ai-card">

          {/* Mode badge */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
            <span className={`mode-badge ${msg.mode}`}>
              {modeLabel}
            </span>
            {hasGuardian && (
              <span className={`guardian-badge ${msg.guardian_passed ? 'pass' : 'fail'}`}>
                {msg.guardian_passed
                  ? `Guardian verified (${guardianConfidence}%)`
                  : 'Guardian blocked'}
              </span>
            )}
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

          {/* Guardian log */}
          <GuardianPanel msg={msg} />

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
