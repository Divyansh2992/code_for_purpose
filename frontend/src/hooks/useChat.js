import { useState, useRef, useCallback } from 'react';
import { sendQuery, fetchCorrelationMatrix } from '../api/client';

// Keywords that should route to the pandas /correlation-matrix endpoint
// instead of the DuckDB /query endpoint.
const CORR_KEYWORDS = [
  'correlation matrix',
  'corr matrix',
  'correlation heatmap',
  'draw correlation',
  'show correlation',
  'plot correlation',
  'generate correlation',
  'heatmap',
];

function isCorrRequest(question) {
  const q = question.toLowerCase();
  return CORR_KEYWORDS.some((kw) => q.includes(kw));
}

/**
 * Manages chat state: messages, loading, session context memory.
 * session_id is stable per browser session so the LLM can use
 * conversation history for follow-up queries.
 */
export function useChat(datasetId, mode, guardianEnabled = true) {
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const sessionId = useRef(crypto.randomUUID()).current;

  const ask = useCallback(
    async (question) => {
      if (!question.trim() || !datasetId) return;

      // Append user message
      const userMsg = { role: 'user', text: question, id: Date.now() };
      setMessages((prev) => [...prev, userMsg]);
      setIsLoading(true);

      try {
        let aiMsg;

        if (isCorrRequest(question)) {
          // ── Correlation matrix: use pandas endpoint, not DuckDB SQL ──────
          const data = await fetchCorrelationMatrix({ datasetId });
          aiMsg = {
            role: 'ai',
            id: Date.now() + 1,
            sql: `# Python (pandas)\ndf.select_dtypes(include='number').corr(method='${data.method}')`,
            result: data.data,          // [{col_a, col_b, correlation}]
            columns: ['col_a', 'col_b', 'correlation'],
            chart_type: 'correlation_matrix',
            chart_x: null,
            chart_y: [],
            explanation: `Pearson correlation matrix computed using pandas across ${data.columns.length} numeric column(s): ${data.columns.join(', ')}.`,
            insights: [
              'Values close to +1.0 indicate strong positive correlation.',
              'Values close to -1.0 indicate strong negative correlation.',
              'Values near 0 indicate weak or no linear relationship.',
            ],
            why_analysis: data.note || '',
            data_health: { missing_pct: 0, outliers: 0, rows_used: 0, confidence: 100 },
            preprocessing_log: ['🐍 Computed via pandas df.corr() — no SQL required.'],
            mode,
            guardian_enabled: false,
            guardian_passed: false,
            guardian_confidence: 0,
            guardian_retries: 0,
            guardian_log: [],
            guardian_steps: [],
            error: null,
          };
        } else {
          // ── Normal query: LLM → DuckDB SQL ───────────────────────────────
          const data = await sendQuery({ datasetId, question, mode, sessionId, guardianEnabled });
          aiMsg = { role: 'ai', id: Date.now() + 1, ...data };
        }

        setMessages((prev) => [...prev, aiMsg]);
      } catch (err) {
        const errMsg = {
          role: 'ai',
          id: Date.now() + 1,
          error: err.message,
          sql: '',
          result: [],
          columns: [],
          explanation: '',
          insights: [],
          data_health: { missing_pct: 0, outliers: 0, rows_used: 0, confidence: 0 },
          preprocessing_log: [],
          mode,
          guardian_enabled: false,
          guardian_passed: false,
          guardian_confidence: 0,
          guardian_retries: 0,
          guardian_log: [],
          guardian_steps: [],
        };
        setMessages((prev) => [...prev, errMsg]);
      } finally {
        setIsLoading(false);
      }
    },
    [datasetId, mode, sessionId, guardianEnabled],
  );

  const clear = useCallback(() => setMessages([]), []);

  return { messages, isLoading, ask, clear, sessionId };
}
