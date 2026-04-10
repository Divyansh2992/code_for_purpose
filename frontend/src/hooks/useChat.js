import { useState, useRef, useCallback } from 'react';
import { sendQuery } from '../api/client';

/**
 * Manages chat state: messages, loading, session context memory.
 * session_id is stable per browser session so the LLM can use
 * conversation history for follow-up queries.
 */
export function useChat(datasetId, mode) {
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
        const data = await sendQuery({
          datasetId,
          question,
          mode,
          sessionId,
        });

        const aiMsg = {
          role: 'ai',
          id: Date.now() + 1,
          ...data,
        };
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
        };
        setMessages((prev) => [...prev, errMsg]);
      } finally {
        setIsLoading(false);
      }
    },
    [datasetId, mode, sessionId],
  );

  const clear = useCallback(() => setMessages([]), []);

  return { messages, isLoading, ask, clear, sessionId };
}
