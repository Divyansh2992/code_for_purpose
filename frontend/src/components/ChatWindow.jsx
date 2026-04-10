import { useEffect, useRef, useState, useCallback } from 'react';
import { Send } from 'lucide-react';
import MessageBubble from './MessageBubble';
import { useChat } from '../hooks/useChat';

export default function ChatWindow({ datasetId, mode, onSuggestionSelect, pendingQuestion, onPendingConsumed, onResult }) {
  const { messages, isLoading, ask } = useChat(datasetId, mode);
  const [input, setInput] = useState('');
  const bottomRef = useRef(null);
  const textareaRef = useRef(null);

  // Notify parent of latest result for the dashboard
  useEffect(() => {
    const lastMsg = messages[messages.length - 1];
    if (lastMsg && lastMsg.role === 'ai' && !lastMsg.error) {
      onResult?.(lastMsg);
    }
  }, [messages, onResult]);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isLoading]);

  // Consume pending question from parent (suggested questions)
  useEffect(() => {
    if (pendingQuestion) {
      setInput(pendingQuestion);
      onPendingConsumed?.();
      textareaRef.current?.focus();
    }
  }, [pendingQuestion, onPendingConsumed]);

  const submit = useCallback(async () => {
    const q = input.trim();
    if (!q || !datasetId || isLoading) return;
    setInput('');
    await ask(q);
  }, [input, datasetId, isLoading, ask]);

  const onKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      submit();
    }
  };

  const isEmpty = messages.length === 0 && !isLoading;

  return (
    <>
      {/* Header */}
      <div className="chat-header">
        <div>
          <div className="chat-header-title">💬 Ask your data</div>
          <div className="chat-header-sub">
            {datasetId
              ? 'Dataset loaded — type a question below'
              : 'Upload a CSV to get started'}
          </div>
        </div>
      </div>

      {/* Messages */}
      <div className="chat-messages">
        {isEmpty ? (
          <div className="chat-empty">
            <div className="chat-empty-icon">🗂️</div>
            <h2>{datasetId ? 'Ready to explore!' : 'No dataset uploaded yet'}</h2>
            <p>
              {datasetId
                ? 'Ask anything about your data in plain English. Try a suggested question from the sidebar.'
                : 'Upload a CSV file from the sidebar, then ask questions in natural language.'}
            </p>
          </div>
        ) : (
          <>
            {messages.map((msg) => (
              <MessageBubble key={msg.id} msg={msg} />
            ))}
            {isLoading && (
              <div className="message-row">
                <div className="msg-avatar ai">🤖</div>
                <div className="ai-card" style={{ padding: '12px 16px' }}>
                  <div className="loading-dots">
                    <div className="loading-dot" />
                    <div className="loading-dot" />
                    <div className="loading-dot" />
                  </div>
                </div>
              </div>
            )}
          </>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="chat-input-area">
        <div className="chat-input-row">
          <textarea
            id="chat-input"
            ref={textareaRef}
            className="chat-input"
            rows={1}
            placeholder={
              datasetId
                ? 'Ask a question about your data… (Enter to send, Shift+Enter for newline)'
                : 'Upload a CSV first to start asking questions…'
            }
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={onKeyDown}
            disabled={!datasetId || isLoading}
          />
          <button
            id="send-btn"
            className="send-btn"
            onClick={submit}
            disabled={!datasetId || !input.trim() || isLoading}
            title="Send (Enter)"
          >
            <Send size={18} />
          </button>
        </div>
      </div>
    </>
  );
}
