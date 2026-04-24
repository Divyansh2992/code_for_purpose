import { useEffect, useRef, useState, useCallback } from 'react';
import { Send, Sparkles } from 'lucide-react';
import MessageBubble from './MessageBubble';
import { useChat } from '../hooks/useChat';

export default function ChatWindow({ datasetId, mode, guardianEnabled = true, onSuggestionSelect, pendingQuestion, onPendingConsumed, onResult, onMessages }) {
  const { messages, isLoading, ask } = useChat(datasetId, mode, guardianEnabled);
  const [input, setInput] = useState('');
  const bottomRef   = useRef(null);
  const textareaRef = useRef(null);

  useEffect(() => { onMessages?.(messages); }, [messages, onMessages]);

  useEffect(() => {
    const lastMsg = messages[messages.length - 1];
    if (lastMsg && lastMsg.role === 'ai' && !lastMsg.error) onResult?.(lastMsg);
  }, [messages, onResult]);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages, isLoading]);

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
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(); }
  };

  const isEmpty = messages.length === 0 && !isLoading;

  return (
    <>
      {/* Header */}
      <div className="chat-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{
            width: 32, height: 32,
            borderRadius: 8,
            background: 'var(--gradient)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            boxShadow: '0 4px 14px rgba(139,92,246,0.4)',
          }}>
            <Sparkles size={15} color="#fff" />
          </div>
          <div>
            <div className="chat-header-title">Ask your data</div>
            <div className="chat-header-sub">
              {datasetId ? 'Dataset ready — type a question below' : 'Upload a CSV to begin'}
            </div>
          </div>
        </div>
      </div>

      {/* Messages */}
      <div className="chat-messages">
        {isEmpty ? (
          <div className="chat-empty">
            <div className="chat-empty-icon">
              {datasetId ? '✨' : '📂'}
            </div>
            <h2>{datasetId ? 'Ready to explore!' : 'No dataset yet'}</h2>
            <p>
              {datasetId
                ? 'Ask anything about your data in plain English. Try a suggested question from the sidebar, or type your own query below.'
                : 'Upload a CSV file from the sidebar, then ask questions in natural language. No SQL needed.'}
            </p>
            {datasetId && (
              <div style={{
                marginTop: 8,
                display: 'flex', gap: 8, flexWrap: 'wrap', justifyContent: 'center',
              }}>
                {['What are the top 10 rows?', 'Show me the data distribution', 'Any missing values?'].map((hint, i) => (
                  <button
                    key={i}
                    onClick={() => setInput(hint)}
                    style={{
                      padding: '8px 14px',
                      background: 'rgba(139,92,246,0.08)',
                      border: '1px solid rgba(139,92,246,0.22)',
                      borderRadius: 'var(--radius-pill)',
                      color: 'var(--primary-light)',
                      fontSize: 12,
                      fontFamily: 'var(--font-body)',
                      cursor: 'pointer',
                      transition: 'var(--transition)',
                    }}
                  >
                    {hint}
                  </button>
                ))}
              </div>
            )}
          </div>
        ) : (
          <>
            {messages.map((msg) => <MessageBubble key={msg.id} msg={msg} />)}
            {isLoading && (
              <div className="message-row">
                <div className="msg-avatar ai">
                  <Sparkles size={16} color="#fff" />
                </div>
                <div className="ai-card" style={{ padding: '16px 20px' }}>
                  <div className="ai-typing">
                    <span/><span/><span/>
                  </div>
                </div>
              </div>
            )}
          </>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div
        className="glass-card chat-input-area"
        style={{
          position: 'sticky',
          bottom: 20,
          margin: '0 24px',
          borderRadius: 20,
          boxShadow: '0 12px 48px rgba(0,0,0,0.6), 0 0 0 1px rgba(139,92,246,0.08)',
        }}
      >
        <div className="chat-input-row">
          <textarea
            id="chat-input"
            ref={textareaRef}
            className="chat-input"
            rows={1}
            placeholder={
              datasetId
                ? 'Ask anything about your data… (↵ send, ⇧↵ newline)'
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
            <Send size={17} />
          </button>
        </div>
      </div>
    </>
  );
}
