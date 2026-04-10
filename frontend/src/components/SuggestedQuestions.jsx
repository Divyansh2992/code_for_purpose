import { Lightbulb } from 'lucide-react';

export default function SuggestedQuestions({ questions, onSelect }) {
  if (!questions || questions.length === 0) return null;

  return (
    <div>
      <p className="section-label" style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
        <Lightbulb size={11} />
        Suggested Questions
      </p>
      <div className="suggested-questions">
        {questions.map((q, i) => (
          <button
            key={i}
            id={`suggestion-${i}`}
            className="suggestion-btn"
            onClick={() => onSelect(q)}
          >
            {q}
          </button>
        ))}
      </div>
    </div>
  );
}
