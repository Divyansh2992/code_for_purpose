"""
LLM Service — Groq-powered NL→SQL generation and result explanation.
Uses llama3-70b-8192 for fast, accurate responses.
Schema + sample (≤5 rows) are sent — never the full dataset.
"""
import os
import re
import json
from typing import List, Dict, Any, Tuple

from groq import Groq
from dotenv import load_dotenv

load_dotenv()

_client = Groq(api_key=os.environ.get("GROQ_API_KEY", ""))
MODEL = "llama-3.3-70b-versatile"


# ── SQL Generation ─────────────────────────────────────────────────────────────

def generate_sql(
    schema: List[Dict[str, Any]],
    sample: List[Dict[str, Any]],
    question: str,
    history: List[Dict[str, str]] = [],
) -> str:
    """
    Convert a natural language question to a DuckDB SQL query.
    The table is always named "data".
    Returns a clean SQL string (no markdown, no explanation).
    """
    schema_lines = "\n".join(
        f"  - {c['name']} ({c['type']}, null%={c.get('null_pct', 0):.1f}"
        + (f", mean={c['mean']:.2f}" if c.get("mean") is not None else "")
        + ")"
        for c in schema
    )
    sample_str = json.dumps(_serialise(sample[:5]), indent=2)

    system_prompt = (
        "You are an expert DuckDB SQL analyst.\n"
        "RULES:\n"
        "1. The table name is ALWAYS 'data' — never use any other table name.\n"
        "2. Return ONLY the SQL query — no markdown, no explanation, no preamble.\n"
        "3. Always quote column names with double-quotes to handle spaces/special chars.\n"
        "4. Use DuckDB-compatible syntax (e.g. PERCENTILE_CONT for percentiles).\n"
        "5. For comparative questions (e.g. 'versus', 'compare'), ensure you GROUP BY the relevant column to show a comparison.\n"
        "6. If the user asks for a plot, graph, or chart, ensure the query returns at least one categorical/date column and one numeric column for visualization.\n"
        "7. Keep the query concise and efficient.\n\n"
        f"Table schema:\n{schema_lines}\n\n"
        f"Sample rows (first 5):\n{sample_str}"
    )

    messages = [{"role": "system", "content": system_prompt}]

    # Inject conversation history for follow-up context (last 6 turns = 3 exchanges)
    for h in history[-6:]:
        messages.append(h)

    messages.append({"role": "user", "content": f"Question: {question}"})

    response = _client.chat.completions.create(
        model=MODEL,
        messages=messages,
        temperature=0.05,
        max_tokens=600,
    )

    raw = response.choices[0].message.content.strip()
    return _clean_sql(raw)


# ── Explanation & Insights ─────────────────────────────────────────────────────

def explain_result(
    question: str,
    sql: str,
    result: List[Dict[str, Any]],
    columns: List[str],
) -> Dict[str, Any]:
    """
    Generate a plain-English explanation, bullet insights, and 'Why did this happen?'
    from the query result.
    """
    result_preview = json.dumps(_serialise(result[:15]), indent=2)

    prompt = (
        f'The user asked: "{question}"\n'
        f"SQL executed: {sql}\n"
        f"Result (up to 15 rows):\n{result_preview}\n\n"
        "Provide the following — use EXACTLY the section headers shown:\n\n"
        "EXPLANATION: <2-3 sentences in plain English summarising the key finding>\n\n"
        "INSIGHTS:\n"
        "• <insight 1>\n"
        "• <insight 2>\n"
        "• <insight 3>\n"
        "• <insight 4 if applicable>\n\n"
        "WHY: <1-2 sentences on possible root causes or driving factors>"
    )

    response = _client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a concise, insightful data analyst. "
                    "Always respond in the exact format requested. "
                    "Be specific and data-driven — reference actual numbers from the result."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
        max_tokens=800,
    )

    content = response.choices[0].message.content
    return _parse_explanation(content)


# ── Auto-Suggested Questions ───────────────────────────────────────────────────

def suggest_questions(
    schema: List[Dict[str, Any]],
    sample: List[Dict[str, Any]],
) -> List[str]:
    """Generate 4 interesting questions the user might want to ask about their dataset."""
    schema_lines = "\n".join(f"  - {c['name']} ({c['type']})" for c in schema)
    sample_str = json.dumps(_serialise(sample[:3]), indent=2)

    response = _client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a data analyst suggesting insightful questions about a dataset.",
            },
            {
                "role": "user",
                "content": (
                    f"Schema:\n{schema_lines}\n\n"
                    f"Sample:\n{sample_str}\n\n"
                    "Suggest exactly 4 interesting, specific questions a business analyst "
                    "would want to ask about this dataset.\n"
                    "Return ONLY a JSON array of 4 strings. No other text."
                ),
            },
        ],
        temperature=0.5,
        max_tokens=350,
    )

    try:
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"```json\s*", "", raw)
        raw = re.sub(r"```\s*", "", raw)
        questions = json.loads(raw)
        return [str(q) for q in questions[:4]]
    except Exception:
        return [
            "What are the top 5 records by value?",
            "Show average values grouped by category.",
            "Are there any trends over time?",
            "Which group has the highest total?",
        ]


# ── Private helpers ────────────────────────────────────────────────────────────

def _clean_sql(raw: str) -> str:
    """Strip markdown fences and leading/trailing whitespace."""
    raw = re.sub(r"```sql\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"```\s*", "", raw)
    # Remove any lines that are clearly not SQL (e.g. explanatory sentences)
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    return "\n".join(lines).strip()


def _parse_explanation(content: str) -> Dict[str, Any]:
    """Parse the structured LLM response into explanation, insights, and why."""
    explanation = ""
    insights: List[str] = []
    why = ""
    current = None

    lines = content.splitlines()
    for i, line in enumerate(lines):
        line = line.strip()
        upper = line.upper()
        
        if upper.startswith("EXPLANATION:"):
            explanation = line[len("EXPLANATION:"):].strip()
            current = "explanation"
        elif upper.startswith("INSIGHTS:"):
            current = "insights"
        elif upper.startswith("WHY:"):
            why = line[len("WHY:"):].strip()
            current = "why"
        elif (line.startswith("•") or line.startswith("-") or (re.match(r"^\d+\.", line))) and current == "insights":
            txt = re.sub(r"^[•\-\d\.]+\s*", "", line).strip()
            if txt:
                insights.append(txt)
        elif current == "explanation" and line and not any(h in upper for h in ["INSIGHTS:", "WHY:"]):
            explanation = (explanation + " " + line).strip()
        elif current == "why" and line:
            why = (why + " " + line).strip()

    # Fallback: if no headers found, treat first 2 lines as explanation
    if not explanation and lines:
        explanation = lines[0].strip()
        if len(lines) > 1 and not lines[1].strip().startswith(("-", "•")):
            explanation += " " + lines[1].strip()

    return {
        "explanation": explanation or "Query executed successfully.",
        "insights": insights[:5],
        "why_analysis": why,
    }


def _serialise(obj: Any) -> Any:
    """Make Python objects JSON-serialisable (handle NaN, timestamps, etc.)."""
    import math
    import datetime
    if isinstance(obj, list):
        return [_serialise(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return str(obj)
    return obj
