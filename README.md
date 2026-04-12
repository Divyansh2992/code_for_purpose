# 🧠 DataLens — AI-Powered CSV Query Engine

> Upload a CSV, ask questions in plain English, get SQL-backed answers with charts, data health, and preprocessing transparency.

---
🌐 **Live Demo:** https://code-for-purpose-ynou.onrender.com/

---
## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Node.js 18+
- A **Groq API key** from [console.groq.com](https://console.groq.com)

### 1. Backend Setup

```bash
cd backend

# Copy env file and add your Groq key
copy .env.example .env
# Edit .env → set GROQ_API_KEY=your_actual_key_here

# Install dependencies
pip install -r requirements.txt

# Start backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Backend runs at → `http://localhost:8000`  
Swagger docs → `http://localhost:8000/docs`

### 2. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start frontend
npm run dev
```

Frontend runs at → `http://localhost:5173`

---
### 3. Docker setup 
```bash
docker compose up -build
```

## 🏗️ Project Structure

```
natwest hackathon/
├── backend/
│   ├── main.py                   # FastAPI app entry
│   ├── state.py                  # In-memory dataset/session registry
│   ├── requirements.txt
│   ├── .env                      # GROQ_API_KEY (do not commit)
│   ├── uploads/                  # Saved CSV files
│   ├── models/
│   │   └── schemas.py            # Pydantic request/response models
│   ├── routers/
│   │   ├── upload.py             # POST /upload
│   │   └── query.py              # POST /query
│   └── services/
│       ├── csv_analyzer.py       # DuckDB schema + stats extraction
│       ├── llm_service.py        # Groq: NL→SQL + explanation
│       ├── query_engine.py       # DuckDB SQL execution
│       ├── preprocessing.py      # Smart Mode: imputation + outlier detection
│       └── data_health.py        # Health metrics + confidence score
│
└── frontend/
    ├── index.html
    ├── vite.config.js            # Proxy → localhost:8000
    └── src/
        ├── main.jsx
        ├── App.jsx               # Root: sidebar + chat layout
        ├── index.css             # Glassmorphism design system
        ├── api/client.js         # fetch wrappers for /upload and /query
        ├── hooks/useChat.js      # Chat state + session memory
        └── components/
            ├── UploadPanel.jsx   # Drag-drop CSV + schema viewer
            ├── ModeToggle.jsx    # Raw / Smart toggle
            ├── ChatWindow.jsx    # Scrollable messages + input bar
            ├── MessageBubble.jsx # User & AI response cards
            ├── DataHealthPanel.jsx # Missing%, outliers, confidence
            ├── ChartRenderer.jsx # Recharts bar/line auto-chart
            ├── ResultTable.jsx   # Scrollable data table
            └── SuggestedQuestions.jsx # LLM-generated question list
```

---

## 🧩 Architecture

```
User Query
    │
    ▼
LLM (Groq) — NL → SQL
(Schema + 5 sample rows sent — NEVER full dataset)
    │
    ▼
DuckDB — Execute SQL
    │
    ├── Raw Mode  → Direct execution on original data
    │                Note: "Results based on raw data"
    │
    └── Smart Mode → Preprocessing first:
                      • Null detection + imputation (mean/median/mode)
                      • IQR outlier detection
                      • Transparency log generated
    │
    ▼
Data Health Panel
    • Missing value %
    • Outlier count
    • Rows used
    • Confidence score (0-100)
    │
    ▼
LLM (Groq) — Explanation + Insights + "Why?"
    │
    ▼
Frontend: Chat + Chart + Health Panel + Preprocessing Log
```

---

## 🔌 API Reference

### `POST /upload`
Upload a CSV file. Returns schema, statistics, and LLM-suggested questions.

**Request:** `multipart/form-data` — `file: <csv>`

**Response:**
```json
{
  "dataset_id": "uuid",
  "filename": "sales.csv",
  "row_count": 5000,
  "columns": [
    { "name": "revenue", "type": "DOUBLE", "null_pct": 8.2, "mean": 12340.5, "min": 0, "max": 99000 }
  ],
  "sample": [...],
  "suggested_questions": ["What is total revenue by region?", ...]
}
```

### `POST /query`
Ask a natural language question about an uploaded dataset.

**Request:**
```json
{
  "dataset_id": "uuid",
  "question": "What is the average revenue by category?",
  "mode": "smart",
  "session_id": "optional-uuid-for-context-memory"
}
```

**Response:**
```json
{
  "sql": "SELECT category, AVG(revenue) FROM data GROUP BY category",
  "result": [...],
  "columns": ["category", "avg(revenue)"],
  "explanation": "Electronics leads with $45k average revenue...",
  "insights": ["Electronics: +22% vs avg", "Books: lowest performer"],
  "chart_type": "bar",
  "chart_x": "category",
  "chart_y": ["avg(revenue)"],
  "data_health": { "missing_pct": 8.2, "outliers": 3, "rows_used": 4960, "confidence": 89.0 },
  "preprocessing_log": ["✅ 'revenue': 8.2% nulls filled using median (skewed distribution)"],
  "mode": "smart",
  "why_analysis": "The revenue gap likely reflects seasonal demand patterns..."
}
```

---

## ✨ Features

| Feature | Status |
|---|---|
| CSV drag-and-drop upload | ✅ |
| Schema extraction (types, null%, mean/min/max) | ✅ |
| Natural language → SQL (Groq LLM) | ✅ |
| DuckDB query execution | ✅ |
| Raw Mode (no preprocessing) | ✅ |
| Smart Mode (auto imputation + outlier detection) | ✅ |
| Skewness-aware imputation (mean vs median) | ✅ |
| IQR outlier detection | ✅ |
| Preprocessing transparency log | ✅ |
| Data Health Panel (missing%, outliers, confidence) | ✅ |
| Plain English explanation | ✅ |
| Bullet insights | ✅ |
| "Why did this happen?" analysis | ✅ |
| Auto bar/line chart (Recharts) | ✅ |
| Suggested questions (LLM-generated) | ✅ |
| Session-based context memory (follow-ups) | ✅ |
| Dark glassmorphism UI | ✅ |
| Privacy-safe (only schema+5 rows to LLM) | ✅ |

---
---

## 📸 UI Snapshots

### 🏠 Landing Page
<img width="1848" height="860" alt="Screenshot 2026-04-12 235438" src="https://github.com/user-attachments/assets/4f1a6a0a-5726-4c2c-b7c0-7abcf05fb8c7" />


> Upload your CSV and get started with AI-powered querying.

---

### 💬 Chat Interface
<img width="1919" height="864" alt="Screenshot 2026-04-12 235740" src="https://github.com/user-attachments/assets/ef62e775-d4de-420f-be29-76d8a84299d5" />


> Ask questions in plain English and get SQL-backed answers with explanations.

---

### 📊 Visualization Dashboard
![WhatsApp Image 2026-04-13 at 12 05 31 AM](https://github.com/user-attachments/assets/a4e10b34-a33b-4976-a0c0-b8f7d633f024)


> Automatically generated charts and insights from your data.

---

### 🧠 Data Health Panel
<img width="211" height="169" alt="Screenshot 2026-04-13 000829" src="https://github.com/user-attachments/assets/18e694d1-ae0c-43a7-8816-2e0e256efc20" />


> View missing values, outliers, and confidence score.

---

### 🔍 Query + Insights Output
![WhatsApp Image 2026-04-13 at 12 05 15 AM](https://github.com/user-attachments/assets/d4d0af50-3f7b-4537-b557-00595a8c9804)


> Get explanations, insights, and “why” analysis for your queries.

---
## 🔒 Privacy & Safety

- **Only schema + 5 sample rows** are sent to the Groq LLM
- Full dataset stays local, queried by DuckDB
- All SQL queries are validated to be `SELECT`-only
- No data leaves your machine except the schema summary

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.11 + FastAPI |
| Data Engine | DuckDB 0.10 |
| LLM | Groq (llama3-70b-8192) |
| Frontend | React 19 + Vite 8 |
| Charts | Recharts |
| Icons | Lucide React |
| Styling | Vanilla CSS (glassmorphism) |
