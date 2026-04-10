"""
FastAPI application entry point.
Run with: uvicorn main:app --reload --host 0.0.0.0 --port 8000
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

from routers import upload, query

app = FastAPI(
    title="Talk to Data API",
    description="AI-powered CSV querying system using DuckDB + Groq LLM",
    version="1.0.0",
)

# Allow frontend dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router, tags=["Upload"])
app.include_router(query.router, tags=["Query"])


@app.get("/", tags=["Health"])
async def root():
    return {"status": "ok", "message": "Talk to Data API is running."}


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy"}
