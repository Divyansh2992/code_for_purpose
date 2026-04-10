@echo off
echo ==========================================
echo   Talk to Data - Starting all services
echo ==========================================

:: Check for .env
if not exist "backend\.env" (
    echo ERROR: backend\.env not found!
    echo Please copy backend\.env.example to backend\.env
    echo and set your GROQ_API_KEY.
    pause
    exit /b 1
)

echo [1/2] Starting FastAPI backend on port 8000...
start "Talk-to-Data Backend" cmd /k "cd backend && uvicorn main:app --reload --host 0.0.0.0 --port 8000"

timeout /t 3 /nobreak >nul

echo [2/2] Starting Vite frontend on port 5173...
start "Talk-to-Data Frontend" cmd /k "cd frontend && npm run dev"

timeout /t 4 /nobreak >nul

echo.
echo ==========================================
echo   Both servers started!
echo   Backend:  http://localhost:8000
echo   Frontend: http://localhost:5173
echo   API Docs: http://localhost:8000/docs
echo ==========================================

start http://localhost:5173
