const DEFAULT_PROD_API_URL = 'https://code-for-purpose-ynou.onrender.com';
const configuredBaseUrl = (import.meta.env.VITE_API_URL || '').trim();
const BASE = (configuredBaseUrl || (import.meta.env.PROD ? DEFAULT_PROD_API_URL : '')).replace(/\/$/, '');

export async function uploadCSV(file, onProgress) {
  const form = new FormData();
  form.append('file', file);

  const res = await fetch(`${BASE}/upload`, {
    method: 'POST',
    body: form,
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || 'Upload failed');
  }
  return res.json();
}

export async function sendQuery({ datasetId, question, mode, sessionId }) {
  const res = await fetch(`${BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      dataset_id: datasetId,
      question,
      mode,
      session_id: sessionId,
    }),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || 'Query failed');
  }
  return res.json();
}
