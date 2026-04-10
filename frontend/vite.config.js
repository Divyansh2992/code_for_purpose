import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')

  // Only proxy in local dev (when VITE_API_URL is not set to a remote URL)
  const apiUrl = env.VITE_API_URL || ''
  const isRemote = apiUrl.startsWith('http')

  return {
    plugins: [react()],
    server: {
      port: 5173,
      // Proxy only applies when running locally against localhost backend
      ...(isRemote ? {} : {
        proxy: {
          '/upload': 'http://localhost:8000',
          '/query':  'http://localhost:8000',
          '/health': 'http://localhost:8000',
        },
      }),
    },
  }
})
