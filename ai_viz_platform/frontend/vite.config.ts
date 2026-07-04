import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const backendTarget = process.env.VITE_BACKEND_TARGET ?? 'http://127.0.0.1:8000'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/health': {
        target: backendTarget,
        changeOrigin: true,
      },
      '/ws': {
        target: backendTarget,
        ws: true,
        changeOrigin: true,
      },
      // Not used by the dashboard itself, but lets tools (and the bridge,
      // if pointed at the dev server) reach the backend through one origin.
      '/ingest': {
        target: backendTarget,
        changeOrigin: true,
      },
    },
  },
})
