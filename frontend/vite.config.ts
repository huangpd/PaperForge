import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev: the console talks to the real API through this proxy (no mock data anywhere).
export default defineConfig({
  plugins: [react()],
  server: { port: 5173, proxy: { "/api": "http://127.0.0.1:8000" } },
  build: { chunkSizeWarningLimit: 1200 },
});
