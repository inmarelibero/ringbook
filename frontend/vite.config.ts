import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

const engine = process.env.RINGBOOK_API ?? "http://127.0.0.1:8080";

export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      "/api": engine,
      "/ws": { target: engine, ws: true },
    },
  },
});
