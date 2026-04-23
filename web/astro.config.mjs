import { defineConfig } from "astro/config";

// Static output only. No server renderers. No API routes.
// The site consumes precomputed JSON from ../public-data/.
export default defineConfig({
  output: "static",
  site: "https://the-bus-factor.github.io",
  trailingSlash: "ignore",
  build: {
    format: "directory",
  },
  vite: {
    resolve: {
      alias: {
        "@public-data": new URL("../public-data/", import.meta.url).pathname,
      },
    },
  },
});
