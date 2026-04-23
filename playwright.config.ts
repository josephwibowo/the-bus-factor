import { defineConfig, devices } from "@playwright/test";

// One desktop + one mobile smoke pass over the built static site.
export default defineConfig({
  testDir: "./web/tests",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: "http://127.0.0.1:4321/the-bus-factor",
    trace: "on-first-retry",
  },
  projects: [
    { name: "desktop-chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "mobile-safari", use: { ...devices["iPhone 14"] } },
  ],
  webServer: {
    command: "pnpm --filter ./web preview --port 4321 --host 127.0.0.1",
    url: "http://127.0.0.1:4321",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
