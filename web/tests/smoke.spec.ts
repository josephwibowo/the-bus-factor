import { expect, test } from "@playwright/test";

test.describe("static site smoke", () => {
  test("leaderboard renders and is navigable", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { name: /leaderboard/i })).toBeVisible();
    // Every top-level nav link should resolve.
    for (const label of ["Leaderboard", "Weekly", "Analysis", "Methodology", "Positioning"]) {
      await expect(page.getByRole("link", { name: label, exact: false }).first()).toBeVisible();
    }
  });

  test("methodology page exposes severity bands", async ({ page }) => {
    await page.goto("/methodology/");
    await expect(page.getByRole("heading", { name: /methodology/i })).toBeVisible();
    await expect(page.getByText(/risk score ≥ 75/i)).toBeVisible();
  });

  test("weekly page mentions snapshot week", async ({ page }) => {
    await page.goto("/weekly/");
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  });
});
