// Flat-config ESLint. Real rulesets are added when web/ grows real components.
import js from "@eslint/js";
import tseslint from "typescript-eslint";
import astro from "eslint-plugin-astro";

export default [
  { ignores: ["**/dist/**", "**/.astro/**", "**/node_modules/**", "**/public-data/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  ...astro.configs.recommended,
];
