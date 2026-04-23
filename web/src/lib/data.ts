import {
  analysisSchema,
  coverageSchema,
  leaderboardSchema,
  metadataSchema,
  packagesSchema,
  positioningSchema,
  sourcesSchema,
  weeklySchema,
  type Analysis,
  type Coverage,
  type Leaderboard,
  type Metadata,
  type Packages,
  type Positioning,
  type Sources,
  type Weekly,
} from "./schemas";

/**
 * The data layer is the only place we touch ../../public-data/*.json.
 * Every loader parses the JSON through its Zod schema so malformed exports
 * fail the Astro build instead of shipping broken pages.
 *
 * Each loader returns `null` when the file is missing. Pages MUST render
 * an honest empty state in that case per web/AGENTS.md.
 */

type JsonRecord = unknown;

async function loadJson<T>(
  importPromise: Promise<{ default: JsonRecord }>,
  schema: { parse: (data: unknown) => T },
  label: string,
): Promise<T | null> {
  try {
    const module = await importPromise;
    return schema.parse(module.default);
  } catch (error) {
    if (
      error instanceof Error &&
      (error.message.includes("Failed to resolve") ||
        error.message.includes("Cannot find module") ||
        error.message.includes("ENOENT"))
    ) {
      return null;
    }
    console.error(`[data] Failed to load/validate ${label}:`, error);
    throw error;
  }
}

export async function loadMetadata(): Promise<Metadata | null> {
  return loadJson(
    import("@public-data/metadata.json"),
    metadataSchema,
    "metadata.json",
  );
}

export async function loadLeaderboard(): Promise<Leaderboard | null> {
  return loadJson(
    import("@public-data/leaderboard.json"),
    leaderboardSchema,
    "leaderboard.json",
  );
}

export async function loadPackages(): Promise<Packages | null> {
  return loadJson(
    import("@public-data/packages.json"),
    packagesSchema,
    "packages.json",
  );
}

export async function loadWeekly(): Promise<Weekly | null> {
  return loadJson(
    import("@public-data/weekly.json"),
    weeklySchema,
    "weekly.json",
  );
}

export async function loadCoverage(): Promise<Coverage | null> {
  return loadJson(
    import("@public-data/coverage.json"),
    coverageSchema,
    "coverage.json",
  );
}

export async function loadSources(): Promise<Sources | null> {
  return loadJson(
    import("@public-data/sources.json"),
    sourcesSchema,
    "sources.json",
  );
}

export async function loadAnalysis(): Promise<Analysis | null> {
  return loadJson(
    import("@public-data/analysis.json"),
    analysisSchema,
    "analysis.json",
  );
}

export async function loadPositioning(): Promise<Positioning | null> {
  return loadJson(
    import("@public-data/positioning.json"),
    positioningSchema,
    "positioning.json",
  );
}

export function severityBadgeClass(tier: string): string {
  switch (tier) {
    case "Critical":
      return "badge badge--critical";
    case "High":
      return "badge badge--high";
    case "Elevated":
      return "badge badge--elevated";
    case "Watch":
      return "badge badge--watch";
    default:
      return "badge badge--stable";
  }
}
