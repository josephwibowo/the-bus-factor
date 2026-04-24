import { z } from "zod";

const ecosystemEnum = z.enum(["npm", "pypi"]);
const severityEnum = z.enum(["Stable", "Watch", "Elevated", "High", "Critical"]);
const confidenceEnum = z.enum(["low", "medium", "high"]);
const sourceStatusEnum = z.enum(["ok", "degraded", "failed"]);

export const sourceStatusSchema = z
  .object({
    source_name: z.string(),
    status: sourceStatusEnum,
    last_success_at: z.string().nullable(),
    stale: z.boolean(),
    failure_count: z.number().int().nonnegative(),
    note: z.string().nullable().optional(),
  })
  .passthrough();
export type SourceStatus = z.infer<typeof sourceStatusSchema>;

export const metadataSchema = z
  .object({
    snapshot_week: z.string(),
    snapshot_week_label: z.string(),
    methodology_version: z.string(),
    generated_at: z.string(),
    ecosystems_covered: z.array(ecosystemEnum),
    package_counts: z.record(z.number().int().nonnegative()),
    sources: z.array(sourceStatusSchema),
    data_license: z.string(),
    notes: z.array(z.string()),
  })
  .passthrough();
export type Metadata = z.infer<typeof metadataSchema>;

export const leaderboardEntrySchema = z
  .object({
    ecosystem: ecosystemEnum,
    package_name: z.string(),
    slug: z.string(),
    rank_within_ecosystem: z.number().int(),
    risk_score: z.number(),
    severity_tier: severityEnum,
    flagged: z.boolean(),
    importance_score: z.number(),
    fragility_score: z.number(),
    confidence: confidenceEnum,
    primary_finding: z.string(),
  })
  .strict();
export type LeaderboardEntry = z.infer<typeof leaderboardEntrySchema>;

export const leaderboardSchema = z
  .object({
    snapshot_week: z.string(),
    methodology_version: z.string(),
    entries: z.array(leaderboardEntrySchema),
  })
  .strict();
export type Leaderboard = z.infer<typeof leaderboardSchema>;

export const fragilitySignalSchema = z
  .object({
    name: z.enum([
      "release_recency",
      "commit_recency",
      "release_cadence_decay",
      "issue_responsiveness",
      "all_time_contribution_concentration",
      "recent_commit_concentration_365d",
      "openssf_scorecard",
    ]),
    contribution: z.number(),
    evidence: z.string(),
  })
  .strict();
export type FragilitySignal = z.infer<typeof fragilitySignalSchema>;

export const mappingConfidenceSchema = z
  .object({
    points: z.number().int(),
    bucket: confidenceEnum,
    rationale: z.array(z.string()),
  })
  .strict();

export const packageDetailSchema = z
  .object({
    ecosystem: ecosystemEnum,
    package_name: z.string(),
    slug: z.string(),
    snapshot_week: z.string(),
    methodology_version: z.string(),
    severity_tier: severityEnum,
    flagged: z.boolean(),
    risk_score: z.number(),
    importance_score: z.number(),
    fragility_score: z.number(),
    confidence: confidenceEnum,
    repository_url: z.string().nullable(),
    mapping_confidence: mappingConfidenceSchema,
    fragility_signals: z.array(fragilitySignalSchema),
    registry_url: z.string().nullable(),
    first_release_date: z.string(),
    latest_release_date: z.string(),
    last_commit_date: z.string().nullable(),
    is_archived: z.boolean(),
    is_deprecated: z.boolean(),
    exclusion_reason: z.string().nullable().optional(),
  })
  .strict();
export type PackageDetail = z.infer<typeof packageDetailSchema>;

export const packagesSchema = z
  .object({
    snapshot_week: z.string(),
    methodology_version: z.string(),
    entries: z.array(packageDetailSchema),
  })
  .strict();
export type Packages = z.infer<typeof packagesSchema>;

export const weeklyFindingSchema = z
  .object({
    rank: z.number().int(),
    ecosystem: ecosystemEnum,
    package_name: z.string(),
    slug: z.string(),
    severity_tier: severityEnum,
    risk_score: z.number(),
    primary_finding: z.string(),
  })
  .strict();

export const weeklySchema = z
  .object({
    headline: z
      .object({
        headline: z.string(),
        summary: z.string(),
        methodology_version: z.string(),
        snapshot_week: z.string(),
        ecosystem_breakdown: z.record(z.number().int().nonnegative()),
      })
      .strict(),
    findings: z.array(weeklyFindingSchema),
    zero_flagged_fallback_copy: z.string().nullable().optional(),
  })
  .strict();
export type Weekly = z.infer<typeof weeklySchema>;

export const coverageSchema = z
  .object({
    snapshot_week: z.string(),
    methodology_version: z.string(),
    rows: z.array(
      z
        .object({
          ecosystem: ecosystemEnum,
          tracked: z.number().int().nonnegative(),
          eligible: z.number().int().nonnegative(),
          flagged: z.number().int().nonnegative(),
          excluded_unmappable: z.number().int().nonnegative(),
          excluded_archived: z.number().int().nonnegative(),
          excluded_too_new: z.number().int().nonnegative(),
          excluded_stub_types: z.number().int().nonnegative(),
        })
        .strict(),
    ),
  })
  .strict();
export type Coverage = z.infer<typeof coverageSchema>;

export const sourcesSchema = z
  .object({
    snapshot_week: z.string(),
    sources: z.array(sourceStatusSchema),
  })
  .strict();
export type Sources = z.infer<typeof sourcesSchema>;

export const analysisExampleSchema = z
  .object({
    example_id: z.string(),
    prompt: z.string(),
    answer_summary: z.string(),
    screenshot_path: z.string(),
    dataset_version: z.string(),
    methodology_version: z.string(),
    capture_date: z.string(),
    capture_source: z.string(),
  })
  .strict();

export const analysisSchema = z
  .object({
    snapshot_week: z.string(),
    entries: z.array(analysisExampleSchema),
  })
  .strict();
export type Analysis = z.infer<typeof analysisSchema>;

export const positioningRowSchema = z
  .object({
    row_order: z.number().int(),
    category: z.string(),
    example_products: z.string(),
    primary_question_answered: z.string(),
    relationship_to_bus_factor: z.string(),
  })
  .strict();

export const positioningSchema = z
  .object({
    snapshot_week: z.string(),
    rows: z.array(positioningRowSchema),
  })
  .strict();
export type Positioning = z.infer<typeof positioningSchema>;
