# Maintainer response template

> Used verbatim (with per-case detail substituted) when a maintainer pushes back on a flagged-status in the Slack thread or as a GitHub issue. Do not improvise.

## Principles

1. **Thank them first.** They took the time to tell you.
2. **Never argue about whether their project is "actually" abandoned.** That is not what the label means.
3. **Cite the evidence in the current snapshot**, not opinion.
4. **Offer to adjust**: reduce confidence, re-score, or exclude via a reviewable process.
5. **Own false positives publicly** if warranted — this earns votes rather than losing them.

## Template

> Hi @maintainer — thanks for flagging this. A few clarifications:
>
> The Bus Factor doesn't claim `<package>` is abandoned, unsafe, or poorly maintained. It surfaces **public-data signals** about continuity risk — specifically: `<evidence 1>`, `<evidence 2>`, and `<evidence 3>` — weighted against `<package>`'s ecosystem importance. The scoring is ecosystem-relative percentiles, conservative thresholds, and requires **two independent signals ≥ 40** before a package can be flagged. Maintainers are never named individually.
>
> That said, if the signals don't match what's actually happening — for example, `<package>` is deliberately in a stable/low-churn phase, or the repo we mapped to isn't the canonical one, or we missed an active release branch — we want to know. Two ways to fix this:
>
> 1. **Repo mapping issue**: open an issue with the repo you'd expect us to track, and we'll re-run mapping and publish the corrected snapshot next week.
> 2. **Signal issue**: open an issue with the specific evidence row (package, snapshot_week, evidence type) and we'll review. If we got it wrong, we bump `methodology_version` and retract in the next weekly report.
>
> Everything is in one public Bruin pipeline — you can reproduce it locally in ~10 minutes (`bruin run -e fixture`). Methodology page: `<link>`.

## Hold-the-launch trigger

If before posting you find a sensitive false positive (e.g., a project that just had a tagged 1.0 release after a long quiet period, or a maintainer who has publicly explained a hiatus), **hold the Slack post**, fix the evidence or exclusion, and relaunch once the known-state fixture agrees.
