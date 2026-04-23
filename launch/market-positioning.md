# How The Bus Factor differs from SCA and related tools

> This table powers the README "How this differs from SCA tools" section and the LinkedIn comparison paragraph. Rows are derived from `docs/init.md` §"Competitive positioning".

| Tool category | Examples | What they do | Relationship to The Bus Factor |
| --- | --- | --- | --- |
| Commercial SCA / OSS risk platforms | Snyk, Sonatype, Endor Labs | Scan an org's apps/repos; inventory dependencies; prioritize vulnerabilities, license, and risk; support remediation. | Closest broad category, but not the same deliverable. The Bus Factor is a public, reproducible dataset — not an enterprise scanner. |
| Supply-chain attack / malicious-package detection | Socket | Detect malicious packages, typosquats, risky install behavior, unstable ownership. | Inspirational for package-level heuristics; The Bus Factor focuses on continuity fragility weighted by systemic importance, not malware. |
| Repository security posture checks | OpenSSF Scorecard | Automated repo posture and dev-practice checks. | Complementary input signal — not a substitute for the pipeline or weekly report. |
| The Bus Factor | (this project) | Combines dependency reach, downloads, release/commit activity, issue responsiveness, contributor concentration, and Scorecard signals into a weekly "importance × fragility" snapshot for npm and PyPI. | Public-interest data product + competition showcase. |

## One-paragraph framing

The Bus Factor is a **public, reproducible "importance × continuity fragility" dataset for widely used packages**, built with Bruin and explained through a static showcase plus Bruin AI Data Analyst examples. It is **not** a replacement for commercial SCA, a malware detector, a dependency scanner for your private app, or a definitive abandoned-package classifier. The fact that related tools exist is a feature for the competition story: the professional market agrees open-source risk matters, and The Bus Factor answers a different public question — *which widely depended-on packages look structurally fragile this week, and what evidence supports that?*
