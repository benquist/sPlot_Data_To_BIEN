# splot-open-data Chat Provenance Log

Tracks prompts that create or modify code, scripts, or outputs in splot-open-data.

## Entries

5. Date: 2026-06-30
Prompt: Fix the underlying GVS relay timeout behavior for batches like 299 and 300 rather than only surfacing the stall.
Source session: current workspace session
Outcome: Replayed failed GVS batch 299 against the live relay and verified size-dependent behavior: 50-row requests timed out while 25-row and smaller requests succeeded. Updated R/02_run_bien_loader_pipeline.R to recursively split retryable timeout-sized GVS batches into smaller sub-batches, retained failed-batch writeback for non-recoverable cases, and documented the adaptive split behavior in README.md.

4. Date: 2026-06-30
Prompt: Audit why GVS checkpoint counts are not advancing even though batches are still being attempted; inspect failed-batch artifacts for 299 and 300; harden launcher and monitoring so stalled checkpoints are flagged automatically.
Source session: current workspace session
Outcome: Audited live GVS state and confirmed processed counts were flat while batch attempts continued because checkpoint timestamps were refreshing without successful progress; inspected failed batch payloads for gvs_failed_batch_0299.tsv and gvs_failed_batch_0300.tsv; updated R/03_live_progress_dashboard.R and R/04_terminal_status_line.R to flag stalled services from progress-history age; hardened scripts/restart_monitoring_jobs.sh with --restart and --stall_minutes support; updated README.md and validated live STALLED output for GVS and NSR.

2. Date: 2026-04-28
Prompt: Make a minimal edit in R/01_build_bien_staging.R to keep deterministic duplicate_source_reason precedence while prioritizing overlap sources in this order: SALVIAS, VegBank, CVS, then CTFS, FIA, gillespie, TEAM; update nearby comment to state this policy; do not change matching patterns or unrelated logic.
Source session: current workspace session
Outcome: Updated only duplicate_source_reason precedence and adjacent policy comment in R/01_build_bien_staging.R; matching patterns and duplicate flag logic unchanged.

1. Date: 2026-04-28
Prompt: Implement a non-interactive R script to run splot staging through BIEN Data Loader services (TNRS, GNRS, GVS, NSR) with batching, retries, checkpoints, and validated output writeback.
Source session: current workspace session
Outcome: Added R/02_run_bien_loader_pipeline.R with strict service order, per-service validation outputs, resume/checkpoint support, failed-batch capture, and validated staging export.
