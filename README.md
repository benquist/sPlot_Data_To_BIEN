# sPlot to BIEN Database Loading Workflow

This repository contains a two-stage, service-backed pipeline that transforms sPlot Open data into a BIEN loader-ready staging table and then validates and enriches that table using BIEN relay services.

The workflow is intentionally explicit and operationally auditable:

1. Build BIEN staging table from raw sPlot archive.
2. Run staged records through TNRS, GNRS, GVS, and NSR in controlled API batches.
3. Persist intermediate service outputs and checkpoints for resume/restart.
4. Emit a final validated staging table for BIEN Data Loader ingestion.

## Repository Layout

- `R/`
  - `01_build_bien_staging.R`: Reads sPlot zip text files, applies base QA filters/flags, maps records to BIEN staging schema.
  - `02_run_bien_loader_pipeline.R`: Runs TNRS, GNRS, GVS, NSR batched validation and writes validated output.
- `data/`
  - Intended location for local data artifacts if you adapt paths.
- `output/`
  - Intended root for generated outputs.
- `output/validation/`
  - Validation service outputs, checkpoints, and failed batch queues.

## Architecture and Processing Model

### Stage 1: Staging Build (R/01_build_bien_staging.R)

Primary responsibilities:

1. Read two files from the sPlot zip archive:
   - `sPlotOpen_header(2).txt`
   - `sPlotOpen_DT(1).txt`
2. Select required columns only.
3. Inner join on `PlotObservationID`.
4. Apply row-level filters:
   - species must be non-empty and appear binomial (contains a space)
   - latitude and longitude must be present
5. Create QA flags:
   - `coord_uncertainty_flag` when location uncertainty exceeds 1000 m
   - `null_island_flag` when absolute latitude and longitude are both below 0.1
6. Map fields to BIEN-style staging columns.
7. Build unique `occurrenceID` and enforce uniqueness.
8. Write three output files:
   - full staging table
   - balanced subset based on `resample_1_consensus == TRUE`
   - unique submitted names queue for TNRS batching

Important implementation note:

- This script contains hard-coded absolute defaults for zip input and output directory in the original source code that was copied unchanged.
- If your local files differ, either:
  - place files at the expected absolute paths, or
  - adapt those constants in your own local branch.

### Stage 2: Validation and Enrichment Pipeline (R/02_run_bien_loader_pipeline.R)

Primary responsibilities:

1. Read staging table (`--input`).
2. Ensure required columns exist for downstream merges.
3. TNRS pass (name resolution):
   - queue unique submitted names
   - call TNRS in batches
   - persist `tnrs_results.tsv`
   - merge matched names and taxonomic metadata into staging
4. GNRS pass (geography resolution):
   - queue unique geography tuples
   - call GNRS in batches
   - persist `gnrs_results.tsv`
   - merge matched country/state/county values
5. GVS pass (geospatial validation):
   - queue unique coordinate pairs
   - call GVS in batches
   - persist `gvs_results.tsv`
   - merge centroid flags
6. NSR pass (native status resolution):
   - queue unique taxon plus location tuples
   - call NSR in batches
   - persist `nsr_results.tsv`
   - merge native status and introduced/cultivated outputs
7. Persist final validated staging table:
   - `splot_bien_staging_validated.tsv`
8. Persist checkpoints and failed batches for operational restart.

Service order is fixed and intentional:

1. TNRS resolves names before native status checks.
2. GNRS standardizes geography before NSR lookups.
3. GVS independently validates coordinates and centroid behavior.
4. NSR evaluates native/introduced/cultivated context with resolved taxon and geography.

## Prerequisites

### Software

1. R version:
   - R 4.1 or higher is recommended.
2. System tools:
   - `unzip` executable must be available on PATH for stage 1 streaming reads.
3. Network access:
   - outbound HTTPS access to relay endpoints used by stage 2.

### R packages

Install required packages:

```bash
Rscript -e "install.packages(c('data.table','lubridate','httr','jsonlite'), repos='https://cloud.r-project.org')"
```

Package usage by script:

- `01_build_bien_staging.R`: `data.table`, `lubridate`
- `02_run_bien_loader_pipeline.R`: `data.table`, `httr`, `jsonlite`

## Run Commands From Repository Root

Run all commands from:

```bash
cd /Users/brianjenquist/VSCode/sPlot_BIENdb_Loading
```

### 1) Build staging table

```bash
Rscript R/01_build_bien_staging.R
```

Default behavior:

- Reads zip from the script's hard-coded `ZIP` absolute path.
- Writes outputs to the script's hard-coded `OUTPUT_DIR` absolute path.

### 2) Run TNRS -> GNRS -> GVS -> NSR pipeline

Example using explicit paths in this repository:

```bash
Rscript R/02_run_bien_loader_pipeline.R \
  --input=output/splot_bien_staging_full.tsv \
  --output_dir=output \
  --validation_dir=output/validation \
  --tnrs_batch_size=500 \
  --gnrs_batch_size=1000 \
  --gvs_batch_size=2000 \
  --nsr_batch_size=500 \
  --max_retries=5 \
  --retry_base_seconds=5 \
  --timeout_seconds=120 \
  --resume=TRUE \
  --pause_seconds=0.25
```

## Full CLI Argument Documentation (R/02_run_bien_loader_pipeline.R)

All arguments are passed as `--key=value`.

- `--input`
  - Type: path string
  - Default: `/Users/brianjenquist/VSCode/splot-open-data/output/splot_bien_staging_full.tsv`
  - Meaning: input staging TSV to validate and enrich.

- `--output_dir`
  - Type: path string
  - Default: `/Users/brianjenquist/VSCode/splot-open-data/output`
  - Meaning: root output directory for validated table and validation artifacts.

- `--validation_dir`
  - Type: path string or empty
  - Default: `NA` in parser, then resolved to `<output_dir>/validation`
  - Meaning: directory that stores service result tables, checkpoints, failed batches.

- `--tnrs_batch_size`
  - Type: integer
  - Default: `500`
  - Meaning: number of unique names per TNRS request batch.

- `--gnrs_batch_size`
  - Type: integer
  - Default: `1000`
  - Meaning: number of unique geography tuples per GNRS request batch.

- `--gvs_batch_size`
  - Type: integer
  - Default: `2000`
  - Meaning: number of unique coordinate pairs per GVS request batch.

- `--nsr_batch_size`
  - Type: integer
  - Default: `500`
  - Meaning: number of unique taxon+location tuples per NSR request batch.

- `--max_retries`
  - Type: integer
  - Default: `5`
  - Meaning: max retries per batch for retryable failures (network, 429, 5xx).

- `--retry_base_seconds`
  - Type: numeric
  - Default: `5`
  - Meaning: exponential backoff base seconds.
  - Effective wait progression per attempt uses `base * 2^(attempt-1)`.

- `--timeout_seconds`
  - Type: integer
  - Default: `120`
  - Meaning: per-request timeout applied to service calls.

- `--resume`
  - Type: logical (`TRUE/FALSE`, also accepts yes/no style tokens)
  - Default: `TRUE`
  - Meaning:
    - `TRUE`: load existing `*_results.tsv`, skip already completed queue keys.
    - `FALSE`: process as fresh run using current queue state.

- `--pause_seconds`
  - Type: numeric
  - Default: `0.25`
  - Meaning: sleep between batches for service politeness and reduced burst pressure.

Parsing behavior details:

- Unknown `--flag` without `=` is interpreted as `TRUE` for that key.
- Numeric parsing falls back to script defaults if invalid values are supplied.
- Boolean parsing accepts common true/false tokens.

## Expected Output Files and Interpretation

### Stage 1 outputs

- `splot_bien_staging_full.tsv`
  - Full staging table after filters and mapping.
- `splot_bien_staging_balanced.tsv`
  - Subset where `resample_1_consensus == TRUE`.
- `splot_unique_names_for_tnrs.tsv`
  - Unique submitted names plus count per name for TNRS queueing.

### Stage 2 outputs

Main output:

- `splot_bien_staging_validated.tsv`
  - Final enriched staging table after TNRS, GNRS, GVS, NSR merges.

Validation directory outputs (`output/validation` by default):

- `tnrs_results.tsv`
  - Raw or flattened TNRS batch responses keyed by submitted name.
- `gnrs_results.tsv`
  - Raw or flattened GNRS batch responses keyed by geography tuple.
- `gvs_results.tsv`
  - Raw or flattened GVS batch responses keyed by submitted coordinate pair.
- `nsr_results.tsv`
  - Raw or flattened NSR batch responses keyed by taxon and geography.

Checkpoints:

- `tnrs_checkpoint.tsv`
- `gnrs_checkpoint.tsv`
- `gvs_checkpoint.tsv`
- `nsr_checkpoint.tsv`

Checkpoint meaning:

- Tracks processed count, total queue size, and remaining count with timestamp.

Failed batch queues:

- `output/validation/failed_batches/tnrs_failed_batch_XXXX.tsv`
- `output/validation/failed_batches/gnrs_failed_batch_XXXX.tsv`
- `output/validation/failed_batches/gvs_failed_batch_XXXX.tsv`
- `output/validation/failed_batches/nsr_failed_batch_XXXX.tsv`

Failed batch meaning:

- Captures queue items that were not resolved successfully after retry limits.
- Supports targeted rerun or incident triage.

## Troubleshooting

### HTTP 429 (rate limit)

Symptoms:

- Logs show `HTTP 429` with retry lines.

Actions:

1. Reduce pressure:
   - lower batch sizes
   - increase `pause_seconds`
2. Keep `resume=TRUE` to avoid reprocessing completed keys.
3. Re-run command after cooldown.

Recommended conservative settings:

```bash
Rscript R/02_run_bien_loader_pipeline.R \
  --input=output/splot_bien_staging_full.tsv \
  --output_dir=output \
  --validation_dir=output/validation \
  --tnrs_batch_size=200 \
  --gnrs_batch_size=400 \
  --gvs_batch_size=800 \
  --nsr_batch_size=200 \
  --pause_seconds=1.5 \
  --resume=TRUE
```

### Service timeout or transient network failure

Symptoms:

- Network errors, timeout messages, 5xx responses.

Actions:

1. Increase `timeout_seconds`.
2. Increase `max_retries` and optionally `retry_base_seconds`.
3. Re-run with `resume=TRUE`.
4. Inspect failed batches under `output/validation/failed_batches`.

### Missing columns in input staging table

Symptoms:

- Downstream merges or queue construction produce sparse outputs.

Notes:

- Script attempts to add many expected columns when absent.
- However, core fields still need meaningful values for best results:
  - species name fields for TNRS and NSR
  - coordinate fields for GVS
  - geography fields for GNRS and NSR

Actions:

1. Confirm stage 1 output integrity.
2. Confirm delimiter is tab and file is not corrupted.
3. Validate required columns exist and contain non-empty values.

### Restart and resume behavior

`resume=TRUE` behavior:

1. Existing `*_results.tsv` are loaded.
2. Queue keys already present in prior results are skipped.
3. Checkpoints are continuously updated.
4. Failed batches remain available for audit.

`resume=FALSE` behavior:

1. Queue is treated as fresh in-memory workload.
2. Existing result files are not automatically deleted; if you need clean artifacts, remove old outputs manually before rerun.

## Data, Provenance, and Audit Expectations

Operational provenance goals:

1. Every validated row should be traceable to original staging record context.
2. Every service enrichment should be reproducible from persisted service result files.
3. Every run should preserve enough metadata to explain acceptance, rejection, or unresolved records.

Important audit fields from staging and pipeline:

- Source traceability:
  - `source_row_index`
  - `source_archive`
  - `plot_observation_id`
  - `occurrenceID`
- Taxonomy and name-resolution traceability:
  - `tnrs_submitted_name`
  - `tnrs_matched_name`
  - `tnrs_taxon_id`
  - `tnrs_authority`
  - `tnrs_match_score`
  - `tnrs_match_method`
  - `tnrs_name_changed`
  - `taxon_resolution_status`
- Geography and coordinate QA:
  - `latitude`, `longitude`
  - `location_uncertainty_m`
  - `coord_uncertainty_flag`
  - `null_island_flag`
  - `is_centroid`
- Native status and ecological context:
  - `native_status`
  - `native_status_reason`
  - `native_status_country`
  - `native_status_state_province`
  - `native_status_county_parish`
  - `is_introduced`
  - `is_cultivated_observation`

## Relationship to BIEN Data Loader and BIEN Staging

Conceptual mapping:

1. This repository creates and enriches a BIEN-compatible staging table.
2. Stage 1 constructs the base table from sPlot source rows.
3. Stage 2 enriches that table with external resolver outputs.
4. The final table is ready for BIEN Data Loader import workflow and review.

Pipeline role separation:

- Here: deterministic batch processing, service calls, and artifact persistence.
- BIEN Data Loader: interactive review, additional QA workflows, and load operations into BIEN-managed structures.

Staging compatibility intent:

- Column naming and enrichment fields are aligned with expected BIEN loader staging semantics (taxonomy resolution metadata, geographic normalization, and native status context).

## End-to-End Example Command Block

Run from repository root:

```bash
cd /Users/brianjenquist/VSCode/sPlot_BIENdb_Loading

# 1) Build staging table
Rscript R/01_build_bien_staging.R

# 2) Validate and enrich through TNRS -> GNRS -> GVS -> NSR
Rscript R/02_run_bien_loader_pipeline.R \
  --input=output/splot_bien_staging_full.tsv \
  --output_dir=output \
  --validation_dir=output/validation \
  --tnrs_batch_size=500 \
  --gnrs_batch_size=1000 \
  --gvs_batch_size=2000 \
  --nsr_batch_size=500 \
  --max_retries=5 \
  --retry_base_seconds=5 \
  --timeout_seconds=120 \
  --resume=TRUE \
  --pause_seconds=0.25

# 3) Verify final output exists
ls -lh output/splot_bien_staging_validated.tsv output/validation
```

## Minimal Validation Check

From repository root, parse both scripts:

```bash
Rscript -e "parse(file='R/01_build_bien_staging.R'); parse(file='R/02_run_bien_loader_pipeline.R')"
```

If parsing succeeds, R exits with status code 0 and no parse error.
