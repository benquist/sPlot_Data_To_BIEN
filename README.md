# sPlot to BIEN Database Loading Workflow

This repository is the operational workflow for converting sPlot Open v2.0 plot data into a BIEN loader-ready validation pipeline. It is intended to be published as the project at `https://github.com/benquist/sPlot_Data_To_BIEN`.

The workflow is designed for transparency, reproducibility, and end-to-end auditability:

1. Build a BIEN-compatible staging table from raw sPlot archive data.
2. Apply initial QA filters and data cleaning.
3. Resolve taxonomic names, geography, coordinate validity, and native status via BIEN relay services.
4. Emit a validated staging table with service provenance for BIEN ingest.

## Project Goals and Intentions

The primary objectives of this project are:

- Capture sPlot plot observation data in a BIEN-compatible staging schema.
- Preserve original sPlot metadata and deliver clear lineage from source archive to validated records.
- Use controlled batch validation through BIEN relay endpoints to avoid black-box transformation.
- Keep intermediate service outputs and checkpoints so runs can resume or be audited later.
- Support a transparent, manual companion report for dataset understanding and interpretation.

This repository is not meant to publish the raw sPlot archive itself. Instead, it stores:

- the ingestion and validation pipeline code,
- a rendered summary report (`splot_overview.html`),
- the source report (`splot_overview.Rmd`),
- a README documenting the full workflow.

## High-level Workflow

### Stage 1: Build BIEN staging table

Script: `R/01_build_bien_staging.R`

Key actions:

- Reads sPlotOpen header and species×plot details from the source zip archive.
- Joins the metadata and species files using `PlotObservationID`.
- Filters records where species names are absent or non-binomial.
- Filters records missing latitude or longitude.
- Generates QA flags for coordinate uncertainty and null-island records.
- Maps raw sPlot fields into a structured BIEN staging schema.
- Writes:
  - full staging table,
  - balanced `resample_1_consensus == TRUE` subset,
  - a unique TNRS names queue.

Intentional mapping details:

- `scrubbed_species_binomial` and `tnrs_submitted_name` are sourced from `Species`.
- `verbatim_scientific_name` preserves the raw `Original_species` string.
- `latitude` / `longitude` are preserved as reported by sPlot.
- `dataset`, `datasource`, `dataowner`, and `collection_code` are retained for provenance.
- `occurrenceID` is generated from plot ID, species name, and source row index.

### Stage 2: Relay validation and enrichment

Script: `R/02_run_bien_loader_pipeline.R`

Key pipeline stages:

1. TNRS name resolution
2. GNRS geography standardization
3. GVS geospatial validation
4. NSR native/introduced/cultivated context

The stage 2 pipeline is designed to merge relay outputs back into the original staging records while preserving provenance and checkpoint state.

## Data Cleaning and Corrections

### Filtering and QA in stage 1

The project applies the following cleaning decisions before BIEN mapping:

- Keep only records where `Species` looks like a binomial name.
- Remove records with missing coordinates.
- Flag `coord_uncertainty_flag` when `Location_uncertainty > 1000`.
- Flag `null_island_flag` when both latitude and longitude are within 0.1° of zero.
- Keep raw source archive names and coordinates for audit.
- Do not automatically drop records with service failures: failures are persisted and can be rerun.

### Naming and geographic reconciliation in stage 2

Validation is intentionally split into separate service passes:

- TNRS must resolve names before taxon-level checks are used.
- GNRS standardizes country / state / county values before location-based native status lookups.
- GVS checks whether reported lat/lon are centroids or otherwise suspicious.
- NSR is only meaningful after taxon and geography resolution are available.

### Correction philosophy

This repository favors:

- explicit flags over silent overwrites,
- preserving the original source values alongside normalized fields,
- treating relay enrichment as additive metadata rather than destructive replacement.

## BIEN Mapping Details

The staging schema is mapped to BIEN concepts as follows:

- `scrubbed_species_binomial` = species name for TNRS / BIEN taxonomy.
- `verbatim_scientific_name` = original species string from sPlot.
- `latitude`, `longitude` = raw coordinates used for GVS and NSR.
- `country`, `continent`, `biome` = source geography and biome context.
- `plot_observation_id` = unique plot-level identifier from sPlot.
- `dataset` = `sPlotOpen`.
- `datasource` = `iDiv sPlotOpen v2.0`.
- `collection_code` = `GIVD_ID`.
- `basisOfRecord` = `HumanObservation`.

Additional BIEN-compatible metadata retained for provenance:

- `source_archive` = zip filename used for ingestion,
- `source_row_index` = row ordinal from the tsv file join,
- `verbatim_date_collected` and `year_collected`.

## Report and Data Summary

This repository includes a data summary report containing:

- total plot records and species×plot observations,
- unique binomials submitted to TNRS,
- geographic coverage and plot distribution,
- temporal coverage of record dates,
- biome and habitat composition,
- data quality flags such as coordinate uncertainty and null-island.

### Report files included in this repo

- `splot_overview.Rmd` — the source R Markdown analysis and visualization.
- `splot_overview.html` — pre-rendered HTML with an interactive Leaflet map.

### What the report summarizes

- plot density across the sPlot dataset,
- geographic coverage of plots,
- counts of unique species and datasets,
- data quality issues relevant to BIEN ingestion,
- missing taxonomy and coordinate issues,
- trait coverage in the sample-level mean trait table.

## Repository Layout

- `R/01_build_bien_staging.R` — stage 1 build and QA.
- `R/02_run_bien_loader_pipeline.R` — stage 2 relay validation.
- `splot_overview.Rmd` — dataset summary and QA report.
- `splot_overview.html` — rendered dataset summary report.
- `data/` — local archive input path only; raw zip is intentionally excluded.
- `output/` — generated outputs and validation artifacts.
- `.gitignore` — excludes raw data and generated TSV output.

## Running the workflow

### Step 1: Prepare the source archive

Download the sPlotOpen v2.0 archive and place it at the expected path:

```bash
mkdir -p data
cp "/path/to/iDiv Data Repository_3474_v55__20260429.zip" data/
```

### Step 2: Build the staging table

```bash
Rscript R/01_build_bien_staging.R
```

This creates the staging table and TNRS unique-name queue. Note that the current script contains absolute `ZIP` and `OUTPUT_DIR` defaults; adapt those values locally if needed.

### Step 3: Run the BIEN relay validation pipeline

```bash
Rscript R/02_run_bien_loader_pipeline.R \
  --input=output/splot_bien_staging_full.tsv \
  --output_dir=output \
  --validation_dir=output/validation \
  --tnrs_batch_size=500 \
  --gnrs_batch_size=1000 \
  --gvs_batch_size=2000 \
  --nsr_batch_size=500 \
  --resume=TRUE
```

### Recommended conservative settings

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

## Validation artifact semantics

The stage 2 pipeline writes persistent artifacts so the exact state of each run is inspectable.

- `tnrs_results.tsv`, `gnrs_results.tsv`, `gvs_results.tsv`, `nsr_results.tsv`
  - contain accumulated service responses.
- `output/validation/failed_batches/`
  - stores any batches that failed after retries.
- checkpoint files record processed counts and remaining queue size.

## Data provenance and audit

This repository emphasizes reproducibility by:

- preserving raw sPlot source values,
- writing explicit service result tables,
- flagging records that are uncertain or out-of-scope,
- making every validation stage restartable.

## Notes on pushing to GitHub

This repo is intended to be pushed to `https://github.com/benquist/sPlot_Data_To_BIEN`.

If you want to preserve the original upstream remote while adding the new remote, use:

```bash
git remote add splot_bien_old https://github.com/benquist/sPlot_BIENdb_Loading.git
``` 

Then push the current branch to the new remote:

```bash
git remote set-url origin https://github.com/benquist/sPlot_Data_To_BIEN.git

git push -u origin master
```

## Important caveats

- The repo does not include the sPlotOpen archive itself.
- Absolute file paths in the stage 1 script must be updated for different local layouts.
- Downstream enrichment depends on live BIEN relay services.
- The current HTML report is pre-rendered and may need rerendering if source data paths change.

## Contact and provenance

Source dataset: Sabatini et al. 2021, Global Ecology and Biogeography; iDiv sPlotOpen v2.0.

If this repository is used for production BIEN ingestion, verify the output staging table against BIEN loader expectations and keep all validation artifacts with the submission.
