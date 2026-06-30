#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(argv) {
  defaults <- list(
    validation_dir = "/Users/brianjenquist/VSCode/splot-open-data/output/validation",
    output_md = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/live_completion_dashboard.md",
    output_csv = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/live_completion_snapshot.csv",
    history_csv = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/progress_history.csv",
    watch = FALSE,
    interval_seconds = 60,
    bands_low_factor = 1.4,
    bands_high_factor = 0.7,
    stall_minutes = 20
  )

  out <- defaults
  if (length(argv) == 0L) return(out)

  for (arg in argv) {
    if (!startsWith(arg, "--")) next
    kv <- sub("^--", "", arg)
    if (!grepl("=", kv, fixed = TRUE)) {
      out[[kv]] <- TRUE
      next
    }
    key <- sub("=.*$", "", kv)
    val <- sub("^[^=]*=", "", kv)
    out[[key]] <- val
  }

  out$watch <- tolower(as.character(out$watch)) %in% c("1", "true", "t", "yes", "y")
  out$interval_seconds <- suppressWarnings(as.numeric(out$interval_seconds))
  if (is.na(out$interval_seconds) || out$interval_seconds < 5) out$interval_seconds <- defaults$interval_seconds
  out$bands_low_factor <- suppressWarnings(as.numeric(out$bands_low_factor))
  if (is.na(out$bands_low_factor) || out$bands_low_factor <= 0) out$bands_low_factor <- defaults$bands_low_factor
  out$bands_high_factor <- suppressWarnings(as.numeric(out$bands_high_factor))
  if (is.na(out$bands_high_factor) || out$bands_high_factor <= 0) out$bands_high_factor <- defaults$bands_high_factor
  out$stall_minutes <- suppressWarnings(as.numeric(out$stall_minutes))
  if (is.na(out$stall_minutes) || out$stall_minutes <= 0) out$stall_minutes <- defaults$stall_minutes

  out
}

last_progress_time <- function(history, service_name) {
  h <- history[service == service_name]
  if (nrow(h) == 0L) return(as.POSIXct(NA, tz = "UTC"))
  h <- h[!is.na(observed_at_utc)]
  if (nrow(h) == 0L) return(as.POSIXct(NA, tz = "UTC"))
  setorder(h, observed_at_utc)
  changed <- c(TRUE, diff(h$processed) != 0)
  h$observed_at_utc[max(which(changed))]
}

classify_status <- function(processed, total, progress_age_minutes, checkpoint_time_utc, now_utc, stall_minutes) {
  if (!is.finite(total) || total <= 0) return("empty")
  if (is.finite(processed) && processed >= total) return("complete")
  if (is.na(checkpoint_time_utc)) return("not_started")
  if (is.finite(progress_age_minutes) && progress_age_minutes >= stall_minutes) return("stalled")
  "active"
}

read_checkpoint <- function(path, service) {
  if (!file.exists(path)) {
    return(data.table(
      service = service,
      timestamp_utc = as.character(NA),
      processed = 0,
      total = 0,
      remaining = 0
    ))
  }

  dt <- fread(path, sep = "\t", na.strings = c("", "NA"), showProgress = FALSE)
  if (nrow(dt) == 0L) {
    return(data.table(
      service = service,
      timestamp_utc = as.character(NA),
      processed = 0,
      total = 0,
      remaining = 0
    ))
  }

  out <- dt[1]
  out[, service := tolower(as.character(service))]
  out[, timestamp_utc := as.character(timestamp_utc)]
  out[, processed := as.numeric(processed)]
  out[, total := as.numeric(total)]
  out[, remaining := as.numeric(remaining)]
  out
}

safe_time <- function(x) {
  y <- as.character(x)
  y <- sub("Z$", "", y)
  y <- gsub("T", " ", y, fixed = TRUE)
  suppressWarnings(as.POSIXct(y, tz = "UTC", format = "%Y-%m-%d %H:%M:%S"))
}

fmt_num <- function(x) {
  format(as.integer(round(x)), big.mark = ",", scientific = FALSE)
}

fmt_pct <- function(processed, total) {
  if (is.na(total) || total <= 0) return("0.0%")
  sprintf("%.1f%%", 100 * processed / total)
}

fmt_hours <- function(hours) {
  if (is.na(hours) || !is.finite(hours) || hours < 0) return("NA")
  if (hours < 1) return(sprintf("%.0f min", hours * 60))
  if (hours < 24) return(sprintf("%.1f h", hours))
  sprintf("%.1f d", hours / 24)
}

estimate_rate_per_hour <- function(history, service) {
  svc <- as.character(service)
  h <- history[service == svc]
  if (nrow(h) < 2L) return(NA_real_)
  setorder(h, observed_at_utc)
  h <- h[!is.na(observed_at_utc)]
  if (nrow(h) < 2L) return(NA_real_)

  recent <- tail(h, 6)
  t1 <- recent$observed_at_utc[1]
  t2 <- recent$observed_at_utc[nrow(recent)]
  p1 <- recent$processed[1]
  p2 <- recent$processed[nrow(recent)]

  elapsed_h <- as.numeric(difftime(t2, t1, units = "hours"))
  delta <- p2 - p1
  if (is.na(elapsed_h) || elapsed_h <= 0 || is.na(delta) || delta <= 0) return(NA_real_)
  delta / elapsed_h
}

render_dashboard <- function(checkpoints, history, output_md, output_csv, low_factor, high_factor, stall_minutes) {
  now_utc <- as.POSIXct(Sys.time(), tz = "UTC")

  cp <- copy(checkpoints)
  cp[, percent_complete := ifelse(total > 0, 100 * processed / total, 0)]
  cp[, eta_mid_h := ifelse(!is.na(rate_per_h) & rate_per_h > 0, remaining / rate_per_h, NA_real_)]
  cp[, eta_low_h := eta_mid_h / low_factor]
  cp[, eta_high_h := ifelse(high_factor > 0, eta_mid_h / high_factor, NA_real_)]
  cp[, checkpoint_age_minutes := as.numeric(difftime(now_utc, checkpoint_time_utc, units = "mins"))]
  cp[, last_progress_utc := vapply(service, function(svc) {
    x <- last_progress_time(history, svc)
    if (is.na(x)) return(NA_character_)
    format(x, tz = "UTC", usetz = FALSE)
  }, character(1))]
  cp[, progress_age_minutes := as.numeric(difftime(now_utc, safe_time(last_progress_utc), units = "mins"))]
  cp[, status := vapply(seq_len(.N), function(i) {
    classify_status(processed[i], total[i], progress_age_minutes[i], checkpoint_time_utc[i], now_utc, stall_minutes)
  }, character(1))]

  fwrite(cp[, .(
    service,
    timestamp_utc,
    processed,
    total,
    remaining,
    rate_per_h,
    checkpoint_age_minutes,
    progress_age_minutes,
    status,
    eta_low_h,
    eta_mid_h,
    eta_high_h
  )], output_csv)

  lines <- c(
    "# sPlot BIEN Validation Live Completion Dashboard",
    "",
    sprintf("Updated (UTC): %s", format(now_utc, "%Y-%m-%d %H:%M:%S")),
    "",
    "## Service Progress",
    "",
    "| Service | Status | Processed | Total | Remaining | Complete | Rate (/h) | Checkpoint Age (min) | Progress Age (min) | ETA Low | ETA Mid | ETA High |",
    "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
  )

  for (i in seq_len(nrow(cp))) {
    r <- cp[i]
    lines <- c(lines, sprintf(
      "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |",
      toupper(r$service),
      toupper(r$status),
      fmt_num(r$processed),
      fmt_num(r$total),
      fmt_num(r$remaining),
      fmt_pct(r$processed, r$total),
      ifelse(is.na(r$rate_per_h), "NA", sprintf("%.1f", r$rate_per_h)),
      ifelse(is.na(r$checkpoint_age_minutes), "NA", sprintf("%.1f", r$checkpoint_age_minutes)),
      ifelse(is.na(r$progress_age_minutes), "NA", sprintf("%.1f", r$progress_age_minutes)),
      fmt_hours(r$eta_low_h),
      fmt_hours(r$eta_mid_h),
      fmt_hours(r$eta_high_h)
    ))
  }

  lines <- c(lines, "", "## Notes", "")
  lines <- c(lines, "- ETA bands are throughput-based projections from recent checkpoint velocity.")
  lines <- c(lines, "- `ETA Low` assumes faster sustained throughput; `ETA High` assumes slower sustained throughput.")
  lines <- c(lines, "- If checkpoint progress is flat, ETA is reported as `NA` until new progress is observed.")
  lines <- c(lines, sprintf("- A service is marked `STALLED` when its processed count has not advanced for %.0f minutes while work remains.", stall_minutes))

  writeLines(lines, output_md)
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(dirname(args$output_md), recursive = TRUE, showWarnings = FALSE)

  checkpoint_paths <- list(
    tnrs = file.path(args$validation_dir, "tnrs_checkpoint.tsv"),
    gnrs = file.path(args$validation_dir, "gnrs_checkpoint.tsv"),
    gvs = file.path(args$validation_dir, "gvs_checkpoint.tsv"),
    nsr = file.path(args$validation_dir, "nsr_checkpoint.tsv")
  )

  repeat {
    cps <- rbindlist(lapply(names(checkpoint_paths), function(svc) {
      read_checkpoint(checkpoint_paths[[svc]], svc)
    }), fill = TRUE)

    cps[, observed_at_utc := as.POSIXct(Sys.time(), tz = "UTC")]
    cps[, checkpoint_time_utc := safe_time(timestamp_utc)]

    history <- if (file.exists(args$history_csv)) {
      fread(args$history_csv, showProgress = FALSE)
    } else {
      data.table()
    }

    if (nrow(history) > 0L) {
      if (!"observed_at_utc" %in% names(history)) history[, observed_at_utc := as.character(NA)]
      history[, observed_at_utc := suppressWarnings(as.POSIXct(observed_at_utc, tz = "UTC"))]
      history[, processed := as.numeric(processed)]
      history[, total := as.numeric(total)]
      history[, remaining := as.numeric(remaining)]
    }

    snap <- cps[, .(service, observed_at_utc, timestamp_utc, processed, total, remaining)]
    if (nrow(history) == 0L) {
      history <- snap
    } else {
      history <- rbindlist(list(history, snap), fill = TRUE)
      setorder(history, service, observed_at_utc)
      history <- unique(history, by = c("service", "timestamp_utc", "processed", "total", "remaining"))
    }

    fwrite(history, args$history_csv)

    cps[, rate_per_h := vapply(service, function(svc) estimate_rate_per_hour(history, svc), numeric(1))]

    render_dashboard(
      checkpoints = cps,
      history = history,
      output_md = args$output_md,
      output_csv = args$output_csv,
      low_factor = args$bands_low_factor,
      high_factor = args$bands_high_factor,
      stall_minutes = args$stall_minutes
    )

    cat(sprintf("%s | dashboard updated: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), args$output_md))

    if (!isTRUE(args$watch)) break
    Sys.sleep(args$interval_seconds)
  }
}

main()
