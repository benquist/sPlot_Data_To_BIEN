#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(argv) {
  defaults <- list(
    snapshot_csv = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/live_completion_snapshot.csv",
    watch = FALSE,
    interval_seconds = 60
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

  out
}

fmt_pct <- function(processed, total) {
  if (is.na(total) || total <= 0) return("0.0%")
  sprintf("%.2f%%", 100 * processed / total)
}

fmt_eta <- function(hours) {
  if (is.na(hours) || !is.finite(hours) || hours < 0) return("NA")
  if (hours < 1) return(sprintf("%.0fm", hours * 60))
  if (hours < 24) return(sprintf("%.1fh", hours))
  sprintf("%.1fd", hours / 24)
}

service_row <- function(dt, service_name) {
  if (nrow(dt) == 0L) {
    return(data.table(
      service = service_name,
      processed = 0,
      total = 0,
      remaining = 0,
      status = "unknown",
      eta_low_h = NA_real_,
      eta_mid_h = NA_real_,
      eta_high_h = NA_real_
    ))
  }
  x <- dt[tolower(service) == tolower(service_name)]
  if (nrow(x) == 0L) {
    return(data.table(
      service = service_name,
      processed = 0,
      total = 0,
      remaining = 0,
      status = "unknown",
      eta_low_h = NA_real_,
      eta_mid_h = NA_real_,
      eta_high_h = NA_real_
    ))
  }
  x[1]
}

emit_status_line <- function(snapshot_csv) {
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  if (!file.exists(snapshot_csv)) {
    cat(sprintf("%s | status=waiting snapshot_missing=%s\n", now, snapshot_csv))
    return(invisible(NULL))
  }

  dt <- tryCatch(
    fread(snapshot_csv, showProgress = FALSE),
    error = function(e) data.table()
  )

  gvs <- service_row(dt, "gvs")
  nsr <- service_row(dt, "nsr")

  label_for <- function(x) {
    status <- tolower(as.character(x$status))
    base <- toupper(as.character(x$service))
    if (!nzchar(status) || status %in% c("active", "complete", "unknown", "na")) return(base)
    paste(base, toupper(status))
  }

  line <- sprintf(
    "%s | %s %s (%s/%s) ETA[%s|%s|%s] | %s %s (%s/%s) ETA[%s|%s|%s]",
    now,
    label_for(gvs),
    fmt_pct(gvs$processed, gvs$total),
    format(as.integer(gvs$processed), big.mark = ",", scientific = FALSE),
    format(as.integer(gvs$total), big.mark = ",", scientific = FALSE),
    fmt_eta(gvs$eta_low_h),
    fmt_eta(gvs$eta_mid_h),
    fmt_eta(gvs$eta_high_h),
    label_for(nsr),
    fmt_pct(nsr$processed, nsr$total),
    format(as.integer(nsr$processed), big.mark = ",", scientific = FALSE),
    format(as.integer(nsr$total), big.mark = ",", scientific = FALSE),
    fmt_eta(nsr$eta_low_h),
    fmt_eta(nsr$eta_mid_h),
    fmt_eta(nsr$eta_high_h)
  )

  cat(line, "\n", sep = "")
  invisible(NULL)
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  repeat {
    emit_status_line(args$snapshot_csv)
    if (!isTRUE(args$watch)) break
    Sys.sleep(args$interval_seconds)
  }
}

main()
