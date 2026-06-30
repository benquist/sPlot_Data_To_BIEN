#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(argv) {
  defaults <- list(
    snapshot_csv = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/live_completion_snapshot.csv",
    alert_file = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/first_non_na_eta_alert.txt",
    interval_seconds = 60,
    timeout_minutes = 180
  )

  out <- defaults
  if (length(argv) == 0L) return(out)

  for (arg in argv) {
    if (!startsWith(arg, "--")) next
    kv <- sub("^--", "", arg)
    if (!grepl("=", kv, fixed = TRUE)) next
    key <- sub("=.*$", "", kv)
    val <- sub("^[^=]*=", "", kv)
    out[[key]] <- val
  }

  out$interval_seconds <- suppressWarnings(as.numeric(out$interval_seconds))
  if (is.na(out$interval_seconds) || out$interval_seconds < 5) out$interval_seconds <- defaults$interval_seconds
  out$timeout_minutes <- suppressWarnings(as.numeric(out$timeout_minutes))
  if (is.na(out$timeout_minutes) || out$timeout_minutes <= 0) out$timeout_minutes <- defaults$timeout_minutes

  out
}

fmt_eta <- function(hours) {
  if (is.na(hours) || !is.finite(hours) || hours < 0) return("NA")
  if (hours < 1) return(sprintf("%.0fm", hours * 60))
  if (hours < 24) return(sprintf("%.1fh", hours))
  sprintf("%.1fd", hours / 24)
}

row_for <- function(dt, svc) {
  x <- dt[tolower(service) == tolower(svc)]
  if (nrow(x) == 0L) {
    data.table(service = svc, eta_low_h = NA_real_, eta_mid_h = NA_real_, eta_high_h = NA_real_, processed = 0, total = 0)
  } else {
    x[1]
  }
}

has_eta <- function(r) {
  any(is.finite(c(r$eta_low_h, r$eta_mid_h, r$eta_high_h)), na.rm = TRUE)
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(dirname(args$alert_file), recursive = TRUE, showWarnings = FALSE)

  start <- Sys.time()
  deadline <- start + args$timeout_minutes * 60

  repeat {
    now <- Sys.time()
    if (now > deadline) {
      msg <- sprintf("%s | timeout waiting for non-NA ETA bands within %.0f minutes", format(now, "%Y-%m-%d %H:%M:%S"), args$timeout_minutes)
      writeLines(msg, args$alert_file)
      cat(msg, "\n", sep = "")
      quit(save = "no", status = 1)
    }

    if (file.exists(args$snapshot_csv)) {
      dt <- tryCatch(fread(args$snapshot_csv, showProgress = FALSE), error = function(e) data.table())
      if (nrow(dt) > 0L) {
        dt[, eta_low_h := suppressWarnings(as.numeric(eta_low_h))]
        dt[, eta_mid_h := suppressWarnings(as.numeric(eta_mid_h))]
        dt[, eta_high_h := suppressWarnings(as.numeric(eta_high_h))]
        gvs <- row_for(dt, "gvs")
        nsr <- row_for(dt, "nsr")

        if (has_eta(gvs) || has_eta(nsr)) {
          msg <- paste0(
            format(now, "%Y-%m-%d %H:%M:%S"),
            " | FIRST_NON_NA_ETA",
            " | GVS ETA[", fmt_eta(gvs$eta_low_h), "|", fmt_eta(gvs$eta_mid_h), "|", fmt_eta(gvs$eta_high_h), "]",
            " processed=", as.integer(gvs$processed), "/", as.integer(gvs$total),
            " | NSR ETA[", fmt_eta(nsr$eta_low_h), "|", fmt_eta(nsr$eta_mid_h), "|", fmt_eta(nsr$eta_high_h), "]",
            " processed=", as.integer(nsr$processed), "/", as.integer(nsr$total)
          )
          writeLines(msg, args$alert_file)
          cat(msg, "\n", sep = "")
          quit(save = "no", status = 0)
        }
      }
    }

    Sys.sleep(args$interval_seconds)
  }
}

main()
