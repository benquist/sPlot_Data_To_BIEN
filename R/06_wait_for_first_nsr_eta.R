#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(argv) {
  defaults <- list(
    snapshot_csv = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/live_completion_snapshot.csv",
    alert_file = "/Users/brianjenquist/VSCode/splot-open-data/output/validation/first_non_na_nsr_eta_alert.txt",
    interval_seconds = 60,
    timeout_minutes = 720
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

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(dirname(args$alert_file), recursive = TRUE, showWarnings = FALSE)

  start <- Sys.time()
  deadline <- start + args$timeout_minutes * 60

  repeat {
    now <- Sys.time()
    if (now > deadline) {
      msg <- sprintf("%s | timeout waiting for first NSR non-NA ETA within %.0f minutes", format(now, "%Y-%m-%d %H:%M:%S"), args$timeout_minutes)
      writeLines(msg, args$alert_file)
      cat(msg, "\n", sep = "")
      quit(save = "no", status = 1)
    }

    if (file.exists(args$snapshot_csv)) {
      dt <- tryCatch(fread(args$snapshot_csv, showProgress = FALSE), error = function(e) data.table())
      if (nrow(dt) > 0L) {
        dt[, service := tolower(as.character(service))]
        nsr <- dt[service == "nsr"]
        if (nrow(nsr) > 0L) {
          nsr <- nsr[1]
          eta_low <- suppressWarnings(as.numeric(nsr$eta_low_h))
          eta_mid <- suppressWarnings(as.numeric(nsr$eta_mid_h))
          eta_high <- suppressWarnings(as.numeric(nsr$eta_high_h))
          if (any(is.finite(c(eta_low, eta_mid, eta_high)), na.rm = TRUE)) {
            msg <- paste0(
              format(now, "%Y-%m-%d %H:%M:%S"),
              " | FIRST_NON_NA_NSR_ETA",
              " | NSR ETA[", fmt_eta(eta_low), "|", fmt_eta(eta_mid), "|", fmt_eta(eta_high), "]",
              " processed=", as.integer(nsr$processed), "/", as.integer(nsr$total),
              " remaining=", as.integer(nsr$remaining)
            )
            writeLines(msg, args$alert_file)
            cat(msg, "\n", sep = "")
            quit(save = "no", status = 0)
          }
        }
      }
    }

    Sys.sleep(args$interval_seconds)
  }
}

main()
