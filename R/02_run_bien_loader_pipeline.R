#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(httr)
  library(jsonlite)
})

TNRS_URL <- "https://bien-relay-tnrs.benquist.workers.dev"
GNRS_URL <- "https://bien-relay-gnrs.benquist.workers.dev"
GVS_URL  <- "https://bien-relay-gvs.benquist.workers.dev"
NSR_URL  <- "https://bien-relay-nsr.benquist.workers.dev"

log_msg <- function(...) {
  message(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " | ", paste(..., collapse = " "))
}

parse_args <- function(argv) {
  defaults <- list(
    input = "/Users/brianjenquist/VSCode/splot-open-data/output/splot_bien_staging_full.tsv",
    output_dir = "/Users/brianjenquist/VSCode/splot-open-data/output",
    validation_dir = NA_character_,
    tnrs_batch_size = 500L,
    gnrs_batch_size = 1000L,
    gvs_batch_size = 2000L,
    nsr_batch_size = 500L,
    max_retries = 5L,
    retry_base_seconds = 5,
    timeout_seconds = 120L,
    resume = TRUE,
    pause_seconds = 0.25
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

  as_int <- function(x, fallback) {
    y <- suppressWarnings(as.integer(x))
    if (is.na(y)) fallback else y
  }
  as_num <- function(x, fallback) {
    y <- suppressWarnings(as.numeric(x))
    if (is.na(y)) fallback else y
  }
  as_bool <- function(x, fallback) {
    if (is.logical(x)) return(x)
    z <- tolower(trimws(as.character(x)))
    if (z %in% c("1", "true", "t", "yes", "y")) return(TRUE)
    if (z %in% c("0", "false", "f", "no", "n")) return(FALSE)
    fallback
  }

  out$tnrs_batch_size <- as_int(out$tnrs_batch_size, defaults$tnrs_batch_size)
  out$gnrs_batch_size <- as_int(out$gnrs_batch_size, defaults$gnrs_batch_size)
  out$gvs_batch_size <- as_int(out$gvs_batch_size, defaults$gvs_batch_size)
  out$nsr_batch_size <- as_int(out$nsr_batch_size, defaults$nsr_batch_size)
  out$max_retries <- as_int(out$max_retries, defaults$max_retries)
  out$timeout_seconds <- as_int(out$timeout_seconds, defaults$timeout_seconds)
  out$retry_base_seconds <- as_num(out$retry_base_seconds, defaults$retry_base_seconds)
  out$pause_seconds <- as_num(out$pause_seconds, defaults$pause_seconds)
  out$resume <- as_bool(out$resume, defaults$resume)

  out
}

truthy <- function(x) {
  z <- tolower(trimws(as.character(x)))
  !is.na(z) & z %in% c("1", "true", "t", "yes", "y")
}

trim_na <- function(x) {
  y <- trimws(as.character(x))
  y[is.na(x) | y == ""] <- NA_character_
  y
}

first_non_empty <- function(dt, candidates) {
  cols <- intersect(candidates, names(dt))
  if (length(cols) == 0L) return(rep(NA_character_, nrow(dt)))
  out <- rep(NA_character_, nrow(dt))
  for (col in cols) {
    v <- trim_na(dt[[col]])
    idx <- is.na(out) & !is.na(v)
    out[idx] <- v[idx]
  }
  out
}

ensure_columns <- function(dt, cols, default = NA_character_) {
  for (nm in cols) {
    if (!nm %in% names(dt)) dt[, (nm) := default]
  }
  invisible(dt)
}

safe_read_tsv <- function(path) {
  if (!file.exists(path)) return(data.table())
  fread(path, sep = "\t", na.strings = c("", "NA", "N/A"), showProgress = FALSE, fill = TRUE)
}

write_tsv <- function(dt, path) {
  fwrite(dt, path, sep = "\t", na = "", quote = FALSE)
}

extract_data_frame <- function(obj) {
  if (is.data.frame(obj)) return(as.data.table(obj))
  if (is.list(obj)) {
    for (nm in names(obj)) {
      if (is.data.frame(obj[[nm]]) && nrow(obj[[nm]]) > 0L) {
        return(as.data.table(obj[[nm]]))
      }
    }
  }
  data.table()
}

post_with_retry <- function(url, payload, max_retries, retry_base_seconds, timeout_seconds, service_name, batch_label) {
  attempt <- 1L
  while (attempt <= max_retries) {
    resp <- tryCatch(
      POST(
        url,
        body = payload,
        content_type("application/json"),
        add_headers(Accept = "application/json", charset = "UTF-8"),
        config(connecttimeout = 20),
        timeout(timeout_seconds)
      ),
      error = function(e) e
    )

    if (inherits(resp, "error")) {
      if (attempt == max_retries) {
        stop(service_name, " ", batch_label, " network error after retries: ", conditionMessage(resp), call. = FALSE)
      }
      wait_s <- retry_base_seconds * (2 ^ (attempt - 1L))
      log_msg(service_name, batch_label, "network error, retry", attempt, "of", max_retries, "in", wait_s, "s")
      Sys.sleep(wait_s)
      attempt <- attempt + 1L
      next
    }

    code <- status_code(resp)
    txt <- tryCatch(content(resp, "text", encoding = "UTF-8"), error = function(e) "")

    if (code == 200L) return(txt)

    retryable <- code == 429L || (code >= 500L && code <= 599L)
    if (retryable && attempt < max_retries) {
      wait_s <- retry_base_seconds * (2 ^ (attempt - 1L))
      log_msg(service_name, batch_label, "HTTP", code, "retry", attempt, "of", max_retries, "in", wait_s, "s")
      Sys.sleep(wait_s)
      attempt <- attempt + 1L
      next
    }

    stop(service_name, " ", batch_label, " HTTP ", code, ": ", substr(txt, 1, 400), call. = FALSE)
  }

  stop(service_name, " ", batch_label, " failed after retries", call. = FALSE)
}

append_results <- function(new_dt, result_path, key_cols) {
  old <- safe_read_tsv(result_path)
  all_dt <- rbindlist(list(old, new_dt), fill = TRUE, use.names = TRUE)
  if (nrow(all_dt) == 0L) {
    write_tsv(all_dt, result_path)
    return(invisible(all_dt))
  }
  keep <- intersect(key_cols, names(all_dt))
  if (length(keep) > 0L) {
    for (k in keep) all_dt[[k]] <- trim_na(all_dt[[k]])
    all_dt <- unique(all_dt, by = keep)
  } else {
    all_dt <- unique(all_dt)
  }
  write_tsv(all_dt, result_path)
  invisible(all_dt)
}

write_checkpoint <- function(path, service, processed, total, remaining) {
  dt <- data.table(
    service = service,
    timestamp_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    processed = processed,
    total = total,
    remaining = remaining
  )
  write_tsv(dt, path)
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))

  if (is.na(args$validation_dir) || args$validation_dir == "") {
    args$validation_dir <- file.path(args$output_dir, "validation")
  }

  dir.create(args$output_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(args$validation_dir, recursive = TRUE, showWarnings = FALSE)
  failed_dir <- file.path(args$validation_dir, "failed_batches")
  dir.create(failed_dir, recursive = TRUE, showWarnings = FALSE)

  final_out <- file.path(args$output_dir, "splot_bien_staging_validated.tsv")

  log_msg("Loading staging:", args$input)
  stg <- fread(args$input, sep = "\t", na.strings = c("", "NA", "N/A"), showProgress = TRUE)

  ensure_columns(stg, c(
    "scrubbed_family", "scrubbed_genus", "scrubbed_author", "scrubbed_taxonomic_status",
    "tnrs_submitted_name", "tnrs_matched_name", "tnrs_taxon_id", "tnrs_authority",
    "tnrs_match_score", "tnrs_match_method", "tnrs_name_changed", "taxon_resolution_status",
    "country", "state_province", "county", "is_centroid",
    "native_status", "native_status_reason", "native_status_country",
    "native_status_state_province", "native_status_county_parish",
    "is_introduced", "is_cultivated_observation"
  ))

  # Force character type on all TNRS/resolution columns (fread may read all-NA cols as logical)
  for (.col in c(
    "scrubbed_family", "scrubbed_genus", "scrubbed_author", "scrubbed_taxonomic_status",
    "tnrs_submitted_name", "tnrs_matched_name", "tnrs_taxon_id", "tnrs_authority",
    "tnrs_match_method", "tnrs_name_changed", "taxon_resolution_status",
    "country", "state_province", "county", "is_centroid",
    "native_status", "native_status_reason", "native_status_country",
    "native_status_state_province", "native_status_county_parish",
    "is_introduced", "is_cultivated_observation"
  )) {
    if (.col %in% names(stg) && !is.character(stg[[.col]])) {
      stg[, (.col) := as.character(get(.col))]
    }
  }
  stg[, tnrs_match_score := suppressWarnings(as.numeric(tnrs_match_score))]

  stg[, tnrs_submit_key := fifelse(
    !is.na(scrubbed_species_binomial_splot) & trimws(scrubbed_species_binomial_splot) != "",
    trimws(scrubbed_species_binomial_splot),
    trimws(scrubbed_species_binomial)
  )]

  # TNRS ---------------------------------------------------------------------
  tnrs_result_path <- file.path(args$validation_dir, "tnrs_results.tsv")
  tnrs_checkpoint <- file.path(args$validation_dir, "tnrs_checkpoint.tsv")

  tnrs_queue <- unique(data.table(Name_submitted = trim_na(stg$tnrs_submit_key)))
  tnrs_queue <- tnrs_queue[!is.na(Name_submitted)]

  tnrs_done <- if (isTRUE(args$resume) && file.exists(tnrs_result_path)) {
    x <- safe_read_tsv(tnrs_result_path)
    unique(trim_na(x$Name_submitted))
  } else {
    character(0)
  }

  tnrs_remaining <- tnrs_queue[!Name_submitted %in% tnrs_done]
  log_msg("TNRS queue:", nrow(tnrs_queue), "unique names; remaining:", nrow(tnrs_remaining))

  if (nrow(tnrs_remaining) > 0L) {
    tnrs_batches <- split(tnrs_remaining, ceiling(seq_len(nrow(tnrs_remaining)) / args$tnrs_batch_size))

    for (i in seq_along(tnrs_batches)) {
      batch <- as.data.table(tnrs_batches[[i]])
      batch_n <- nrow(batch)
      label <- paste0("batch ", i, "/", length(tnrs_batches), " (n=", batch_n, ")")
      log_msg("TNRS", label)

      payload_dt <- data.frame(
        id = seq_len(batch_n),
        Name_submitted = batch$Name_submitted,
        stringsAsFactors = FALSE
      )
      payload <- toJSON(
        list(opts = list(mode = "resolve", matches = "best", sources = "wcvp,wfo", acc = 1L), data = payload_dt),
        auto_unbox = TRUE
      )

      batch_ok <- TRUE
      out <- tryCatch({
        txt <- post_with_retry(
          url = TNRS_URL,
          payload = payload,
          max_retries = args$max_retries,
          retry_base_seconds = args$retry_base_seconds,
          timeout_seconds = args$timeout_seconds,
          service_name = "TNRS",
          batch_label = label
        )
        obj <- fromJSON(txt, flatten = TRUE)
        dt <- extract_data_frame(obj)
        if (nrow(dt) == 0L) stop("TNRS response had zero rows", call. = FALSE)
        if (!"Name_submitted" %in% names(dt)) {
          if ("id" %in% names(dt)) {
            id_int <- suppressWarnings(as.integer(dt$id))
            dt[, Name_submitted := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), batch$Name_submitted[id_int], NA_character_)]
          } else if (nrow(dt) == nrow(batch)) {
            dt[, Name_submitted := batch$Name_submitted]
          }
        }
        dt[, Name_submitted := trim_na(Name_submitted)]
        dt[, tnrs_timestamp_utc := format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")]
        dt
      }, error = function(e) {
        batch_ok <<- FALSE
        log_msg("TNRS", label, "FAILED:", conditionMessage(e))
        failed_path <- file.path(failed_dir, sprintf("tnrs_failed_batch_%04d.tsv", i))
        write_tsv(batch, failed_path)
        data.table()
      })

      if (batch_ok && nrow(out) > 0L) {
        append_results(out, tnrs_result_path, key_cols = c("Name_submitted"))
      }

      done_now <- length(unique(c(tnrs_done, unique(trim_na(out$Name_submitted)))))
      write_checkpoint(tnrs_checkpoint, "tnrs", done_now, nrow(tnrs_queue), nrow(tnrs_queue) - done_now)
      if (args$pause_seconds > 0) Sys.sleep(args$pause_seconds)
    }
  }

  tnrs_res <- safe_read_tsv(tnrs_result_path)
  if (nrow(tnrs_res) > 0L) {
    tnrs_res[, Name_submitted := trim_na(Name_submitted)]
    tnrs_res[, matched_name := first_non_empty(.SD, c("Accepted_name", "Name_matched")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_family := first_non_empty(.SD, c("Accepted_family", "Family")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_genus := first_non_empty(.SD, c("Genus_matched", "Genus")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_author := first_non_empty(.SD, c("Accepted_name_author", "Author_matched")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_status := first_non_empty(.SD, c("Taxonomic_status", "Name_matched_status")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_taxon_id := first_non_empty(.SD, c("Accepted_species_id", "Name_matched_id", "Taxon_id")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_authority := first_non_empty(.SD, c("Accepted_name_author", "Author_matched", "Name_matched_author")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_score := first_non_empty(.SD, c("Overall_score", "Name_score", "Score")), .SDcols = names(tnrs_res)]
    tnrs_res[, matched_method := first_non_empty(.SD, c("Name_matched_method", "Match_type", "Warnings")), .SDcols = names(tnrs_res)]
    tnrs_res <- unique(tnrs_res[!is.na(Name_submitted)], by = "Name_submitted")

    stg <- merge(
      stg,
      tnrs_res[, .(
        Name_submitted,
        matched_name,
        matched_family,
        matched_genus,
        matched_author,
        matched_status,
        matched_taxon_id,
        matched_authority,
        matched_score,
        matched_method
      )],
      by.x = "tnrs_submit_key",
      by.y = "Name_submitted",
      all.x = TRUE,
      sort = FALSE
    )

    stg[!is.na(matched_name), scrubbed_species_binomial := matched_name]
    stg[!is.na(matched_family), scrubbed_family := matched_family]
    stg[!is.na(matched_genus), scrubbed_genus := matched_genus]
    stg[!is.na(matched_author), scrubbed_author := matched_author]
    stg[!is.na(matched_status), scrubbed_taxonomic_status := matched_status]

    stg[, tnrs_submitted_name := tnrs_submit_key]
    stg[!is.na(matched_name), tnrs_matched_name := matched_name]
    stg[!is.na(matched_taxon_id), tnrs_taxon_id := matched_taxon_id]
    stg[!is.na(matched_authority), tnrs_authority := matched_authority]
    stg[!is.na(matched_score), tnrs_match_score := suppressWarnings(as.numeric(matched_score))]
    stg[!is.na(matched_method), tnrs_match_method := matched_method]
    stg[!is.na(tnrs_submitted_name) & !is.na(tnrs_matched_name), tnrs_name_changed := trimws(tnrs_submitted_name) != trimws(tnrs_matched_name)]
    stg[!is.na(tnrs_matched_name), taxon_resolution_status := "resolved_tnrs"]
    stg[is.na(tnrs_matched_name), taxon_resolution_status := fifelse(
      is.na(taxon_resolution_status) | trimws(taxon_resolution_status) == "",
      "unresolved_tnrs",
      taxon_resolution_status
    )]

    stg[, c("matched_name", "matched_family", "matched_genus", "matched_author", "matched_status", "matched_taxon_id", "matched_authority", "matched_score", "matched_method") := NULL]
  }

  # GNRS ---------------------------------------------------------------------
  gnrs_result_path <- file.path(args$validation_dir, "gnrs_results.tsv")
  gnrs_checkpoint <- file.path(args$validation_dir, "gnrs_checkpoint.tsv")

  ensure_columns(stg, c("country", "state_province", "county"))

  gnrs_queue <- unique(stg[, .(
    country = trim_na(country),
    state_province = trim_na(state_province),
    county = trim_na(county)
  )])
  gnrs_queue <- gnrs_queue[!is.na(country) | !is.na(state_province) | !is.na(county)]
  gnrs_queue[, key := paste0(fifelse(is.na(country), "", country), "\u001f", fifelse(is.na(state_province), "", state_province), "\u001f", fifelse(is.na(county), "", county))]

  gnrs_done <- character(0)
  if (isTRUE(args$resume) && file.exists(gnrs_result_path)) {
    g <- safe_read_tsv(gnrs_result_path)
    if (nrow(g) > 0L) {
      sc <- if ("country_verbatim" %in% names(g)) trim_na(g$country_verbatim) else if ("country" %in% names(g)) trim_na(g$country) else rep(NA_character_, nrow(g))
      ss <- if ("state_province_verbatim" %in% names(g)) trim_na(g$state_province_verbatim) else rep(NA_character_, nrow(g))
      sy <- if ("county_parish_verbatim" %in% names(g)) trim_na(g$county_parish_verbatim) else rep(NA_character_, nrow(g))
      gnrs_done <- unique(paste0(fifelse(is.na(sc), "", sc), "\u001f", fifelse(is.na(ss), "", ss), "\u001f", fifelse(is.na(sy), "", sy)))
    }
  }

  gnrs_remaining <- gnrs_queue[!key %in% gnrs_done]
  log_msg("GNRS queue:", nrow(gnrs_queue), "unique geographies; remaining:", nrow(gnrs_remaining))

  if (nrow(gnrs_remaining) > 0L) {
    gnrs_batches <- split(gnrs_remaining, ceiling(seq_len(nrow(gnrs_remaining)) / args$gnrs_batch_size))

    for (i in seq_along(gnrs_batches)) {
      batch <- as.data.table(gnrs_batches[[i]])
      batch_n <- nrow(batch)
      label <- paste0("batch ", i, "/", length(gnrs_batches), " (n=", batch_n, ")")
      log_msg("GNRS", label)

      payload_dt <- data.frame(
        id = seq_len(batch_n),
        country = ifelse(is.na(batch$country), "", batch$country),
        stateProvince = ifelse(is.na(batch$state_province), "", batch$state_province),
        county = ifelse(is.na(batch$county), "", batch$county),
        stringsAsFactors = FALSE
      )

      payload <- toJSON(
        list(opts = list(mode = "resolve", sources = "geonames,gadm"), data = payload_dt),
        auto_unbox = TRUE
      )

      batch_ok <- TRUE
      out <- tryCatch({
        txt <- post_with_retry(
          url = GNRS_URL,
          payload = payload,
          max_retries = args$max_retries,
          retry_base_seconds = args$retry_base_seconds,
          timeout_seconds = args$timeout_seconds,
          service_name = "GNRS",
          batch_label = label
        )
        obj <- fromJSON(txt, flatten = TRUE)
        dt <- extract_data_frame(obj)
        if (nrow(dt) == 0L) stop("GNRS response had zero rows", call. = FALSE)
        if (!"country" %in% names(dt) && "id" %in% names(dt)) {
          id_int <- suppressWarnings(as.integer(dt$id))
          dt[, country := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), ifelse(is.na(batch$country[id_int]), "", batch$country[id_int]), "")]
          dt[, stateProvince := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), ifelse(is.na(batch$state_province[id_int]), "", batch$state_province[id_int]), "")]
          dt[, county := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), ifelse(is.na(batch$county[id_int]), "", batch$county[id_int]), "")]
        }
        dt[, gnrs_timestamp_utc := format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")]
        dt
      }, error = function(e) {
        batch_ok <<- FALSE
        log_msg("GNRS", label, "FAILED:", conditionMessage(e))
        failed_path <- file.path(failed_dir, sprintf("gnrs_failed_batch_%04d.tsv", i))
        write_tsv(batch, failed_path)
        data.table()
      })

      if (batch_ok && nrow(out) > 0L) {
        append_results(out, gnrs_result_path, key_cols = c("country_verbatim", "state_province_verbatim", "county_parish_verbatim"))
      }

      done_now <- uniqueN(c(gnrs_done, if (nrow(out) > 0L) {
        cv <- if ("country_verbatim" %in% names(out)) trim_na(out$country_verbatim) else trim_na(out$country)
        sv <- if ("state_province_verbatim" %in% names(out)) trim_na(out$state_province_verbatim) else rep(NA_character_, nrow(out))
        yv <- if ("county_parish_verbatim" %in% names(out)) trim_na(out$county_parish_verbatim) else rep(NA_character_, nrow(out))
        paste0(fifelse(is.na(cv), "", cv), "\u001f", fifelse(is.na(sv), "", sv), "\u001f", fifelse(is.na(yv), "", yv))
      } else character(0)))
      write_checkpoint(gnrs_checkpoint, "gnrs", done_now, nrow(gnrs_queue), nrow(gnrs_queue) - done_now)
      if (args$pause_seconds > 0) Sys.sleep(args$pause_seconds)
    }
  }

  gnrs_res <- safe_read_tsv(gnrs_result_path)
  if (nrow(gnrs_res) > 0L) {
    # Actual GNRS response cols: country_verbatim, state_province_verbatim, county_parish_verbatim (submitted)
    # and country, state_province, county_parish (matched). No stateProvince or county.
    ensure_columns(gnrs_res, c("country_verbatim", "state_province_verbatim", "county_parish_verbatim",
                               "country", "state_province", "county_parish"))
    gnrs_res[, country_verbatim := trim_na(country_verbatim)]
    gnrs_res[, state_province_verbatim := trim_na(state_province_verbatim)]
    gnrs_res[, county_parish_verbatim := trim_na(county_parish_verbatim)]

    gnrs_res[, country_matched_out := first_non_empty(.SD, c("country", "Country_matched", "country_matched")), .SDcols = names(gnrs_res)]
    gnrs_res[, state_matched_out := first_non_empty(.SD, c("state_province", "StateProvince_matched", "stateProvince_matched", "stateProvince")), .SDcols = names(gnrs_res)]
    gnrs_res[, county_matched_out := first_non_empty(.SD, c("county_parish", "County_matched", "county_matched", "county")), .SDcols = names(gnrs_res)]

    gnrs_res[, key := paste0(
      fifelse(is.na(country_verbatim), "", country_verbatim), "\u001f",
      fifelse(is.na(state_province_verbatim), "", state_province_verbatim), "\u001f",
      fifelse(is.na(county_parish_verbatim), "", county_parish_verbatim)
    )]
    gnrs_res <- unique(gnrs_res, by = "key")

    stg[, gnrs_key := paste0(
      fifelse(is.na(trim_na(country)), "", trim_na(country)), "\u001f",
      fifelse(is.na(trim_na(state_province)), "", trim_na(state_province)), "\u001f",
      fifelse(is.na(trim_na(county)), "", trim_na(county))
    )]

    stg <- merge(
      stg,
      gnrs_res[, .(key, country_matched_out, state_matched_out, county_matched_out)],
      by.x = "gnrs_key",
      by.y = "key",
      all.x = TRUE,
      sort = FALSE
    )

    stg[!is.na(country_matched_out), country := country_matched_out]
    stg[!is.na(state_matched_out), state_province := state_matched_out]
    stg[!is.na(county_matched_out), county := county_matched_out]
    stg[, c("country_matched_out", "state_matched_out", "county_matched_out") := NULL]
  }

  # GVS ----------------------------------------------------------------------
  gvs_result_path <- file.path(args$validation_dir, "gvs_results.tsv")
  gvs_checkpoint <- file.path(args$validation_dir, "gvs_checkpoint.tsv")

  gvs_queue <- unique(stg[, .(
    latitude = suppressWarnings(as.numeric(trim_na(latitude))),
    longitude = suppressWarnings(as.numeric(trim_na(longitude)))
  )])
  gvs_queue <- gvs_queue[!is.na(latitude) & !is.na(longitude)]
  gvs_queue[, key := paste0(sprintf("%.8f", latitude), "\u001f", sprintf("%.8f", longitude))]

  gvs_done <- character(0)
  if (isTRUE(args$resume) && file.exists(gvs_result_path)) {
    g <- safe_read_tsv(gvs_result_path)
    if (nrow(g) > 0L) {
      lat_col <- if ("submitted_latitude" %in% names(g)) "submitted_latitude" else if ("latitude_verbatim" %in% names(g)) "latitude_verbatim" else if ("latitude" %in% names(g)) "latitude" else NA_character_
      lon_col <- if ("submitted_longitude" %in% names(g)) "submitted_longitude" else if ("longitude_verbatim" %in% names(g)) "longitude_verbatim" else if ("longitude" %in% names(g)) "longitude" else NA_character_
      if (!is.na(lat_col) && !is.na(lon_col)) {
        glat <- suppressWarnings(as.numeric(trim_na(g[[lat_col]])))
        glon <- suppressWarnings(as.numeric(trim_na(g[[lon_col]])))
        ok <- !is.na(glat) & !is.na(glon)
        gvs_done <- unique(paste0(sprintf("%.8f", glat[ok]), "\u001f", sprintf("%.8f", glon[ok])))
      }
    }
  }

  gvs_remaining <- gvs_queue[!key %in% gvs_done]
  log_msg("GVS queue:", nrow(gvs_queue), "unique coordinate pairs; remaining:", nrow(gvs_remaining))

  if (nrow(gvs_remaining) > 0L) {
    gvs_batches <- split(gvs_remaining, ceiling(seq_len(nrow(gvs_remaining)) / args$gvs_batch_size))

    for (i in seq_along(gvs_batches)) {
      batch <- as.data.table(gvs_batches[[i]])
      batch_n <- nrow(batch)
      label <- paste0("batch ", i, "/", length(gvs_batches), " (n=", batch_n, ")")
      log_msg("GVS", label)

      payload_data <- lapply(seq_len(batch_n), function(j) c(batch$latitude[j], batch$longitude[j]))
      payload <- toJSON(
        list(opts = list(mode = "resolve"), data = payload_data),
        auto_unbox = TRUE
      )

      batch_ok <- TRUE
      out <- tryCatch({
        txt <- post_with_retry(
          url = GVS_URL,
          payload = payload,
          max_retries = args$max_retries,
          retry_base_seconds = args$retry_base_seconds,
          timeout_seconds = args$timeout_seconds,
          service_name = "GVS",
          batch_label = label
        )
        obj <- fromJSON(txt, flatten = TRUE)
        dt <- extract_data_frame(obj)
        if (nrow(dt) == 0L) stop("GVS response had zero rows", call. = FALSE)

        if (!"submitted_latitude" %in% names(dt) || !"submitted_longitude" %in% names(dt)) {
          lat_col <- if ("latitude_verbatim" %in% names(dt)) "latitude_verbatim" else if ("latitude" %in% names(dt)) "latitude" else NA_character_
          lon_col <- if ("longitude_verbatim" %in% names(dt)) "longitude_verbatim" else if ("longitude" %in% names(dt)) "longitude" else NA_character_
          if (!is.na(lat_col) && !is.na(lon_col)) {
            dt[, submitted_latitude := suppressWarnings(as.numeric(trim_na(get(lat_col))))]
            dt[, submitted_longitude := suppressWarnings(as.numeric(trim_na(get(lon_col))))]
          } else if (nrow(dt) == nrow(batch)) {
            dt[, submitted_latitude := batch$latitude]
            dt[, submitted_longitude := batch$longitude]
          }
        }

        dt[, gvs_timestamp_utc := format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")]
        dt
      }, error = function(e) {
        batch_ok <<- FALSE
        log_msg("GVS", label, "FAILED:", conditionMessage(e))
        failed_path <- file.path(failed_dir, sprintf("gvs_failed_batch_%04d.tsv", i))
        write_tsv(batch, failed_path)
        data.table()
      })

      if (batch_ok && nrow(out) > 0L) {
        append_results(out, gvs_result_path, key_cols = c("submitted_latitude", "submitted_longitude"))
      }

      done_now <- uniqueN(c(gvs_done, if (nrow(out) > 0L) {
        olat <- suppressWarnings(as.numeric(trim_na(out$submitted_latitude)))
        olon <- suppressWarnings(as.numeric(trim_na(out$submitted_longitude)))
        ok <- !is.na(olat) & !is.na(olon)
        paste0(sprintf("%.8f", olat[ok]), "\u001f", sprintf("%.8f", olon[ok]))
      } else character(0)))
      write_checkpoint(gvs_checkpoint, "gvs", done_now, nrow(gvs_queue), nrow(gvs_queue) - done_now)
      if (args$pause_seconds > 0) Sys.sleep(args$pause_seconds)
    }
  }

  gvs_res <- safe_read_tsv(gvs_result_path)
  if (nrow(gvs_res) > 0L) {
    lat <- suppressWarnings(as.numeric(trim_na(gvs_res$submitted_latitude)))
    lon <- suppressWarnings(as.numeric(trim_na(gvs_res$submitted_longitude)))
    gvs_res[, submitted_latitude := lat]
    gvs_res[, submitted_longitude := lon]

    centroid_cols <- intersect(c("is_country_centroid", "is_state_centroid", "is_county_centroid"), names(gvs_res))
    if (length(centroid_cols) > 0L) {
      centroid_mat <- as.data.table(lapply(gvs_res[, ..centroid_cols], function(x) truthy(x)))
      gvs_res[, is_centroid_out := ifelse(Reduce(`|`, centroid_mat), "1", "0")]
    } else {
      gvs_res[, is_centroid_out := "0"]
    }

    gvs_res[, key := ifelse(!is.na(submitted_latitude) & !is.na(submitted_longitude), paste0(sprintf("%.8f", submitted_latitude), "\u001f", sprintf("%.8f", submitted_longitude)), NA_character_)]
    gvs_res <- unique(gvs_res[!is.na(key)], by = "key")

    stg[, gvs_key := ifelse(
      !is.na(suppressWarnings(as.numeric(latitude))) & !is.na(suppressWarnings(as.numeric(longitude))),
      paste0(sprintf("%.8f", suppressWarnings(as.numeric(latitude))), "\u001f", sprintf("%.8f", suppressWarnings(as.numeric(longitude)))),
      NA_character_
    )]

    stg <- merge(
      stg,
      gvs_res[, .(key, is_centroid_out)],
      by.x = "gvs_key",
      by.y = "key",
      all.x = TRUE,
      sort = FALSE
    )

    stg[!is.na(is_centroid_out), is_centroid := is_centroid_out]
    stg[, is_centroid_out := NULL]
  }

  # NSR ----------------------------------------------------------------------
  nsr_result_path <- file.path(args$validation_dir, "nsr_results.tsv")
  nsr_checkpoint <- file.path(args$validation_dir, "nsr_checkpoint.tsv")

  nsr_queue <- unique(stg[, .(
    taxon = trim_na(scrubbed_species_binomial),
    country = trim_na(country),
    state_province = trim_na(state_province),
    county_parish = trim_na(county)
  )])
  nsr_queue <- nsr_queue[!is.na(taxon)]
  nsr_queue[, key := paste0(
    fifelse(is.na(taxon), "", taxon), "\u001f",
    fifelse(is.na(country), "", country), "\u001f",
    fifelse(is.na(state_province), "", state_province), "\u001f",
    fifelse(is.na(county_parish), "", county_parish)
  )]

  nsr_done <- character(0)
  if (isTRUE(args$resume) && file.exists(nsr_result_path)) {
    n <- safe_read_tsv(nsr_result_path)
    if (nrow(n) > 0L) {
      tax_col <- if ("taxon" %in% names(n)) "taxon" else if ("species" %in% names(n)) "species" else NA_character_
      st_col <- if ("state_province" %in% names(n)) "state_province" else if ("stateProvince" %in% names(n)) "stateProvince" else NA_character_
      ct_col <- if ("county_parish" %in% names(n)) "county_parish" else if ("county" %in% names(n)) "county" else NA_character_
      if (!is.na(tax_col)) {
        nsr_done <- unique(paste0(
          fifelse(is.na(trim_na(n[[tax_col]])), "", trim_na(n[[tax_col]])), "\u001f",
          fifelse(is.na(trim_na(n$country)), "", trim_na(n$country)), "\u001f",
          fifelse(is.na(trim_na(n[[st_col]])), "", trim_na(n[[st_col]])), "\u001f",
          fifelse(is.na(trim_na(n[[ct_col]])), "", trim_na(n[[ct_col]]))
        ))
      }
    }
  }

  nsr_remaining <- nsr_queue[!key %in% nsr_done]
  log_msg("NSR queue:", nrow(nsr_queue), "unique taxon/location combos; remaining:", nrow(nsr_remaining))

  if (nrow(nsr_remaining) > 0L) {
    nsr_batches <- split(nsr_remaining, ceiling(seq_len(nrow(nsr_remaining)) / args$nsr_batch_size))

    for (i in seq_along(nsr_batches)) {
      batch <- as.data.table(nsr_batches[[i]])
      batch_n <- nrow(batch)
      label <- paste0("batch ", i, "/", length(nsr_batches), " (n=", batch_n, ")")
      log_msg("NSR", label)

      payload_dt <- data.frame(
        taxon = ifelse(is.na(batch$taxon), "", batch$taxon),
        country = ifelse(is.na(batch$country), "", batch$country),
        state_province = ifelse(is.na(batch$state_province), "", batch$state_province),
        county_parish = ifelse(is.na(batch$county_parish), "", batch$county_parish),
        user_id = seq_len(batch_n),
        stringsAsFactors = FALSE
      )

      payload <- toJSON(
        list(opts = list(mode = "resolve"), data = payload_dt),
        auto_unbox = TRUE
      )

      batch_ok <- TRUE
      out <- tryCatch({
        txt <- post_with_retry(
          url = NSR_URL,
          payload = payload,
          max_retries = args$max_retries,
          retry_base_seconds = args$retry_base_seconds,
          timeout_seconds = args$timeout_seconds,
          service_name = "NSR",
          batch_label = label
        )

        raw <- fromJSON(txt)
        dt <- data.table()
        if (is.list(raw) && "id" %in% names(raw)) {
          col_names <- as.character(raw$id)
          row_ids <- setdiff(names(raw), "id")
          if (length(row_ids) > 0L) {
            rows <- lapply(row_ids, function(k) {
              vals <- raw[[k]]
              vals <- as.list(vals)
              names(vals) <- col_names
              as.data.table(vals)
            })
            dt <- rbindlist(rows, fill = TRUE)
          }
        } else {
          dt <- extract_data_frame(raw)
        }

        if (nrow(dt) == 0L) stop("NSR response had zero rows", call. = FALSE)

        if (!"taxon" %in% names(dt) && !"species" %in% names(dt)) {
          if ("user_id" %in% names(dt)) {
            id_int <- suppressWarnings(as.integer(dt$user_id))
            dt[, taxon := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), batch$taxon[id_int], NA_character_)]
            dt[, country := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), batch$country[id_int], NA_character_)]
            dt[, state_province := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), batch$state_province[id_int], NA_character_)]
            dt[, county_parish := ifelse(!is.na(id_int) & id_int >= 1L & id_int <= nrow(batch), batch$county_parish[id_int], NA_character_)]
          } else if (nrow(dt) == nrow(batch)) {
            dt[, taxon := batch$taxon]
            dt[, country := batch$country]
            dt[, state_province := batch$state_province]
            dt[, county_parish := batch$county_parish]
          }
        }

        dt[, nsr_timestamp_utc := format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")]
        dt
      }, error = function(e) {
        batch_ok <<- FALSE
        log_msg("NSR", label, "FAILED:", conditionMessage(e))
        failed_path <- file.path(failed_dir, sprintf("nsr_failed_batch_%04d.tsv", i))
        write_tsv(batch, failed_path)
        data.table()
      })

      if (batch_ok && nrow(out) > 0L) {
        append_results(out, nsr_result_path, key_cols = c("taxon", "country", "state_province", "county_parish"))
      }

      done_now <- uniqueN(c(nsr_done, if (nrow(out) > 0L) {
        tax_col <- if ("taxon" %in% names(out)) "taxon" else if ("species" %in% names(out)) "species" else NA_character_
        st_col <- if ("state_province" %in% names(out)) "state_province" else if ("stateProvince" %in% names(out)) "stateProvince" else NA_character_
        ct_col <- if ("county_parish" %in% names(out)) "county_parish" else if ("county" %in% names(out)) "county" else NA_character_
        if (!is.na(tax_col)) {
          paste0(
            fifelse(is.na(trim_na(out[[tax_col]])), "", trim_na(out[[tax_col]])), "\u001f",
            fifelse(is.na(trim_na(out$country)), "", trim_na(out$country)), "\u001f",
            fifelse(is.na(trim_na(out[[st_col]])), "", trim_na(out[[st_col]])), "\u001f",
            fifelse(is.na(trim_na(out[[ct_col]])), "", trim_na(out[[ct_col]]))
          )
        } else character(0)
      } else character(0)))
      write_checkpoint(nsr_checkpoint, "nsr", done_now, nrow(nsr_queue), nrow(nsr_queue) - done_now)
      if (args$pause_seconds > 0) Sys.sleep(args$pause_seconds)
    }
  }

  nsr_res <- safe_read_tsv(nsr_result_path)
  if (nrow(nsr_res) > 0L) {
    tax_col <- if ("taxon" %in% names(nsr_res)) "taxon" else if ("species" %in% names(nsr_res)) "species" else NA_character_
    st_col <- if ("state_province" %in% names(nsr_res)) "state_province" else if ("stateProvince" %in% names(nsr_res)) "stateProvince" else NA_character_
    ct_col <- if ("county_parish" %in% names(nsr_res)) "county_parish" else if ("county" %in% names(nsr_res)) "county" else NA_character_

    if (!is.na(tax_col)) {
      nsr_res[, taxon_in := trim_na(get(tax_col))]
      nsr_res[, country_in := trim_na(country)]
      nsr_res[, state_in := if (!is.na(st_col)) trim_na(get(st_col)) else NA_character_]
      nsr_res[, county_in := if (!is.na(ct_col)) trim_na(get(ct_col)) else NA_character_]
      nsr_res[, key := paste0(
        fifelse(is.na(taxon_in), "", taxon_in), "\u001f",
        fifelse(is.na(country_in), "", country_in), "\u001f",
        fifelse(is.na(state_in), "", state_in), "\u001f",
        fifelse(is.na(county_in), "", county_in)
      )]
      nsr_res <- unique(nsr_res[!is.na(taxon_in)], by = "key")

      stg[, nsr_key := paste0(
        fifelse(is.na(trim_na(scrubbed_species_binomial)), "", trim_na(scrubbed_species_binomial)), "\u001f",
        fifelse(is.na(trim_na(country)), "", trim_na(country)), "\u001f",
        fifelse(is.na(trim_na(state_province)), "", trim_na(state_province)), "\u001f",
        fifelse(is.na(trim_na(county)), "", trim_na(county))
      )]

      map_cols <- intersect(c(
        "native_status", "native_status_reason", "native_status_country",
        "native_status_state_province", "native_status_county_parish",
        "isIntroduced", "is_introduced",
        "isCultivatedNSR", "is_cultivated_observation"
      ), names(nsr_res))

      stg <- merge(
        stg,
        nsr_res[, c("key", map_cols), with = FALSE],
        by.x = "nsr_key",
        by.y = "key",
        all.x = TRUE,
        sort = FALSE,
        suffixes = c("", "_nsr")
      )

      for (nm in c("native_status", "native_status_reason", "native_status_country", "native_status_state_province", "native_status_county_parish")) {
        src <- paste0(nm, "_nsr")
        if (src %in% names(stg)) stg[!is.na(trim_na(get(src))), (nm) := trim_na(get(src))]
      }
      if ("isIntroduced_nsr" %in% names(stg)) stg[!is.na(trim_na(isIntroduced_nsr)), is_introduced := trim_na(isIntroduced_nsr)]
      if ("is_introduced_nsr" %in% names(stg)) stg[!is.na(trim_na(is_introduced_nsr)), is_introduced := trim_na(is_introduced_nsr)]
      if ("isCultivatedNSR_nsr" %in% names(stg)) stg[!is.na(trim_na(isCultivatedNSR_nsr)), is_cultivated_observation := trim_na(isCultivatedNSR_nsr)]
      if ("is_cultivated_observation_nsr" %in% names(stg)) stg[!is.na(trim_na(is_cultivated_observation_nsr)), is_cultivated_observation := trim_na(is_cultivated_observation_nsr)]

      drop_cols <- grep("_nsr$", names(stg), value = TRUE)
      if (length(drop_cols) > 0L) stg[, (drop_cols) := NULL]
    }
  }

  # Finalize -----------------------------------------------------------------
  drop_tmp <- intersect(c("tnrs_submit_key", "gnrs_key", "gvs_key", "nsr_key"), names(stg))
  if (length(drop_tmp) > 0L) stg[, (drop_tmp) := NULL]

  write_tsv(stg, final_out)

  log_msg("Pipeline complete")
  log_msg("Final staging rows:", format(nrow(stg), big.mark = ","))
  log_msg("TNRS results:", if (file.exists(tnrs_result_path)) nrow(safe_read_tsv(tnrs_result_path)) else 0L)
  log_msg("GNRS results:", if (file.exists(gnrs_result_path)) nrow(safe_read_tsv(gnrs_result_path)) else 0L)
  log_msg("GVS results:", if (file.exists(gvs_result_path)) nrow(safe_read_tsv(gvs_result_path)) else 0L)
  log_msg("NSR results:", if (file.exists(nsr_result_path)) nrow(safe_read_tsv(nsr_result_path)) else 0L)
  log_msg("Validated output:", final_out)
}

main()
