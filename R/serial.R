# 2026- EDG rtemis.org

# Arrow IPC encoders for `job.result` payloads. Pairs with
# `decode_arrow_ipc()` in `rtemislive_data.R` (which goes the other way).
#
# Encoders return raw vectors ready to attach as the `payload` of a wire
# frame. The matching response header carries a small JSON pointer
# (`{rows, cols, columns}`) so the client can size buffers before decoding.

# %% encode_arrow_ipc --------------------------------------------------------

#' Encode a tabular object as Arrow IPC stream bytes
#'
#' Used by `job.result` slices that ship bulk tabular data (predictions,
#' full datasets, etc.). The byte stream is what `arrow::read_ipc_stream()`
#' / `arrow.js` / DuckDB-WASM accept directly.
#'
#' @param df data.frame, data.table, or `arrow::Table` - Anything coercible
#'   to an Arrow Table via [arrow::arrow_table()].
#'
#' @return Raw vector - Arrow IPC stream bytes.
#'
#' @author EDG
#' @keywords internal
#' @noRd
encode_arrow_ipc <- function(df) {
  rtemis.core::check_dependencies("arrow")
  tbl <- tryCatch(
    arrow::arrow_table(df),
    error = function(e) {
      rtemis.core::abort(
        "Could not coerce object to an Arrow Table: ",
        conditionMessage(e),
        parent = e
      )
    }
  )
  buf <- arrow::write_to_raw(tbl, format = "stream")
  as.raw(buf)
}


# %% Predictions table -------------------------------------------------------

#' Build a long-format predictions table for a `Supervised` result
#'
#' [rtemis::get_predictions()] of every stored sample, with the wire's column
#' names: `split` (the sample), `fold` (the outer resample, resampled results
#' only), `actual`, `predicted`, and one `prob_<class>` column per stored
#' probability column of a classifier.
#'
#' @param sup `Supervised` or `SupervisedRes` object. Other inputs error -
#'   `job.result` callers should switch on `inherits` first.
#'
#' @return `data.table`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
predictions_table <- function(sup) {
  if (
    !inherits(sup, "rtemis::Supervised") &&
      !inherits(sup, "rtemis::SupervisedRes")
  ) {
    rtemis.core::abort(
      "`predictions` slice requires a `Supervised` or `SupervisedRes` result.",
      class = "rtemislive_invalid_params"
    )
  }
  out <- rtemis::get_predictions(sup)
  data.table::setnames(
    out,
    c("sample", "resample", "true"),
    c("split", "fold", "actual"),
    skip_absent = TRUE
  )
  out
}


# %% ROC curve table ---------------------------------------------------------

#' Build a long-format ROC-curve table for a classification result
#'
#' [rtemis::roc_table()] of every sample with predicted probabilities, the data
#' `rtemis::plot_roc()` draws, under the wire's column names: `split`, `class`,
#' `fold`, `fpr`, `tpr`, `auc`. `fold` is `"aggregate"` for the curve pooled
#' across all resamples and the resample identifier for the per-resample
#' curves of a resampled fit; a single fit has the aggregate curves only.
#' Regression results and classifiers without predicted probabilities yield a
#' zero-row table.
#'
#' @param sup `Supervised` / `SupervisedRes` object.
#' @param max_points Integer: Cap on curve vertices per curve.
#'
#' @return `data.table` with columns `split`, `class`, `fold`, `fpr`, `tpr`,
#'   `auc`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
roc_table <- function(sup, max_points = 1000L) {
  columns <- c("split", "class", "fold", "fpr", "tpr", "auc")
  empty <- data.table::data.table(
    split = character(0),
    class = character(0),
    fold = character(0),
    fpr = numeric(0),
    tpr = numeric(0),
    auc = numeric(0)
  )
  if (
    !inherits(sup, "rtemis::Classification") &&
      !inherits(sup, "rtemis::ClassificationRes")
  ) {
    return(empty)
  }
  # rtemis raises `rtemis_null_input` for a classifier that stores no
  # predicted probabilities.
  aggregate <- tryCatch(
    rtemis::roc_table(sup, max_points = max_points),
    rtemis_null_input = function(e) NULL
  )
  if (is.null(aggregate)) {
    return(empty)
  }
  aggregate[, let(fold = "aggregate")]
  pieces <- list(aggregate)
  if (inherits(sup, "rtemis::ClassificationRes")) {
    per_resample <- rtemis::roc_table(
      sup,
      variant = "per_resample",
      max_points = max_points
    )
    data.table::setnames(per_resample, "resample", "fold")
    pieces[[2L]] <- per_resample
  }
  out <- data.table::rbindlist(pieces, use.names = TRUE, fill = TRUE)
  data.table::setnames(out, "sample", "split")
  out[, columns, with = FALSE]
}


# %% Variable importance table ----------------------------------------------

#' Variable importance table for the `varimp` slice
#'
#' The ranking of [rtemis::varimp_ranking()]: one row per measure, variable
#' and resample with an available score, with `measure`, `variable`, `fold`
#' (resampled results only), `value`, `summary` (the mean across resamples)
#' and `rank`, in rank order within each measure. `top_n` keeps the rows of
#' ranks `1` to `top_n` of each measure.
#'
#' @param sup `Supervised` or `SupervisedRes`.
#' @param top_n Optional Integer `[1, Inf)`: Number of ranks to keep per
#'   measure. `NULL` keeps every rank.
#'
#' @return Named list: `table`, a data.table, and `variables`, the number of
#'   ranked variables of each measure before `top_n` is applied, keyed by
#'   measure; or `NULL` when `sup` is not a supervised result or reports no
#'   importance.
#'
#' @author EDG
#' @keywords internal
#' @noRd
varimp_table <- function(sup, top_n = NULL) {
  ranking <- tryCatch(rtemis::varimp_ranking(sup), error = function(e) NULL)
  if (is.null(ranking)) {
    return(NULL)
  }
  measures <- unique(ranking[["measure"]])
  variables <- lapply(
    stats::setNames(nm = measures),
    function(m) max(ranking[["rank"]][ranking[["measure"]] == m])
  )
  if (!is.null(top_n)) {
    ranking <- ranking[ranking[["rank"]] <= top_n, , drop = FALSE]
  }
  list(table = data.table::as.data.table(ranking), variables = variables)
}


# %% Report Markdown ---------------------------------------------------------

#' Markdown of a supervised result's review or writeup
#'
#' Renders `rtemis::review()` or `rtemis::writeup()` of `sup` with
#' `rtemis::to_markdown()`. Top-level sections are `##` headings.
#'
#' @param sup `Supervised` or `SupervisedRes`.
#' @param report Character: `"review"` or `"writeup"`.
#'
#' @return Character scalar.
#'
#' @author EDG
#' @keywords internal
#' @noRd
report_markdown <- function(sup, report = c("review", "writeup")) {
  report <- match.arg(report)
  if (
    !inherits(sup, "rtemis::Supervised") &&
      !inherits(sup, "rtemis::SupervisedRes")
  ) {
    rtemis.core::abort(
      "`",
      report,
      "` slice requires a `Supervised` or `SupervisedRes` result.",
      class = "rtemislive_invalid_params"
    )
  }
  x <- switch(
    report,
    review = rtemis::review(sup),
    writeup = rtemis::writeup(sup)
  )
  rtemis::to_markdown(x)
}


# %% Session timeline table --------------------------------------------------

#' Execution timeline table for the `session` slice
#'
#' [rtemis::session_timeline()] of a `Supervised` / `SupervisedRes` result:
#' one row per execution node of the session `rtemis::train()` recorded, in
#' depth-first order, with columns `label`, `start`, `end` (milliseconds from
#' session start), `kind`, `status`, `failed`, and `tip`.
#' The per-kind fill colors ship separately in the slice pointer (see
#' `handle_job_result`), via [rtemis::session_kind_colors()].
#'
#' @param sup `Supervised` / `SupervisedRes` object.
#'
#' @return `data.table`, or `NULL` when the result is not a supervised model,
#'   carries no session, or the session recorded no events.
#'
#' @author EDG
#' @keywords internal
#' @noRd
session_table <- function(sup) {
  if (
    !inherits(sup, "rtemis::Supervised") &&
      !inherits(sup, "rtemis::SupervisedRes")
  ) {
    return(NULL)
  }
  timeline <- rtemis::session_timeline(sup)
  if (NROW(timeline) == 0L) {
    return(NULL)
  }
  timeline
}


# %% Decomposition slices ---------------------------------------------------

#' Component scores for the `transformed` slice
#'
#' [rtemis::transformed()] of a `Decomposition`: one row per fitted case, one
#' column per component, named by the decomposition.
#'
#' @param dec `Decomposition`.
#'
#' @return data.table.
#'
#' @author EDG
#' @keywords internal
#' @noRd
transformed_table <- function(dec) {
  rtemis::transformed(dec)
}


#' Loadings matrix for the `loadings` slice
#'
#' Wraps `rtemis::get_loadings()` of a `Decomposition`, a features by
#' components matrix, as a data.table with a leading `variable` column
#' holding the feature names.
#'
#' @param dec `Decomposition`.
#'
#' @return data.table, or NULL when the algorithm has no loadings.
#'
#' @author EDG
#' @keywords internal
#' @noRd
loadings_table <- function(dec) {
  m <- rtemis::get_loadings(dec)
  if (is.null(m)) {
    return(NULL)
  }
  dt <- data.table::as.data.table(m)
  dt[, let(variable = rownames(m))]
  data.table::setcolorder(dt, "variable")
  dt
}


#' Cluster assignments for the `assignments` slice
#'
#' [rtemis::clusters()] of a `Clustering` as a one-column data.table named
#' `cluster`. Row order matches the input data row order, so the client can
#' left-join the assignment column back to the parent table via `rowid`, as
#' for the decomposition `transformed` slice.
#'
#' @param clu `Clustering`.
#'
#' @return data.table with a single integer column `cluster`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
assignments_table <- function(clu) {
  data.table::data.table(cluster = rtemis::clusters(clu))
}


# %% Response-with-payload helper -------------------------------------------

#' Build a `{header, payload}` response envelope
#'
#' Handlers normally return a header (a plain JSON-able list). When a
#' handler wants to ship a binary payload alongside the JSON header, it
#' returns the result of `make_response_payload()` instead. The loop
#' detects the wrapped form by the absence of `v` at the top level (see
#' `process_connection()` in `rtemislive_loop.R`).
#'
#' @param id Character: Request correlation id.
#' @param result Optional list: Response JSON body.
#' @param payload Raw vector: Binary blob to attach.
#'
#' @return `list(header, payload)`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
make_response_payload <- function(id, result, payload) {
  if (!is.raw(payload)) {
    rtemis.core::abort("`payload` must be a raw vector.")
  }
  list(
    header = make_response(id, result),
    payload = payload
  )
}


# %% summary_json -----------------------------------------------------------

# Names of fields stripped from the summary slice - they have dedicated
# slices that ship as Arrow IPC. Listed both for `Supervised`
# (`varimp`) and `SupervisedRes` (`varimp_per_resample`).
# For `Decomposition`: `transformed` is the projection matrix and `decom`
# the raw backend model - both can be many MB and have dedicated slices
# (`transformed`, `loadings`).
.SUMMARY_STRIP_TOP <- c(
  "varimp",
  "varimp_per_resample",
  "transformed",
  "decom",
  "clusters",
  "clust"
)

# Inside `MetricsRes`-shaped slots, `res_metrics` is the per-resample
# breakdown - replaceable from `varimp` slice queries downstream, and
# typically huge for many-fold runs.
.SUMMARY_STRIP_METRICS <- c("res_metrics")


#' Headline JSON for the `summary` slice
#'
#' Builds the lightweight summary envelope: full `to_json(result)` with
#' heavy tabular sub-trees pruned (varimp, per-resample residuals). The
#' pruned fields are available via their own slices as Arrow IPC.
#'
#' @param result A trained `Supervised` / `SupervisedRes` (or other
#'   to_json-able object).
#'
#' @return Named list suitable for `make_response()`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
summary_json <- function(result) {
  out <- to_json(result)
  for (k in .SUMMARY_STRIP_TOP) {
    if (!is.null(out[[k]])) {
      # Replace with a small pointer so clients know data exists and can
      # fetch the dedicated slice.
      out[[k]] <- list(available = TRUE, fetch_via = k)
    }
  }
  for (split in c("metrics_training", "metrics_validation", "metrics_test")) {
    slice <- out[[split]]
    if (is.list(slice)) {
      for (k in .SUMMARY_STRIP_METRICS) {
        slice[[k]] <- NULL
      }
      out[[split]] <- slice
    }
  }
  out
}


# %% record_json ------------------------------------------------------------

# Result classes `rtemis::record()` has a method for. Checked up front so a
# result without one (a data job's table) is reported unavailable rather than
# reaching the client as an S7 dispatch failure.
.RECORDABLE <- c(
  "rtemis::Supervised",
  "rtemis::SupervisedRes",
  "rtemis::Decomposition",
  "rtemis::Clustering"
)

# The record rides in the frame *header*, which `encode_frame()` caps at
# `.RTEMISLIVE_HEADER_MAX`; the envelope around it (`v`, `id`, `ok`) is a few
# dozen bytes, and the margin covers that with room to spare. Over the cap
# `encode_frame()` aborts inside the write loop, where `serve.R` catches it into
# a warning and sends *nothing* -- the client's request would never resolve. So
# the size is measured here, where there is still a response to send.
.RTEMISLIVE_RECORD_MAX <- .RTEMISLIVE_HEADER_MAX - 4096L

#' Run record for the `record` slice
#'
#' `rtemis::record()` states what the run resolved: every config field with the
#' origin it came from (`user` / `default` / `derived` / `tuned` / `unset`), the
#' values each fold settled on, the tuning grid, and provenance -- versions,
#' platform, timing, and a `DataFingerprint` per dataset. The document validates
#' against `https://schema.rtemis.org/<family>/v1/record.json`, so it is
#' returned bare: what the client copies or downloads is exactly what the
#' registry validates.
#'
#' Absence is reported the way the tabular slices report it -- a small pointer
#' rather than an error frame -- because neither case below is a failure of the
#' request:
#'
#' - a result with no record method (a data job's table);
#' - a model carrying no input config: only a top-level `train()` stores one, so
#'   a per-fold sub-model or a model from an older rtemis cannot say what was
#'   asked for, and `rtemis::record()` aborts rather than guess;
#' - a record larger than the frame header allows.
#'
#' @param result A job result object.
#'
#' @return Named list: the record document, or
#'   `list(available = FALSE, reason = ..., bytes = ...)`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
record_json <- function(result) {
  if (!inherits(result, .RECORDABLE)) {
    return(list(available = FALSE, reason = "unsupported"))
  }
  rec <- tryCatch(
    rtemis::record(result),
    rtemis_null_input = function(e) NULL
  )
  if (is.null(rec)) {
    return(list(available = FALSE, reason = "no_input_config"))
  }
  bytes <- length(charToRaw(as.character(jsonlite::toJSON(
    rec,
    auto_unbox = TRUE,
    null = "null",
    na = "null"
  ))))
  if (bytes > .RTEMISLIVE_RECORD_MAX) {
    return(list(available = FALSE, reason = "too_large", bytes = bytes))
  }
  rec
}
