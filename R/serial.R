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
      cli::cli_abort(
        "Could not coerce object to an Arrow Table.",
        parent = e
      )
    }
  )
  buf <- arrow::write_to_raw(tbl, format = "stream")
  as.raw(buf)
} # /rtemis::encode_arrow_ipc


# %% Predictions table -------------------------------------------------------

#' Build a long-format predictions table for a `Supervised` result
#'
#' Stacks training / validation / test predictions into a single
#' `data.table` with `split`, `actual`, `predicted` columns (and, for
#' classification with class probabilities, one `prob_<class>` column per
#' level). Splits without observations are omitted.
#'
#' This is the v1 schema for the `predictions` slice; the goal is a
#' tidy, browser-friendly shape that DuckDB-WASM can query without
#' reshape.
#'
#' @param sup `Supervised` object (Regression / Classification / SurvivalRes,
#'   etc.). For non-Supervised inputs this errors - `job.result` callers
#'   should switch on `inherits` first.
#'
#' @return `data.table` (zero rows is possible if no split has data).
#'
#' @author EDG
#' @keywords internal
#' @noRd
predictions_table <- function(sup) {
  if (!inherits(sup, "rtemis::Supervised")) {
    cli::cli_abort(
      "`predictions` slice requires a `Supervised` result.",
      class = "rtemislive_invalid_params"
    )
  }

  splits <- list(
    training = list(
      actual = prop(sup, "y_training"),
      predicted = prop(sup, "predicted_training")
    ),
    validation = list(
      actual = prop(sup, "y_validation"),
      predicted = prop(sup, "predicted_validation")
    ),
    test = list(
      actual = prop(sup, "y_test"),
      predicted = prop(sup, "predicted_test")
    )
  )

  prob_props <- list(
    training = "predicted_prob_training",
    validation = "predicted_prob_validation",
    test = "predicted_prob_test"
  )
  has_probs <- inherits(sup, "rtemis::Classification")

  pieces <- list()
  for (split_name in names(splits)) {
    split <- splits[[split_name]]
    actual <- split[["actual"]]
    predicted <- split[["predicted"]]
    if (is.null(actual) || is.null(predicted)) {
      next
    }
    n <- length(predicted)
    if (n == 0L) {
      next
    }
    # Align actual to predicted length defensively - for resampled fits
    # `y_training` may be a list-of-vectors; only the simple vector case
    # gets a real `actual` column.
    if (length(actual) != n) {
      actual <- rep(NA, n)
    }
    piece <- data.table::data.table(
      split = split_name,
      actual = actual,
      predicted = predicted
    )
    if (has_probs) {
      probs <- tryCatch(
        prop(sup, prob_props[[split_name]]),
        error = function(e) NULL
      )
      if (!is.null(probs) && (is.data.frame(probs) || is.matrix(probs))) {
        probs <- as.data.frame(probs, stringsAsFactors = FALSE)
        if (NROW(probs) == n) {
          colnames(probs) <- paste0("prob_", colnames(probs))
          piece <- cbind(piece, probs)
        }
      }
    }
    pieces[[split_name]] <- piece
  }

  if (length(pieces) == 0L) {
    return(data.table::data.table(
      split = character(0),
      actual = numeric(0),
      predicted = numeric(0)
    ))
  }
  data.table::rbindlist(pieces, fill = TRUE, use.names = TRUE)
} # /rtemis::predictions_table


# %% Variable importance table ----------------------------------------------

#' Extract the variable-importance table from a `Supervised`
#'
#' Returns the underlying `data.table` from `VariableImportance` so it can
#' either be JSON-serialized (small) or Arrow-encoded (large). `NULL`
#' when the algorithm exposes no varimp.
#'
#' @param sup `Supervised`.
#'
#' @return data.table or NULL.
#'
#' @author EDG
#' @keywords internal
#' @noRd
varimp_table <- function(sup) {
  if (!inherits(sup, "rtemis::Supervised")) {
    return(NULL)
  }
  vi <- prop(sup, "varimp")
  if (is.null(vi)) {
    return(NULL)
  }
  if (inherits(vi, "rtemis::VariableImportance")) {
    return(prop(vi, "data"))
  }
  NULL
} # /rtemis::varimp_table


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
    cli::cli_abort("`payload` must be a raw vector.")
  }
  list(
    header = make_response(id, result),
    payload = payload
  )
} # /rtemis::make_response_payload


# %% summary_json -----------------------------------------------------------

# Names of fields stripped from the summary slice - they have dedicated
# slices that ship as Arrow IPC. Listed both for `Supervised`
# (`varimp`) and `SupervisedRes` (`varimp_per_resample`).
.SUMMARY_STRIP_TOP <- c("varimp", "varimp_per_resample")

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
} # /rtemis::summary_json
