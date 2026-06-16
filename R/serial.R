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
  if (inherits(sup, "rtemis::SupervisedRes")) {
    return(predictions_table_resampled(sup))
  }
  if (!inherits(sup, "rtemis::Supervised")) {
    rtemis.core::abort(
      "`predictions` slice requires a `Supervised` or `SupervisedRes` result.",
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
    # Align actual to predicted length defensively.
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
}


#' Long-format predictions table for a `SupervisedRes` result
#'
#' For resampled fits `y_training` / `predicted_training` / `y_test` /
#' `predicted_test` are *lists* of per-fold vectors. We stack them into
#' a single long table with `split`, `fold`, `actual`, `predicted`
#' columns (no validation split exists for SupervisedRes).
#'
#' Probabilities are skipped for v1 - the per-fold structure makes it
#' awkward to cbind heterogeneous prob columns and the UI doesn't need
#' them yet.
#'
#' @param sup `SupervisedRes` object.
#'
#' @return `data.table`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
predictions_table_resampled <- function(sup) {
  splits <- list(
    training = list(
      actual = prop(sup, "y_training"),
      predicted = prop(sup, "predicted_training")
    ),
    test = list(
      actual = prop(sup, "y_test"),
      predicted = prop(sup, "predicted_test")
    )
  )

  pieces <- list()
  for (split_name in names(splits)) {
    split <- splits[[split_name]]
    actual_list <- split[["actual"]]
    pred_list <- split[["predicted"]]
    if (is.null(actual_list) || is.null(pred_list)) {
      next
    }
    if (!is.list(actual_list)) {
      actual_list <- list(actual_list)
    }
    if (!is.list(pred_list)) {
      pred_list <- list(pred_list)
    }
    fold_labels <- names(pred_list)
    if (is.null(fold_labels) || any(!nzchar(fold_labels))) {
      fold_labels <- as.character(seq_along(pred_list))
    }
    for (i in seq_along(pred_list)) {
      predicted <- pred_list[[i]]
      actual <- if (i <= length(actual_list)) actual_list[[i]] else NULL
      n <- length(predicted)
      if (n == 0L) {
        next
      }
      if (is.null(actual) || length(actual) != n) {
        actual <- rep(NA, n)
      }
      pieces[[length(pieces) + 1L]] <- data.table::data.table(
        split = split_name,
        fold = fold_labels[i],
        actual = actual,
        predicted = predicted
      )
    }
  }

  if (length(pieces) == 0L) {
    return(data.table::data.table(
      split = character(0),
      fold = character(0),
      actual = numeric(0),
      predicted = numeric(0)
    ))
  }
  data.table::rbindlist(pieces, fill = TRUE, use.names = TRUE)
}


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
  vi <- tryCatch(rtemis::get_varimp(sup), error = function(e) NULL)
  if (is.null(vi)) {
    return(NULL)
  }

  vi_data <- function(x) {
    if (inherits(x, "rtemis::VariableImportance")) {
      prop(x, "data")
    } else if (data.table::is.data.table(x) || is.data.frame(x)) {
      data.table::as.data.table(x)
    } else {
      NULL
    }
  }

  # Single Supervised: one VariableImportance.
  if (inherits(vi, "rtemis::VariableImportance")) {
    return(vi_data(vi))
  }

  # SupervisedRes: list of VariableImportance, one per fold. Combine
  # into a long-by-fold table so the UI can render a boxplot. Fold
  # names come from the list names when present, else integer indices.
  if (is.list(vi)) {
    fold_labels <- names(vi)
    if (is.null(fold_labels) || any(!nzchar(fold_labels))) {
      fold_labels <- as.character(seq_along(vi))
    }
    pieces <- list()
    for (i in seq_along(vi)) {
      dt <- vi_data(vi[[i]])
      if (is.null(dt) || NROW(dt) == 0L) {
        next
      }
      dt <- data.table::copy(dt)
      dt[, let(fold = fold_labels[i])]
      # Move `fold` to the second column for stable display order
      # (`variable` stays first).
      cols <- names(dt)
      ordered <- c(
        "variable",
        "fold",
        setdiff(cols, c("variable", "fold"))
      )
      data.table::setcolorder(dt, ordered)
      pieces[[length(pieces) + 1L]] <- dt
    }
    if (length(pieces) == 0L) {
      return(NULL)
    }
    return(data.table::rbindlist(pieces, use.names = TRUE, fill = TRUE))
  }

  NULL
}


# %% Decomposition slices ---------------------------------------------------

#' Projection matrix for the `transformed` slice
#'
#' Extracts `prop(result, "transformed")` from a `Decomposition` and
#' wraps it as a data.table. Column names are preserved when the
#' decomposition backend names them (e.g. `PC1`, `PC2`, ...); otherwise
#' synthesised as `V1`, `V2`, ...
#'
#' @param dec `Decomposition`.
#'
#' @return data.table or NULL when no projection is available.
#'
#' @author EDG
#' @keywords internal
#' @noRd
transformed_table <- function(dec) {
  m <- tryCatch(prop(dec, "transformed"), error = function(e) NULL)
  if (is.null(m)) {
    return(NULL)
  }
  m <- as.matrix(m)
  if (is.null(colnames(m))) {
    colnames(m) <- paste0("V", seq_len(NCOL(m)))
  }
  data.table::as.data.table(m)
}


#' Loadings matrix for the `loadings` slice
#'
#' Extracts the algorithm-specific loadings (interpretable variables ×
#' components matrix) from a `Decomposition` and wraps it as a
#' data.table with a leading `variable` column. Returns NULL for
#' algorithms that have no loadings concept (UMAP, tSNE, Isomap).
#'
#' Per-algorithm sources (see `R/decomp_*.R`):
#'
#' - PCA: `decom$rotation` (variables × PCs)
#' - ICA: `decom$A` (mixing matrix; sources × variables, transposed)
#' - NMF: `NMF::basis(decom)` (variables × components)
#'
#' @param dec `Decomposition`.
#'
#' @return data.table or NULL when loadings are not defined for the
#'   algorithm.
#'
#' @author EDG
#' @keywords internal
#' @noRd
loadings_table <- function(dec) {
  algo <- tryCatch(prop(dec, "algorithm"), error = function(e) NA_character_)
  decom <- tryCatch(prop(dec, "decom"), error = function(e) NULL)
  if (is.null(decom) || is.na(algo)) {
    return(NULL)
  }

  m <- switch(
    algo,
    PCA = decom[["rotation"]],
    ICA = {
      a <- decom[["A"]]
      if (is.null(a)) NULL else t(a)
    },
    NMF = tryCatch(
      asNamespace("NMF")[["basis"]](decom),
      error = function(e) NULL
    ),
    NULL
  )
  if (is.null(m)) {
    return(NULL)
  }
  m <- as.matrix(m)
  if (is.null(colnames(m))) {
    colnames(m) <- paste0("V", seq_len(NCOL(m)))
  }
  varnames <- rownames(m)
  if (is.null(varnames)) {
    varnames <- paste0("X", seq_len(NROW(m)))
  }
  dt <- data.table::as.data.table(m)
  dt[, let(variable = varnames)]
  data.table::setcolorder(dt, c("variable", setdiff(names(dt), "variable")))
  dt
}


#' Cluster assignments for the `assignments` slice
#'
#' Extracts `prop(result, "clusters")` from a `Clustering` and wraps it
#' as a one-column data.table named `cluster`. Row order matches the
#' input data row order, so the client can left-join the assignment
#' column back to the parent table via `rowid` (analogous to the
#' decomposition `transformed` slice).
#'
#' @param clu `Clustering`.
#'
#' @return data.table with a single integer column `cluster`, or NULL
#'   when no assignments are available.
#'
#' @author EDG
#' @keywords internal
#' @noRd
assignments_table <- function(clu) {
  v <- tryCatch(prop(clu, "clusters"), error = function(e) NULL)
  if (is.null(v)) {
    return(NULL)
  }
  if (is.list(v)) {
    v <- unlist(v, use.names = FALSE)
  }
  data.table::data.table(cluster = as.integer(v))
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


#' Convert a confusion-matrix `table` to a wire-safe long table
#'
#' A `table` has no `jsonlite::asJSON` method, so it cannot ship as-is.
#' `as.data.frame()` on a `table` yields the canonical long form with
#' `Reference`, `Predicted`, `Freq` columns, which serializes as an array of
#' records. Row order follows rtemis's factor levels (positive class first),
#' so the client recovers class order from first appearance — never sorting
#' or recomputing.
#'
#' @param cm A confusion-matrix `table`, or `NULL`.
#'
#' @return `data.frame` (`Reference`, `Predicted`, `Freq`) or `NULL`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
confusion_to_df <- function(cm) {
  if (is.null(cm)) {
    return(NULL)
  }
  df <- as.data.frame(cm)
  # `cm` from rtemis metrics always names its margins `Reference`/`Predicted`,
  # but rename by index so a `table` from any source still serializes cleanly.
  if (ncol(df) == 3L) {
    names(df)[1:2] <- c("Reference", "Predicted")
    df[["Reference"]] <- as.character(df[["Reference"]])
    df[["Predicted"]] <- as.character(df[["Predicted"]])
  }
  df
}


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
      # Replace the raw confusion-matrix `table` (no JSON method) with an
      # explicit {classes, counts}. The resampled aggregate lives at
      # `confusion_matrix`; a single fit's is at `metrics$Confusion_Matrix`.
      cm <- slice[["confusion_matrix"]]
      slice[["confusion_matrix"]] <- NULL
      if (is.null(cm) && is.list(slice[["metrics"]])) {
        cm <- slice[["metrics"]][["Confusion_Matrix"]]
        slice[["metrics"]][["Confusion_Matrix"]] <- NULL
      }
      if (!is.null(cm)) {
        slice[["confusion"]] <- confusion_to_df(cm)
      }
      out[[split]] <- slice
    }
  }
  out
}
