# config.R
# ::rtemis.server::
# 2026- EDG rtemis.org

# %% Wire -> rtemis config objects -------------------------------------------
#
# rtemis owns the whole list -> config-object reconstruction layer:
# `.list_to_SuperConfig()` and its per-block friends accept both the flat shape
# rtemislive's forms emit and the nested shape published at schema.rtemis.org,
# strip `$`-prefixed document metadata, drop keys the target `setup_*()` does
# not model, and validate through the same constructors `read_config()` uses.
# Reimplementing any of that here would be a second copy of a contract rtemis
# already publishes — and a copy that drifts, which is exactly how the
# resampler vocabulary gap went unnoticed.
#
# So the server translates the *wire* and nothing else, which is three things:
#
#   1. Frame JSON is decoded with `simplifyVector = FALSE`, so every JSON array
#      arrives as a list of length-1 atomics where rtemis wants an atomic
#      vector (`.collapse_scalar_lists()`).
#   2. An empty `positive_class` means "unset" to a client but would be a real
#      outcome level to rtemis (`.from_wire()`).
#   3. The wire carries `algorithm` beside a flat hyperparameter map, the shape
#      of a `setup_*()` call; a config nests the two (`.nest_hyperparameters()`).
#
# Everything after that is a `.list_to_*()` call.
#
# This layer performs no *name* translation: field names are identical across
# the rtemislive form, the wire, the rtemis config object and the published
# schema. Shape differs; names do not.

#' Collapse JSON-array values into atomic vectors
#'
#' Frame-level JSON decode uses `simplifyVector = FALSE` (heterogeneous
#' payloads survive intact), so a JSON array like `[100, 500, 1000]` arrives as
#' an R *list* of length-1 atomics, not a numeric vector. rtemis's `setup_*()`
#' validators need atomic vectors.
#'
#' A JSON array decodes to an *unnamed* list and a JSON object to a *named*
#' one, which is the discriminator: unnamed lists of length-1 atomics collapse
#' to a vector; named lists are objects and are recursed into, so a genuine
#' nested config (`impute_missRanger_params`, or a whole `hyperparameters`
#' block) keeps its list shape. Anything else is returned unchanged — notably
#' lists of lists such as `inbag`.
#'
#' A search space is a tagged object (`{"candidates": [...]}`, see
#' [rtemis::tune_over()]), so it recurses like any other and its array
#' collapses to a vector — the tag is what marks it, never the length.
#'
#' @param x Value as decoded from a wire params payload.
#'
#' @return `x` with JSON-array values replaced by atomic vectors.
#'
#' @author EDG
#' @keywords internal
#' @noRd
.collapse_scalar_lists <- function(x) {
  if (!is.list(x)) {
    return(x)
  }
  if (
    is.null(names(x)) &&
      length(x) > 0L &&
      all(vapply(x, function(e) is.atomic(e) && length(e) == 1L, logical(1)))
  ) {
    return(unlist(x, use.names = FALSE))
  }
  lapply(x, .collapse_scalar_lists)
}


#' Normalize wire params to the rtemis config vocabulary
#'
#' The wire speaks `setup_*()` formals — that is what the `*.describe` methods
#' publish and what rtemislive's forms are generated from. rtemis's config
#' objects, and the schemas at schema.rtemis.org, speak *object property*
#' names. The two vocabularies coincide, so this function performs no renaming:
#' it collapses scalar lists and normalizes one empty-string value.
#'
#' rtemis's `.list_to_*()` reconstructors now **reject** a key the config does
#' not declare, so transport params must be removed here rather than left for
#' rtemis to ignore. `.TRANSPORT_KEYS` names them; anything else unknown is a
#' real mistake and should surface as an error.
#'
#' @param params Named list of wire params.
#'
#' @return `params` with array values collapsed and names normalized.
#'
#' @author EDG
#' @keywords internal
#' @noRd
# Wire params that address the *transport*, not the config: `data_handle` names
# in-memory data the frame already resolved, and rtemis's `SuperConfig` speaks
# `dat_training_path`. Removed before the config is built, since rtemis no
# longer ignores what it does not declare.
.TRANSPORT_KEYS <- c("data_handle")


.from_wire <- function(params) {
  params <- .collapse_scalar_lists(params)
  params[.TRANSPORT_KEYS] <- NULL
  # An empty `positive_class` is a client's "no selection" (a select with
  # nothing chosen, or a non-binary target). Map it to NULL, which is rtemis's
  # unset value and means "use the second factor level" — the positive-class
  # convention for binary classification. Left as `""` it would be stored as a
  # real outcome level.
  pc <- params[["positive_class"]]
  if (is.character(pc) && length(pc) == 1L && !nzchar(pc)) {
    params[["positive_class"]] <- NULL
  }
  params
}


#' Nest the wire's `algorithm` + flat hyperparameter map
#'
#' The wire speaks `setup_*()` formals: `algorithm` names the learner and
#' `hyperparameters` is its flat name -> value map, which is what the
#' `algorithm.describe` schema publishes and what rtemislive's form emits. A
#' `SuperConfig` names the algorithm once, inside `hyperparameters`, as the
#' discriminator of the `{algorithm, hyperparameters}` union. This is the
#' flat-wire / nested-config shape difference, and lifting it here leaves
#' rtemis's reconstructor with exactly one shape to accept.
#'
#' @param params Named list of wire params.
#'
#' @return `params` with `algorithm` folded into `hyperparameters`.
#'
#' @author EDG
#' @keywords internal
#' @noRd
.nest_hyperparameters <- function(params) {
  algorithm <- params[["algorithm"]]
  if (is.null(algorithm)) {
    return(params)
  }
  params[["algorithm"]] <- NULL
  params[["hyperparameters"]] <- list(
    algorithm = algorithm,
    hyperparameters = params[["hyperparameters"]] %||% list()
  )
  params
}


#' Build a `SuperConfigLive` from wire params
#'
#' Delegates the whole config to `rtemis::read_config()`'s own reconstructor and
#' then binds the in-memory data. `.list_to_SuperConfig()` produces the
#' portable, path-based recipe; rtemislive's data arrives over a frame rather
#' than from disk, so the run needs the `SuperConfigLive` variant. The two
#' classes carry identical properties apart from `.PORTABLE_ONLY_PROPERTIES`,
#' so the properties are copied across generically — re-listing every block
#' here is what would drift as `SuperConfig` grows.
#'
#' @param params Named list of `train` wire params.
#' @param dat_training `data.table`: Training data resolved from `data_handle`.
#'
#' @return `SuperConfigLive` object.
#'
#' @author EDG
#' @keywords internal
#' @noRd
# `SuperConfig` properties a `SuperConfigLive` does not model, dropped before
# the live config is built. The path trio is replaced by the data itself;
# `character2factor` says how a *file* is read, and this config holds a frame
# that has already been read -- the equivalent decision was made on arrival,
# where `read_ipc_bytes()` coerces every character column to a factor; `outdir`
# falls back to the live default (NULL), since a live run writes nothing to disk
# and hands its result back over the wire.
#
# Every other property must be a `setup_SuperConfigLive()` formal, which
# `test_config.R` pins: the two lists are compared there, so a property added to
# `SuperConfig` without a live counterpart fails a test run rather than a user's
# `train` frame.
.PORTABLE_ONLY_PROPERTIES <- c(
  "dat_training_path",
  "dat_validation_path",
  "dat_test_path",
  "character2factor",
  "outdir"
)


build_super_config <- function(params, dat_training) {
  args <- S7::props(.list_to_SuperConfig(.nest_hyperparameters(.from_wire(
    params
  ))))
  args[.PORTABLE_ONLY_PROPERTIES] <- NULL
  args[["dat_training"]] <- dat_training
  do.call(rtemis::setup_SuperConfigLive, args)
}
