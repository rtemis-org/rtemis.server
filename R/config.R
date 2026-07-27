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
# So the server translates the *wire* and nothing else, which is two things:
#
#   1. Frame JSON is decoded with `simplifyVector = FALSE`, so every JSON array
#      arrives as a list of length-1 atomics where rtemis wants an atomic
#      vector (`.collapse_scalar_lists()`).
#   2. An empty `positive_class` means "unset" to a client but would be a real
#      outcome level to rtemis (`.from_wire()`).
#
# Everything after that is a `.list_to_*()` call.
#
# This layer performs no *name* translation: field names are identical across
# the rtemislive form, the wire, the rtemis config object and the published
# schema. A name that differs between two of our own components is a bug in one
# of them, not something to absorb here.

#' Collapse JSON-array values into atomic vectors
#'
#' Frame-level JSON decode uses `simplifyVector = FALSE` (heterogeneous
#' payloads survive intact), so a JSON array like `[100, 500, 1000]` arrives as
#' an R *list* of length-1 atomics, not a numeric vector. rtemis's `setup_*()`
#' validators and the tuner (which branches on `length(x) > 1`) need atomic
#' vectors.
#'
#' A JSON array decodes to an *unnamed* list and a JSON object to a *named*
#' one, which is the discriminator: unnamed lists of length-1 atomics collapse
#' to a vector; named lists are objects and are recursed into, so a genuine
#' nested config (`impute_missRanger_params`, or a whole `hyperparameters`
#' block) keeps its list shape. Anything else is returned unchanged — notably
#' lists of lists such as `inbag`.
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
#' Extra keys need no handling: every `.list_to_*()` reads the properties it
#' knows and ignores the rest, so `verbosity`, a stray `train_p` on a KFold, or
#' a `$schema` marker all fall away on their own. (Do not build on that: strict
#' rejection of unknown keys is planned — see rtemis `plan/wire-vocabulary.md`.)
#'
#' @param params Named list of wire params.
#'
#' @return `params` with array values collapsed and names normalized.
#'
#' @author EDG
#' @keywords internal
#' @noRd
.from_wire <- function(params) {
  params <- .collapse_scalar_lists(params)
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


#' Build a `SuperConfigLive` from wire params
#'
#' Delegates the whole config to `rtemis::read_config()`'s own reconstructor and
#' then binds the in-memory data. `.list_to_SuperConfig()` produces the
#' portable, path-based recipe; rtemislive's data arrives over a frame rather
#' than from disk, so the run needs the `SuperConfigLive` variant. The two
#' classes carry identical properties apart from `dat_*_path` / `dat_*`, so the
#' properties are copied across generically — re-listing every block here is
#' what would drift as `SuperConfig` grows.
#'
#' @param params Named list of `train` wire params.
#' @param dat_training `data.table`: Training data resolved from `data_handle`.
#'
#' @return `SuperConfigLive` object.
#'
#' @author EDG
#' @keywords internal
#' @noRd
build_super_config <- function(params, dat_training) {
  args <- S7::props(.list_to_SuperConfig(.from_wire(params)))
  # Drop the path trio in favour of the data itself, and drop `outdir` so the
  # live default (NULL) applies: a portable recipe defaults it to "results/",
  # but a live run writes nothing to disk and hands its result back over the
  # wire. Every remaining property is a `setup_SuperConfigLive()` formal, so a
  # property added to `SuperConfig` without a live counterpart fails loudly
  # here rather than being silently dropped.
  args[c(
    "dat_training_path",
    "dat_validation_path",
    "dat_test_path",
    "outdir"
  )] <- NULL
  args[["dat_training"]] <- dat_training
  do.call(rtemis::setup_SuperConfigLive, args)
}
