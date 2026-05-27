# zzz.R
# ::rtemis.server::

# Package-level binding to rtemis's internal `live` env. Initialized at
# load time (`.onLoad`) so all `live[["x"]]` lookups in rtemis.server
# resolve to the same shared env that rtemis uses - that's how the
# progress sink, session registry, and daemon-side job-id stamps stay
# coordinated across the two packages.
#
# `live` is unexported from rtemis on purpose (internal mutable state),
# so we reach it via `asNamespace("rtemis")` rather than `rtemis::live`.
#
# Note: lazy bindings to rtemis's other internals (e.g. `msg`,
# `get_alg_name`) live in `00_init.R` as top-level `getFromNamespace`
# calls - that file is sourced first (alphabetical) so the bindings
# exist for the rest of the package. `live` needs `.onLoad` because
# `asNamespace("rtemis")` returns an env (mutable shared state) and
# we want the binding established once R has fully resolved rtemis's
# namespace, not at source-eval time.

live <- NULL

.onLoad <- function(libname, pkgname) {
  live <<- asNamespace("rtemis")[["live"]]
} # /.onLoad
