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

live <- NULL

.onLoad <- function(libname, pkgname) {
  live <<- asNamespace("rtemis")[["live"]]
} # /.onLoad
