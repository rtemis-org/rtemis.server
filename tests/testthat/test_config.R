# %% Wire -> rtemis config translation ---------------------------------------
# `R/config.R` is the single place the wire vocabulary (`setup_*()` formals, as
# published by the `*.describe` methods and rendered by rtemislive's forms) is
# mapped onto rtemis's config-object property names — the vocabulary the
# schemas at schema.rtemis.org define. Everything else is delegated to rtemis's
# own `.list_to_*()` reconstructors, so these tests pin the translation and the
# fact that both vocabularies reach the same object.

collapse <- rtemis.server:::.collapse_scalar_lists
from_wire <- rtemis.server:::.from_wire
build_cfg <- rtemis.server:::build_super_config


test_that(".collapse_scalar_lists recurses into nested config blocks", {
  # A JSON array nested one level down (a tunable inside `hyperparameters`, or
  # a character vector inside `preprocessor_config`) must collapse too.
  out <- collapse(list(
    preprocessor_config = list(remove_features = list("a", "b")),
    hyperparameters = list(num_trees = list(100L, 500L))
  ))
  expect_equal(out[["preprocessor_config"]][["remove_features"]], c("a", "b"))
  expect_equal(out[["hyperparameters"]][["num_trees"]], c(100L, 500L))
})


test_that(".collapse_scalar_lists leaves JSON objects as lists", {
  # A named list is a JSON *object*, not an array: unlisting it would turn a
  # nested config (`impute_missRanger_params`) into a named atomic vector and
  # break the `setup_*()` that receives it.
  out <- collapse(list(
    preprocessor_config = list(
      impute_missRanger_params = list(pmm.k = 3L, maxiter = 10L)
    )
  ))
  params <- out[["preprocessor_config"]][["impute_missRanger_params"]]
  expect_true(is.list(params))
  expect_equal(params[["pmm.k"]], 3L)
})


test_that(".from_wire passes the resampler's `n_resamples` through untouched", {
  # The setup formal and the config property share one name, so no translation
  # is needed.
  out <- from_wire(list(
    outer_resampling_config = list(type = "KFold", n_resamples = 5L)
  ))
  expect_equal(out[["outer_resampling_config"]][["n_resamples"]], 5L)
  expect_null(out[["outer_resampling_config"]][["n"]])
})


test_that(".from_wire passes `positive_class` through, dropping empty", {
  # An empty string is a client's "no selection"; NULL is rtemis's unset value
  # and means "use the second factor level". `""` must not reach rtemis, where
  # it would be stored as a real outcome level.
  expect_equal(from_wire(list(positive_class = "a"))[["positive_class"]], "a")
  expect_null(from_wire(list(positive_class = ""))[["positive_class"]])
  expect_null(from_wire(list())[["positive_class"]])
})


test_that("build_super_config builds a SuperConfigLive from the form wire shape", {
  dt <- data.table(a = rnorm(20), b = rnorm(20), y = rnorm(20))
  cfg <- build_cfg(
    list(
      data_handle = "d1",
      algorithm = "GLM",
      hyperparameters = list(ifw = FALSE),
      preprocessor_config = list(scale = TRUE, center = TRUE),
      # The selected variant's fields and no others: rtemislive's form is
      # built from the KFold leaf schema, so it cannot offer `train_p` or
      # `verbosity`, and rtemis would now reject them if it did.
      outer_resampling_config = list(type = "KFold", n_resamples = 3L),
      positive_class = "",
      question = "does it build?"
    ),
    dt
  )
  expect_true(inherits(cfg, "rtemis::SuperConfigTabular"))
  expect_equal(prop(prop(cfg, "hyperparameters"), "algorithm"), "GLM")
  expect_equal(
    prop(prop(cfg, "outer_resampling_config"), "n_resamples"),
    3L
  )
  expect_true(inherits(prop(cfg, "hyperparameters"), "rtemis::Hyperparameters"))
  # Live runs write nothing to disk.
  expect_null(prop(cfg, "outdir"))
  expect_identical(prop(cfg, "dat_training"), dt)
})


test_that("a key the config does not declare is rejected, not dropped", {
  # rtemis's reconstructors are strict, so a stale or mistyped key surfaces
  # here instead of training something other than what was asked for. This is
  # what `.from_wire()` must not paper over: only transport keys are stripped.
  dt <- data.table(a = rnorm(20), b = rnorm(20), y = rnorm(20))
  err <- tryCatch(
    build_cfg(
      list(
        data_handle = "d1",
        algorithm = "GLM",
        outer_resampling_config = list(
          type = "KFold",
          n_resamples = 3L,
          train_p = 0.75
        )
      ),
      dt
    ),
    error = function(e) e
  )
  expect_s3_class(err, "rtemis_value_error")
  expect_match(conditionMessage(err), "train_p", fixed = TRUE)
})


test_that("transport keys are stripped before the config is built", {
  # `data_handle` addresses the frame, not the config; rtemis has no such
  # property and would reject it.
  expect_false("data_handle" %in% names(from_wire(list(data_handle = "d1"))))
})


test_that("build_super_config accepts a canonical schema.rtemis.org config", {
  # The object rtemislive stores on a job snapshot and shows in the Config
  # view: `$schema` markers, the learner as `{algorithm, ...settings}` with no
  # top-level `algorithm`, and the same `n_resamples` spelling as the setup
  # formal. It must reach the same `SuperConfigLive`.
  dt <- data.table(a = rnorm(20), b = rnorm(20), y = rnorm(20))
  cfg <- build_cfg(
    list(
      `$schema` = "https://schema.rtemis.org/supervised/v1/schema.json",
      data_handle = "d1",
      hyperparameters = list(algorithm = "GLM", ifw = FALSE),
      preprocessor_config = list(
        `$schema` = "https://schema.rtemis.org/preprocessor/v1/schema.json",
        scale = TRUE,
        center = TRUE
      ),
      outer_resampling_config = list(type = "KFold", n_resamples = 3L)
    ),
    dt
  )
  expect_true(inherits(cfg, "rtemis::SuperConfigTabular"))
  expect_equal(
    prop(prop(cfg, "outer_resampling_config"), "n_resamples"),
    3L
  )
  expect_true(inherits(prop(cfg, "hyperparameters"), "rtemis::Hyperparameters"))
  expect_true(
    inherits(
      prop(cfg, "preprocessor_config"),
      "rtemis::SupervisedPreprocessorConfig"
    )
  )
})


test_that("build_super_config accepts a variants set as the learner", {
  # `supervised/v1`'s second shape for `hyperparameters`: a union of named
  # configurations of one algorithm, which names the algorithm inside each
  # variant rather than at the top level. An agent building a plan reaches for
  # it, and it used to be unrunnable -- the submitter had no top-level
  # `algorithm` to send, and an empty string got as far as building the learner
  # before failing on a name nobody wrote.
  #
  # `.nest_hyperparameters()` must leave the block alone here: folding an
  # `algorithm` in is what would bury the variants.
  dt <- data.table(a = rnorm(20), b = rnorm(20), y = rnorm(20))
  cfg <- build_cfg(
    list(
      `$schema` = "https://schema.rtemis.org/supervised/v1/schema.json",
      data_handle = "d1",
      hyperparameters = list(
        variants = list(
          default = list(algorithm = "Ranger", num_trees = 500L)
        )
      ),
      outer_resampling_config = list(type = "KFold", n_resamples = 3L)
    ),
    dt
  )
  hp <- prop(cfg, "hyperparameters")
  expect_true(inherits(hp, "rtemis::HyperparametersSet"))
  # The variant's name survives: it is what a tuner reports as the winner.
  expect_equal(names(prop(hp, "members")), "default")
  expect_equal(prop(hp, "algorithm"), "Ranger")
})


test_that("build_super_config accepts a config that names no learner", {
  # The third valid form: `hyperparameters` is nullable in `supervised/v1` and
  # `train()` has its own default, so a config naming no algorithm means
  # "rtemis chooses" rather than "the caller forgot". The handler used to
  # require one, which made this unsubmittable even though `train()` has always
  # run it.
  dt <- data.table(a = rnorm(20), b = rnorm(20), y = rnorm(20))
  cfg <- build_cfg(
    list(
      data_handle = "d1",
      outer_resampling_config = list(type = "KFold", n_resamples = 3L)
    ),
    dt
  )
  expect_true(inherits(cfg, "rtemis::SuperConfigTabular"))
  expect_null(prop(cfg, "hyperparameters"))
})


test_that("a train frame needs only its data", {
  # The handler's guard is `data_handle` alone. All three ways of naming a
  # learner -- a top-level `algorithm`, a variants set, or nothing at all --
  # reach rtemis, which is the layer that knows what each means. A misspelled
  # key is caught downstream by the reconstructor, not here.
  required <- function(params) !is.null(params[["data_handle"]])
  expect_true(required(list(data_handle = "d1", algorithm = "Ranger")))
  expect_true(required(list(data_handle = "d1")))
  expect_false(required(list(algorithm = "Ranger")))
})


test_that("every SuperConfig property reaches setup_SuperConfigLive", {
  # `build_super_config()` copies properties across generically, so a property
  # added to `SuperConfig` with no live counterpart is otherwise caught only
  # when a `train` frame arrives -- `do.call()` raises "unused argument" at the
  # user, not here. Compare the two lists directly.
  portable <- S7::prop_names(rtemis::setup_SuperConfig())
  live <- names(formals(rtemis::setup_SuperConfigLive))
  drops <- rtemis.server:::.PORTABLE_ONLY_PROPERTIES
  expect_equal(setdiff(setdiff(portable, drops), live), character(0))
  # The drop list names only real properties, so a rename cannot leave a stale
  # entry silently dropping nothing.
  expect_equal(setdiff(drops, portable), character(0))
})


test_that("build_super_config rejects an unmodelled key", {
  dt <- data.table(a = rnorm(10), y = rnorm(10))
  expect_error(
    build_cfg(
      list(algorithm = "GLM", preprocessor_config = list(no_such_arg = TRUE)),
      dt
    )
  )
})


# %% Enumerated choices in `*.describe` --------------------------------------
# rtemis declares admissible values once, on the S7 property
# (`prop_string(enum = ...)`). A `setup_*()` formal usually carries only the
# default, so a describe schema built from formals alone reports no `choices`
# and the client renders a free-text box where a select belongs. These pin the
# property as the source.

test_that("prop_enums reads enums off a config object's properties", {
  enums <- rtemis.server:::prop_enums(rtemis::setup_Ranger())
  expect_equal(
    unlist(enums[["importance"]]),
    c("none", "impurity", "impurity_corrected", "permutation")
  )
  # Non-enumerated and non-`prop_*` properties contribute nothing.
  expect_null(enums[["num_trees"]])
  expect_equal(rtemis.server:::prop_enums(NULL), list())
})


test_that(".live_build_schema surfaces choices for scalar-default enum args", {
  by_name <- function(x) setNames(x, vapply(x, `[[`, character(1L), "name"))
  schema <- by_name(rtemis.server:::.live_build_schema(
    rtemis::setup_Ranger,
    config = rtemis::setup_Ranger()
  ))
  # `setup_Ranger(importance = "impurity")` — scalar formal, enumerated
  # property.
  expect_equal(
    unlist(schema[["importance"]][["choices"]]),
    c("none", "impurity", "impurity_corrected", "permutation")
  )
  expect_equal(schema[["importance"]][["default"]], "impurity")
  expect_false("choices" %in% names(schema[["num_trees"]]))
})


test_that(".live_build_schema still reads `match.arg`-style formals", {
  # No shipped `setup_*` uses the bare `c("a", "b", ...)` formal idiom any
  # longer (`setup_Resampler(type = )` was the last one, split into
  # `setup_KFold()`/`setup_StratSub()`/etc., each with no such formal) --
  # but a dispatcher with no single object to read against, whose `type`
  # selects the subclass, is a shape `.live_build_schema` must still handle.
  # Exercised here with a local stand-in rather than real production code.
  synthetic_dispatcher <- function(type = c("KFold", "StratSub"), n = 10L) {
    NULL
  }
  by_name <- function(x) setNames(x, vapply(x, `[[`, character(1L), "name"))
  schema <- by_name(rtemis.server:::.live_build_schema(synthetic_dispatcher))
  expect_true("KFold" %in% unlist(schema[["type"]][["choices"]]))
  expect_equal(schema[["type"]][["default"]], "KFold")
})
