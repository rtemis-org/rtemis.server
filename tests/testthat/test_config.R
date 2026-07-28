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
  expect_true(inherits(cfg, "rtemis::SuperConfigLive"))
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
  # view: `$schema` markers, nested `{algorithm, hyperparameters}` with no
  # top-level `algorithm`, and the same `n_resamples` spelling as the setup
  # formal. It must reach the same `SuperConfigLive`.
  dt <- data.table(a = rnorm(20), b = rnorm(20), y = rnorm(20))
  cfg <- build_cfg(
    list(
      `$schema` = "https://schema.rtemis.org/supervised/v1/schema.json",
      data_handle = "d1",
      hyperparameters = list(
        algorithm = "GLM",
        hyperparameters = list(ifw = FALSE)
      ),
      preprocessor_config = list(
        `$schema` = "https://schema.rtemis.org/preprocessor/v1/schema.json",
        scale = TRUE,
        center = TRUE
      ),
      outer_resampling_config = list(type = "KFold", n_resamples = 3L)
    ),
    dt
  )
  expect_true(inherits(cfg, "rtemis::SuperConfigLive"))
  expect_equal(
    prop(prop(cfg, "outer_resampling_config"), "n_resamples"),
    3L
  )
  expect_true(inherits(prop(cfg, "hyperparameters"), "rtemis::Hyperparameters"))
  expect_true(
    inherits(prop(cfg, "preprocessor_config"), "rtemis::PreprocessorConfig")
  )
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
  # `resampler.describe` has no single object to read (`type` selects the
  # subclass), so the `c("a", "b", ...)` formal remains the fallback.
  by_name <- function(x) setNames(x, vapply(x, `[[`, character(1L), "name"))
  schema <- by_name(rtemis.server:::.live_build_schema(rtemis::setup_Resampler))
  expect_true("KFold" %in% unlist(schema[["type"]][["choices"]]))
  expect_equal(schema[["type"]][["default"]], "KFold")
})
