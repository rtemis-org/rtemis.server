# rtemis internal functions
#
# Resolved once here, at top level, so every call site shares one binding and
# none of them reach for `:::`. The rest of rtemis's list -> config
# reconstruction family is exported and reached through `rtemis::`
# (`.list_to_DecompositionConfig()`, `.list_to_Hyperparameters()`,
# `.list_to_ResamplerConfig()`, `.list_to_TunerConfig()`, `.drop_meta_keys()`);
# `.list_to_SuperConfig()` is the one member that is not.
get_alg_name <- utils::getFromNamespace("get_alg_name", "rtemis")
get_decom_name <- utils::getFromNamespace("get_decom_name", "rtemis")
get_clust_name <- utils::getFromNamespace("get_clust_name", "rtemis")
.list_to_SuperConfig <- utils::getFromNamespace(
  ".list_to_SuperConfig",
  "rtemis"
)
