# init
# 2026- EDG rtemis.org

# non-exported functions from rtemis
# %% list_to_Hyperparameters ----
list_to_Hyperparameters <- function(x) {
  fn <- paste0("setup_", x[["algorithm"]])
  if (!exists(fn, mode = "function")) {
    cli::cli_abort(".val Invalid algorithm: {x[['algorithm']]}.")
  }
  args <- x[["hyperparameters"]]
  # Keep only arguments that are in the setup function
  setup_formals <- names(formals(get(fn)))
  args <- args[names(args) %in% setup_formals]
  do.call(fn, args)
}
