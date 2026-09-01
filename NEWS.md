# rtemis.server news

This file starts here: entries below cover changes from this point forward
rather than reconstructing the package's earlier history.

## 0.2.1

- **`job.load` registers a previously-saved model as a completed job — the
  counterpart of `job.save`.** A model trained in a bare R console (`saveRDS()`,
  or `train(..., outdir=)`), with no server or browser connection at all, had
  no way back into `rtemislive`: nothing accepted an `.rds` the server did not
  itself produce. `job.load` reads an uploaded `.rds` payload, rejects it
  outright unless it is a `Supervised` or `SupervisedRes` result — the same
  pair every downstream slice already accepts, a resampled/cross-validated
  fit being exactly as valid a `train()` outcome as a single one, and failing
  here with one message beats failing later inside whichever slice a client
  asks for first — and registers it in the session's job table with
  `status = "complete"`. `job.result`, `job.status`, and `job.save` needed no
  changes at all — none of them ever checked how a job env was built, only
  `job[["status"]]` and `job[["result"]]`, which is what makes this a
  registration step rather than a second implementation of the slicing logic
  those already have. `submitted_at`/`started_at`/`completed_at` come from
  the model's own observability session (`@session`, `started`/`finished`)
  when it has one, not from the moment of upload — otherwise a real training
  run's elapsed time collapses to zero in the client's "Done in Ns" badge,
  since submit, start and finish would all be the same instant.
- **`job.load` has a chunked counterpart (`job.load.begin`/`.chunk`/`.end`/
  `.cancel`) for models too large for a single WebSocket frame.** A real
  fitted model can exceed the frame size `nanonext`'s WebSocket handler
  enforces, closing the connection with code 1009 ("message too large")
  before `job.load` ever ran — nanonext exposes no option to raise that
  limit. The chunked path reuses `data.upload`'s own assembly logic
  (`assemble_chunked_upload()`, factored out of `end_upload()`) and
  `load_model_job()`'s finalization (factored out of the single-shot
  handler), so both entry points converge on identical validation and
  produce identical jobs.
- **Updated the `algorithms` handler to use new `rtemis::supervised_algorithms` 
  column names.**
- **`train` accepts a set of named variants as its learner.** `supervised/v1`
  allows `hyperparameters` to be either one configuration or a union of named
  configurations of the same algorithm, and rtemis has reconstructed both for
  as long as the union has existed. The handler did not: it required a
  top-level `algorithm`, which a variants config has nowhere to put, because
  each variant names the algorithm itself. A submitter with a schema-valid
  config had no way to run it, and one that sent an empty `algorithm` to get
  past the check was refused much later, by a message naming an algorithm
  nobody had written. Exactly one of the two now says what to fit: an
  `algorithm` beside a flat hyperparameter map, or a `hyperparameters` block of
  the form `{ variants: { <name>: { algorithm, hyperparameters } } }`.
- **The wire's learner check uses rtemis's own discriminator.** The question
  "does this payload name its algorithm at the top level or inside each
  variant?" is answered here the same way `.list_to_SuperConfig()` answers it,
  through the newly exported `rtemis::is_wire_hyperparameters_set()`. Two
  spellings of one rule is how a config becomes submittable on one path and
  unreadable on the other.
