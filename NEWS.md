# rtemis.server 0.2.6

- The server keeps answering while the folder dialog is open. The dialog closes when its connection closes, when the server stops, and after 290 seconds. A second request while one is open is answered with `busy`, and a missing or failing dialog program with `unavailable` and its reason.
- An unfinished upload is dropped when the connection that began it leaves the session, and 600 seconds after its last chunk. A session holds at most 4.
- The `metrics` slice of `job.result` is removed; the `summary` slice carries the metrics.
- The help page of `serve()` describes when the server waits for the browser.

# rtemis.server 0.2.5

- `job.result` serves an `observed_predicted` slice for regression models: the line, band, and R-squared of each sample from every case, and a bounded random sample of points, from `rtemis::observed_predicted_table()`; requires rtemis 1.4.5. The reply states how many cases and points each sample has and how the points were chosen.
- The `roc` and `pr` slices give `n_vertices`, the number of vertices of each curve before the cap, and `max_points`, the cap.
- The `predictions` slice answers `payload_too_large` with the number of predictions when they do not fit a reply.
- The `summary` slice leaves out the case indices of the outer resamples, so its size is independent of the number of cases.

# rtemis.server 0.2.4

- The third failed `auth` attempt closes the connection, as does any other request before authentication, and a connection must authenticate within 10 seconds.
- A request id has at most 128 bytes.
- `serve()` requires a supplied `token` to have at least 16 printable ASCII characters without spaces, prints the token only when it generates it, and requires `token` when `verbosity` is 0.
- The origin check reads the `Origin` header whatever the case of its name.
- Ctrl-C makes `serve()` return, with exit status 0 under Rscript. A stop releases the port, the background work, the sessions, and `options(rtemis.server.verbosity)`, so a new server can start in the same R process.
- `serve()` runs its workers on the mirai compute profile "rtemis.server" and leaves the caller's daemons running.
- A worker that ends unexpectedly is replaced, and its job fails with a message.
- `serve()` announces the server after its port is bound, and an unavailable port is an error that names it. `verbosity = 0L` prints nothing.
- `host` takes "127.0.0.1" or "localhost".
- `shutdown()` is removed.
- Numbers are sent with 15 significant digits.
- `info` reports the number of connected workers.
- A response that exceeds the frame limits is answered with an error, and an event whose last connection fails is kept for the next `session.join`.
- `session.delete` cancels the session's unfinished jobs and detaches its other connections.
- Requires R 4.4.0.

# rtemis.server 0.2.3

- `job.result` serves a `pr` slice: the precision-recall curves of `rtemis::pr_table()`, aggregate and, for a resampled fit, per resample, with columns `split`, `class`, `fold`, `recall`, `precision`, `ap`, and `prevalence`; requires rtemis 1.4.4.

# rtemis.server 0.2.2

- The `varimp` slice sends `rtemis::varimp_ranking()`: one row per measure, variable, and resample, with each variable's mean across resamples and its rank, in rank order within each measure. `top_n` keeps ranks 1 to `top_n` of each measure, and the pointer's `variables` gives each measure's number of ranked variables; requires rtemis 1.4.3.

# rtemis.server 0.2.1

- Decomposition and clustering jobs build their config through rtemis's wire readers, so a setting that is itself an object (PAMK's criterion, a spectral variant's Nystrom approximation) is read the same way as from a file; requires rtemis 1.4.1.
- Schema validation examples and record expectations use language-qualified R paths.
- The `varimp` slice builds its table with `rtemis::varimp_table()`: one row per predictor and one column per importance measure, with a `fold` column for a resampled fit; requires rtemis 1.4.2.
- `job.result` serves `review` and `writeup` slices: `{markdown}`, the Markdown of `rtemis::review()` or `rtemis::writeup()` of a supervised result, rendered by `rtemis::to_markdown()`.

- The wire's `algorithm` is written beside its flat hyperparameter map, as in `hyperparameters/v1`: `.nest_hyperparameters()` produces `{algorithm, ...settings}`. `decomp` and `cluster` read `hyperparameters` alone and reject a nested `config` key.
- `job.load` registers an `.rds` file of a `Supervised` or `SupervisedRes` model as a completed job, the counterpart of `job.save`. The job's times are those of the model's own session, when it has one.
- `job.load.begin`, `job.load.chunk`, `job.load.end`, and `job.load.cancel` load a model larger than a WebSocket frame.
- The `algorithms` handler reads the column names of `rtemis::supervised_algorithms`.
- `train` accepts a set of named variants of one algorithm as `hyperparameters`: `{ variants: { <name>: { algorithm, hyperparameters } } }`. `rtemis::is_wire_hyperparameters_set()` distinguishes it from an `algorithm` beside a flat map.
