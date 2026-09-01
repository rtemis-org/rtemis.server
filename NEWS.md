# rtemis.server news

This file starts here: entries below cover changes from this point forward
rather than reconstructing the package's earlier history.

## 0.2.1

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
