# tetral-tests

Tests of the workspace as a whole. Each file of `tests/` is one test target:

- `crate_graph.rs`: the rules of the crate graph, checked over the output of
  `cargo metadata`, and the rule that no library crate prints, checked over
  the library crates' sources.

No workspace crate depends on this package.
