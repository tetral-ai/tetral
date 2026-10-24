# tetral-tests

Tests of the workspace as a whole. Each file of `tests/` is one test target:

- `crate_graph.rs`: the rules of the crate graph, checked over the output of
  `cargo metadata`.

No workspace crate depends on this package.
