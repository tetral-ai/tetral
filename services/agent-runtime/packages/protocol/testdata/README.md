# Shared test vectors: Bridge and the Runtime

Each file here is input to both sides' tests, so a change on one side shows in
the other side's tests without running the two together.

| Vectors | Written by | Read by |
|---|---|---|
| `current-request-selection.json` | by hand: the truth table both sides implement | Bridge `bridge_api_context_current_request_test.go`; Runtime `core/test/unit/thread-loop/current-request-selection.test.ts` |
| `installed-builtin-policy.json` | by hand | Bridge `content_context_test.go`; Runtime `runtime-pod/test/unit/command.test.ts` |
| `bridge-context/*.json` | Bridge's tests and the integration tests: each is the envelope `LoadContext` returned for one stored state | Runtime `runtime-pod/test/unit/bridge-context-vectors.test.ts` |
| `tool-settlements/*.json` | Runtime `runtime-pod/test/unit/tool-settlement-vectors.test.ts`: the settlement its Bridge writer sends for one Tool outcome | Bridge `bridge_api_mcp_test.go` |
| `runtime-hot-context/*.json` | integration tests: the provider context a live Runtime process sent before Bridge closed the request out | Runtime `runtime-pod/test/unit/bridge-context-vectors.test.ts`, against the cold context of the envelope with the same name |

A vector a test writes is the producer's real output, with generated
identifiers renamed in order of first appearance. The producer's test
compares its output with the file and fails on any difference. It rewrites
the file only when `TETRAL_UPDATE_VECTORS=1` is set, which CI never does; run
the readers after a rewrite. A reader fails on a vector it has no expectation
for and on an expectation without a vector.

A reader cannot see a vector its producer stopped writing. Each producer
package lists the vectors it writes, with the top-level test that writes each,
in its `testdata/pinned-vectors.json`, which the rewrite updates. The package's
run fails when a test it ran whole did not write a listed vector, and
`internal/testvector` fails on a vector here that no producer lists. A Runtime
producer test fails when a row it ran did not write its vector.
