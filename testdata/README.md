# Shared test vectors across services

Each file here is input to the tests of more than one service, so a change on
one side shows in the other side's tests without running them together.

| Vectors | Written by | Read by |
|---|---|---|
| `run-tool-canonical-vectors.json` | by hand | Bridge `bridge_api_tools_test.go`; Runtime `runtime-pod/test/unit/tool-runner.test.ts` |
| `attachment-only-runtime-input.json` | by hand | integration `runtime_input_first_delivery_test.go`; Runtime `runtime-pod/test/unit/attachment-input-composition.test.ts` |
| `provider-requests/*.json` | Runtime `runtime-pod/test/unit/bridge-context-vectors.test.ts`: the ProviderRequest it builds from one Bridge context | Gateway `provider-gateway/test/unit/runtime-provider-requests.test.ts` |
A vector a test writes is the producer's real output. The producer's test
compares its output with the file and fails on any difference. It rewrites the
file only when `TETRAL_UPDATE_VECTORS=1` is set, which CI never does; run the
readers after a rewrite. A reader fails on a vector it has no expectation for
and on an expectation without a vector.
