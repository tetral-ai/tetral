# GitHub and Slack interoperability runner

This explicit command exercises a bound deployed Engine through its Runtime tool
runner, Connector, Bridge and Vault. It is outside hermetic test discovery:

```sh
bun run packages/mcp-connector/test/live/github-slack-interoperability.ts \
  --fixture "$MCP_LIVE_FIXTURE" --output "$MCP_EVIDENCE_DIR"
```

The repository supplies the orchestration and validation contract. An environment
adapter executable binds the actual deployment; that executable and its exact
revision/hash must be reviewed and bound before live execution. No deployment or
credential binding is inferred. Missing fixture/adapter inputs fail preflight.
`--offline-preflight` verifies the local fixture and executable hash without
invoking the adapter. Unit tests use explicitly synthetic observations and cannot
establish external interoperability.

The `tetral-mcp-interoperability-v1` fixture includes Engine/SDK revisions,
environment, Workspace, Session and visible Vault references, the dedicated Slack
credential reference, and an executable `driver` with entrypoint, argv, SHA-256,
revision, reviewed transitive adapter `sourceSha256`, and declared independent cumulative-counter provenance. The executable SHA-256 binds only `entrypoint` bytes. If that entrypoint is an interpreter, its hash does not bind the scripts in `argv`; the revision and source SHA-256 must bind the complete reviewed adapter and its dependencies before remote binding. Local offline preflight verifies the executable hash; full adapter-source review and binding remain separate prerequisites. References must
not contain token/secret material. The configured names are `work-github` and
`work-slack`. `providerContracts` pins each retrieved provider contract/catalog revision and SHA-256; preflight must report the matching retrieved identities. Each tool binding supplies its exact discovered name, complete
arguments, canonical full-definition SHA-256 and independent result oracles.
Each oracle has `providerPath`, `runtimePath`, `committedPath` and the expected
string/number. The runner checks all three boundaries. GitHub must use `get_me`
with `{}` and independent login/numeric ID. Bind Slack's authenticated profile
read tool, `users:read`, independent user/team IDs, confidential-client refresh,
and `https://slack.com/api/oauth.v2.user.access`.

The adapter accepts one JSON request on stdin and emits exactly one JSON
observation on stdout. The exported TypeScript `DriverRequest` and
`DriverObservation` are the schema authority. Each request has contract/run ID,
fixture, operation, remaining time and remaining external-call/token/discovery
budgets. Reads also contain cold/warm/refresh phase, fresh Tool Use identity and
complete call JSON. The adapter must enforce those bounds before external
traffic, including SDK authentication retries and Bridge acceptance verification;
it cannot return a verdict in place of observations.

The runner invokes:

1. `preflight`: verify source/environment/scope, actual authenticated full tool
   definitions, retrieved provider contract/catalog identities, cumulative counters, and cold client reset/readback. Report actual JSON or SSE response mode, negotiated protocol version and advertised server capabilities for both adapters; absent notification support is valid. Catalog
   discovery and verification already consume the discovery budget.
2. One cold and one warm read for each adapter, using fresh Tool Uses. Observe
   SDK client identity and cumulative initialize/list/call counts. Cold execution
   creates readiness; warm execution must retain its client without re-listing.
   Record backend Pod identities, original claim, successful durable result and
   matching Runtime result, with independent account oracles at every boundary. Every read reports transport/capability facts, the fresh claim, SDK client and backend Pod identities.
3. `expire-slack`: make only the dedicated stored expiry due while preserving
   its real refresh material. Do not restore an invalidated refresh token.
4. A fresh Slack read: observe actual token-endpoint success, encrypted
   write-back, replacement credential use and the same independent identity.
5. `cleanup` on every entered run, with readback confirming removal of the test
   Session/client state and retention of the latest rotated credential.

One run allows at most 15 minutes, 6 external calls, 2 token requests and 6 complete
discovery passes per adapter, including setup/verification. Thirty seconds of
that total are reserved for cleanup. `externalCalls` counts every dispatched MCP `tools/call` attempt, including rejected authentication attempts. `successfulToolResponses` independently counts successful external read responses, with provenance declared by the reviewed adapter; it makes no downstream exactly-once effect claim. Every fresh Tool Use must add exactly one successful response. One extra raw attempt is permitted only with observed genuine HTTP 401/403 rejection provenance; JSON-RPC codes, text and generic replay do not justify a retry. The total six-raw-call bound still applies. Counters are independently observed, monotonic and reconciled after every operation; limits stop further dispatch.
The command adapter bounds stdout to 1 MiB and stderr to 64 KiB while streaming,
terminates then kills an unjoined child within its deadline, and joins it before
returning. Child output/errors and raw external results never enter evidence.
Only fixed failure classifications, source hashes, fresh Tool Use IDs, named numeric counters, transport mode/version, safe advertised-capability flags and a full capability hash are persisted. Claim, SDK client and backend Pod identities are bounded, then hashed within the run; those hashes retain cold/warm and receipt correlation without persisting arbitrary child strings. Account identifiers and external result bodies are not persisted. Output creation is exclusive, preserving an
existing first result. Cleanup failure cannot erase an established contract FAIL.
Setup/binding/provider unavailability is INCONCLUSIVE; external acceptance needs
an actual live run. Synthetic offline tests never meet that gate.
