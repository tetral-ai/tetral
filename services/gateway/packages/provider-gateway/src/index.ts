/**
 * @packageDocumentation
 * `@tetral/provider-gateway` — the provider-gateway process package: the gRPC data plane
 * and per-turn provider orchestration that turns one `ProviderRequest` into one
 * terminal-exact `ProviderStreamEvent` sequence.
 *
 * Process startup enters through `runProviderGatewayCommand`, which loads
 * configuration, composes authentication, SQL-backed credential access,
 * attachment resolution, provider clients, and the application lifecycle.
 * Runtime provider calls then cross the gRPC adapter into the service shell;
 * the shell authenticates and admits each turn before resolving inputs and
 * streaming normalized events. A separate operations listener publishes live
 * health, readiness, and in-memory metrics. `RunWeb` is authenticated and
 * validated on the registered compatibility RPC, then returns the package's
 * fixed unimplemented response without executing Web work.
 *
 * OWNS:
 *   - the `ProviderGatewayService` gRPC surface (`StreamProviderRequest`, `RunWeb`)
 *     and the ops-plane HTTP server;
 *   - per-turn orchestration: credential resolution, attachment-ref resolution,
 *     provider streaming, and pre-first-provider-event platform-key failover;
 *   - the process-package bounds that the pure protocol package cannot hold
 *     (`validateRunWebRequest`, `grpcServerOptions`);
 *   - the in-process provider egress allowlist (in `providers/clients.ts`);
 *     Web tool SSRF classification belongs to the `web-connector` service.
 *
 * STATE MACHINE:
 *   This package retains only disposable process-local state: listener and
 *   readiness lifecycle, admission and metrics counters, per-turn fragment and
 *   attempt state, and the per-replica platform-key pool (ACTIVE / COOLING /
 *   QUARANTINED). It issues exactly one durable SQL transition: provider OAuth
 *   credential rotation in `providers/openai-oauth-refresh.ts`, serialized
 *   across replicas by a database row lock.
 *
 * INVARIANTS:
 *   - Credential reads are read-only: `session_provider_auth` (with its joined
 *     `credentials` row) and `platform_provider_keys`. The other request-content
 *     reads are Bridge attachment RPCs: transient payload resolution plus file
 *     metadata and chunk reads. The sole durable write is OAuth rotation.
 *   - Dependency direction is strict: provider-gateway -> lowering -> protocol.
 *     A lowering-or-protocol import back into this package is a rejection.
 *   - Custom-fetch request representation changes at three as-built boundaries:
 *     `providers/clients.ts` strips stateless OpenAI Responses `item.id` values and
 *     reconstructs allowlisted manual redirects, while `providers/openai-oauth.ts`
 *     swaps the authorization header and subscription URL and may inject the
 *     ChatGPT account-identity header.
 *   - Provider egress is an in-process host allowlist enforced at the custom-fetch
 *     layer; it never shares a code path with the Web tool SSRF validator.
 *
 * UPDATE-WITH: services/gateway/packages/provider-gateway/src/providers/clients.ts,
 *              services/gateway/packages/provider-gateway/src/providers/openai-oauth.ts,
 *              services/gateway/packages/provider-gateway/src/providers/openai-oauth-refresh.ts,
 *              services/gateway/packages/provider-gateway/src/providers/pool.ts,
 *              services/gateway/packages/provider-gateway/src/providers/credentials.ts
 */
export { createProviderGatewayApp } from "./app.js";
export { authenticateGatewayCaller, KubernetesTokenReviewClient } from "./auth.js";
export { validateProviderRequest, validateProviderStreamEvent } from "@tetral/gateway-protocol/src/bounds.js";
export { validateRunWebRequest } from "./bounds.js";
export { buildProviderGatewayCommandDependencies, runProviderGatewayCommand } from "./command.js";
export { loadProviderGatewayConfig, loadProviderGatewayConfigFromEnv } from "./config.js";
export { createGatewayGrpcServer } from "./grpc-server.js";
export { createJsonLogger } from "./logger.js";
export { ProviderGatewayServiceShell } from "./service.js";
