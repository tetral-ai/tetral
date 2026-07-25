/**
 * @packageDocumentation
 *
 * Request lowering produces `LoweredProviderRequest` with
 * `LoweredProviderMessage` values. Provider-gateway alone converts that
 * SDK-free plan to concrete AI SDK `ModelMessage` values.
 *
 * Pure transformer between Runtime shapes and SDK-free provider request/stream
 * plans. It lowers RuntimeMessages to intermediate provider messages, raises
 * package-defined Gateway stream parts to ProviderStreamEvents, normalizes
 * provider usage, and classifies provider errors. It runs no I/O of its own;
 * provider-gateway performs both concrete AI SDK adaptations.
 *
 * On the request side, the concrete output is an intermediate
 * `LoweredProviderRequest`, not an AI SDK `ModelMessage` array. The plan avoids
 * SDK runtime types while retaining the schema discriminator that the
 * provider-gateway adapter converts through the AI SDK.
 *
 * OWNS:
 * - RuntimeMessage -> LoweredProviderMessage planning (request.ts:
 *   lowerProviderRequest) including attachment lowering, cache-control marking,
 *   output-schema / provider-option assembly, and JSON-schema lowering.
 *   The concrete message records are this package's `LoweredProviderMessage`
 *   intermediate values; provider-gateway performs the final SDK conversion.
 * - GatewayStreamPart -> ProviderStreamEvent raising and the terminal latch
 *   (stream.ts: ProviderStreamRaiser). Provider-gateway first maps concrete AI
 *   SDK fullStream parts into this package-owned union.
 * - Provider usage normalization across wire families (usage.ts:
 *   normalizeProviderUsage).
 * - Provider error classification and retryability (errors.ts).
 * - Per-provider lowering rules (rules/).
 * It owns no database tables, queues, RPCs, or network endpoints.
 *
 * STATE MACHINE (stream raise; full table lives in stream.ts):
 * - open -> terminal: writer ProviderStreamRaiser.map on a "finish" part.
 * - terminal -> (none): once terminal, any further part other than "raw" or
 *   "finish-step" throws.
 *
 * INVARIANTS:
 * - Pure and self-contained: performs no network, database, or runtime-
 *   environment access; the only dependency is the generated protocol types
 *   package @tetral/gateway-protocol.
 * - Attachment bytes arrive already resolved: callers pass
 *   ResolvedProviderRequestAttachment (data: Uint8Array); this package never
 *   dereferences or reads an attachment reference itself.
 * - No connector-protocol token appears anywhere in this package's source;
 *   provider lowering and the connector wire are separate packages.
 * - Lowering runs in a fixed order (attachment + message normalization, then
 *   cache-control marking, then output-schema / provider-option assembly);
 *   reordering is outbound-wire visible (see request.ts: lowerProviderRequest).
 * - Producer-side stream-contract enforcement (single terminal, stable fragment
 *   ids, tool-call name agrees with streamed tool input) mirrors the consumer
 *   ProviderStreamValidator.
 *   This producer guarantee is deliberately partial: only synthesized ids for
 *   id-less fragments are stable here. Runtime owns full fragment lifecycle,
 *   duplicate, placement, and terminal validation.
 * - SDK inputTokens is the cache-inclusive input total; uncached input is
 *   derived per wire family (see usage.ts).
 *
 * UPDATE-WITH:
 * - services/gateway/packages/provider-gateway/src/providers/clients.ts
 *   (constructs requests and raisers, assigns each model its usage wire family,
 *   and routes each provider to a client).
 * - services/agent-runtime/packages/core/src/llm/llm-service.ts
 *   (ProviderStreamValidator, the consumer that re-checks the stream contract).
 * - services/gateway/packages/protocol/src (generated RuntimeMessage /
 *   ProviderStreamEvent / RequestUsage shapes these transforms target).
 */
export {};
