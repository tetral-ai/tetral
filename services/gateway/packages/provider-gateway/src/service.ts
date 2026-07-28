/**
 * @packageDocumentation
 *
 * Orchestrates the provider-gateway data plane for one Runtime request. The
 * service shell authenticates the caller, checks readiness and request bounds,
 * verifies the Runtime binding, admits the turn, resolves attachments and
 * credentials, and streams catalog-approved provider events back to gRPC.
 * Platform credentials may switch only before the provider streamer emits its
 * first event; session credentials always receive one attempt. Caught provider
 * faults close open fragments before the shell emits a terminal provider error.
 *
 * The application and gRPC server construct and call this module. It delegates
 * binding checks to `RuntimeBindingTokenVerifier`, attachment reads to
 * `ProviderAttachmentResolver`, credential selection to
 * `ProviderCredentialResolver`, provider lowering and I/O to
 * `ProviderRequestStreamer`, and stream liveness to `controlProviderStream`.
 * The shell holds only per-process admission and metrics state and does not own
 * session, message, event, or usage persistence.
 */
import { Metadata, status } from "@grpc/grpc-js";
import { semanticErrorFields } from "@tetral/ts-observability";
import type {
  ProviderAttachmentRejection,
  ProviderRequest,
  ProviderStreamEvent,
  RunWebRequest,
  RunWebResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { validateProviderRequest, validateProviderStreamEvent } from "@tetral/gateway-protocol/src/bounds.js";
import { classifyProviderStreamError, ProviderStreamTimeoutError, providerErrorEvent } from "@tetral/gateway-lowering/src/errors.js";
import { GrpcStatusError } from "./errors.js";
import type { RuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import type { GatewayLogger } from "./logger.js";
import { lookupGatewayModel } from "./providers/catalog.js";
import { controlProviderStream } from "./providers/stream-control.js";
import { classifyProviderFailure, PlatformKeyPoolConstants, ProviderKeyFailureError } from "./providers/pool.js";
import type { ProviderCredentialResolver, ResolvedProviderCredential } from "./providers/credentials.js";
import type { ProviderErrorInput } from "@tetral/gateway-lowering/src/errors.js";
import type { ResolvedProviderRequestAttachment } from "@tetral/gateway-lowering/src/request.js";
import { ProviderGatewayMetricsRegistry } from "./metrics.js";

/**
 * Authenticates the internal gRPC caller and returns the workload identity
 * whose pod UID participates in Runtime binding verification.
 */
export interface GatewayAuthenticator {
  authenticate(input: { readonly metadata: Metadata; readonly method: string }): Promise<{
    readonly ok: true;
    readonly serviceAccount: { readonly namespace: string; readonly name: string; readonly podUid: string };
  } | {
    readonly ok: false;
    readonly code: "Unauthenticated" | "PermissionDenied";
    readonly message: string;
  }>;
}

/**
 * Supplies the admission, identity, provider, attachment, timeout, logging,
 * and observability dependencies used by `ProviderGatewayServiceShell`.
 */
export interface ProviderGatewayServiceOptions {
  readonly authenticator: GatewayAuthenticator;
  readonly runtimeBindingTokenVerifier: RuntimeBindingTokenVerifier;
  readonly logger: GatewayLogger;
  readonly ready: () => boolean;
  readonly providerStreamer?: ProviderRequestStreamer | undefined;
  readonly credentialResolver?: ProviderCredentialResolver | undefined;
  readonly attachmentResolver?: ProviderAttachmentResolver | undefined;
  readonly maxConcurrentTurns?: number | undefined;
  readonly providerStreamTimeouts?: ProviderStreamTimeoutOptions | undefined;
  readonly metrics?: ProviderGatewayMetricsRegistry | undefined;
}

/**
 * Coordinates authenticated provider turns for the gRPC and ops-plane server
 * adapters.
 *
 * The shell performs request-wide admission and orchestration while injected
 * resolvers own external reads and the provider streamer owns provider-specific
 * lowering and transport. Its mutable state is limited to the local concurrent-
 * turn counter and metrics registry.
 */
export class ProviderGatewayServiceShell {
  private readonly providerStreamer: ProviderRequestStreamer;
  private readonly admission: TurnAdmissionGate;
  private readonly metrics: ProviderGatewayMetricsRegistry;

  constructor(private readonly options: ProviderGatewayServiceOptions) {
    this.providerStreamer = options.providerStreamer ?? new CatalogGatedProviderStreamer();
    this.admission = new TurnAdmissionGate(options.maxConcurrentTurns ?? 8);
    this.metrics = options.metrics ?? new ProviderGatewayMetricsRegistry();
  }

  /**
   * Returns a lazy event stream for one provider request.
   *
   * Iteration performs TokenReview authentication and binding checks before
   * attachment, credential, or provider I/O, holds one admission slot through
   * completion, and propagates caller aborts to attachment and provider work
   * while ending its wait for credential resolution.
   */
  streamProviderRequest(
    request: ProviderRequest,
    metadata: Metadata,
    options: { readonly abortSignal?: AbortSignal } = {},
  ): AsyncIterable<ProviderStreamEvent> {
    return this.streamProviderRequestGenerator(request, metadata, options.abortSignal);
  }

  private async *streamProviderRequestGenerator(
    request: ProviderRequest,
    metadata: Metadata,
    abortSignal: AbortSignal | undefined,
  ): AsyncGenerator<ProviderStreamEvent> {
    const started = performance.now();
    let requestOutcome: "ok" | "failed" = "failed";
    let errorClass = "runtime_error";
    let errorCode = "stream_incomplete";
    let callerServiceAccount: string | undefined;
    let requestIdentity: {
      readonly "workspace.id": string;
      readonly "session.id": string;
      readonly "thread.id": string;
      readonly "request.id": string;
      readonly "provider.request.id": string;
      readonly "model_request.id": string;
    } | undefined;
    try {
      const caller = await this.authorize(metadata, "/tetral.provider_gateway.v1.ProviderGatewayService/StreamProviderRequest");
      callerServiceAccount = `${caller.serviceAccount.namespace}/${caller.serviceAccount.name}`;
      this.ensureReady();
      const validation = validateProviderRequest(request);
      if (!validation.ok) {
        throw new GrpcStatusError(status.INVALID_ARGUMENT, validation.message);
      }
      requestIdentity = {
        "workspace.id": request.workspaceId,
        "session.id": request.sessionId,
        "thread.id": request.sessionThreadId,
        "request.id": request.requestId,
        "provider.request.id": request.modelRequestId,
        "model_request.id": request.modelRequestId,
      };
      if ((request.model?.variant ?? "") !== "") {
        throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
      }
      const limits = request.limits;
      if (limits === undefined) {
        throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
      }
      if (!this.options.runtimeBindingTokenVerifier.verify({
        request,
        runtimeBindingToken: request.runtimeBindingToken,
        runtimePodUid: caller.serviceAccount.podUid,
      })) {
        throw new GrpcStatusError(status.PERMISSION_DENIED, "runtime binding token rejected");
      }
      const release = this.admission.tryAcquire();
      if (release === undefined) {
        errorClass = "provider_error";
        errorCode = "provider_unavailable";
        yield providerErrorEvent(request, {
          code: "provider_unavailable",
          message: "Gateway provider capacity is exhausted.",
          retryable: true,
          fatal: false,
          statusCode: 503,
        });
        return;
      }
      const finishMetrics = this.metrics.startProviderStream();
      let failed = false;
      const providerAbortController = new AbortController();
      const providerDeadline = performance.now() + limits.timeoutMs;
      const forwardAbort = (): void => providerAbortController.abort(abortSignal?.reason ?? new DOMException("Provider request cancelled.", "AbortError"));
      if (abortSignal?.aborted === true) {
        forwardAbort();
      } else {
        abortSignal?.addEventListener("abort", forwardAbort, { once: true });
      }
      try {
        for await (const event of this.streamProviderAttempts(
          request,
          caller.serviceAccount.podUid,
          providerAbortController.signal,
          providerDeadline,
          () => providerAbortController.abort(new DOMException("Provider request timed out.", "AbortError")),
        )) {
          if (event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR) {
            failed = true;
            errorClass = "provider_error";
            errorCode = providerLogErrorCode(event.providerError?.error?.code);
          }
          yield event;
        }
      } catch (error) {
        failed = true;
        if (error instanceof GrpcStatusError) {
          errorClass = "grpc_status";
          errorCode = String(error.code);
          throw error;
        }
        const classified = classifyProviderStreamError(error);
        errorClass = "provider_error";
        errorCode = providerLogErrorCode(classified.code);
        yield providerErrorEvent(request, classified);
      } finally {
        abortSignal?.removeEventListener("abort", forwardAbort);
        finishMetrics(failed);
        release();
      }
      requestOutcome = failed ? "failed" : "ok";
    } catch (error) {
      requestOutcome = "failed";
      if (error instanceof GrpcStatusError) {
        errorClass = "grpc_status";
        errorCode = String(error.code);
      } else if (errorCode === "stream_incomplete") {
        errorCode = "internal";
      }
      throw error;
    } finally {
      const record = {
        event: "provider_request_streamed",
        "event.kind": "provider_request_streamed",
        operation: "StreamProviderRequest",
        component: "gateway",
        "grpc.method": "/tetral.provider_gateway.v1.ProviderGatewayService/StreamProviderRequest",
        "caller.service_account": callerServiceAccount,
        ...requestIdentity,
        "request.outcome": requestOutcome,
        "duration.ms": Math.round(performance.now() - started),
      } as const;
      try {
        this.options.logger.info(requestOutcome === "ok"
          ? record
          : {
              ...record,
              ...semanticErrorFields({
                errorClass,
                errorCode,
                messageSafe: "provider request failed",
              }),
            });
      } catch {
        // Observability must not replace the request's outcome.
      }
    }
  }

  /** Renders the current provider-stream metrics with live readiness state. */
  metricsText(): string {
    return this.metrics.render({ ready: this.options.ready() });
  }

  /**
   * Rejects the provider port's Web RPC. Web execution belongs to the
   * `web-connector` container, which serves this same service definition on its
   * own port; the Runtime Pod dials it directly. A call arriving here is a
   * misrouted client, not a deferred feature.
   */
  async runWeb(_request: RunWebRequest, metadata: Metadata): Promise<RunWebResponse> {
    const caller = await this.authorize(metadata, "/tetral.provider_gateway.v1.ProviderGatewayService/RunWeb");
    this.options.logger.info({
      event: "gateway_web_misrouted",
      "event.kind": "gateway_web_misrouted",
      operation: "RunWeb",
      component: "gateway",
      "grpc.method": "/tetral.provider_gateway.v1.ProviderGatewayService/RunWeb",
      "caller.service_account": `${caller.serviceAccount.namespace}/${caller.serviceAccount.name}`,
      ...semanticErrorFields({
        errorClass: "runtime_error",
        errorCode: "web_not_served_here",
        messageSafe: "web execution is served by the web-connector port",
      }),
    });
    throw new GrpcStatusError(status.UNIMPLEMENTED, "web execution is served by the web-connector port");
  }

  private async authorize(metadata: Metadata, method: string): Promise<{ readonly serviceAccount: { readonly namespace: string; readonly name: string; readonly podUid: string } }> {
    const caller = await this.options.authenticator.authenticate({ metadata, method });
    if (!caller.ok) {
      throw new GrpcStatusError(caller.code === "Unauthenticated" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, caller.message);
    }
    return caller;
  }

  private ensureReady(): void {
    if (!this.options.ready()) {
      throw new GrpcStatusError(status.UNAVAILABLE, "gateway service not ready");
    }
  }

  private providerStreamTimeouts(overallTimeoutMs: number): ResolvedProviderStreamTimeoutOptions {
    return {
      firstByteTimeoutMs: providerTimeoutMs(
        this.options.providerStreamTimeouts?.firstByteTimeoutMs,
        DefaultProviderChunkTimeoutMs,
        overallTimeoutMs,
      ),
      interChunkTimeoutMs: providerTimeoutMs(
        this.options.providerStreamTimeouts?.interChunkTimeoutMs,
        DefaultProviderChunkTimeoutMs,
        overallTimeoutMs,
      ),
    };
  }

  private async *streamProviderAttempts(
    request: ProviderRequest,
    runtimePodUid: string,
    abortSignal: AbortSignal,
    providerDeadline: number,
    abortOnTimeout: () => void,
  ): AsyncGenerator<ProviderStreamEvent> {
    const catalogError = this.catalogGate(request);
    if (catalogError !== undefined) {
      yield providerErrorEvent(request, catalogError);
      return;
    }
    const attachmentResolution = await withinProviderDeadline(
      this.resolveAttachments(request, runtimePodUid, abortSignal),
      abortSignal,
      providerDeadline,
      abortOnTimeout,
    );
    if (!attachmentResolution.ok) {
      yield providerErrorEvent(request, attachmentResolution.error);
      return;
    }
    if (attachmentResolution.rejections.length > 0) {
      yield {
        requestId: request.requestId,
        modelRequestId: request.modelRequestId,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS,
        attachmentRejections: {
          rejections: [...attachmentResolution.rejections],
        },
      };
    }
    const providerRequest = {
      ...request,
      attachments: attachmentResolution.attachments.map(({ data: _data, ...attachment }) => attachment),
    };
    // Per-turn provider loop. The `emitted` latch is set by the first event from
    // the provider client and splits platform-key failover from Runtime-owned
    // recovery. Attachment-rejections are emitted before this loop and do not
    // close the failover window:
    //
    //   | phase                     | credential | on provider fault                        | re-enters loop? |
    //   | ------------------------- | ---------- | ---------------------------------------- | --------------- |
    //   | emitted == false          | platform   | classify; optionally cool/quarantine;     | yes, unless     |
    //   |                           |            | exclude this key for the turn              | fail-fast or    |
    //   |                           |            |                                            | max attempts    |
    //   | emitted == false          | session    | one attempt only; forward provider-error | no              |
    //   | emitted == true (any src) | any        | no switch, no retry; forward terminal;    | no              |
    //   |                           |            | Runtime owns recovery                     |                 |
    //
    // A fault that arrives mid-fragment first drains ProviderOpenFragmentTracker
    // -end events to close any open text/reasoning/tool-input fragment, THEN emits
    // the provider-error terminal, so no fragment is left unclosed downstream.
    const attemptedPlatformKeyIds = new Set<string>();
    let platformAttempts = 0;
    let lastPlatformProviderError: ProviderErrorInput | undefined;
    while (true) {
      const credential = await withinProviderDeadline(
        this.resolveCredential(request, attemptedPlatformKeyIds, lastPlatformProviderError),
        abortSignal,
        providerDeadline,
        abortOnTimeout,
      );
      if (!credential.ok) {
        yield providerErrorEvent(request, credential.error);
        return;
      }
      const resolvedCredential = credential.credential;
      let emitted = false;
      const openFragments = new ProviderOpenFragmentTracker();
      try {
        const providerEvents = controlProviderStream(this.providerStreamer.stream({
          request: providerRequest,
          abortSignal,
          credential: resolvedCredential,
          resolvedAttachments: attachmentResolution.attachments,
        }), {
          abortSignal,
          abortOnTimeout,
          ...this.providerStreamTimeouts(providerDeadlineRemainingMs(providerDeadline)),
          overallTimeoutMs: providerDeadlineRemainingMs(providerDeadline),
        });
        for await (const event of providerEvents) {
          const eventValidation = validateProviderStreamEvent(event, request);
          if (!eventValidation.ok) {
            throw new GrpcStatusError(status.INTERNAL, "gateway provider stream contract violation");
          }
          emitted = true;
          openFragments.record(event);
          yield event;
        }
        return;
      } catch (error) {
        if (error instanceof GrpcStatusError) {
          throw error;
        }
        const providerKeyFailure = error instanceof ProviderKeyFailureError
          ? error
          : resolvedCredential?.source === "platform" && !emitted
            ? new ProviderKeyFailureError(classifyProviderFailure(resolvedCredential.providerId, providerAttemptFailureInput(error)))
            : undefined;
        if (providerKeyFailure === undefined) {
          for (const closeEvent of openFragments.closeEvents(request)) {
            yield closeEvent;
          }
          yield providerErrorEvent(request, classifyProviderStreamError(error));
          return;
        }
        if (resolvedCredential?.source !== "platform" || emitted) {
          for (const closeEvent of openFragments.closeEvents(request)) {
            yield closeEvent;
          }
          yield providerErrorEvent(request, providerKeyFailure.classification.providerError);
          return;
        }
        const platformKeyId = resolvedCredential.platformKey.keyId;
        this.options.credentialResolver?.recordPlatformFailure(platformKeyId, providerKeyFailure.classification);
        attemptedPlatformKeyIds.add(platformKeyId);
        platformAttempts += 1;
        lastPlatformProviderError = providerKeyFailure.classification.providerError;
        if (providerKeyFailure.classification.action === "fail-fast" || platformAttempts >= PlatformKeyPoolConstants.maxKeySwitchesPerTurn) {
          yield providerErrorEvent(request, providerKeyFailure.classification.providerError);
          return;
        }
      }
    }
  }

  private async resolveCredential(
    request: ProviderRequest,
    attemptedPlatformKeyIds: ReadonlySet<string>,
    lastPlatformProviderError: ProviderErrorInput | undefined,
  ): Promise<{ readonly ok: true; readonly credential: ResolvedProviderCredential | undefined } | { readonly ok: false; readonly error: ProviderErrorInput }> {
    if (this.options.credentialResolver === undefined) {
      return { ok: true, credential: undefined };
    }
    const resolved = await this.options.credentialResolver.resolve(request, {
      excludedPlatformKeyIds: attemptedPlatformKeyIds,
    });
    if (resolved.ok) {
      return resolved;
    }
    if (lastPlatformProviderError !== undefined && resolved.error.code === "platform_keys_exhausted") {
      return { ok: false, error: lastPlatformProviderError };
    }
    return resolved;
  }

  private async resolveAttachments(
    request: ProviderRequest,
    runtimePodUid: string,
    abortSignal: AbortSignal,
  ): Promise<{
    readonly ok: true;
    readonly attachments: readonly ResolvedProviderRequestAttachment[];
    readonly rejections: readonly ProviderAttachmentRejection[];
  } | { readonly ok: false; readonly error: ProviderErrorInput }> {
    if (request.attachments.length === 0) {
      return { ok: true, attachments: [], rejections: [] };
    }
    if (this.options.attachmentResolver === undefined) {
      return { ok: false, error: attachmentUnavailableProviderError() };
    }
    try {
      return await this.options.attachmentResolver.resolve({ request, runtimePodUid, abortSignal });
    } catch {
      return { ok: false, error: attachmentUnavailableProviderError() };
    }
  }

  private catalogGate(request: ProviderRequest): ProviderErrorInput | undefined {
    const model = request.model;
    const entry = model === undefined ? undefined : lookupGatewayModel(model.providerId, model.modelId);
    if (entry === undefined) {
      return {
        code: "provider_unavailable",
        message: "Provider model is not approved by the Gateway catalog.",
        retryable: false,
        fatal: true,
        statusCode: 503,
      };
    }
    return undefined;
  }
}

function providerLogErrorCode(value: string | undefined): string {
  return value !== undefined && /^[a-z0-9_.-]{1,128}$/i.test(value)
    ? value
    : "provider_error";
}

function providerAttemptFailureInput(error: unknown): Parameters<typeof classifyProviderFailure>[1] {
  if (error instanceof DOMException && error.name === "AbortError") {
    return { networkError: true };
  }
  if (error instanceof TypeError) {
    return { networkError: true };
  }
  const record = typeof error === "object" && error !== null ? error as Record<string, unknown> : {};
  return {
    statusCode: typeof record.statusCode === "number" ? record.statusCode : undefined,
    body: record.data ?? record.responseBody ?? record.body,
    headers: providerAttemptHeaders(record.responseHeaders ?? record.headers),
    networkError: record.networkError === true,
    timeout: record.timeout === true,
  };
}

function providerAttemptHeaders(value: unknown): Readonly<Record<string, string | undefined>> | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return undefined;
  }
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .filter((entry): entry is [string, string] => typeof entry[1] === "string")
      .map(([key, headerValue]) => [key.toLowerCase(), headerValue]),
  );
}

class ProviderOpenFragmentTracker {
  private readonly textIds = new Set<string>();
  private readonly reasoningIds = new Set<string>();
  private readonly toolInputs = new Map<string, string>();

  record(event: ProviderStreamEvent): void {
    switch (event.type) {
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START:
        this.add(this.textIds, event.text?.id);
        return;
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END:
        this.delete(this.textIds, event.text?.id);
        return;
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START:
        this.add(this.reasoningIds, event.reasoning?.id);
        return;
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END:
        this.delete(this.reasoningIds, event.reasoning?.id);
        return;
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START:
        if (event.toolInput !== undefined && event.toolInput.id.length > 0) {
          this.toolInputs.set(event.toolInput.id, event.toolInput.name);
        }
        return;
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END:
      case ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL:
        this.toolInputs.delete(event.toolInput?.id ?? event.toolCall?.id ?? "");
        return;
      default:
        return;
    }
  }

  closeEvents(request: ProviderRequest): ProviderStreamEvent[] {
    const events: ProviderStreamEvent[] = [];
    for (const id of this.textIds) {
      events.push({
        requestId: request.requestId,
        modelRequestId: request.modelRequestId,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END,
        text: { id, text: "", metadataJson: "{}" },
      });
    }
    for (const id of this.reasoningIds) {
      events.push({
        requestId: request.requestId,
        modelRequestId: request.modelRequestId,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END,
        reasoning: { id, text: "", metadataJson: "{}" },
      });
    }
    for (const [id, name] of this.toolInputs) {
      events.push({
        requestId: request.requestId,
        modelRequestId: request.modelRequestId,
        type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END,
        toolInput: { id, name, text: "", metadataJson: "{}" },
      });
    }
    this.textIds.clear();
    this.reasoningIds.clear();
    this.toolInputs.clear();
    return events;
  }

  private add(ids: Set<string>, id: string | undefined): void {
    if (id !== undefined && id.length > 0) {
      ids.add(id);
    }
  }

  private delete(ids: Set<string>, id: string | undefined): void {
    if (id !== undefined && id.length > 0) {
      ids.delete(id);
    }
  }
}

/**
 * Resolves request attachment references into bounded bytes and reports
 * identity-bearing per-reference rejections without owning those bytes.
 */
export interface ProviderAttachmentResolver {
  readonly resolve: (input: ProviderAttachmentResolveInput) => Promise<
    {
      readonly ok: true;
      readonly attachments: readonly ResolvedProviderRequestAttachment[];
      readonly rejections: readonly ProviderAttachmentRejection[];
    } |
    { readonly ok: false; readonly error: ProviderErrorInput }
  >;
}

/** Carries the authenticated Runtime scope and cancellation signal for attachment resolution. */
export interface ProviderAttachmentResolveInput {
  readonly request: ProviderRequest;
  readonly runtimePodUid: string;
  readonly abortSignal?: AbortSignal | undefined;
}

/**
 * Streams normalized provider events for one resolved, catalog-approved
 * provider attempt. The service shell validates each returned event and, when
 * the implementation throws a non-gRPC-status fault, closes any tracked open
 * fragments before emitting the terminal provider error. Explicit gRPC status
 * failures propagate to the transport adapter instead.
 */
export interface ProviderRequestStreamer {
  readonly stream: (input: ProviderRequestStreamInput) => AsyncIterable<ProviderStreamEvent>;
}

/**
 * Carries the request, selected credential, resolved attachment bytes, and
 * cancellation signal from orchestration into a provider client adapter.
 */
export interface ProviderRequestStreamInput {
  readonly request: ProviderRequest;
  readonly abortSignal?: AbortSignal | undefined;
  readonly credential?: ResolvedProviderCredential | undefined;
  readonly resolvedAttachments?: readonly ResolvedProviderRequestAttachment[] | undefined;
}

/** Configures first-event and between-event liveness watchdog budgets. */
export interface ProviderStreamTimeoutOptions {
  readonly firstByteTimeoutMs?: number | undefined;
  readonly interChunkTimeoutMs?: number | undefined;
}

interface ResolvedProviderStreamTimeoutOptions {
  readonly firstByteTimeoutMs: number;
  readonly interChunkTimeoutMs: number;
}

class CatalogGatedProviderStreamer implements ProviderRequestStreamer {
  async *stream(input: ProviderRequestStreamInput): AsyncGenerator<ProviderStreamEvent> {
    input.abortSignal?.throwIfAborted();
    const model = input.request.model;
    const entry = model === undefined ? undefined : lookupGatewayModel(model.providerId, model.modelId);
    const message = entry === undefined
      ? "Provider model is not approved by the Gateway catalog."
      : "Provider Gateway streamer is not configured.";
    yield providerErrorEvent(input.request, {
      code: "provider_unavailable",
      message,
      retryable: false,
      fatal: true,
      statusCode: 503,
    });
  }
}

class TurnAdmissionGate {
  private inFlight = 0;

  constructor(private readonly maxConcurrentTurns: number) {}

  tryAcquire(): (() => void) | undefined {
    if (this.maxConcurrentTurns <= 0 || this.inFlight >= this.maxConcurrentTurns) {
      return undefined;
    }
    this.inFlight += 1;
    let released = false;
    return () => {
      if (!released) {
        released = true;
        this.inFlight -= 1;
      }
    };
  }
}

// The normalized provider-event watchdog default, applied as both the first-event
// and between-event budget (each clamped down to the overall deadline). This
// watchdog is the guard that ends a stalled event stream: it re-arms per event,
// so an actively emitting provider adapter keeps resetting it. The raw HTTP body
// has a separate inter-chunk watchdog in clients.ts. The overall ProviderRequest
// deadline (request.limits.timeoutMs, applied via providerDeadline) bounds total
// duration and is sized at deployment (default 1,800 s, tunable) ABOVE legitimate
// long generations, so a healthy long stream finishes under the watchdog rather
// than being truncated by the overall deadline.
const DefaultProviderChunkTimeoutMs = 30_000;

function attachmentUnavailableProviderError(): ProviderErrorInput {
  return {
    code: "attachment_unavailable",
    message: "Attachment bytes are not available to the provider request.",
    retryable: true,
    fatal: false,
    statusCode: 503,
  };
}

function providerTimeoutMs(value: number | undefined, fallback: number, overallTimeoutMs: number): number {
  if (value !== undefined && Number.isInteger(value) && value > 0) {
    return Math.min(value, overallTimeoutMs);
  }
  return Math.min(fallback, overallTimeoutMs);
}

function providerDeadlineRemainingMs(deadline: number): number {
  return Math.max(1, Math.ceil(deadline - performance.now()));
}

async function withinProviderDeadline<T>(
  operation: Promise<T>,
  abortSignal: AbortSignal,
  deadline: number,
  abortOnTimeout: () => void,
): Promise<T> {
  abortSignal.throwIfAborted();
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  let abortListener: (() => void) | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timeoutHandle = setTimeout(() => {
      reject(new ProviderStreamTimeoutError("overall_timeout"));
      abortOnTimeout();
    }, providerDeadlineRemainingMs(deadline));
  });
  const aborted = new Promise<never>((_resolve, reject) => {
    abortListener = () => reject(providerAbortError(abortSignal));
    abortSignal.addEventListener("abort", abortListener, { once: true });
  });
  try {
    return await Promise.race([operation, timeout, aborted]);
  } finally {
    if (timeoutHandle !== undefined) {
      clearTimeout(timeoutHandle);
    }
    if (abortListener !== undefined) {
      abortSignal.removeEventListener("abort", abortListener);
    }
  }
}

function providerAbortError(signal: AbortSignal): DOMException {
  return signal.reason instanceof DOMException && signal.reason.name === "AbortError"
    ? signal.reason
    : new DOMException("Provider request cancelled.", "AbortError");
}
