/**
 * @packageDocumentation
 *
 * Orchestrates the provider-gateway data plane for one Runtime request. The
 * service shell authenticates the caller, checks readiness and request bounds,
 * verifies the Runtime binding, admits the turn, resolves attachments and
 * credentials, and streams catalog-approved provider events back to gRPC.
 * Platform credentials may switch only before the provider streamer emits its
 * first event; session credentials always receive one attempt. The request-local
 * block assembler emits complete content and forwards success only after every
 * explicit fragment lifecycle closes; provider faults discard partial fragments and retain their own error.
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
import { NormalizedProviderEventType, validateNormalizedProviderEvent } from "@tetral/gateway-lowering/src/normalized-stream.js";
import type { NormalizedProviderEvent } from "@tetral/gateway-lowering/src/normalized-stream.js";
import { ProviderBlockAssembler, ProviderIncompleteStreamError, ProviderAssemblyLimitError } from "./providers/block-assembler.js";
import type { ProviderAssemblyBounds, ProviderPreviewOffer, ProviderAssemblyResources } from "./providers/block-assembler.js";
import { ProviderSdkRetentionLimitError } from "./providers/sdk-retention-guard.js";
import { ProviderAssemblyCalibrationCandidate } from "./providers/resource-policy.js";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MaxIdBytes, validateProviderRequest } from "@tetral/gateway-protocol/src/bounds.js";
import { classifyProviderStreamError, ProviderRequestLoweringError, ProviderStreamTimeoutError, providerErrorEvent } from "@tetral/gateway-lowering/src/errors.js";
import { GrpcStatusError } from "./errors.js";
import type { RuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import type { GatewayLogger } from "./logger.js";
import { lookupGatewayModel } from "./providers/catalog.js";
import { controlProviderStream } from "./providers/stream-control.js";
import type { ProviderStreamWatchdogKind } from "./providers/stream-control.js";
import { classifyProviderFailure, PlatformKeyPoolConstants, ProviderKeyFailureError } from "./providers/pool.js";
import type { ProviderFailureClassification } from "./providers/pool.js";
import type { ProviderCredentialResolver, ResolvedProviderCredential } from "./providers/credentials.js";
import type { ProviderErrorInput } from "@tetral/gateway-lowering/src/errors.js";
import type { ResolvedProviderRequestAttachment } from "@tetral/gateway-lowering/src/request.js";
import { ProviderGatewayMetricsRegistry } from "./metrics.js";
import type { ProviderStageSample, ProviderStageOutcome, ProviderContentKind } from "./metrics.js";
import { ProviderStreamEvent as ProviderStreamFrame } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

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
  readonly assemblyBounds?: ProviderAssemblyBounds;
  readonly allocateEventId?: () => string;
  readonly offerPreview?: ProviderPreviewOffer;
  readonly observeAssemblyResources?: (resources:ProviderAssemblyResources,requestId:string)=>void;
  readonly observationClock?: ()=>number;
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
  private stopping = false;
  private readonly frameDeadlines = new WeakMap<ProviderStreamEvent,{deadline:number;request:ProviderRequest;startedAt:number}>();
  providerFrameDeadline(event:ProviderStreamEvent):number|undefined { return this.frameDeadlines.get(event)?.deadline; }
  recordProviderWriteTimeout(event:ProviderStreamEvent):void {
    const frame=this.frameDeadlines.get(event);if(frame!==undefined)logGatewayProviderTimeout(this.options.logger,frame.request,{kind:"overall_timeout",elapsedMs:performance.now()-frame.startedAt});
  }
  private readonly completeFrameTimes = new WeakMap<ProviderStreamEvent, { completedAt: number; requestId: string; kind: ProviderContentKind; canonicalBytes: number; encodedBytes: number; release?: () => void; callbackAt?: number }>();
  recordCompleteFrameWriteStarted(event: ProviderStreamEvent): void {
    const frame = this.completeFrameTimes.get(event);
    if (frame === undefined || frame.release !== undefined) return;
    try { frame.release = this.metrics.holdCompleteFrame(frame.encodedBytes); } catch { /* Observability is fail-open. */ }
  }
  recordCompleteFrameWriteCallback(event: ProviderStreamEvent): void {
    const frame = this.completeFrameTimes.get(event);
    if (frame !== undefined) frame.callbackAt = this.clock();
  }
  recordCompleteFrameWrite(event: ProviderStreamEvent, outcome: ProviderStageOutcome = "success"): void {
    const frame = this.completeFrameTimes.get(event);
    this.frameDeadlines.delete(event);
    if (frame === undefined) return;
    this.completeFrameTimes.delete(event);
    try { frame.release?.(); } catch { /* Observability is fail-open. */ }
    this.observeStage({stage:"complete_frame_write", outcome, kind:frame.kind, durationMs:(frame.callbackAt ?? this.clock()) - frame.completedAt, canonicalBytes:frame.canonicalBytes, encodedBytes:frame.encodedBytes},frame.requestId);
  }
  private observeStage(sample: ProviderStageSample, requestId: string): void {
    try { this.metrics.observeProviderStage(sample); } catch { /* Observability is fail-open. */ }
    try { this.options.logger.info({event:"provider.stage_completed", "event.kind":"stage_completed", component:"gateway", operation:"provider.stream", "request.id":requestId, stage:sample.stage, outcome:sample.outcome, kind:sample.kind, duration_ms:Math.max(0,sample.durationMs), bytes_in:sample.canonicalBytes, bytes_out:sample.encodedBytes}); } catch { /* The raw sample sink cannot affect delivery. */ }
  }
  private clock():number { return this.options.observationClock?.() ?? performance.now(); }
  private readonly workers = new Map<AbortController, Promise<void>>();

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
    let assemblyMetrics:ReturnType<ProviderGatewayMetricsRegistry["startContentAssembly"]> | undefined;
    try { assemblyMetrics=this.metrics.startContentAssembly(); } catch { /* Fail-open metrics. */ }
    const assembler = new ProviderBlockAssembler({
      bounds: this.options.assemblyBounds ?? ProviderAssemblyCalibrationCandidate,
      request,
      observeResources: resources=>{
        try { assemblyMetrics?.observe(resources); } catch { /* Fail-open metrics. */ }
        try { this.options.observeAssemblyResources?.(resources,request.requestId); } catch { /* Fail-open metrics. */ }
      },
      ...(this.options.allocateEventId === undefined ? {} : { allocateEventId: this.options.allocateEventId }),
      ...(this.options.offerPreview === undefined ? {} : { offerPreview: this.options.offerPreview }),
    });
    const processController = new AbortController();
    abortSignal =
      abortSignal === undefined
        ? processController.signal
        : AbortSignal.any([abortSignal, processController.signal]);
    let done!: () => void;
    this.workers.set(
      processController,
      new Promise<void>((resolve) => {
        done = resolve;
      }),
    );
    const started = performance.now();
    let requestOutcome: "ok" | "failed" = "failed";
    let errorClass = "runtime_error";
    let errorCode = "stream_incomplete";
    let providerStatusCode: number | undefined;
    let providerFailureClass = "provider_error";
    let validationMember: string | undefined;
    let callerServiceAccount: string | undefined;
    let requestIdentity: Partial<{
      readonly "workspace.id": string;
      readonly "session.id": string;
      readonly "thread.id": string;
      readonly "request.id": string;
      readonly "model_request.id": string;
    }> | undefined;
    try {
      const caller = await this.authorize(metadata, "/tetral.provider_gateway.v1.ProviderGatewayService/StreamProviderRequest");
      callerServiceAccount = `${caller.serviceAccount.namespace}/${caller.serviceAccount.name}`;
      this.ensureReady();
      requestIdentity = safeRequestIdentity(request);
      const validation = validateProviderRequest(request);
      if (!validation.ok) {
        errorClass = "request_validation";
        errorCode = validation.code ?? "invalid_request_shape";
        validationMember = validation.member;
        throw new GrpcStatusError(status.INVALID_ARGUMENT, validation.message);
      }
      const limits = request.limits;
      // Request validation above proves this required member is present.
      if (limits === undefined) {
        throw new Error("validated provider limits are absent");
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
        yield* assembler.accept(providerErrorEvent({
          code: "provider_unavailable",
          message: "Gateway provider capacity is exhausted.",
          retryable: true,
          fatal: false,
          statusCode: 503,
        }));
        return;
      }
      const finishMetrics = this.metrics.startProviderStream();
      let failed = false;
      const providerAbortController = new AbortController();
      const providerStartedAt = started;
      const providerDeadline = providerStartedAt + limits.timeoutMs;
      let providerTimeoutLogged = false;
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
          assembler,
          (timeout) => {
            providerAbortController.abort(new DOMException("Provider request timed out.", "AbortError"));
            if (providerTimeoutLogged) {
              return;
            }
            providerTimeoutLogged = true;
            logGatewayProviderTimeout(this.options.logger, request, {
              ...timeout,
              elapsedMs: performance.now() - providerStartedAt,
            });
          },
          (origin) => {
            providerFailureClass = origin === "http_rejection"
              ? "provider_http_rejection"
              : "provider_transport_failure";
          },
        )) {
          if (event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR) {
            failed = true;
            errorCode = providerLogErrorCode(event.providerError?.error?.code);
            providerStatusCode = boundedProviderStatusCode(event.providerError?.error?.statusCode);
            errorClass = errorCode === "provider_configuration_invalid"
              ? "provider_configuration"
              : providerFailureClass;
          }
          this.frameDeadlines.set(event,{deadline:providerDeadline,request,startedAt:providerStartedAt});
          yield event;
        }
      } catch (error) {
        failed = true;
        if (assembler.isTerminal) throw new GrpcStatusError(status.INTERNAL, "gateway provider stream failed after terminal");
        if (error instanceof GrpcStatusError) {
          errorClass = "grpc_status";
          errorCode = String(error.code);
          throw error;
        }
        const classified = classifyProviderStreamError(error);
        errorClass = "provider_error";
        errorCode = providerLogErrorCode(classified.code);
        yield* assembler.accept(providerErrorEvent(classified));
      } finally {
        abortSignal?.removeEventListener("abort", forwardAbort);
        finishMetrics(failed);
        release();
      }
      requestOutcome = failed ? "failed" : "ok";
    } catch (error) {
      requestOutcome = "failed";
      if (error instanceof GrpcStatusError && errorCode === "stream_incomplete") {
        errorClass = "grpc_status";
        errorCode = String(error.code);
      } else if (errorCode === "stream_incomplete") {
        errorCode = "internal";
      }
      throw error;
    } finally {
      done();
      assembler.release();
      try { assemblyMetrics?.close(); } catch { /* Fail-open metrics. */ }
      this.workers.delete(processController);
      const record = {
        event: "provider_request_streamed",
        "event.kind": "provider_request_streamed",
        operation: "StreamProviderRequest",
        component: "gateway",
        "grpc.method": "/tetral.provider_gateway.v1.ProviderGatewayService/StreamProviderRequest",
        "caller.service_account": callerServiceAccount,
        ...requestIdentity,
        ...(validationMember === undefined ? {} : { "validation.member": validationMember }),
        "request.outcome": requestOutcome,
        "duration.ms": Math.round(performance.now() - started),
      } as const;
      try {
        if (requestOutcome === "ok") {
          this.options.logger.info(record);
        } else {
          // Ordinary denial/cancellation is expected control flow; failure meaning is retained.
          const emit = errorClass === "request_validation" || (errorClass === "grpc_status" && ["1", "3", "7", "16"].includes(errorCode))
            ? this.options.logger.info : this.options.logger.error;
          emit.call(this.options.logger, {
              ...record,
              ...semanticErrorFields({
                errorClass,
                errorCode,
                messageSafe: "provider request failed",
              }),
              ...(providerStatusCode === undefined ? {} : { "provider.status_code": providerStatusCode }),
            });
        }
      } catch {
        // Observability must not replace the request's outcome.
      }
    }
  }

  /**
   * Stops admission, lets admitted workers finish until `drainDeadline`, then aborts their provider
   * and attachment work and waits for them to join until `deadline`. A worker suspended on a stream
   * write held by HTTP/2 flow control cannot observe that abort, so when the cancellation and join
   * window expires `closeStreams` force-closes the remaining streams before the final join and the
   * missed-deadline error.
   */
  async shutdown(
    deadline: Date,
    drainDeadline: Date = deadline,
    closeStreams?: () => void,
  ): Promise<void> {
    this.stopping = true;
    const joined = Promise.all([...this.workers.values()]);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const cutoff = new Promise<"cutoff">((resolve) => {
      timer = setTimeout(
        () => resolve("cutoff"),
        Math.max(0, drainDeadline.getTime() - Date.now()),
      );
    });
    const result = await Promise.race([joined, cutoff]);
    if (timer !== undefined) clearTimeout(timer);
    if (result === "cutoff") {
      for (const controller of this.workers.keys())
        controller.abort(new Error("Provider process draining"));
      const end = new Promise<"expired">((resolve) => {
        timer = setTimeout(
          () => resolve("expired"),
          Math.max(0, deadline.getTime() - Date.now()),
        );
      });
      let expired = false;
      try {
        expired = await Promise.race([
          joined.then(() => false),
          end.then(() => true),
        ]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
      // Keep ownership through forced cancellation even when a dependency ignores it.
      // Commands may close required clients only after this promise has joined.
      if (expired) {
        closeStreams?.();
        await joined;
        throw new Error(
          "Provider workers did not join before shutdown deadline",
        );
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
    if (this.stopping || !this.options.ready()) {
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
      semanticProgressTimeoutMs: providerTimeoutMs(
        this.options.providerStreamTimeouts?.semanticProgressTimeoutMs,
        DefaultProviderSemanticProgressTimeoutMs,
        overallTimeoutMs,
      ),
    };
  }

  private async *streamProviderAttempts(
    request: ProviderRequest,
    runtimePodUid: string,
    abortSignal: AbortSignal,
    providerDeadline: number,
    assembler: ProviderBlockAssembler,
    abortOnTimeout: (timeout: ProviderTimeoutObservation) => void,
    recordFailureOrigin: (origin: "http_rejection" | "transport_failure") => void,
  ): AsyncGenerator<ProviderStreamEvent> {
    const catalogError = this.catalogGate(request);
    if (catalogError !== undefined) {
      yield* assembler.accept(providerErrorEvent(catalogError));
      return;
    }
    const attachmentResolution = await withinProviderDeadline(
      this.resolveAttachments(
        request,
        runtimePodUid,
        abortSignal,
        Date.now() + Math.max(0, providerDeadline - performance.now()),
      ),
      abortSignal,
      providerDeadline,
      () => abortOnTimeout({ kind: "overall_timeout" }),
    );
    if (!attachmentResolution.ok) {
      yield* assembler.accept(providerErrorEvent(attachmentResolution.error));
      return;
    }
    if (attachmentResolution.rejections.length > 0) {
      yield* assembler.accept({
        type: NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS,
        attachmentRejections: { rejections: [...attachmentResolution.rejections] },
      });
    }
    const providerRequest = {
      ...request,
      attachments: attachmentResolution.attachments.map(({ data: _data, ...attachment }) => attachment),
    };
    // Per-turn provider loop. The `emitted` latch is set by the first event from
    // the provider client and splits HTTP credential rejection from Runtime-owned
    // stream recovery. Transport and watchdog failures without a captured
    // provider semantic signal never mutate or switch credentials. Attachment
    // rejections are emitted before this loop and do not close the failover window:
    //
    //   | phase                     | credential | on provider fault                        | re-enters loop? |
    //   | ------------------------- | ---------- | ---------------------------------------- | --------------- |
    //   | emitted == false          | platform   | classify; optionally cool/quarantine;     | yes, unless     |
    //   |                           |            | exclude this key for the turn              | fail-fast or    |
    //   |                           |            |                                            | max attempts    |
    //   | emitted == false          | session    | one attempt only; forward provider-error | no              |
    //   | emitted == true (platform)| platform   | record key failure; no switch or retry;    | no              |
    //   |                           |            | forward terminal; Runtime owns recovery   |                 |
    //   | emitted == true (session) | session    | no switch or retry; forward terminal;      | no              |
    //   |                           |            | Runtime owns recovery                     |                 |
    //
    // The request-local assembler validates this attempt. A nominal finish or
    // EOF with unresolved fragments becomes one retryable provider-stream failure;
    // an explicit provider fault retains its classification without synthetic ENDs.
    const attemptedPlatformKeyIds = new Set<string>();
    let platformAttempts = 0;
    let lastPlatformProviderError: ProviderErrorInput | undefined;
    let lastPlatformFailureAction: ProviderFailureClassification["action"] | undefined;
    let lastPlatformFailureOrigin: "http_rejection" | "transport_failure" | undefined;
    while (true) {
      const credential = await withinProviderDeadline(
        this.resolveCredential(request, attemptedPlatformKeyIds, lastPlatformProviderError, lastPlatformFailureAction),
        abortSignal,
        providerDeadline,
        () => abortOnTimeout({ kind: "overall_timeout" }),
      );
      if (!credential.ok) {
        if (lastPlatformProviderError !== undefined && lastPlatformFailureOrigin !== undefined) {
          recordFailureOrigin(lastPlatformFailureOrigin);
        }
        yield* assembler.accept(providerErrorEvent(credential.error));
        return;
      }
      const resolvedCredential = credential.credential;
      let emitted = false;
      let lastEventAt: number | undefined;
      let lastEventKind: string | undefined;
      let lastTransportActivityAt = Date.now();
      const dispatchedAt = this.clock();
      let firstFragmentAt: number | undefined;
      let firstFragmentKind: ProviderContentKind = "none";
      let firstComplete: { at: number; kind: ProviderContentKind; canonicalBytes: number; encodedBytes: number } | undefined;
      let attemptOutcome: ProviderStageOutcome = "cancelled";
      try {
        const providerEvents = controlProviderStream(this.providerStreamer.stream({
          request: providerRequest,
          abortSignal,
          credential: resolvedCredential,
          resolvedAttachments: attachmentResolution.attachments,
          onTransportActivity: () => {
            lastTransportActivityAt = Date.now();
          },
        }), {
          abortSignal,
          abortOnTimeout: (kind) => abortOnTimeout({
            kind,
            ...(lastEventAt === undefined ? {} : {
              interEventGapMs: performance.now() - lastEventAt,
              lastEventKind,
            }),
          }),
          ...this.providerStreamTimeouts(providerDeadlineRemainingMs(providerDeadline)),
          overallTimeoutMs: providerDeadlineRemainingMs(providerDeadline),
          isSemanticProgress: isProviderSemanticProgress,
          joinIteratorReturn: this.providerStreamer.joinIteratorReturn===true,
          transportActivityAt: () => lastTransportActivityAt,
        });
        for await (const event of providerEvents) {
          const eventValidation = validateNormalizedProviderEvent(event);
          if (!eventValidation) {
            throw new GrpcStatusError(status.INTERNAL, "gateway provider stream contract violation");
          }
          emitted = true;
          const fragmentKind = normalizedContentKind(event);
          if (firstFragmentAt === undefined && fragmentKind !== "none") {
            firstFragmentAt = this.clock();
            firstFragmentKind = fragmentKind;
          }
          if (event.providerError !== undefined) attemptOutcome = "error";
          else if (event.finish !== undefined) attemptOutcome = "success";
          lastEventAt = performance.now();
          lastEventKind = boundedProviderEventKind(event.type);
          for(const frame of assembler.accept(event)) {
            if(frame.textComplete!==undefined || frame.reasoningComplete!==undefined || frame.toolCallComplete!==undefined){
              const completedAt = this.clock();
              const description = completeFrameDescription(frame);
              this.completeFrameTimes.set(frame,{completedAt,requestId:request.requestId,...description});
              firstComplete ??= {at:completedAt,...description};
            }
            yield frame;
          }
        }
        assembler.assertComplete("eof");
        return;
      } catch (error) {
        attemptOutcome = abortSignal.aborted ? "cancelled" : "error";
        if (assembler.isTerminal) throw new GrpcStatusError(status.INTERNAL, "gateway provider stream failed after terminal");
        if (error instanceof GrpcStatusError) {
          throw error;
        }
        if (error instanceof ProviderAssemblyLimitError || error instanceof ProviderSdkRetentionLimitError) {
          yield* assembler.accept(providerErrorEvent({code:"provider_stream_limit_exceeded",message:"Provider output exceeded Gateway resource limits.",retryable:false,fatal:true,statusCode:413}));
          return;
        }
        if (error instanceof ProviderRequestLoweringError) {
          yield* assembler.accept(providerErrorEvent(error.providerError));
          return;
        }
        const providerKeyFailure = error instanceof ProviderKeyFailureError
          ? error
          : resolvedCredential?.source === "platform" && !emitted
            ? (() => {
                const input = providerAttemptFailureInput(error);
                return new ProviderKeyFailureError(
                  classifyProviderFailure(resolvedCredential.providerId, input),
                  input.networkError === true || input.timeout === true || input.statusCode === undefined
                    ? "transport_failure"
                    : "http_rejection",
                );
              })()
            : undefined;
        const capturedSemanticSignal = providerKeyFailure?.classification.semanticSignal === "deepseek_insufficient_balance";
        if (
          error instanceof ProviderStreamTimeoutError ||
          (providerKeyFailure?.origin === "transport_failure" && !capturedSemanticSignal)
        ) {
          recordFailureOrigin("transport_failure");
          yield* assembler.accept(providerErrorEvent(classifyProviderStreamError(error)));
          return;
        }
        if (providerKeyFailure === undefined) {
          if (error instanceof ProviderIncompleteStreamError) {
            logProviderStreamIncomplete(this.options.logger, request, error);
          }
          recordFailureOrigin("transport_failure");
          yield* assembler.accept(providerErrorEvent(classifyProviderStreamError(error)));
          return;
        }
        if (resolvedCredential?.source === "session") {
          recordFailureOrigin(providerKeyFailure.origin);
          yield* assembler.accept(providerErrorEvent(
            providerKeyFailure.classification.action === "quarantine" ||
            providerKeyFailure.classification.providerError.statusCode === 401 ||
            providerKeyFailure.classification.providerError.statusCode === 403
              ? sessionCredentialUnavailableError(providerKeyFailure.classification.providerError.statusCode)
              : providerKeyFailure.classification.providerError,
          ));
          return;
        }
        if (resolvedCredential?.source !== "platform") {
          recordFailureOrigin(providerKeyFailure.origin);
          yield* assembler.accept(providerErrorEvent(
            providerKeyFailure.classification.action === "quarantine"
              ? platformCredentialPoolUnavailableError(providerKeyFailure.classification.providerError.statusCode)
              : providerKeyFailure.classification.providerError,
          ));
          return;
        }
        const platformKeyId = resolvedCredential.platformKey.keyId;
        this.options.credentialResolver?.recordPlatformFailure(platformKeyId, providerKeyFailure.classification);
        if (emitted) {
          recordFailureOrigin(providerKeyFailure.origin);
          yield* assembler.accept(providerErrorEvent(
            providerKeyFailure.classification.action === "quarantine"
              ? platformCredentialPoolUnavailableError(providerKeyFailure.classification.providerError.statusCode)
              : providerKeyFailure.classification.providerError,
          ));
          return;
        }
        attemptedPlatformKeyIds.add(platformKeyId);
        platformAttempts += 1;
        lastPlatformProviderError = providerKeyFailure.classification.providerError;
        lastPlatformFailureAction = providerKeyFailure.classification.action;
        lastPlatformFailureOrigin = providerKeyFailure.origin;
        if (providerKeyFailure.classification.action === "fail-fast") {
          recordFailureOrigin(providerKeyFailure.origin);
          yield* assembler.accept(providerErrorEvent(providerKeyFailure.classification.providerError));
          return;
        }
        if (platformAttempts >= PlatformKeyPoolConstants.maxKeySwitchesPerTurn) {
          recordFailureOrigin(providerKeyFailure.origin);
          yield* assembler.accept(providerErrorEvent(
            providerKeyFailure.classification.action === "quarantine"
              ? platformCredentialPoolUnavailableError(providerKeyFailure.classification.providerError.statusCode)
              : providerKeyFailure.classification.providerError,
          ));
          return;
        }
      } finally {
        if (abortSignal.aborted) attemptOutcome = "cancelled";
        const finishedAt = this.clock();
        this.observeStage({stage:"provider_first_fragment",outcome:attemptOutcome,kind:firstFragmentKind,durationMs:(firstFragmentAt ?? finishedAt)-dispatchedAt,canonicalBytes:firstComplete?.canonicalBytes ?? 0,encodedBytes:firstComplete?.encodedBytes ?? 0},request.requestId);
        this.observeStage({stage:"provider_first_complete",outcome:attemptOutcome,kind:firstComplete?.kind ?? "none",durationMs:(firstComplete?.at ?? finishedAt)-(firstFragmentAt ?? dispatchedAt),canonicalBytes:firstComplete?.canonicalBytes ?? 0,encodedBytes:firstComplete?.encodedBytes ?? 0},request.requestId);
      }
    }
  }

  private async resolveCredential(
    request: ProviderRequest,
    attemptedPlatformKeyIds: ReadonlySet<string>,
    lastPlatformProviderError: ProviderErrorInput | undefined,
    lastPlatformFailureAction: ProviderFailureClassification["action"] | undefined,
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
    if (resolved.error.code === "platform_keys_exhausted") {
      return {
        ok: false,
        error: lastPlatformProviderError !== undefined && lastPlatformFailureAction !== "quarantine"
          ? lastPlatformProviderError
          : platformCredentialPoolUnavailableError(lastPlatformProviderError?.statusCode ?? resolved.error.statusCode),
      };
    }
    return resolved;
  }

  private async resolveAttachments(
    request: ProviderRequest,
    runtimePodUid: string,
    abortSignal: AbortSignal,
    deadline: number,
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
      return await this.options.attachmentResolver.resolve({
        request,
        runtimePodUid,
        abortSignal,
        deadline,
      });
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

function platformCredentialPoolUnavailableError(statusCode: number | undefined): ProviderErrorInput {
  return {
    code: "provider_unavailable",
    message: "Provider is unavailable.",
    retryable: false,
    fatal: false,
    ...(statusCode === undefined ? {} : { statusCode }),
  };
}

function sessionCredentialUnavailableError(statusCode: number | undefined): ProviderErrorInput {
  return {
    code: "provider_key_unavailable",
    message: "The supplied provider credential is not usable.",
    retryable: false,
    fatal: true,
    ...(statusCode === undefined ? {} : { statusCode }),
  };
}

interface ProviderTimeoutObservation {
  readonly kind: ProviderStreamWatchdogKind;
  readonly interEventGapMs?: number | undefined;
  readonly lastEventKind?: string | undefined;
}

function logGatewayProviderTimeout(
  logger: GatewayLogger,
  request: ProviderRequest,
  timeout: ProviderTimeoutObservation & {
    readonly elapsedMs: number;
  },
): void {
  const kind = timeout.kind === "first_byte_timeout"
    ? "first_event"
    : timeout.kind === "inter_chunk_timeout"
      ? "inter_event"
      : timeout.kind === "semantic_progress_timeout"
        ? "semantic_progress"
        : "overall";
  const semanticProgressExpired = timeout.kind === "semantic_progress_timeout";
  try {
    logger.error({
      event: "gateway_provider_timeout",
      "event.kind": "gateway_provider_timeout",
      operation: "StreamProviderRequest",
      component: "gateway",
      ...safeRequestIdentity(request),
      "timeout.kind": kind,
      "timeout.elapsed_ms": Math.max(0, Math.round(timeout.elapsedMs)),
      ...(timeout.interEventGapMs === undefined ? {} : {
        "timeout.inter_event_gap_ms": Math.max(0, Math.round(timeout.interEventGapMs)),
      }),
      ...(timeout.lastEventKind === undefined ? {} : { "stream.last_event.kind": timeout.lastEventKind }),
      ...semanticErrorFields({
        errorClass: semanticProgressExpired
          ? "provider_transport_failure"
          : "provider_timeout",
        errorCode: semanticProgressExpired
          ? "provider_stream_error"
          : "provider_timeout",
        messageSafe: semanticProgressExpired
          ? "provider stream made no semantic progress"
          : "provider stream watchdog expired",
      }),
    });
  } catch {
    // Provider timeout settlement does not depend on observability delivery.
  }
}

function boundedProviderEventKind(type: NormalizedProviderEventType): string {
  switch (type) {
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START: return "text_start";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA: return "text_delta";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END: return "text_end";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START: return "reasoning_start";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA: return "reasoning_delta";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END: return "reasoning_end";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START: return "tool_input_start";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA: return "tool_input_delta";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END: return "tool_input_end";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL: return "tool_call";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH: return "finish";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR: return "provider_error";
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS: return "attachment_rejections";
    default: return "unknown";
  }
}

function safeRequestIdentity(request: ProviderRequest): Partial<{
  readonly "workspace.id": string;
  readonly "session.id": string;
  readonly "thread.id": string;
  readonly "request.id": string;
  readonly "model_request.id": string;
  readonly "provider.id": string;
  readonly "model.id": string;
}> {
  const bounded = (value: string): string | undefined =>
    value.length > 0 && new TextEncoder().encode(value).byteLength <= MaxIdBytes ? value : undefined;
  const workspaceId = bounded(request.workspaceId);
  const sessionId = bounded(request.sessionId);
  const threadId = bounded(request.sessionThreadId);
  const requestId = bounded(request.requestId);
  const modelRequestId = bounded(request.modelRequestId);
  const providerId = bounded(request.model?.providerId ?? "");
  const modelId = bounded(request.model?.modelId ?? "");
  return {
    ...(workspaceId !== undefined ? { "workspace.id": workspaceId } : {}),
    ...(sessionId !== undefined ? { "session.id": sessionId } : {}),
    ...(threadId !== undefined ? { "thread.id": threadId } : {}),
    ...(requestId !== undefined ? { "request.id": requestId } : {}),
    ...(modelRequestId !== undefined ? { "model_request.id": modelRequestId } : {}),
    ...(providerId !== undefined ? { "provider.id": providerId } : {}),
    ...(modelId !== undefined ? { "model.id": modelId } : {}),
  };
}

function providerLogErrorCode(value: string | undefined): string {
  return value !== undefined && /^[a-z0-9_.-]{1,128}$/i.test(value)
    ? value
    : "provider_error";
}

function boundedProviderStatusCode(value: number | undefined): number | undefined {
  return Number.isInteger(value) && value !== undefined && value >= 100 && value <= 599
    ? value
    : undefined;
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

function logProviderStreamIncomplete(
  logger: GatewayLogger,
  request: ProviderRequest,
  error: ProviderIncompleteStreamError,
): void {
  try {
    logger.error({
      event: "provider_stream_incomplete",
      "event.kind": "provider_stream_incomplete",
      operation: "StreamProviderRequest",
      component: "gateway",
      ...safeRequestIdentity(request),
      "stream.terminal_category": error.category,
      "stream.open_text_count": error.counts.text,
      "stream.open_reasoning_count": error.counts.reasoning,
      "stream.open_tool_input_count": error.counts.toolInput,
      ...semanticErrorFields({
        errorClass: "provider_stream",
        errorCode: "provider_stream_incomplete",
        messageSafe: "provider stream ended with incomplete fragments",
      }),
    });
  } catch {
    // Provider settlement does not depend on observability delivery.
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
  readonly deadline?: number;
}

/**
 * Streams normalized provider events for one resolved, catalog-approved
 * provider attempt. The service shell validates each returned event and, when
 * the implementation throws a non-gRPC-status fault, closes any tracked open
 * fragments before emitting the terminal provider error. Explicit gRPC status
 * failures propagate to the transport adapter instead.
 */
export interface ProviderRequestStreamer {
  /** Actual native SDK adapter guarantees abortable iterator closure. */
  readonly joinIteratorReturn?: true;
  readonly stream: (input: ProviderRequestStreamInput) => AsyncIterable<NormalizedProviderEvent>;
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
  readonly onTransportActivity?: (() => void) | undefined;
}

/** Configures first-event, between-event, and semantic-progress watchdog budgets. */
export interface ProviderStreamTimeoutOptions {
  readonly firstByteTimeoutMs?: number | undefined;
  readonly interChunkTimeoutMs?: number | undefined;
  readonly semanticProgressTimeoutMs?: number | undefined;
}

interface ResolvedProviderStreamTimeoutOptions {
  readonly firstByteTimeoutMs: number;
  readonly interChunkTimeoutMs: number;
  readonly semanticProgressTimeoutMs: number;
}

class CatalogGatedProviderStreamer implements ProviderRequestStreamer {
  async *stream(input: ProviderRequestStreamInput): AsyncGenerator<NormalizedProviderEvent> {
    input.abortSignal?.throwIfAborted();
    const model = input.request.model;
    const entry = model === undefined ? undefined : lookupGatewayModel(model.providerId, model.modelId);
    const message = entry === undefined
      ? "Provider model is not approved by the Gateway catalog."
      : "Provider Gateway streamer is not configured.";
    yield providerErrorEvent({
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
const DefaultProviderSemanticProgressTimeoutMs = 60_000;

function isProviderSemanticProgress(value: unknown): boolean {
  const event = value as NormalizedProviderEvent;
  switch (event.type) {
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA:
      return (event.text?.text.length ?? 0) > 0;
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA:
      return (event.reasoning?.text.length ?? 0) > 0;
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA:
      return (event.toolInput?.text.length ?? 0) > 0;
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL:
      return true;
    default:
      return false;
  }
}

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

function normalizedContentKind(event: NormalizedProviderEvent): ProviderContentKind {
  if (event.text !== undefined) return "text";
  if (event.reasoning !== undefined) return "reasoning";
  if (event.toolInput !== undefined || event.toolCall !== undefined) return "tool";
  return "none";
}
function completeFrameDescription(frame: ProviderStreamEvent): {kind: ProviderContentKind; canonicalBytes: number; encodedBytes: number} {
  const encoder = new TextEncoder();
  const canonical = frame.textComplete !== undefined ? JSON.stringify(frame.textComplete.text)
    : frame.reasoningComplete !== undefined ? JSON.stringify(frame.reasoningComplete.text)
    : frame.toolCallComplete?.inputJson ?? "";
  const metadata = frame.reasoningComplete?.providerMetadataJson ?? frame.toolCallComplete?.providerMetadataJson ?? "";
  return {kind: frame.textComplete !== undefined ? "text" : frame.reasoningComplete !== undefined ? "reasoning" : "tool", canonicalBytes:encoder.encode(canonical).byteLength + encoder.encode(metadata).byteLength, encodedBytes:ProviderStreamFrame.encode(frame).finish().byteLength};
}
