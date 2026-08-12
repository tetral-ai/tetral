/**
 * Implements the authenticated Bridge-to-Runtime command boundary for the Runtime Pod.
 *
 * The service authenticates and authorizes callers before command admission, validates the command
 * envelope and selected pod, fences commands against the active session binding, and coalesces only
 * concurrent delivery of the same runtime input while Runtime Core owns completed replay decisions.
 * Durable control ACKs precede hot state updates except for the explicit interrupt closeout
 * ordering below. Task-notification transport ACKs only admit pending work; the owning loop
 * performs its durable semantic commit. The gRPC server invokes this service; `createRuntimePodApp` supplies the
 * authenticator, lifecycle runner, Bridge-backed committers, Runtime Core hosts, cleanup controller,
 * logger, and metrics sink. The service calls those dependencies and protocol validators but does
 * not contact Gateway or providers directly.
 */
import { status } from "@grpc/grpc-js";
import {
  RuntimeCommandKind,
  RuntimeCommandStatus,
  RuntimeInputErrorCode,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import { NoopRuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { RuntimeMetricsSink } from "@tetral/agent-runtime-core/src/runtime/metrics.js";
import { validateRuntimeInputCommandRequest } from "@tetral/agent-runtime-protocol/src/bounds.js";
import { runtimeInputErrorCodeName, runtimeInputErrorCodeOrGeneric } from "@tetral/agent-runtime-protocol/src/error-codes.js";
import type { RuntimeInputErrorCodeName } from "@tetral/agent-runtime-protocol/src/error-codes.js";
import type { Metadata } from "@grpc/grpc-js";
import type { RuntimeCommandLease } from "./lifecycle.js";
import type {
  RuntimeInputCommandRequest,
  RuntimeInputCommandResponse,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type { ServiceAccountIdentity } from "./auth.js";
import type { RuntimePodLogger } from "./logger.js";
import type {
  RuntimeDeclarationReceipt,
  RuntimeMessage,
  RuntimeMessageCreate,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import type {
  RuntimeControlInputCommitResult,
  RuntimeControlInputDeclaration,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-state.js";
import { GrpcStatusError } from "./errors.js";
import { runtimeMessageFromPublicAgentMail } from "./agent-mail.js";

export { GrpcStatusError } from "./errors.js";

/**
 * Workload-authentication port used before request validation or Runtime Core access.
 */
export interface RuntimeAuthenticator {
  readonly authenticate: (input: { readonly metadata: Metadata; readonly method: string }) => Promise<
    | { readonly ok: true; readonly serviceAccount: ServiceAccountIdentity }
    | { readonly ok: false; readonly code: "Unauthenticated" | "PermissionDenied"; readonly message: string }
  >;
}

/**
 * In-process Runtime Core command port for hot session and thread state.
 *
 * Implementations report local capacity and control outcomes without exposing Effect values or
 * Runtime Core service objects across the Runtime Pod boundary.
 */
export interface RuntimeSessionRunHost {
  readonly handleAcceptInput: (command: RuntimeMessagesCommand) => Promise<
    | {
        readonly ok: true;
        readonly sessionId: string;
        readonly created: boolean;
        readonly started: boolean;
        readonly duplicate?: true | undefined;
      }
    | {
        readonly ok: false;
        readonly sessionId: string;
        readonly reason: "local_session_capacity_exceeded" | "control_conflict" | "context_load_failed";
        readonly retryable?: boolean | undefined;
      }
  >;
  readonly handleAgentMail: (command: RuntimeAgentMailCommand) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly applied: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "thread_not_receivable" | "context_load_failed"; readonly retryable?: boolean | undefined }
  >;
  readonly handleInterruptControl: (
    sessionId: string,
    command: RuntimeCommandScope & { readonly origin: "user" | "agent" },
    commitInterruptInput: (declaration: RuntimeControlInputDeclaration) => Promise<RuntimeControlInputCommitResult>,
  ) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly interrupted: boolean; readonly idleInterrupt: boolean; readonly duplicate?: true | undefined; readonly stale?: true | undefined }
    | {
        readonly ok: false;
        readonly sessionId: string;
        readonly reason: "local_session_capacity_exceeded" | "control_busy" | "context_load_failed";
        readonly retryable?: boolean | undefined;
        readonly errorCode?: string | number | undefined;
      }
  >;
  readonly handleToolConfirmation: (
    sessionId: string,
    command: RuntimeCommandScope & {
      readonly sourceEventId: string;
      readonly toolUseEventId: string;
      readonly decision: "allow" | "deny";
      readonly denyMessage?: string | undefined;
    },
    commit: (declaration: RuntimeControlInputDeclaration) => Promise<RuntimeControlInputCommitResult>,
  ) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly applied: boolean; readonly stale?: true | undefined }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "control_busy" | "control_conflict" | "context_load_failed" }
  >;
  readonly handleTaskNotification: (
    sessionId: string,
    command: RuntimeCommandScope & {
      readonly taskId: string;
      readonly sourceToolUseEventId: string;
      readonly status: "completed" | "failed" | "cancelled" | "expired";
      readonly payloadJson: string;
    },
  ) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly applied: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "control_busy" | "control_conflict" | "context_load_failed" }
  >;
  readonly handleRuntimeConfigPatch: (
    sessionId: string,
    command: RuntimeCommandScope & {
      readonly generation?: number;
      readonly mcpServerName?: string;
      readonly manifestETag?: string;
      readonly manifestReadiness?: "ready" | "unready";
      readonly manifestDiagnostic?: string;
      readonly payloadJson: string;
    },
  ) => Promise<
    | {
        readonly ok: true;
        readonly sessionId: string;
        readonly created: boolean;
        readonly applied: boolean;
        readonly noResidency?: true | undefined;
      }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "control_busy" | "control_conflict" | "context_load_failed" }
  >;
}

/**
 * Validated command identity, binding fence, and event-sequence evidence passed to in-process hosts
 * and Bridge-backed committers.
 */
export interface RuntimeCommandScope {
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly runtimeInputId: string;
  readonly eventIds: readonly string[];
  readonly sequenceFrom: number;
  readonly sequenceTo: number;
}

/** Accepted message or bounded rejection command passed to Runtime Core. */
export type RuntimeMessagesCommand = RuntimeCommandScope & (
  | { readonly kind: "messages"; readonly payloadJson: string }
  | {
      readonly kind: "rejection";
      readonly reasonCode: "runtime_command_payload_too_large" | "runtime_command_rejected";
    }
);

/** Stored child-completion envelope delivered from its database-stamped input source. */
export interface RuntimeAgentMailCommand extends RuntimeCommandScope {
  readonly deliveryId: string;
  readonly sourceThreadId: string;
  readonly sourceToolUseEventId: string;
  readonly message: RuntimeMessage;
}

/**
 * Bridge-backed durable input port for interrupts and tool confirmations.
 */
export interface RuntimeControlInputCommitter {
  readonly commitControlInput: (input: {
    readonly scope: RuntimeCommandScope;
    readonly inputKind: "interrupt_control" | "tool_confirmation";
    readonly messageCreates: readonly RuntimeMessageCreate[];
  }) => Promise<
    | { readonly ok: true; readonly stale: true }
    | { readonly ok: true; readonly receipt: RuntimeDeclarationReceipt }
    | { readonly ok: false; readonly retryable: boolean; readonly errorCode: string; readonly message: string }
  >;
}

/**
 * Cleanup admission port whose completion resolves only after hot session state is cleared.
 */
export interface RuntimeCleanupController {
  readonly startCleanup: (scope: RuntimeCommandScope, reason: "expired" | "operator_requested") => Promise<
    | {
        readonly ok: true;
        readonly sessionId: string;
        readonly completion: Promise<{ readonly ok: true; readonly sessionId: string; readonly cleaned: boolean }>;
      }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "session_busy" }
  >;
}

/**
 * Lifecycle admission port that tracks a command and supplies its shutdown-aware lease.
 */
export interface RuntimeCommandRunner {
  readonly runCommand: <Response>(command: (lease: RuntimeCommandLease) => Promise<Response>) => Promise<Response>;
}

/**
 * Pod identity, authorization policy, command ports, lifecycle gates, and observability dependencies
 * used by `RuntimeControlService`.
 */
export interface RuntimeControlServiceOptions {
  readonly ownPod: {
    readonly namespace: string;
    readonly name: string;
    readonly uid: string;
    readonly ip: string;
  };
  readonly allowedBridge: ServiceAccountIdentity;
  readonly authenticator: RuntimeAuthenticator;
  readonly runHost: RuntimeSessionRunHost;
  readonly controlInputCommitter?: RuntimeControlInputCommitter;
  readonly cleanupController: RuntimeCleanupController;
  readonly logger: RuntimePodLogger;
  readonly ready: () => boolean;
  readonly commandRunner?: RuntimeCommandRunner;
  readonly metrics?: RuntimeMetricsSink | undefined;
}

const AcceptInputFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput";
const AcceptAgentMailFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptAgentMail";
const AcceptTaskNotificationFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptTaskNotification";
const InterruptFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/Interrupt";
const ResolveToolConfirmationFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/ResolveToolConfirmation";
const ApplyRuntimeConfigFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/ApplyRuntimeConfig";
const CleanupSessionFullMethod = "/tetral.agent_runtime.v1.AgentRuntimePodService/CleanupSession";

type CommandEffect = "wake-run" | "agent-mail" | "cleanup" | "interrupt" | "tool-confirmation" | "task-notification" | "runtime-config";

interface CommandIdentity {
  readonly requestId: string;
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodNamespace: string;
  readonly targetPodName: string;
  readonly targetPodUid: string;
  readonly targetPodIp: string;
  readonly runtimeInputId: string;
  readonly eventIds: readonly string[];
  readonly sequenceFrom: number;
  readonly sequenceTo: number;
  readonly commandKind: RuntimeCommandKind;
  readonly payloadJson: string;
  readonly callerServiceAccount: string;
}

interface InFlightCommand {
  readonly identity: CommandIdentity;
  readonly result: Promise<RuntimeInputCommandResponse>;
  readonly resolve: (response: RuntimeInputCommandResponse) => void;
  readonly reject: (error: unknown) => void;
}

interface ActiveSessionBinding {
  readonly bindingId: string;
  readonly bindingGeneration: number;
}

/**
 * Handles the seven Bridge command RPCs and maps local outcomes to the protocol's in-band responses or
 * pod-scoped gRPC statuses.
 *
 * The service retains Session-scoped config content identities, shares one in-flight result among
 * concurrent deliveries, and reconstructs every response from the current request binding. Ordinary
 * command receipts remain owned by the resident thread and cleanup drops active binding custody.
 */
export class RuntimeControlService {
  private acceptingCommands = true;
  private readonly configContentMemo = new Map<string, Pick<CommandIdentity, "runtimeInputId" | "payloadJson">>();
  private readonly inFlight = new Map<string, InFlightCommand>();
  private readonly activeBindings = new Map<string, ActiveSessionBinding>();

  constructor(private readonly options: RuntimeControlServiceOptions) {}

  /** Closes the service-local admission gate before lifecycle drain begins. */
  beginShutdown(): void {
    this.acceptingCommands = false;
  }

  /** Accepts a user-message command and wakes or creates its hot Runtime Core run. */
  async acceptInput(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      AcceptInputFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_MESSAGES,
      "wake-run",
    );
  }

  /** Accepts one delivered durable agent-mail envelope directly and wakes its target thread. */
  async acceptAgentMail(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      AcceptAgentMailFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      "agent-mail",
    );
  }

  /** Admits a background task notification for settlement by its owning agent run. */
  async acceptTaskNotification(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      AcceptTaskNotificationFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION,
      "task-notification",
    );
  }

  /** Runs the interrupt closeout path and commits its control input at the prescribed closeout point. */
  async interrupt(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      InterruptFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL,
      "interrupt",
    );
  }

  /** Commits and applies an allow or deny decision for a pending tool confirmation. */
  async resolveToolConfirmation(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      ResolveToolConfirmationFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION,
      "tool-confirmation",
    );
  }

  /** Applies a validated runtime or MCP manifest generation patch to hot state. */
  async applyRuntimeConfig(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      ApplyRuntimeConfigFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
      "runtime-config",
    );
  }

  /** Waits for admitted hot-state cleanup before acknowledging the cleanup command. */
  async cleanupSession(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      CleanupSessionFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
      "cleanup",
    );
  }

  private async runRuntimeCommand(
    request: RuntimeInputCommandRequest,
    metadata: Metadata,
    method: string,
    expectedKind: RuntimeCommandKind,
    effect: CommandEffect,
  ): Promise<RuntimeInputCommandResponse> {
    const startedAt = Date.now();
    const caller = await this.authorize(metadata, method);
    if (this.options.commandRunner !== undefined) {
      return await this.options.commandRunner.runCommand(
        async (lease) => await this.runRuntimeCommandAfterAuthorization(request, caller, method, expectedKind, effect, startedAt, true, lease),
      );
    }
    return await this.runRuntimeCommandAfterAuthorization(request, caller, method, expectedKind, effect, startedAt, false);
  }

  private async runRuntimeCommandAfterAuthorization(
    request: RuntimeInputCommandRequest,
    caller: { readonly serviceAccount: ServiceAccountIdentity },
    method: string,
    expectedKind: RuntimeCommandKind,
    effect: CommandEffect,
    startedAt: number,
    acceptedByLifecycle: boolean,
    lease?: RuntimeCommandLease,
  ): Promise<RuntimeInputCommandResponse> {
    this.ensureAccepting(acceptedByLifecycle);
    lease?.throwIfAborted();
    validateRuntimeInputCommandOrThrow(request);
    if (request.commandKind !== expectedKind) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
    }
    const podValidation = this.validateSelectedPod(request);
    if (!podValidation.ok) {
      return this.rejected(request, true, podValidation.errorCode);
    }

    const callerServiceAccount = serviceAccountText(caller.serviceAccount);
    const identity = commandIdentity(request, callerServiceAccount);
    const dedupeKey = runtimeCommandDedupeKey(request);
    const configPatch = expectedKind === RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH;
    const configMemoKey = runtimeConfigContentMemoKey(request);
    const existingConfigContent = configPatch ? this.configContentMemo.get(configMemoKey) : undefined;
    if (
      existingConfigContent !== undefined &&
      (
        existingConfigContent.runtimeInputId !== identity.runtimeInputId ||
        existingConfigContent.payloadJson !== identity.payloadJson
      )
    ) {
      return this.rejected(request, false, "runtime_input_identity_conflict");
    }
    const existingInFlight = this.inFlight.get(dedupeKey);
    if (existingInFlight !== undefined) {
      if (configPatch && !this.activeBindingMatches(request)) {
        return this.rejected(request, true, "binding_identity_mismatch");
      }
      if (!(configPatch ? sameRuntimeConfigPatchIdentity(existingInFlight.identity, identity) : sameCommandIdentity(existingInFlight.identity, identity))) {
        return this.rejected(request, false, "runtime_input_identity_conflict");
      }
      return responseForCurrentRequest(
        request,
        await existingInFlight.result,
        RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_DUPLICATE,
      );
    }
    if (!this.activeBindingMatches(request)) {
      return this.rejected(request, true, "binding_identity_mismatch");
    }

    const reservation = createInFlight(identity);
    this.inFlight.set(dedupeKey, reservation);
    const unregisterAbort = lease?.onAbort(() => {
      this.inFlight.delete(dedupeKey);
      reservation.reject(new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod shutdown drain timed out"));
    });
    try {
      const effectResponse = await this.applyEffect(request, effect);
      if (effectResponse?.status === RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED) {
        this.inFlight.delete(dedupeKey);
        reservation.resolve(effectResponse);
        unregisterAbort?.();
        const errorCode = runtimeInputErrorCodeName(effectResponse.errorCode);
        this.options.logger.info({
          event: "runtime_command_rejected",
          "event.kind": "runtime_command_rejected",
          operation: runtimeCommandOperation(request.commandKind),
          component: "runtime-control-service",
          "grpc.method": method,
          "grpc.code": "OK",
          "caller.service_account": callerServiceAccount,
          "duration.ms": Date.now() - startedAt,
          "workspace.id": request.workspaceId,
          "session.id": request.sessionId,
          "thread.id": request.sessionThreadId,
          "runtime_input.id": request.runtimeInputId,
          "binding.id": request.bindingId,
          "request.id": request.requestId,
          retryable: effectResponse.retryable,
          terminal: !effectResponse.retryable,
          ...semanticErrorFields({
            errorClass: "runtime_command_rejected",
            errorCode,
            messageSafe: "runtime command rejected",
          }),
          error_code: errorCode,
        });
        return effectResponse;
      }
      lease?.throwIfAborted();
      this.recordAcceptedBinding(request, effect);
      const response = effectResponse ?? this.accepted(request);
      if (configPatch) {
        this.configContentMemo.set(configMemoKey, {
          runtimeInputId: identity.runtimeInputId,
          payloadJson: identity.payloadJson,
        });
      }
      this.inFlight.delete(dedupeKey);
      reservation.resolve(response);
      unregisterAbort?.();
      this.options.logger.info({
        event: "runtime_command_accepted",
        "event.kind": "runtime_command_accepted",
        operation: runtimeCommandOperation(request.commandKind),
        component: "runtime-control-service",
        "grpc.method": method,
        "grpc.code": "OK",
        "caller.service_account": callerServiceAccount,
        "duration.ms": Date.now() - startedAt,
        "workspace.id": request.workspaceId,
        "session.id": request.sessionId,
        "thread.id": request.sessionThreadId,
        "runtime_input.id": request.runtimeInputId,
        "binding.id": request.bindingId,
        "request.id": request.requestId,
      });
      return response;
    } catch (error) {
      unregisterAbort?.();
      this.inFlight.delete(dedupeKey);
      reservation.reject(error);
      if (error instanceof GrpcStatusError) {
        throw error;
      }
      throw new GrpcStatusError(status.INTERNAL, "runtime pod command failed");
    }
  }

  private async applyEffect(request: RuntimeInputCommandRequest, effect: CommandEffect): Promise<RuntimeInputCommandResponse | undefined> {
    if (effect === "wake-run") {
      const command = messagesCommandFromRequest(request);
      const result = await this.options.runHost.handleAcceptInput(command);
      if (!result.ok) {
        if (result.reason === "control_conflict") {
          return this.rejected(request, false, "runtime_input_identity_conflict");
        }
        if (result.reason === "context_load_failed") {
          return this.rejected(request, result.retryable ?? false, "runtime_context_load_failed");
        }
        throw new GrpcStatusError(status.RESOURCE_EXHAUSTED, "local runtime capacity exceeded");
      }
      if (result.duplicate === true) {
        return this.duplicate(request);
      }
      return;
    }
    if (effect === "agent-mail") {
      const result = await this.options.runHost.handleAgentMail({
        ...commandScope(request),
        ...agentMailFromPayload(request),
      });
      if (!result.ok) {
        if (result.reason === "local_session_capacity_exceeded") {
          throw new GrpcStatusError(status.RESOURCE_EXHAUSTED, "local runtime capacity exceeded");
        }
        return this.rejected(
          request,
          result.reason === "context_load_failed" ? result.retryable ?? false : false,
          result.reason === "thread_not_receivable" ? "runtime_control_not_accepted" : "runtime_context_load_failed",
        );
      }
      if (result.ok && !result.applied) {
        return this.duplicate(request);
      }
      return;
    }
    // INTERRUPT CLOSEOUT ORDER. The run host freezes new work before settling
    // the command. When a provider request is open, WriteRequestEnd joins the
    // separately owned interrupt CommitInputs envelope; otherwise this callback
    // is that input's CommitInputs boundary. Bridge locks the durable unfinished-
    // Tool census, writes each terminal conversation result, and returns only
    // the minimal projections needed by a current-custody hot Thread.
    //
    //  # | action                                      | durable boundary
    // ---+---------------------------------------------+------------------------------------------
    //  1 | freeze the provider request and stop new ToolFibers | none
    //  2 | cancel provider and join ToolFibers         | none
    //  3 | close an open request and its interrupt     | joined WriteRequestEnd + CommitInputs receipt
    //  4 | otherwise settle the interrupt input        | CommitInputs callback
    //  5 | publish the database-stamped projection     | receipt application
    //  6 | record end_turn                             | FinishIdle after the interrupt receipt
    //
    // INVARIANTS:
    // - Runtime carries cancellation intent; Bridge owns the exhaustive durable
    //   census, terminal result writes, and Sandbox execution consumption.
    // - An open request and its interrupt are one atomic database operation, so
    //   losing either transport response is resolved by declaration replay.
    // - The interrupt receipt is the last input commit; only its FinishIdle may
    //   follow before the run slot is released.
    // - The admitted user.interrupt event already exists. The host callback
    //   commits the intent, marks that source processed, and applies the returned
    //   census projections without creating a second interrupt event.
    // - An admitted idle interrupt still commits its declaration. Only redelivery
    //   after that exact closeout has completed is a no-op; the interrupt fence
    //   sequence rides the command so redelivered events stay ordered.
    // - A late tool result cannot overwrite the Bridge-authored terminal result;
    //   provider cancellation and background cleanup remain independently owned.
    if (effect === "interrupt") {
	  const interruptPayload = parseObjectPayload(request.payloadJson, "interrupt control");
	  const origin = stringField(interruptPayload, "origin");
	  if (origin !== "user" && origin !== "agent") {
		throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid interrupt control origin");
	  }
      let closeoutCommitResult: Awaited<ReturnType<typeof this.commitControlInput>> | undefined;
      const result = await this.options.runHost.handleInterruptControl(
        request.sessionId,
		{ ...commandScope(request), origin },
        async (declaration) => {
          const committed = await this.commitControlInput(request, "interrupt_control", declaration);
          closeoutCommitResult = committed;
          return committed;
        },
      );
      if (!result.ok) {
        if (closeoutCommitResult?.ok === false) {
          return this.rejected(request, closeoutCommitResult.retryable, closeoutCommitResult.errorCode);
        }
        if (result.errorCode !== undefined) {
          return this.rejected(request, result.retryable === true, result.errorCode);
        }
        const accepted = ensureRuntimeControlAccepted(result);
        if (accepted.ok) {
          throw new GrpcStatusError(status.INTERNAL, "invalid interrupt control result");
        }
        return this.rejected(request, accepted.retryable, accepted.errorCode);
      }
      if (result.duplicate === true) {
        return this.duplicate(request);
      }
      if (result.idleInterrupt && closeoutCommitResult === undefined) {
        throw new GrpcStatusError(status.INTERNAL, "idle interrupt completed without a durable declaration");
      }
      return;
    }
    if (effect === "tool-confirmation") {
      const command = { ...commandScope(request), ...toolConfirmationFromPayload(request.runtimeInputId, request.payloadJson) };
      let commitResult: Awaited<ReturnType<typeof this.commitControlInput>> | undefined;
      const result = await this.options.runHost.handleToolConfirmation(request.sessionId, command, async (declaration) => {
        commitResult = await this.commitControlInput(request, "tool_confirmation", declaration);
        return commitResult;
      });
      if (!result.ok && commitResult?.ok === false) {
        return this.rejected(request, commitResult.retryable, commitResult.errorCode);
      }
      const accepted = ensureToolConfirmationAccepted(result);
      if (!accepted.ok) {
        return this.rejected(request, false, accepted.errorCode);
      }
      if (result.ok && "stale" in result) {
        return;
      }
      if (result.ok && !result.applied) {
        return this.duplicate(request);
      }
      return;
    }
    if (effect === "task-notification") {
      const command = taskNotificationFromPayload(request.runtimeInputId, request.payloadJson);
      const result = await this.options.runHost.handleTaskNotification(request.sessionId, {
        ...commandScope(request),
        ...command,
      });
      const accepted = ensureRuntimeControlAccepted(result);
      if (!accepted.ok) {
        return this.rejected(request, accepted.retryable, accepted.errorCode);
      }
      if (result.ok && !result.applied) {
        return this.duplicate(request);
      }
      return;
    }
    if (effect === "runtime-config") {
      const result = await this.options.runHost.handleRuntimeConfigPatch(
        request.sessionId,
        { ...commandScope(request), ...runtimeConfigPatchFromPayload(request.runtimeInputId, request.payloadJson) },
      );
      const accepted = ensureRuntimeControlAccepted(result);
      if (!accepted.ok) {
        return this.rejected(request, accepted.retryable, accepted.errorCode);
      }
      if (result.ok && !result.applied && result.noResidency !== true) {
        return this.duplicate(request);
      }
      return;
    }
    const cleanup = await this.options.cleanupController.startCleanup(commandScope(request), cleanupReasonFromPayload(request.payloadJson));
    if (!cleanup.ok) {
      runtimeMetrics(this.options).recordCleanupCommandOutcome("rejected");
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "cleanup not accepted");
    }
    runtimeMetrics(this.options).recordCleanupCommandOutcome("accepted");
    try {
      await cleanup.completion;
      runtimeMetrics(this.options).recordCleanupCommandOutcome("completed");
    } catch {
      runtimeMetrics(this.options).recordCleanupCommandOutcome("failed");
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "cleanup did not complete");
    }
  }

  private async commitControlInput(
    request: RuntimeInputCommandRequest,
    inputKind: "interrupt_control" | "tool_confirmation",
    declaration: RuntimeControlInputDeclaration,
  ): Promise<RuntimeControlInputCommitResult> {
    if (this.options.controlInputCommitter === undefined) {
      throw new GrpcStatusError(status.INTERNAL, "control input durable committer is unavailable");
    }
    const ack = await this.options.controlInputCommitter.commitControlInput({
      scope: commandScope(request),
      inputKind,
      messageCreates: declaration.messageCreates,
    });
    if (!ack.ok) {
      return {
        ok: false,
        retryable: ack.retryable,
        errorCode: runtimeInputErrorCodeOrGeneric(ack.errorCode),
      };
    }
    return "stale" in ack
      ? { ok: true, stale: true }
      : { ok: true, receipt: ack.receipt };
  }

  private async authorize(metadata: Metadata, method: string): Promise<{ readonly serviceAccount: ServiceAccountIdentity }> {
    const result = await this.options.authenticator.authenticate({ metadata, method });
    if (!result.ok) {
      throw new GrpcStatusError(result.code === "Unauthenticated" ? status.UNAUTHENTICATED : status.PERMISSION_DENIED, result.message);
    }
    if (
      result.serviceAccount.namespace !== this.options.allowedBridge.namespace ||
      result.serviceAccount.name !== this.options.allowedBridge.name
    ) {
      throw new GrpcStatusError(status.PERMISSION_DENIED, "permission denied");
    }
    return { serviceAccount: result.serviceAccount };
  }

  private ensureAccepting(acceptedByLifecycle: boolean): void {
    if (!acceptedByLifecycle && (!this.options.ready() || !this.acceptingCommands)) {
      throw new GrpcStatusError(status.FAILED_PRECONDITION, "runtime pod not ready");
    }
  }

  private validateSelectedPod(request: RuntimeInputCommandRequest): { readonly ok: true } | { readonly ok: false; readonly errorCode: RuntimeInputErrorCodeName } {
    if (
      request.targetPodNamespace !== this.options.ownPod.namespace ||
      request.targetPodName !== this.options.ownPod.name ||
      request.targetPodUid !== this.options.ownPod.uid ||
      request.targetPodIp !== this.options.ownPod.ip
    ) {
      return { ok: false, errorCode: "selected_pod_identity_mismatch" };
    }
    return { ok: true };
  }

  private activeBindingMatches(request: RuntimeInputCommandRequest): boolean {
    const active = this.activeBindings.get(runtimeSessionKey(request));
    return active === undefined ||
      (active.bindingId === request.bindingId && active.bindingGeneration === request.bindingGeneration);
  }

  private recordAcceptedBinding(request: RuntimeInputCommandRequest, effect: CommandEffect): void {
    const key = runtimeSessionKey(request);
    if (effect === "cleanup") {
      this.activeBindings.delete(key);
      return;
    }
    this.activeBindings.set(key, {
      bindingId: request.bindingId,
      bindingGeneration: request.bindingGeneration,
    });
  }

  private accepted(request: RuntimeInputCommandRequest): RuntimeInputCommandResponse {
    return {
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_ACCEPTED,
      retryable: false,
      errorCode: RuntimeInputErrorCode.RUNTIME_INPUT_ERROR_CODE_UNSPECIFIED,
      sessionId: request.sessionId,
      runtimeInputId: request.runtimeInputId,
      bindingId: request.bindingId,
      bindingGeneration: request.bindingGeneration,
    };
  }

  private duplicate(request: RuntimeInputCommandRequest): RuntimeInputCommandResponse {
    return {
      ...this.accepted(request),
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_DUPLICATE,
    };
  }

  private rejected(request: RuntimeInputCommandRequest, retryable: boolean, errorCode: RuntimeInputErrorCode | string): RuntimeInputCommandResponse {
    return {
      status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED,
      retryable,
      errorCode: typeof errorCode === "number" ? errorCode : runtimeInputErrorCodeOrGeneric(errorCode),
      sessionId: request.sessionId,
      runtimeInputId: request.runtimeInputId,
      bindingId: request.bindingId,
      bindingGeneration: request.bindingGeneration,
    };
  }
}

function runtimeCommandOperation(kind: RuntimeCommandKind): string {
  switch (kind) {
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_MESSAGES:
      return "AcceptMessages";
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL:
      return "AcceptInterruptControl";
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION:
      return "AcceptToolConfirmation";
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_TASK_NOTIFICATION:
      return "AcceptTaskNotification";
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL:
      return "AcceptAgentMail";
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH:
      return "AcceptRuntimeConfigPatch";
    case RuntimeCommandKind.RUNTIME_COMMAND_KIND_CLEANUP_SESSION:
      return "CleanupSession";
    default:
      return "AcceptInput";
  }
}

function runtimeCommandDedupeKey(request: RuntimeInputCommandRequest): string {
  return `${request.workspaceId}\u0000${request.runtimeInputId}`;
}

function runtimeConfigContentMemoKey(request: RuntimeInputCommandRequest): string {
  return `${request.workspaceId}\u0000${request.sessionId}\u0000${request.commandKind}\u0000${request.runtimeInputId}`;
}

function runtimeSessionKey(request: RuntimeInputCommandRequest): string {
  return `${request.workspaceId}\u0000${request.sessionId}`;
}

function validateRuntimeInputCommandOrThrow(request: RuntimeInputCommandRequest): void {
  const result = validateRuntimeInputCommandRequest(request);
  if (!result.ok) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, result.message);
  }
}

function commandScope(request: RuntimeInputCommandRequest): RuntimeCommandScope {
  return {
    requestId: request.requestId,
    workspaceId: request.workspaceId,
    sessionId: request.sessionId,
    sessionThreadId: request.sessionThreadId,
    bindingId: request.bindingId,
    bindingGeneration: request.bindingGeneration,
    targetPodUid: request.targetPodUid,
    runtimeInputId: request.runtimeInputId,
    eventIds: [...request.eventIds],
    sequenceFrom: request.sequenceFrom,
    sequenceTo: request.sequenceTo,
  };
}

function messagesCommandFromRequest(request: RuntimeInputCommandRequest): RuntimeMessagesCommand {
  const rejection = boundedRejectionFromPayload(request.payloadJson);
  if (rejection !== undefined) {
    return {
      ...commandScope(request),
      kind: "rejection",
      reasonCode: rejection.reasonCode,
    };
  }
  return {
    ...commandScope(request),
    kind: "messages",
    payloadJson: request.payloadJson,
  };
}

function boundedRejectionFromPayload(
  payloadJson: string,
): { readonly reasonCode: "runtime_command_payload_too_large" | "runtime_command_rejected" } | undefined {
  if (payloadJson.length === 0) {
    return undefined;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(payloadJson);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return undefined;
  }
  const record = parsed as Record<string, unknown>;
  if (record.input_kind !== "rejection") {
    return undefined;
  }
  if (
    Object.keys(record).length !== 2 ||
    (record.reason_code !== "runtime_command_payload_too_large" &&
      record.reason_code !== "runtime_command_rejected")
  ) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid internal request");
  }
  return { reasonCode: record.reason_code };
}

function commandIdentity(request: RuntimeInputCommandRequest, callerServiceAccount: string): CommandIdentity {
  return {
    requestId: request.requestId,
    workspaceId: request.workspaceId,
    sessionId: request.sessionId,
    sessionThreadId: request.sessionThreadId,
    bindingId: request.bindingId,
    bindingGeneration: request.bindingGeneration,
    targetPodNamespace: request.targetPodNamespace,
    targetPodName: request.targetPodName,
    targetPodUid: request.targetPodUid,
    targetPodIp: request.targetPodIp,
    runtimeInputId: request.runtimeInputId,
    eventIds: [...request.eventIds],
    sequenceFrom: request.sequenceFrom,
    sequenceTo: request.sequenceTo,
    commandKind: request.commandKind,
    payloadJson: request.payloadJson,
    callerServiceAccount,
  };
}

function sameCommandIdentity(left: CommandIdentity, right: CommandIdentity): boolean {
  return JSON.stringify({ ...left, requestId: "" }) === JSON.stringify({ ...right, requestId: "" });
}

function sameRuntimeConfigPatchIdentity(left: CommandIdentity, right: CommandIdentity): boolean {
  return left.runtimeInputId === right.runtimeInputId && left.payloadJson === right.payloadJson;
}

function createInFlight(identity: CommandIdentity): InFlightCommand {
  let resolve: (response: RuntimeInputCommandResponse) => void = () => undefined;
  let reject: (error: unknown) => void = () => undefined;
  const result = new Promise<RuntimeInputCommandResponse>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  result.catch(() => undefined);
  return { identity, result, resolve, reject };
}

function responseForCurrentRequest(
  request: RuntimeInputCommandRequest,
  response: RuntimeInputCommandResponse,
  acceptedStatus: RuntimeCommandStatus,
): RuntimeInputCommandResponse {
  return {
    ...response,
    status: response.status === RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED
      ? RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED
      : acceptedStatus,
    sessionId: request.sessionId,
    runtimeInputId: request.runtimeInputId,
    bindingId: request.bindingId,
    bindingGeneration: request.bindingGeneration,
  };
}

function runtimeMetrics(options: RuntimeControlServiceOptions): RuntimeMetricsSink {
  return options.metrics ?? NoopRuntimeMetricsSink;
}

function cleanupReasonFromPayload(payloadJson: string): "expired" | "operator_requested" {
  if (payloadJson.length === 0) {
    return "operator_requested";
  }
  try {
    const parsed = JSON.parse(payloadJson) as { readonly reason?: unknown };
    return parsed.reason === "expired" ? "expired" : "operator_requested";
  } catch {
    return "operator_requested";
  }
}

function ensureRuntimeControlAccepted(result: { readonly ok: boolean; readonly reason?: string }):
  | { readonly ok: true }
  | { readonly ok: false; readonly retryable: boolean; readonly errorCode: string } {
  if (result.ok) {
    return { ok: true };
  }
  if (result.reason === "local_session_capacity_exceeded") {
    throw new GrpcStatusError(status.RESOURCE_EXHAUSTED, "local runtime capacity exceeded");
  }
  if (result.reason === "control_busy") {
    return { ok: false, retryable: true, errorCode: "control_busy" };
  }
  if (result.reason === "control_conflict") {
    return { ok: false, retryable: false, errorCode: "runtime_control_conflict" };
  }
  if (result.reason === "context_load_failed") {
    return { ok: false, retryable: true, errorCode: "runtime_context_load_failed" };
  }
  return { ok: false, retryable: false, errorCode: "runtime_control_not_accepted" };
}

function ensureToolConfirmationAccepted(result: {
  readonly ok: boolean;
  readonly reason?: "local_session_capacity_exceeded" | "control_busy" | "control_conflict" | "context_load_failed";
}): { readonly ok: true } | { readonly ok: false; readonly errorCode: string } {
  if (result.ok) {
    return { ok: true };
  }
  if (result.reason === "local_session_capacity_exceeded") {
    throw new GrpcStatusError(status.RESOURCE_EXHAUSTED, "local runtime capacity exceeded");
  }
  if (result.reason === "control_busy") {
    throw new GrpcStatusError(status.UNAVAILABLE, "runtime control command busy");
  }
  if (result.reason === "control_conflict") {
    return { ok: false, errorCode: "runtime_control_conflict" };
  }
  if (result.reason === "context_load_failed") {
    return { ok: false, errorCode: "runtime_context_load_failed" };
  }
  return { ok: false, errorCode: "runtime_control_not_accepted" };
}

function toolConfirmationFromPayload(
  runtimeInputId: string,
  payloadJson: string,
): {
  readonly runtimeInputId: string;
  readonly sourceEventId: string;
  readonly toolUseEventId: string;
  readonly decision: "allow" | "deny";
  readonly denyMessage?: string | undefined;
} {
  const payload = parseObjectPayload(payloadJson, "tool confirmation");
  const sourceEventId = stringField(payload, "source_event_id");
  const toolUseEventId = stringField(payload, "tool_use_event_id");
  const decision = stringField(payload, "decision");
  if (sourceEventId === undefined || toolUseEventId === undefined || (decision !== "allow" && decision !== "deny")) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid tool confirmation payload");
  }
  return {
    runtimeInputId,
    sourceEventId,
    toolUseEventId,
    decision,
    ...(typeof payload.deny_message === "string" ? { denyMessage: payload.deny_message } : {}),
  };
}

function agentMailFromPayload(
  request: RuntimeInputCommandRequest,
): Pick<RuntimeAgentMailCommand, "deliveryId" | "sourceThreadId" | "sourceToolUseEventId" | "message"> {
  if (
    request.eventIds.length !== 1 ||
    request.sequenceFrom <= 0 ||
    request.sequenceFrom !== request.sequenceTo
  ) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid agent mail payload");
  }
  const payload = parseObjectPayload(request.payloadJson, "agent mail");
  const deliveryId = stringField(payload, "delivery_id");
  const sourceThreadId = stringField(payload, "source_thread_id");
  const sourceToolUseEventId = stringField(payload, "source_tool_use_event_id");
  let message: RuntimeMessage;
  try {
    message = runtimeMessageFromPublicAgentMail(JSON.stringify(payload.message));
  } catch {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid agent mail payload");
  }
  if (
    deliveryId === undefined ||
    sourceThreadId === undefined ||
    sourceToolUseEventId === undefined ||
    request.runtimeInputId !== `agent_mail:${deliveryId}`
  ) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid agent mail payload");
  }
  return {
    deliveryId,
    sourceThreadId,
    sourceToolUseEventId,
    message,
  };
}

function taskNotificationFromPayload(
  runtimeInputId: string,
  payloadJson: string,
): {
  readonly runtimeInputId: string;
  readonly taskId: string;
  readonly sourceToolUseEventId: string;
  readonly status: "completed" | "failed" | "cancelled" | "expired";
  readonly payloadJson: string;
} {
  const payload = parseObjectPayload(payloadJson, "task notification");
  const taskId = stringField(payload, "task_id");
  const sourceToolUseEventId = stringField(payload, "source_tool_use_event_id");
  const statusValue = stringField(payload, "status");
  if (
    taskId === undefined ||
    sourceToolUseEventId === undefined ||
    (statusValue !== "completed" && statusValue !== "failed" && statusValue !== "cancelled" && statusValue !== "expired") ||
    !hasExactFields(payload, ["task_id", "source_tool_use_event_id", "status", "stdout", "stderr"], ["exit_code"])
  ) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid task notification payload");
  }
  if (hasOwn(payload, "exit_code")) {
    nullableSafeInteger(payload.exit_code, "task notification exit_code");
  }
  validateTaskNotificationStream(payload, "stdout");
  validateTaskNotificationStream(payload, "stderr");
  return {
    runtimeInputId,
    taskId,
    sourceToolUseEventId,
    status: statusValue,
    payloadJson,
  };
}

function validateTaskNotificationStream(payload: Record<string, unknown>, field: "stdout" | "stderr"): void {
  if (!hasOwn(payload, field)) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}`);
  }
  const value = payload[field];
  if (!isObjectRecord(value)) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}`);
  }
  const text = value.text;
  const truncated = value.truncated;
  if (
    typeof text !== "string" ||
    typeof truncated !== "boolean" ||
    !hasExactFields(value, ["text", "truncated"], ["original_bytes", "original_lines"])
  ) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}`);
  }
  for (const optional of ["original_bytes", "original_lines"] as const) {
    if (hasOwn(value, optional)) {
      const count = nullableSafeInteger(value[optional], `task notification ${field}.${optional}`);
      if (count !== null && count < 0) {
        throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}.${optional}`);
      }
    }
  }
}

function nullableSafeInteger(value: unknown, field: string): number | null {
  if (value === null) {
    return null;
  }
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return value;
  }
  throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid ${field}`);
}

function runtimeConfigPatchFromPayload(
  runtimeInputId: string,
  payloadJson: string,
): {
  readonly runtimeInputId: string;
  readonly generation?: number;
  readonly mcpServerName?: string;
  readonly manifestETag?: string;
  readonly manifestReadiness?: "ready" | "unready";
  readonly manifestDiagnostic?: string;
  readonly payloadJson: string;
} {
  const payload = parseObjectPayload(payloadJson, "runtime config");
  if (isObjectRecord(payload.mcp_manifest)) {
    const mcpServerName = stringField(payload.mcp_manifest, "mcp_server_name");
    const manifestETag = stringField(payload.mcp_manifest, "manifest_etag");
    const manifestGeneration = numberField(payload.mcp_manifest, "manifest_generation");
    const readinessValue = stringField(payload.mcp_manifest, "readiness") ?? "ready";
    const diagnostic = stringField(payload.mcp_manifest, "diagnostic");
    const tools = payload.mcp_manifest.tools;
    const readinessShape = !hasOwn(payload.mcp_manifest, "readiness") || typeof payload.mcp_manifest.readiness === "string";
    const diagnosticShape = !hasOwn(payload.mcp_manifest, "diagnostic") || payload.mcp_manifest.diagnostic === null || typeof payload.mcp_manifest.diagnostic === "string";
    const etagAbsent = !hasOwn(payload.mcp_manifest, "manifest_etag");
    const ready = readinessValue === "ready" && manifestETag !== undefined && diagnostic === undefined && diagnosticShape;
    const unready = readinessValue === "unready" && etagAbsent && diagnostic !== undefined && diagnosticShape &&
      (tools === undefined || (Array.isArray(tools) && tools.length === 0));
    if (mcpServerName === undefined || manifestGeneration === undefined ||
      !Number.isSafeInteger(manifestGeneration) || manifestGeneration <= 0 || !readinessShape || (!ready && !unready)) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid runtime config payload");
    }
    const manifestReadiness = readinessValue === "unready" ? "unready" : "ready";
    if (ready) {
      validateMcpManifestTools(tools);
    }
    return {
      runtimeInputId,
      generation: manifestGeneration,
      mcpServerName,
      ...(manifestETag !== undefined ? { manifestETag } : {}),
      manifestReadiness,
      ...(diagnostic !== undefined ? { manifestDiagnostic: diagnostic } : {}),
      payloadJson,
    };
  }
  const generation = numberField(payload, "config_generation") ?? numberField(payload, "generation");
  if (generation === undefined || !Number.isSafeInteger(generation) || generation <= 0) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid runtime config payload");
  }
  return { runtimeInputId, generation, payloadJson };
}

function validateMcpManifestTools(value: unknown): void {
  if (!Array.isArray(value)) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid runtime config payload");
  }
  for (const tool of value) {
    if (!isObjectRecord(tool)) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid runtime config payload");
    }
    const extraFields = Object.keys(tool).filter((field) => field !== "name" && field !== "description" && field !== "input_schema");
    if (
      extraFields.length > 0 ||
      stringField(tool, "name") === undefined ||
      typeof tool.description !== "string" ||
      !isObjectRecord(tool.input_schema)
    ) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid runtime config payload");
    }
  }
}

function parseObjectPayload(payloadJson: string, kind: string): Record<string, unknown> {
  if (payloadJson.length === 0) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `missing ${kind} payload`);
  }
  try {
    const parsed = JSON.parse(payloadJson) as unknown;
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid ${kind} payload`);
    }
    return parsed as Record<string, unknown>;
  } catch (error) {
    if (error instanceof GrpcStatusError) {
      throw error;
    }
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid ${kind} payload`);
  }
}

function hasOwn(payload: Record<string, unknown>, field: string): boolean {
  return Object.prototype.hasOwnProperty.call(payload, field);
}

function hasExactFields(
  payload: Record<string, unknown>,
  required: readonly string[],
  optional: readonly string[],
): boolean {
  const fields = new Set(Object.keys(payload));
  return required.every((field) => fields.delete(field)) &&
    optional.every((field) => !fields.has(field) || fields.delete(field)) &&
    fields.size === 0;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(payload: Record<string, unknown>, field: string): string | undefined {
  const value = payload[field];
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function numberField(payload: Record<string, unknown>, field: string): number | undefined {
  const value = payload[field];
  return typeof value === "number" ? value : undefined;
}

function serviceAccountText(serviceAccount: ServiceAccountIdentity): string {
  return `${serviceAccount.namespace}/${serviceAccount.name}`;
}
