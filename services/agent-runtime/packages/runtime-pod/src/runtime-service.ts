/**
 * Implements the authenticated Bridge-to-Runtime command boundary for the Runtime Pod.
 *
 * The service authenticates and authorizes callers before command admission, validates the command
 * envelope and selected pod, fences commands against the active session binding, and deduplicates
 * accepted work by workspace and runtime input identity. Durable control and task ACKs precede hot
 * state updates except for the explicit interrupt closeout ordering below. The gRPC server invokes
 * this service; `createRuntimePodApp` supplies the authenticator, lifecycle runner, Bridge-backed
 * committers, Runtime Core hosts, cleanup controller, logger, and metrics sink. The service calls
 * those dependencies and protocol validators but does not contact Gateway or providers directly.
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
import type { RuntimeMessage } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { GrpcStatusError } from "./errors.js";

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
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly started: boolean; readonly pendingWake: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" }
  >;
  readonly handleAgentMail: (command: RuntimeCommandScope) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly applied: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "context_load_failed" }
  >;
  readonly handleInterruptControl: (
    sessionId: string,
    command: RuntimeCommandScope,
    commitInterruptInput?: (() => Promise<{ readonly ok: true } | { readonly ok: false; readonly retryable: boolean; readonly errorCode: string | number }>),
  ) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly interrupted: boolean; readonly idleInterrupt: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "control_busy" | "context_load_failed" }
  >;
  readonly handleToolConfirmation: (
    sessionId: string,
    command: RuntimeCommandScope & {
      readonly sourceEventId: string;
      readonly toolUseEventId: string;
      readonly decision: "allow" | "deny";
      readonly denyMessage?: string | undefined;
    },
  ) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly applied: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "local_session_capacity_exceeded" | "control_busy" | "control_conflict" | "context_load_failed" }
  >;
  readonly handleTaskNotification: (
    sessionId: string,
    command: RuntimeCommandScope & {
      readonly taskId: string;
      readonly sourceToolUseEventId: string;
      readonly status: "completed" | "failed" | "cancelled" | "expired";
      readonly payloadJson: string;
      readonly bridgeProjection: RuntimeMessage;
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
    | { readonly ok: true; readonly sessionId: string; readonly created: boolean; readonly applied: boolean }
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

/**
 * Accepted message command passed to Runtime Core with its bounded serialized payload.
 */
export interface RuntimeMessagesCommand extends RuntimeCommandScope {
  readonly payloadJson: string;
}

/**
 * Bridge-backed durable settlement port for background task notifications.
 *
 * A successful non-stale result supplies the durable Runtime message that may then enter hot state.
 */
export interface RuntimeTaskNotificationCommitter {
  readonly commitTaskNotification: (input: {
    readonly scope: RuntimeCommandScope;
    readonly command: {
      readonly runtimeInputId: string;
      readonly taskId: string;
      readonly sourceToolUseEventId: string;
      readonly status: "completed" | "failed" | "cancelled" | "expired";
      readonly payloadJson: string;
    };
  }) => Promise<
    | { readonly ok: true; readonly stale: true }
    | { readonly ok: true; readonly runtimeMessage: RuntimeMessage }
    | { readonly ok: false; readonly retryable: boolean; readonly errorCode: string; readonly message: string }
  >;
}

/**
 * Bridge-backed durable input port for interrupts and tool confirmations.
 */
export interface RuntimeControlInputCommitter {
  readonly commitControlInput: (input: {
    readonly scope: RuntimeCommandScope;
    readonly inputKind: "interrupt_control" | "tool_confirmation";
  }) => Promise<
    | { readonly ok: true }
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
  readonly taskNotificationCommitter?: RuntimeTaskNotificationCommitter;
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
 * The service retains accepted receipts and terminal effect rejections for local replay, shares one
 * in-flight result among concurrent deliveries of the same identity, and leaves retryable outcomes
 * uncached. Cleanup removes the active binding only after Runtime Core completion succeeds.
 */
export class RuntimeControlService {
  private acceptingCommands = true;
  private readonly receipts = new Map<string, { readonly identity: CommandIdentity; readonly response: RuntimeInputCommandResponse }>();
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

  /** Rescans durable completion mail for one target thread and wakes it when mail is pending. */
  async acceptAgentMail(request: RuntimeInputCommandRequest, metadata: Metadata): Promise<RuntimeInputCommandResponse> {
    return await this.runRuntimeCommand(
      request,
      metadata,
      AcceptAgentMailFullMethod,
      RuntimeCommandKind.RUNTIME_COMMAND_KIND_AGENT_MAIL,
      "agent-mail",
    );
  }

  /** Durably settles a background task notification before applying its Runtime message. */
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
    const existingReceipt = this.receipts.get(dedupeKey);
    if (existingReceipt !== undefined) {
      if (configPatch && !this.activeBindingMatches(request)) {
        return this.rejected(request, true, "binding_identity_mismatch");
      }
      if (!(configPatch ? sameRuntimeConfigPatchIdentity(existingReceipt.identity, identity) : sameCommandIdentity(existingReceipt.identity, identity))) {
        return this.rejected(request, false, "runtime_input_identity_conflict");
      }
      if (existingReceipt.response.status === RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_REJECTED) {
        return existingReceipt.response;
      }
      return { ...existingReceipt.response, status: RuntimeCommandStatus.RUNTIME_COMMAND_STATUS_DUPLICATE };
    }
    const existingInFlight = this.inFlight.get(dedupeKey);
    if (existingInFlight !== undefined) {
      if (configPatch && !this.activeBindingMatches(request)) {
        return this.rejected(request, true, "binding_identity_mismatch");
      }
      if (!(configPatch ? sameRuntimeConfigPatchIdentity(existingInFlight.identity, identity) : sameCommandIdentity(existingInFlight.identity, identity))) {
        return this.rejected(request, false, "runtime_input_identity_conflict");
      }
      return await existingInFlight.result;
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
      const rejected = await this.applyEffect(request, effect);
      if (rejected !== undefined) {
        this.inFlight.delete(dedupeKey);
        if (!rejected.retryable) {
          this.receipts.set(dedupeKey, { identity, response: rejected });
        }
        reservation.resolve(rejected);
        unregisterAbort?.();
        const errorCode = runtimeInputErrorCodeName(rejected.errorCode);
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
          retryable: rejected.retryable,
          terminal: !rejected.retryable,
          ...semanticErrorFields({
            errorClass: "runtime_command_rejected",
            errorCode,
            messageSafe: "runtime command rejected",
          }),
          error_code: errorCode,
        });
        return rejected;
      }
      lease?.throwIfAborted();
      this.recordAcceptedBinding(request, effect);
      const response = this.accepted(request);
      this.receipts.set(dedupeKey, { identity, response });
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
        throw new GrpcStatusError(status.RESOURCE_EXHAUSTED, "local runtime capacity exceeded");
      }
      return;
    }
    if (effect === "agent-mail") {
      const result = await this.options.runHost.handleAgentMail(commandScope(request));
      if (!result.ok) {
        if (result.reason === "local_session_capacity_exceeded") {
          throw new GrpcStatusError(status.RESOURCE_EXHAUSTED, "local runtime capacity exceeded");
        }
        return this.rejected(request, true, "context_load_failed");
      }
      return;
    }
    // INTERRUPT CLOSEOUT ORDER (hot-first). This handler drives
    // runHost.handleInterruptControl, which runs the closeout in the fixed order
    // below; the commitInterruptInput callback passed in is step 6 (the interrupt
    // snapshot), the LAST input commit of the turn.
    //
    //  # | action                                       | writer                  | ordering rule
    // ---+----------------------------------------------+-------------------------+-------------------------------------
    //  1 | freeze RequestTurn, stop new ToolFibers      | run host                | first; no new hot work after this
    //  2 | cancel the active Gateway stream             | run host                | only when a provider request is live
    //  3 | bounded-join in-flight ToolFibers            | run host                | bounded wait, never unbounded
    //  4 | write error request-end                      | run host                | only if a model span started
    //  5 | repair each committed public agent.tool_use  | run host                | exactly one terminal agent.tool_result
    //    |   with one terminal agent.tool_result        |                         |   per still-open tool use
    //  6 | CommitInputs(interrupt snapshot)             | commitInterruptInput cb | LAST input commit; strictly after 1-5
    //  7 | FinishIdle(end_turn)                         | run host                | user interrupt only; after step-6 ACK
    //
    // INVARIANTS:
    // - Every durable write in steps 4-5 is idempotent-committed BEFORE the step-6
    //   snapshot; that prior commit is what makes the hot-first, ACK-after commit
    //   of steps 6-7 safe.
    // - The interrupt snapshot is the last input commit; nothing commits after it
    //   except the user-interrupt FinishIdle(end_turn).
    // - When the target thread was already idle the host returns idleInterrupt and
    //   this handler commits interrupt_control directly (the result.idleInterrupt
    //   branch below) rather than through the closeout callback.
    // - Queue redelivery of an uncommitted interrupt is idempotent: re-interrupting
    //   an idle thread is a no-op, and the interrupt fence sequence rides the
    //   command so redelivered events stay ordered.
    // - A late tool result for the interrupted request is never appended twice; it
    //   is dropped or folded into the already-committed step-5 repair.
    if (effect === "interrupt") {
      let closeoutCommitResult: Awaited<ReturnType<typeof this.commitControlInput>> | undefined;
      const result = await this.options.runHost.handleInterruptControl(
        request.sessionId,
        commandScope(request),
        async () => {
          const committed = await this.commitControlInput(request, "interrupt_control");
          closeoutCommitResult = committed;
          return committed;
        },
      );
      if (!result.ok) {
        if (closeoutCommitResult?.ok === false) {
          return this.rejected(request, closeoutCommitResult.retryable, closeoutCommitResult.errorCode);
        }
        const accepted = ensureRuntimeControlAccepted(result);
        if (accepted.ok) {
          throw new GrpcStatusError(status.INTERNAL, "invalid interrupt control result");
        }
        return this.rejected(request, false, accepted.errorCode);
      }
      if (result.idleInterrupt) {
        const ack = await this.commitControlInput(request, "interrupt_control");
        if (!ack.ok) {
          return this.rejected(request, ack.retryable, ack.errorCode);
        }
      }
      return;
    }
    if (effect === "tool-confirmation") {
      const command = { ...commandScope(request), ...toolConfirmationFromPayload(request.runtimeInputId, request.payloadJson) };
      const ack = await this.commitControlInput(request, "tool_confirmation");
      if (!ack.ok) {
        return this.rejected(request, ack.retryable, ack.errorCode);
      }
      const result = await this.options.runHost.handleToolConfirmation(request.sessionId, command);
      const accepted = ensureToolConfirmationAccepted(result);
      if (!accepted.ok) {
        return this.rejected(request, false, accepted.errorCode);
      }
      return;
    }
    if (effect === "task-notification") {
      const command = taskNotificationFromPayload(request.runtimeInputId, request.payloadJson);
      if (this.options.taskNotificationCommitter === undefined) {
        throw new GrpcStatusError(status.INTERNAL, "task notification durable committer is unavailable");
      }
      const ack = await this.options.taskNotificationCommitter.commitTaskNotification({
        scope: taskNotificationCommitScope(request),
        command,
      });
      if (!ack.ok) {
        return this.rejected(request, ack.retryable, runtimeInputErrorCodeOrGeneric(ack.errorCode));
      }
      if ("stale" in ack) {
        return;
      }
      const result = await this.options.runHost.handleTaskNotification(request.sessionId, {
        ...commandScope(request),
        ...command,
        bridgeProjection: ack.runtimeMessage,
      });
      const accepted = ensureRuntimeControlAccepted(result);
      if (!accepted.ok) {
        return this.rejected(request, false, accepted.errorCode);
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
        return this.rejected(request, false, accepted.errorCode);
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
  ): Promise<
    | { readonly ok: true }
    | { readonly ok: false; readonly retryable: boolean; readonly errorCode: RuntimeInputErrorCode }
  > {
    if (this.options.controlInputCommitter === undefined) {
      throw new GrpcStatusError(status.INTERNAL, "control input durable committer is unavailable");
    }
    const ack = await this.options.controlInputCommitter.commitControlInput({
      scope: commandScope(request),
      inputKind,
    });
    if (!ack.ok) {
      return {
        ok: false,
        retryable: ack.retryable,
        errorCode: runtimeInputErrorCodeOrGeneric(ack.errorCode),
      };
    }
    return { ok: true };
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

function runtimeSessionKey(request: RuntimeInputCommandRequest): string {
  return `${request.workspaceId}\u0000${request.sessionId}`;
}

function validateRuntimeInputCommandOrThrow(request: RuntimeInputCommandRequest): void {
  const result = validateRuntimeInputCommandRequest(request);
  if (!result.ok) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, result.message);
  }
}

function taskNotificationCommitScope(request: RuntimeInputCommandRequest): Parameters<
  RuntimeTaskNotificationCommitter["commitTaskNotification"]
>[0]["scope"] {
  return commandScope(request);
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
    payloadJson: request.payloadJson,
  };
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

function ensureRuntimeControlAccepted(result: { readonly ok: boolean; readonly reason?: string }): { readonly ok: true } | { readonly ok: false; readonly errorCode: string } {
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
    (statusValue !== "completed" && statusValue !== "failed" && statusValue !== "cancelled" && statusValue !== "expired")
  ) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, "invalid task notification payload");
  }
  const canonicalPayload = canonicalTaskNotificationPayload(payload, taskId, sourceToolUseEventId, statusValue);
  return {
    runtimeInputId,
    taskId,
    sourceToolUseEventId,
    status: statusValue,
    payloadJson: JSON.stringify(canonicalPayload),
  };
}

function canonicalTaskNotificationPayload(
  payload: Record<string, unknown>,
  taskId: string,
  sourceToolUseEventId: string,
  statusValue: "completed" | "failed" | "cancelled" | "expired",
): Record<string, unknown> {
  const canonical: Record<string, unknown> = {
    task_id: taskId,
    source_tool_use_event_id: sourceToolUseEventId,
    status: statusValue,
  };
  if (hasOwn(payload, "exit_code")) {
    canonical.exit_code = nullableSafeInteger(payload.exit_code, "task notification exit_code");
  }
  canonical.stdout = canonicalTaskNotificationStream(payload, "stdout");
  canonical.stderr = canonicalTaskNotificationStream(payload, "stderr");
  return canonical;
}

function canonicalTaskNotificationStream(payload: Record<string, unknown>, field: "stdout" | "stderr"): Record<string, unknown> {
  if (!hasOwn(payload, field)) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}`);
  }
  const value = payload[field];
  if (!isObjectRecord(value)) {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}`);
  }
  const text = value.text;
  const truncated = value.truncated;
  if (typeof text !== "string" || typeof truncated !== "boolean") {
    throw new GrpcStatusError(status.INVALID_ARGUMENT, `invalid task notification ${field}`);
  }
  const canonical: Record<string, unknown> = { text, truncated };
  for (const optional of ["original_bytes", "original_lines"] as const) {
    if (hasOwn(value, optional)) {
      canonical[optional] = nullableSafeInteger(value[optional], `task notification ${field}.${optional}`);
    }
  }
  return canonical;
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
