/**
 * This module is the Effect service facade for Runtime thread and session
 * commands. It guards the narrow command ingress and keeps cleanup, control,
 * reviewer, and shutdown operations on their matching manager methods. Runtime Pod
 * core-host composition calls this facade to build network, sub-agent/reviewer,
 * cleanup, and shutdown adapters; every handler delegates to SessionManager.
 *
 * @packageDocumentation
 */

import { Context, Effect, Layer } from "effect";
import * as SessionManager from "../session/session-manager.js";

/** Command handlers exposed by Runtime Core to the pod service layer. */
export interface Interface {
  readonly handleAcceptInput: (command: Parameters<SessionManager.Interface["acceptInput"]>[0]) => Effect.Effect<SessionManager.AcceptInputResult>;
  readonly handleInterruptControl: (sessionId: string, command: SessionManager.RuntimeInterruptControlCommand, commitInput: Parameters<SessionManager.Interface["interruptControl"]>[2]) => Effect.Effect<SessionManager.InterruptControlResult>;
  readonly handleToolConfirmation: (
    sessionId: string,
    command: Parameters<SessionManager.Interface["resolveToolConfirmation"]>[1],
    commit: Parameters<SessionManager.Interface["resolveToolConfirmation"]>[2],
  ) => Effect.Effect<SessionManager.RuntimeControlResult>;
  readonly handleTaskNotification: (
    sessionId: string,
    command: Parameters<SessionManager.Interface["commitTaskNotification"]>[1],
  ) => Effect.Effect<SessionManager.RuntimeTaskNotificationResult>;
  readonly handleRuntimeConfigPatch: (sessionId: string, command: Parameters<SessionManager.Interface["applyRuntimeConfigPatch"]>[1]) => Effect.Effect<SessionManager.RuntimeControlResult>;
  readonly handleCleanupSession: (sessionId: string, command: SessionManager.RuntimeCleanupSessionCommand) => Effect.Effect<SessionManager.CleanupSessionResult>;
  readonly handlePreloadThread: (command: Parameters<SessionManager.Interface["preloadThread"]>[0]) => Effect.Effect<SessionManager.ThreadLifecycleResult>;
  readonly handleEnsureThreadInstalled: (
    command: Parameters<SessionManager.Interface["ensureThreadInstalled"]>[0],
    options?: Parameters<SessionManager.Interface["ensureThreadInstalled"]>[1],
  ) => Effect.Effect<SessionManager.ThreadLifecycleResult>;
  readonly handleEvictReviewerExecution: (command: Parameters<SessionManager.Interface["evictReviewerExecution"]>[0], token: SessionManager.ReviewerExecutionToken) => Effect.Effect<SessionManager.ReviewerExecutionControlResult>;
  readonly handleInterruptReviewerExecution: (command: Parameters<SessionManager.Interface["interruptReviewerExecution"]>[0], token: SessionManager.ReviewerExecutionToken) => Effect.Effect<SessionManager.ReviewerExecutionControlResult>;
  readonly handleReleaseReviewerExecution: (command: Parameters<SessionManager.Interface["releaseReviewerExecution"]>[0], token: SessionManager.ReviewerExecutionToken) => Effect.Effect<SessionManager.ReviewerExecutionControlResult>;
  readonly handleMarkThreadClosed: (command: Parameters<SessionManager.Interface["markThreadClosed"]>[0]) => Effect.Effect<SessionManager.ThreadLifecycleResult>;
  readonly handleMarkThreadActive: (command: Parameters<SessionManager.Interface["markThreadActive"]>[0]) => Effect.Effect<SessionManager.ThreadLifecycleResult>;
  readonly handleWaitThread: (command: Parameters<SessionManager.Interface["waitThread"]>[0], timeoutMs: number | undefined) => Effect.Effect<SessionManager.ThreadWaitResult>;
  readonly handleWaitReviewerExecution: (command: Parameters<SessionManager.Interface["waitReviewerExecution"]>[0], token: SessionManager.ReviewerExecutionToken, timeoutMs: number | undefined) => Effect.Effect<SessionManager.ReviewerExecutionWaitResult>;
  readonly handleInspectThread: (command: Parameters<SessionManager.Interface["inspectThread"]>[0]) => Effect.Effect<SessionManager.ThreadSnapshotResult>;
  readonly handleInspectReviewerExecution: (command: Parameters<SessionManager.Interface["inspectReviewerExecution"]>[0], token: SessionManager.ReviewerExecutionToken) => Effect.Effect<SessionManager.ReviewerExecutionSnapshotResult>;
  readonly shutdownActiveRuns: () => Effect.Effect<void>;
}

/** Effect service tag for the Runtime command facade. */
export class Service extends Context.Service<Service, Interface>()("tetral-agent/SessionRunHost") {}

/** Builds the command facade by delegating every operation to SessionManager. */
export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const manager = yield* SessionManager.Service;

    return Service.of({
      // Enqueues accepted input and starts or wakes the target thread run slot.
      handleAcceptInput: (command) => manager.acceptInput(command),
      // Interrupt ingress installs cold state when needed, then settles through
      // the supplied declaration committer without calling the provider.
      handleInterruptControl: (sessionId, command, commitInput) =>
        manager.interruptControl(sessionId, command, commitInput),
      handleToolConfirmation: (sessionId, command, commit) => manager.resolveToolConfirmation(sessionId, command, commit),
      handleTaskNotification: (sessionId, command) => manager.commitTaskNotification(sessionId, command),
      handleRuntimeConfigPatch: (sessionId, command) => manager.applyRuntimeConfigPatch(sessionId, command),
      // External cleanup ingress; never loads pending input or context.
      handleCleanupSession: (sessionId, command) => manager.cleanupSession(sessionId, command),
      handlePreloadThread: (command) => manager.preloadThread(command),
      handleEnsureThreadInstalled: (command, options) => manager.ensureThreadInstalled(command, options),
      handleEvictReviewerExecution: (command, token) => manager.evictReviewerExecution(command, token),
      handleInterruptReviewerExecution: (command, token) => manager.interruptReviewerExecution(command, token),
      handleReleaseReviewerExecution: (command, token) => manager.releaseReviewerExecution(command, token),
      handleMarkThreadClosed: (command) => manager.markThreadClosed(command),
      handleMarkThreadActive: (command) => manager.markThreadActive(command),
      handleWaitThread: (command, timeoutMs) => manager.waitThread(command, timeoutMs),
      handleWaitReviewerExecution: (command, token, timeoutMs) => manager.waitReviewerExecution(command, token, timeoutMs),
      handleInspectThread: (command) => manager.inspectThread(command),
      handleInspectReviewerExecution: (command, token) => manager.inspectReviewerExecution(command, token),
      // Runtime process shutdown settlement; distinct from explicit user interrupt commands.
      shutdownActiveRuns: () => manager.shutdownActiveRuns(),
    });
  }),
);
