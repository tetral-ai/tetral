/**
 * This module is the thread-local Runtime aggregate passed into ThreadLoop. It
 * guards the current binding identity while keeping hot ThreadState, reviewer
 * coordination, and session-wide tool coordination under explicit owners.
 * SessionManager creates and refreshes it, ThreadLoop consumes it, and its
 * constructor creates ThreadState and, when needed, a tool coordinator.
 *
 * @packageDocumentation
 */

import { ThreadState } from "./thread-state.js";
import type { AutoApprovalReviewerManager } from "../session/approval-reviewer-manager.js";
import { SessionToolCoordinator } from "../tools/tool-scheduler.js";
import { SessionConfiguration } from "../session/session-configuration.js";

/** Binding and thread identity required by Runtime-to-Bridge calls. */
export interface RuntimeThreadIdentity {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly parentThreadId?: string | undefined;
  readonly parentTaskName?: string | undefined;
  readonly taskName?: string | undefined;
  readonly threadRole?: "main" | "subagent" | "approval_reviewer" | undefined;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly runtimeBindingToken: string;
}

/** Thread-local aggregate joining identity, mutable hot state, and shared coordinators. */
export class ThreadRuntime {
  #identity: RuntimeThreadIdentity;
  readonly sessionId: string;
  readonly state: ThreadState;
  readonly configuration: SessionConfiguration;
  readonly approvalReviewerManager: AutoApprovalReviewerManager | undefined;
  readonly toolCoordinator: SessionToolCoordinator;

  constructor(
    identity: string | RuntimeThreadIdentity,
    approvalReviewerManager?: AutoApprovalReviewerManager,
    toolCoordinator: SessionToolCoordinator = new SessionToolCoordinator(),
    configuration: SessionConfiguration = new SessionConfiguration(),
  ) {
    this.#identity = typeof identity === "string" ? defaultRuntimeThreadIdentity(identity) : identity;
    this.sessionId = this.#identity.sessionId;
    this.state = new ThreadState(this.sessionId);
    this.configuration = configuration;
    this.approvalReviewerManager = approvalReviewerManager;
    this.toolCoordinator = toolCoordinator;
  }

  get identity(): RuntimeThreadIdentity {
    return this.#identity;
  }

  updateIdentity(identity: RuntimeThreadIdentity): void {
    this.#identity = identity;
  }
}

function defaultRuntimeThreadIdentity(sessionId: string): RuntimeThreadIdentity {
  return {
    workspaceId: "workspace-test",
    sessionId,
    sessionThreadId: "thread-test",
    bindingId: "binding-test",
    bindingGeneration: 1,
    targetPodUid: "pod-test",
    runtimeBindingToken: "runtime-binding-token-test",
  };
}
