/**
 * This module is the thread-local Runtime aggregate passed into AgentLoop. It
 * guards the current binding identity while keeping hot SessionState, reviewer
 * coordination, and session-wide tool coordination under explicit owners.
 * SessionManager creates and refreshes it, AgentLoop consumes it, and its
 * constructor creates SessionState and, when needed, a tool coordinator.
 *
 * @packageDocumentation
 */

import { SessionState } from "./session-state.js";
import type { AutoApprovalReviewerManager } from "./approval-reviewer-manager.js";
import { SessionToolCoordinator } from "../tools/tool-scheduler.js";

/** Binding and thread identity required by Runtime-to-Bridge calls. */
export interface RuntimeSessionIdentity {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly sessionThreadId: string;
  readonly parentThreadId?: string | undefined;
  readonly threadRole?: "main" | "subagent" | "approval_reviewer" | undefined;
  readonly bindingId: string;
  readonly bindingGeneration: number;
  readonly targetPodUid: string;
  readonly runtimeBindingToken: string;
}

/** Thread-local aggregate joining identity, mutable hot state, and shared coordinators. */
export class Session {
  #identity: RuntimeSessionIdentity;
  readonly sessionId: string;
  readonly state: SessionState;
  readonly approvalReviewerManager: AutoApprovalReviewerManager | undefined;
  readonly toolCoordinator: SessionToolCoordinator;

  constructor(
    identity: string | RuntimeSessionIdentity,
    approvalReviewerManager?: AutoApprovalReviewerManager,
    toolCoordinator: SessionToolCoordinator = new SessionToolCoordinator(),
  ) {
    this.#identity = typeof identity === "string" ? defaultRuntimeSessionIdentity(identity) : identity;
    this.sessionId = this.#identity.sessionId;
    this.state = new SessionState(this.sessionId);
    this.approvalReviewerManager = approvalReviewerManager;
    this.toolCoordinator = toolCoordinator;
  }

  get identity(): RuntimeSessionIdentity {
    return this.#identity;
  }

  updateIdentity(identity: RuntimeSessionIdentity): void {
    this.#identity = identity;
  }
}

function defaultRuntimeSessionIdentity(sessionId: string): RuntimeSessionIdentity {
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
