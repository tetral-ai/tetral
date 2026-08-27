/**
 * @packageDocumentation
 * Per-Thread Runtime composition passed to ThreadLoop. It joins the current
 * binding-fenced identity, reconstructible ThreadState, configuration and
 * explicitly shared coordinators. SessionManager creates or refreshes it;
 * ThreadLoop owns transitions and execution. This aggregate is hot and owns no
 * durable lifecycle or provider context beyond its named members.
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
