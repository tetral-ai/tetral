/**
 * This module coordinates disposable hot state for automatic approval reviews.
 * It guards one freshly minted trunk identity per parent hot lifetime, trunk
 * exclusivity, sidecar ownership, outcome-versus-cancellation linearization,
 * transcript cursors, committed snapshots, and memo lifetime.
 * SessionManager owns and disposes it, the reviewer runner acquires its leases,
 * and it calls Effect scopes and fibers while retaining copied Runtime messages.
 *
 * @packageDocumentation
 */

import { Effect, Exit, type Fiber, Scope } from "effect";
import type { RuntimeContextEntry } from "../contracts/runtime.js";
import type { ApprovalReviewerOutcome } from "../tools/tool-gate.js";

/** Generation-bound parent message view used to derive a reviewer transcript feed. */
export interface ParentTranscriptView {
	readonly generation: number;
	readonly entries: readonly RuntimeContextEntry[];
}

/** Full re-anchor or append-only delta selected for the next reviewer request. */
export interface ParentTranscriptFeed {
	readonly reanchored: boolean;
	readonly entries: readonly RuntimeContextEntry[];
}

/** Atomic lease used to choose one winner between an acknowledged reviewer outcome and cancellation. */
export interface ApprovalReviewExecutionLease {
	readonly kind: "trunk" | "sidecar";
	readonly installExecutionToken: (token: ReviewerExecutionToken) => boolean;
	readonly executionToken: () => ReviewerExecutionToken | undefined;
	readonly raceState: () => ReviewerExecutionRaceState;
	readonly cancel: () => boolean;
	readonly claimOutcome: () => boolean;
	readonly onCancellation: (listener: () => void) => () => void;
	readonly release: () => void;
}

/** Identity of one concrete reviewer run installed after its thread starts. */
export interface ReviewerExecutionToken {
	readonly reviewId: string;
	readonly reviewerThreadId: string;
	readonly runId: number;
}

/** Linearized pending or terminal owner of one reviewer outcome race. */
export type ReviewerExecutionRaceState =
	| "pending"
	| "outcome_won"
	| "cancellation_won";

/** Read-only snapshot of one manager-owned reviewer execution. */
export interface ReviewerExecutionState {
	readonly kind: ApprovalReviewExecutionLease["kind"];
	readonly raceState: ReviewerExecutionRaceState;
	readonly token?: ReviewerExecutionToken | undefined;
}

interface MutableReviewerExecutionState {
	readonly kind: ApprovalReviewExecutionLease["kind"];
	raceState: ReviewerExecutionRaceState;
	token: ReviewerExecutionToken | undefined;
	readonly cancellationListeners: Set<() => void>;
}

interface ParentTranscriptCursor {
	readonly generation: number;
	readonly fedEntryCount: number;
}

// AutoApprovalReviewerManager: disposable hot state on a public ThreadEntry under
// approve_for_me. Reviews run on isolated internal role=approval_reviewer threads that
// never surface in public event/thread APIs — the trunk by preference, an ephemeral
// sidecar (seeded from the trunk's last-committed snapshot, or unseeded and fed the
// full parent transcript) while the trunk is busy.
//
// Execution race-state, per beginReview lease (trunk or sidecar):
//   | state            | meaning                       | writers         | legal transitions                 |
//   | ---------------- | ----------------------------- | --------------- | --------------------------------- |
//   | pending          | lease held, outcome undecided | beginReview     | -> outcome_won | cancellation_won |
//   | outcome_won      | durable outcome commit ACKed  | claimOutcome    | terminal                          |
//   | cancellation_won | parent cancelled / disposed   | cancel, dispose | terminal                          |
//   The synchronous claim/cancel calls are the linearization point. The reviewer
//   runner calls claimOutcome only after its durable write result arrives, so a
//   cancellation that wins while that write is in flight prevents parent progress;
//   cancellation listeners fire exactly once.
//
// Parent-transcript feed cursor (parentTranscriptFeed):
//   - first review on an unseeded thread, or any review after the cursor is lost or
//     its bound generation no longer matches -> re-anchored FULL feed;
//   - a sidecar inherits the trunk snapshot's cursor position;
//   - every later review -> only the cursor-delta since the last fed position.
//
// Hot-lifetime fallbacks: cursor or snapshot loss within one hot lifetime falls back to a
// full parent-transcript feed, while a cold return creates a new manager and a fresh
// trunk rather than reloading the predecessor. Memo loss re-reviews; old Reviewer
// ledgers are historical audit facts, never cold-resume input. The
// last-committed snapshot is captured ONLY at trunk-review completion (never from a
// running trunk or a request-turn accumulator); a failed trunk review advances
// neither cursor nor snapshot, so the next review re-feeds the same delta.
// UPDATE-WITH: services/agent-runtime/packages/core/src/session/session-manager.ts,
//              services/agent-runtime/packages/core/src/tools/tool-gate.ts
/** Owns one public thread's disposable reviewer coordination and scoped fibers. */
export class AutoApprovalReviewerManager {
	#trunkEnsureOperationId: string | undefined;
	#trunkThreadId: string | undefined;
	#trunkBusy = false;
	#feedCursor: ParentTranscriptCursor | undefined;
	readonly #decisionMemo = new Map<string, ApprovalReviewerOutcome>();
	readonly #ephemeralReviews = new Set<string>();
	readonly #executions = new Map<string, MutableReviewerExecutionState>();
	readonly #scope = Scope.makeUnsafe("parallel");
	#disposed = false;

	/** Returns the one Bridge idempotency identity held for this hot lifetime. */
	trunkEnsureOperationId(createId: (prefix: string) => string): string {
		if (this.#disposed) {
			throw new Error("approval reviewer manager is disposed");
		}
		this.#trunkEnsureOperationId ??= createId("aprv_ensure");
		return this.#trunkEnsureOperationId;
	}

	trunkThreadId(): string | undefined {
		return this.#trunkThreadId;
	}

	installTrunkThreadId(threadId: string): boolean {
		if (this.#disposed || threadId.length === 0) {
			return false;
		}
		if (this.#trunkThreadId !== undefined && this.#trunkThreadId !== threadId) {
			return false;
		}
		this.#trunkThreadId = threadId;
		return true;
	}

	beginReview(reviewId: string): ApprovalReviewExecutionLease {
		if (this.#disposed) {
			throw new Error("approval reviewer manager is disposed");
		}
		if (!this.#trunkBusy) {
			this.#trunkBusy = true;
			return this.lease(reviewId, "trunk", () => {
				this.#trunkBusy = false;
			});
		}
		this.#ephemeralReviews.add(reviewId);
		return this.lease(reviewId, "sidecar", () => {
			this.#ephemeralReviews.delete(reviewId);
		});
	}

	executionState(reviewId: string): ReviewerExecutionState | undefined {
		const execution = this.#executions.get(reviewId);
		if (execution === undefined) {
			return undefined;
		}
		return {
			kind: execution.kind,
			raceState: execution.raceState,
			...(execution.token === undefined ? {} : { token: execution.token }),
		};
	}

	parentTranscriptFeed(view: ParentTranscriptView): ParentTranscriptFeed {
		const cursor = this.#feedCursor;
		if (
			cursor !== undefined &&
			cursor.generation === view.generation &&
			cursor.fedEntryCount <= view.entries.length
		) {
			return {
				reanchored: false,
				entries: view.entries.slice(cursor.fedEntryCount),
			};
		}
		return {
			reanchored: true,
			entries: [...view.entries],
		};
	}

	completeTrunkReview(view: ParentTranscriptView): void {
		if (this.#disposed) {
			return;
		}
		this.#feedCursor = {
			generation: view.generation,
			fedEntryCount: view.entries.length,
		};
	}

	/** Forgets an unusable trunk after unknown settlement or failed quiescence. */
	discardTrunk(reviewerThreadId: string): void {
		if (this.#trunkThreadId !== reviewerThreadId) {
			return;
		}
		this.#trunkThreadId = undefined;
		this.#feedCursor = undefined;
	}

	decisionFor(key: string): ApprovalReviewerOutcome | undefined {
		return this.#decisionMemo.get(key);
	}

	rememberDecision(key: string, decision: ApprovalReviewerOutcome): void {
		if (!this.#disposed) {
			this.#decisionMemo.set(key, decision);
		}
	}

	ephemeralReviewIds(): readonly string[] {
		return [...this.#ephemeralReviews];
	}

	isDisposed(): boolean {
		return this.#disposed;
	}

	fork<A, E>(effect: Effect.Effect<A, E>): Effect.Effect<Fiber.Fiber<A, E>> {
		if (this.#disposed) {
			return Effect.die("approval reviewer manager is disposed");
		}
		return Effect.forkIn(effect, this.#scope);
	}

	dispose(): Effect.Effect<void> {
		return Effect.suspend(() => {
			if (this.#disposed) {
				return Effect.void;
			}
			this.#disposed = true;
			this.#trunkBusy = false;
			this.#feedCursor = undefined;
			this.#decisionMemo.clear();
			this.#ephemeralReviews.clear();
			for (const execution of this.#executions.values()) {
				if (execution.raceState === "pending") {
					execution.raceState = "cancellation_won";
				}
				for (const listener of execution.cancellationListeners) {
					listener();
				}
				execution.cancellationListeners.clear();
			}
			this.#executions.clear();
			return Scope.close(this.#scope, Exit.void).pipe(Effect.asVoid);
		});
	}

	private lease(
		reviewId: string,
		kind: ApprovalReviewExecutionLease["kind"],
		onRelease: () => void,
	): ApprovalReviewExecutionLease {
		let released = false;
		const execution: MutableReviewerExecutionState = {
			kind,
			raceState: "pending",
			token: undefined,
			cancellationListeners: new Set(),
		};
		this.#executions.set(reviewId, execution);
		return {
			kind,
			installExecutionToken: (token) => {
				if (
					released ||
					token.reviewId !== reviewId ||
					token.reviewerThreadId.length === 0 ||
					!Number.isSafeInteger(token.runId) ||
					token.runId <= 0
				) {
					return false;
				}
				if (execution.token !== undefined) {
					return (
						execution.token.reviewId === token.reviewId &&
						execution.token.reviewerThreadId === token.reviewerThreadId &&
						execution.token.runId === token.runId
					);
				}
				execution.token = token;
				return true;
			},
			executionToken: () => execution.token,
			raceState: () => execution.raceState,
			// These synchronous state changes are the race linearization points;
			// asynchronous host callback timing only observes the winner.
			cancel: () => {
				if (released || execution.raceState !== "pending") {
					return false;
				}
				execution.raceState = "cancellation_won";
				for (const listener of execution.cancellationListeners) {
					listener();
				}
				execution.cancellationListeners.clear();
				return true;
			},
			claimOutcome: () => {
				if (released || execution.raceState !== "pending") {
					return false;
				}
				execution.raceState = "outcome_won";
				execution.cancellationListeners.clear();
				return true;
			},
			onCancellation: (listener) => {
				if (released || execution.raceState === "outcome_won") {
					return () => {};
				}
				if (execution.raceState === "cancellation_won") {
					listener();
					return () => {};
				}
				execution.cancellationListeners.add(listener);
				return () => {
					execution.cancellationListeners.delete(listener);
				};
			},
			release: () => {
				if (released) {
					return;
				}
				released = true;
				execution.cancellationListeners.clear();
				if (this.#executions.get(reviewId) === execution) {
					this.#executions.delete(reviewId);
				}
				onRelease();
			},
		};
	}
}
