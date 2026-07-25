/**
 * @packageDocumentation
 * Evaluates the run-local mirror of provider and compaction retry budgets.
 * It guards non-negative counters, separate retry classes, and the transition from an available
 * next attempt to retries_exhausted without claiming ownership of the durable Bridge ceiling.
 * AgentLoop calls this helper with counters from the active run and deployment maxima; the helper
 * performs a pure decision and calls no transport or persistence service.
 */
/** Retry class whose run-local counter is being evaluated. */
export type RuntimeTurnRetryKind = "provider" | "compaction";

// Run-local mirror of the durable turn-retry budget row. Two independent columns:
//   providerAttempts   — provider agent requests (deployment default max 3).
//   compactionAttempts — compaction-summary sub-requests (deployment default max 2).
// The durable columns are incremented ONLY by the winning request-end close (the
// close that owns the reschedule decision) and are reset by EVERY turn-settling
// idle write; there is no compaction-specific reset — one settling idle clears both.
// maxAttempts is supplied by the caller from deployment policy, never hardcoded here.
// UPDATE-WITH: services/agent-runtime/packages/core/src/agent-loop/agent-loop.ts
/** Run-local provider and compaction attempt counts. */
export interface RuntimeTurnRetryCounters {
  readonly providerAttempts: number;
  readonly compactionAttempts: number;
}

/** Decision to schedule one more attempt or settle the turn as exhausted. */
export type RuntimeTurnRetryBudgetDecision =
  | {
      readonly type: "available";
      readonly attempt: number;
      readonly maxAttempts: number;
    }
  | {
      readonly type: "exhausted";
      readonly attempts: number;
      readonly maxAttempts: number;
      readonly stopReason: { readonly type: "retries_exhausted" };
    };

/** Evaluates one retry class against its configured run-local maximum. */
export function evaluateTurnRetryBudget(
  counters: RuntimeTurnRetryCounters,
  kind: RuntimeTurnRetryKind,
  maxAttempts: number,
): RuntimeTurnRetryBudgetDecision {
  assertNonNegativeInteger(counters.providerAttempts, "providerAttempts");
  assertNonNegativeInteger(counters.compactionAttempts, "compactionAttempts");
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 0) {
    throw new Error("maxAttempts must be a non-negative safe integer");
  }

  const attempts = kind === "provider" ? counters.providerAttempts : counters.compactionAttempts;
  if (attempts >= maxAttempts) {
    return {
      type: "exhausted",
      attempts,
      maxAttempts,
      stopReason: { type: "retries_exhausted" },
    };
  }
  return { type: "available", attempt: attempts + 1, maxAttempts };
}

function assertNonNegativeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} must be a non-negative safe integer`);
  }
}
