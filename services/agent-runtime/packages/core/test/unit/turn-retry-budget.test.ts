import { describe, expect, test } from "bun:test";
import { evaluateTurnRetryBudget } from "../../src/runtime/turn-retry-budget.js";

describe("surface-2 turn retry budget", () => {
  test("evaluates provider and compaction counters against the finite Runtime budget", () => {
    const counters = { providerAttempts: 1, compactionAttempts: 2 };

    expect(evaluateTurnRetryBudget(counters, "provider", 2)).toEqual({
      type: "available",
      attempt: 2,
      maxAttempts: 2,
    });
    expect(evaluateTurnRetryBudget(counters, "compaction", 2)).toEqual({
      type: "exhausted",
      attempts: 2,
      maxAttempts: 2,
      stopReason: { type: "retries_exhausted" },
    });
  });

  test("treats a zero deployment budget as immediate exhaustion", () => {
    expect(evaluateTurnRetryBudget({ providerAttempts: 0, compactionAttempts: 0 }, "provider", 0)).toEqual({
      type: "exhausted",
      attempts: 0,
      maxAttempts: 0,
      stopReason: { type: "retries_exhausted" },
    });
  });

  test("rejects malformed persisted counters and budgets", () => {
    expect(() => evaluateTurnRetryBudget({ providerAttempts: -1, compactionAttempts: 0 }, "provider", 2)).toThrow();
    expect(() => evaluateTurnRetryBudget({ providerAttempts: 0, compactionAttempts: 0 }, "provider", -1)).toThrow();
  });

  test("does not manufacture incomplete durable reschedule decisions", () => {
    const decision = evaluateTurnRetryBudget({ providerAttempts: 0, compactionAttempts: 0 }, "provider", 2);
    expect(JSON.stringify(decision)).not.toContain("deadline");
    expect(JSON.stringify(decision)).not.toContain("backoff");
    expect(JSON.stringify(decision)).not.toContain("runtimeInputId");
    expect(JSON.stringify(decision)).not.toContain("reschedule");
  });
});
