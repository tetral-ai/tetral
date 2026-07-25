/**
 * @packageDocumentation
 * Evaluates whether a normalized model tool call is unavailable, runnable, waiting on user
 * approval, waiting on the internal reviewer, or denied. It guards the separation between tool
 * availability and approval, maps reviewer failures back to user approval, and labels each decision
 * with its evaluated permission. AgentLoop calls this gate with catalog policy and optional reviewer
 * outcomes and keeps `review_required` internal by re-evaluating before public tool-use persistence;
 * the gate calls tool-catalog lookup and effective-policy helpers and performs no I/O itself.
 */
import type { ToolCatalog, ToolConfig, ToolEntry } from "./tool-catalog.js";
import { effectivePermissionPolicy, lookupToolEntry } from "./tool-catalog.js";

/** Session approval mode applied to enabled tool calls. */
export type ToolApprovalMode = "full_access" | "ask_for_approval" | "approve_for_me";

/** Authority that supplies the final approval or denial recorded for a tool call. */
export type ToolApprovalSource = "config" | "user" | "auto_reviewer";

/** Risk classification accepted from an approval reviewer decision. */
export type ApprovalReviewRiskLevel = "low" | "medium" | "high" | "critical";
/** User-authorization strength accepted from an approval reviewer decision. */
export type ApprovalReviewUserAuthorization = "unknown" | "low" | "medium" | "high";

/** Closed gate outcome consumed by tool scheduling and public tool-event projection. */
export type ToolGateDecision =
  | {
      readonly type: "invalid";
      readonly reason: "unknown_or_disabled";
      readonly publicEvent: false;
    }
  | {
      readonly type: "run";
      readonly approval: "skipped" | "allowed" | "auto_approved";
      readonly evaluatedPermission: "allow";
      readonly approvalSource: ToolApprovalSource;
    }
  | {
      readonly type: "ask";
      readonly evaluatedPermission: "ask";
      readonly approvalSource: "user";
    }
  | {
      readonly type: "review_required";
      readonly evaluatedPermission: "ask";
      readonly approvalSource: "auto_reviewer";
    }
  | {
      readonly type: "deny";
      readonly evaluatedPermission: "deny";
      readonly approvalSource: ToolApprovalSource;
      readonly message: string;
    };

/** Valid reviewer decision or fail-closed reviewer outcome returned to the parent gate. */
export type ApprovalReviewerOutcome =
  | {
      readonly type: "decision";
      readonly riskLevel: ApprovalReviewRiskLevel;
      readonly userAuthorization: ApprovalReviewUserAuthorization;
      readonly outcome: "allow" | "deny";
      readonly message?: string;
    }
  | {
      readonly type: "failed";
      readonly message?: string;
    };

/** Catalog, tool identity, and approval context required to evaluate one tool call. */
export interface ToolGateInput {
  readonly catalog: ToolCatalog;
  readonly toolName: string;
  readonly approvalMode: ToolApprovalMode;
  readonly reviewerOutcome?: ApprovalReviewerOutcome;
}

/** Evaluates availability before applying approval policy to one normalized tool call. */
export function evaluateToolGate(input: ToolGateInput): ToolGateDecision {
  const entry = lookupToolEntry(input.catalog, input.toolName);
  if (entry === undefined) {
    return { type: "invalid", reason: "unknown_or_disabled", publicEvent: false };
  }
  return evaluateEnabledToolGate({
    entry,
    configs: input.catalog.configs,
    approvalMode: input.approvalMode,
    ...(input.reviewerOutcome !== undefined ? { reviewerOutcome: input.reviewerOutcome } : {}),
  });
}

// Approval decision matrix: resolves (approvalMode x effective permissionPolicy x
// reviewerOutcome) into a ToolGateDecision. The evaluatedPermission on the
// run/ask/deny arms IS exactly the public agent.tool_use.evaluated_permission enum
// (allow|ask|deny); the response-side tetral_agent_toolset / mcp_toolset configs
// projection enumerates the always_allow tools as exceptions over an always_ask
// default, so gate and response read one policy.
//
// | approvalMode     | effective permission_policy | reviewerOutcome | decision                              | evaluated_permission |
// | ---------------- | --------------------------- | --------------- | ------------------------------------- | -------------------- |
// | full_access      | (any)                       | (n/a)           | run(skipped, source=config)           | allow                |
// | ask_for_approval | always_allow                | (n/a)           | run(allowed, source=config)           | allow                |
// | ask_for_approval | always_ask                  | (n/a)           | ask(source=user)                      | ask                  |
// | approve_for_me   | always_allow                | (n/a)           | run(allowed, source=config)           | allow                |
// | approve_for_me   | always_ask                  | undefined       | review_required(source=auto_reviewer) | ask (internal only)  |
// | approve_for_me   | always_ask                  | failed          | ask(source=user)                      | ask                  |
// | approve_for_me   | always_ask                  | decision allow  | run(auto_approved)                    | allow                |
// | approve_for_me   | always_ask                  | decision deny   | deny(source=auto_reviewer)            | deny                 |
// (an unknown/disabled tool short-circuits in evaluateToolGate to invalid, publicEvent=false.)
//
// review_required is an INTERNAL orchestration state, never a public outcome: under
// approve_for_me the gate is re-evaluated WITH the reviewer outcome before any public
// emission, so review_required never appears in the public tool-event mapping; an
// effective always_allow under approve_for_me skips the reviewer and yields run(allowed).
// UPDATE-WITH: services/agent-runtime/packages/core/src/tools/tool-catalog.ts,
//              services/agent-runtime/packages/core/src/tools/tool-scheduler.ts
/** Applies the approval decision matrix to an already enabled catalog entry. */
export function evaluateEnabledToolGate(input: {
  readonly entry: ToolEntry;
  readonly configs: readonly ToolConfig[];
  readonly approvalMode: ToolApprovalMode;
  readonly reviewerOutcome?: ApprovalReviewerOutcome;
}): ToolGateDecision {
  if (input.approvalMode === "full_access") {
    return {
      type: "run",
      approval: "skipped",
      evaluatedPermission: "allow",
      approvalSource: "config",
    };
  }

  const permissionPolicy = effectivePermissionPolicy(input.entry, input.configs);
  if (permissionPolicy === "always_allow") {
    return {
      type: "run",
      approval: "allowed",
      evaluatedPermission: "allow",
      approvalSource: "config",
    };
  }

  if (input.approvalMode === "ask_for_approval") {
    return {
      type: "ask",
      evaluatedPermission: "ask",
      approvalSource: "user",
    };
  }

  if (input.reviewerOutcome === undefined) {
    return {
      type: "review_required",
      evaluatedPermission: "ask",
      approvalSource: "auto_reviewer",
    };
  }

  if (input.reviewerOutcome.type === "failed") {
    return {
      type: "ask",
      evaluatedPermission: "ask",
      approvalSource: "user",
    };
  }

  if (input.reviewerOutcome.outcome === "allow") {
    return {
      type: "run",
      approval: "auto_approved",
      evaluatedPermission: "allow",
      approvalSource: "auto_reviewer",
    };
  }

  return {
    type: "deny",
    evaluatedPermission: "deny",
    approvalSource: "auto_reviewer",
    message: input.reviewerOutcome.message ?? "The approval reviewer denied this tool call.",
  };
}
