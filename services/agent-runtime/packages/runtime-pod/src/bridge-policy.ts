/** Owns the complete Bridge RPC deadline census. Policies do not authorize retries. */
import { AgentRuntimeBridgeServiceService } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";

export type BridgeMethod = keyof typeof AgentRuntimeBridgeServiceService;
export type BridgeDeadlinePolicy =
  | { readonly kind: "fixed"; readonly timeoutMs: number }
  | { readonly kind: "remaining_provider_budget" };

const fixed = (timeoutMs: number): BridgeDeadlinePolicy => ({ kind: "fixed", timeoutMs });

/** Every new protobuf method must choose its own policy before this module compiles. */
export const DefaultBridgeMethodPolicies = {
  loadContext: fixed(30_000),
  refreshRuntimeBindingToken: fixed(3_000),
  commitInputs: fixed(3_000),
  commitTaskNotificationResult: fixed(3_000),
  writeEvent: fixed(3_000),
  settleToolResult: fixed(3_000),
  writeRequestEnd: fixed(3_000),
  finishIdle: fixed(35_000),
  createSubagentThread: fixed(3_000),
  ensureApprovalReviewerTrunk: fixed(3_000),
  ensureApprovalReviewerSidecar: fixed(3_000),
  admitApprovalReviewInput: fixed(3_000),
  resolveChildThread: fixed(3_000),
  listChildThreads: fixed(3_000),
  deliverInterAgentMail: fixed(3_000),
  readAgentMail: fixed(30_000),
  admitChildInterrupt: fixed(3_000),
  awaitChildInterrupt: fixed(3_000),
  closeChildControl: fixed(3_000),
  closeApprovalReviewer: fixed(3_000),
  markChildThreadActive: fixed(3_000),
  acceptSandboxExecution: fixed(3_000),
  awaitSandboxExecution: fixed(35_000),
  readCommandResult: fixed(35_000),
  sendCommandInput: fixed(35_000),
  cancelCommand: fixed(35_000),
  authorizeWebToolExecution: fixed(3_000),
  runMemory: fixed(35_000),
  resolveTransientAttachment: { kind: "remaining_provider_budget" },
  resolveFileAttachmentMetadata: { kind: "remaining_provider_budget" },
  readFileAttachmentChunk: { kind: "remaining_provider_budget" },
  mcpManifestChanged: fixed(5_000),
  claimMcpToolResult: fixed(10_000),
  commitMcpToolResult: fixed(10_000),
  relinquishMcpToolResult: fixed(10_000),
  commitInternalToolRepair: fixed(3_000),
  commitRuntimeTermination: fixed(3_000),
  registerRuntimeProcess: fixed(5_000),
  reportRuntimeProcess: fixed(1_000),
  releaseRuntimeBinding: fixed(5_000),
} as const satisfies Readonly<Record<BridgeMethod, BridgeDeadlinePolicy>>;

export type BridgeMethodPolicies = Readonly<Record<BridgeMethod, BridgeDeadlinePolicy>>;

export function bridgeMethodEnvKey(method: BridgeMethod): string {
  if (method === "registerRuntimeProcess") return "TETRAL_RUNTIME_REGISTER_TIMEOUT_MS";
  if (method === "reportRuntimeProcess") return "TETRAL_RUNTIME_REPORT_TIMEOUT_MS";
  return `TETRAL_BRIDGE_${method.replace(/[A-Z]/g, (letter) => `_${letter}`).toUpperCase()}_TIMEOUT_MS`;
}

export const bridgeMethodEnvKeys = Object.keys(DefaultBridgeMethodPolicies).map((method) =>
  bridgeMethodEnvKey(method as BridgeMethod),
);

/** Parse once at the process config boundary; remaining-provider methods cannot reset that budget. */
export function parseBridgeMethodPolicies(
  env: Readonly<Record<string, string | undefined>>,
): BridgeMethodPolicies | undefined {
  const policies: Record<string, BridgeDeadlinePolicy> = {};
  for (const method of Object.keys(DefaultBridgeMethodPolicies) as BridgeMethod[]) {
    const policy = DefaultBridgeMethodPolicies[method];
    const value = env[bridgeMethodEnvKey(method)];
    if (value === undefined) {
      policies[method] = policy;
      continue;
    }
    if (policy.kind !== "fixed" || !/^[1-9][0-9]*$/.test(value)) return undefined;
    const timeoutMs = Number(value);
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs > 2_147_483_647) return undefined;
    policies[method] = { kind: "fixed", timeoutMs };
  }
  return Object.freeze(policies) as BridgeMethodPolicies;
}

/** Clips a method deadline to the caller's shared phase budget without extending either clock. */
export function bridgeMethodDeadline(
  policies: BridgeMethodPolicies,
  method: BridgeMethod,
  now: number,
  remainingDeadline?: number,
): number {
  const policy = policies[method];
  if (policy.kind === "remaining_provider_budget") {
    if (remainingDeadline === undefined || !Number.isFinite(remainingDeadline)) {
      throw new Error("Bridge attachment call requires a provider deadline");
    }
    return remainingDeadline;
  }
  return Math.min(now + policy.timeoutMs, remainingDeadline ?? Number.POSITIVE_INFINITY);
}
