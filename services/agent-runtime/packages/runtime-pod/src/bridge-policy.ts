/** Owns the complete Bridge RPC deadline census. Policies do not authorize retries. */
import { AgentRuntimeBridgeServiceService } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { SessionEventWriterRetryPolicy } from "@tetral/agent-runtime-core/src/contracts/runtime.js";

export type BridgeMethod = keyof typeof AgentRuntimeBridgeServiceService;
export type BridgeDeadlinePolicy =
  | { readonly kind: "fixed"; readonly timeoutMs: number }
  | { readonly kind: "remaining_provider_budget" };

const fixed = (timeoutMs: number): BridgeDeadlinePolicy => ({ kind: "fixed", timeoutMs });

/** Every new protobuf method must choose its own policy before this module compiles. */
export const DefaultBridgeMethodPolicies = {
  loadContext: fixed(30_000),
  refreshRuntimeBindingToken: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  commitInputs: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  commitTaskNotificationResult: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  writeEvent: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  settleToolResult: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  writeRequestEnd: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  finishIdle: fixed(35_000),
  createSubagentThread: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  ensureApprovalReviewerTrunk: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  ensureApprovalReviewerSidecar: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  admitApprovalReviewInput: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  resolveChildThread: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  listChildThreads: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  deliverInterAgentMail: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  readAgentMail: fixed(30_000),
  admitChildInterrupt: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  awaitChildInterrupt: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  closeChildControl: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  closeApprovalReviewer: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  markChildThreadActive: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  acceptSandboxExecution: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  awaitSandboxExecution: fixed(35_000),
  readCommandResult: fixed(35_000),
  sendCommandInput: fixed(35_000),
  cancelCommand: fixed(35_000),
  authorizeWebToolExecution: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  runMemory: fixed(35_000),
  resolveTransientAttachment: { kind: "remaining_provider_budget" },
  resolveFileAttachmentMetadata: { kind: "remaining_provider_budget" },
  readFileAttachmentChunk: { kind: "remaining_provider_budget" },
  mcpManifestChanged: fixed(5_000),
  claimMcpToolResult: fixed(10_000),
  commitMcpToolResult: fixed(10_000),
  relinquishMcpToolResult: fixed(10_000),
  commitInternalToolRepair: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  commitRuntimeTermination: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
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
