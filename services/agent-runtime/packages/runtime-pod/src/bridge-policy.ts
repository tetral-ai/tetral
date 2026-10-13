/** Owns the complete Bridge RPC deadline census. Policies do not authorize retries. */
import { AgentRuntimeBridgeServiceService } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { SessionEventWriterRetryPolicy } from "@tetral/agent-runtime-core/src/contracts/runtime.js";

export type BridgeMethod = keyof typeof AgentRuntimeBridgeServiceService;
export type BridgeDeadlinePolicy =
  | { readonly kind: "fixed"; readonly timeoutMs: number }
  | {
      readonly kind: "size_scaled";
      readonly timeoutMs: number;
      readonly perMiBMs: number;
      readonly maxTimeoutMs: number;
    }
  | { readonly kind: "remaining_provider_budget" };

const fixed = (timeoutMs: number): BridgeDeadlinePolicy => ({ kind: "fixed", timeoutMs });

/**
 * Bridge work for one WriteEvent grows with its encoded size (up to 64 MiB). The slope allows
 * 0.5 MiB/s below the cap; at the cap the largest legal request (about 32 MiB) needs about
 * 1.1 MiB/s. The cap equals the 30 s context read and stays under the Bridge drain.
 */
export const WriteEventPerMiBTimeoutMs = 2_000;
export const WriteEventMaxTimeoutMs = 30_000;
const MiB = 1024 * 1024;

/** Every new protobuf method must choose its own policy before this module compiles. */
export const DefaultBridgeMethodPolicies = {
  loadContext: fixed(30_000),
  refreshRuntimeBindingToken: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  commitInputs: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  commitTaskNotificationResult: fixed(SessionEventWriterRetryPolicy.timeoutPerAttemptMs),
  writeEvent: {
    kind: "size_scaled",
    timeoutMs: SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
    perMiBMs: WriteEventPerMiBTimeoutMs,
    maxTimeoutMs: WriteEventMaxTimeoutMs,
  },
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

/**
 * Parse once at the process config boundary; remaining-provider methods cannot reset that budget.
 * A size-scaled override replaces only the base timeout.
 */
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
    if (policy.kind === "remaining_provider_budget" || !/^[1-9][0-9]*$/.test(value)) return undefined;
    const timeoutMs = Number(value);
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs > 2_147_483_647) return undefined;
    policies[method] = { ...policy, timeoutMs };
  }
  return Object.freeze(policies) as BridgeMethodPolicies;
}

/** The longest attempt a policy allows; remaining-provider methods have none of their own. */
export function bridgeMethodMaxAttemptMs(policy: BridgeDeadlinePolicy): number {
  if (policy.kind === "remaining_provider_budget") return 0;
  return policy.kind === "fixed" ? policy.timeoutMs : Math.max(policy.timeoutMs, policy.maxTimeoutMs);
}

/** Clips a method deadline to the caller's shared phase budget without extending either clock. */
export function bridgeMethodDeadline(
  policies: BridgeMethodPolicies,
  method: BridgeMethod,
  now: number,
  remainingDeadline?: number,
  requestBytes?: number,
): number {
  const policy = policies[method];
  if (policy.kind === "remaining_provider_budget") {
    if (remainingDeadline === undefined || !Number.isFinite(remainingDeadline)) {
      throw new Error("Bridge attachment call requires a provider deadline");
    }
    return remainingDeadline;
  }
  let timeoutMs = policy.timeoutMs;
  if (policy.kind === "size_scaled") {
    if (requestBytes === undefined || !Number.isFinite(requestBytes) || requestBytes < 0) {
      throw new Error("Bridge size-scaled call requires its encoded request size");
    }
    timeoutMs = Math.min(
      bridgeMethodMaxAttemptMs(policy),
      policy.timeoutMs + Math.ceil((policy.perMiBMs * requestBytes) / MiB),
    );
  }
  return Math.min(now + timeoutMs, remainingDeadline ?? Number.POSITIVE_INFINITY);
}
