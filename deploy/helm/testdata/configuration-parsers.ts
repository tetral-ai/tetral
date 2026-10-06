/** Execute owning startup parsers against rendered configuration without starting services. */
import { loadProviderGatewayConfigFromEnv } from "../../../services/gateway/packages/provider-gateway/src/config.ts";
import { loadMcpConnectorConfigFromEnv } from "../../../services/gateway/packages/mcp-connector/src/config.ts";
import { loadRuntimePodConfigFromEnv } from "../../../services/agent-runtime/packages/runtime-pod/src/config.ts";
import {
  DefaultBridgeMethodPolicies, bridgeMethodEnvKey, bridgeMethodEnvKeys,
  parseBridgeMethodPolicies, bridgeMethodDeadline, type BridgeMethod,
} from "../../../services/agent-runtime/packages/runtime-pod/src/bridge-policy.ts";

// Exercise the actual owner; no key derivation or deadline parser is recreated here.
function bridgePolicyEvidence() {
  const methods = Object.keys(DefaultBridgeMethodPolicies) as BridgeMethod[];
  const remaining = methods.filter((method) => DefaultBridgeMethodPolicies[method].kind === "remaining_provider_budget");
  const fixedKey = bridgeMethodEnvKey("writeEvent");
  const accepts = (value: string) => parseBridgeMethodPolicies({ [fixedKey]: value })?.writeEvent;
  let missingProviderDeadlineRejected = false;
  try { bridgeMethodDeadline(DefaultBridgeMethodPolicies, "readFileAttachmentChunk", 1000); }
  catch { missingProviderDeadlineRejected = true; }
  return {
    defaults: Object.fromEntries(methods.map((method) => {
      const policy = DefaultBridgeMethodPolicies[method];
      return [method, { kind: policy.kind, environment_key: bridgeMethodEnvKey(method),
        ...(policy.kind === "fixed" ? { timeout_ms: policy.timeoutMs } : {}) }];
    })),
    environmentKeys: bridgeMethodEnvKeys,
    defaultPolicies: parseBridgeMethodPolicies({}),
    minimumOverride: accepts("1"), maximumOverride: accepts("2147483647"),
    invalidFixedRejected: ["", "0", "01", "1.5", "+1", "-1", "2147483648", "9007199254740992"]
      .every((value) => accepts(value) === undefined),
    remainingOverridesRejected: remaining.every((method) => ["", "0", "1", "7000"]
      .every((value) => parseBridgeMethodPolicies({ [bridgeMethodEnvKey(method)]: value }) === undefined)),
    fixedDeadline: bridgeMethodDeadline(DefaultBridgeMethodPolicies, "loadContext", 1000),
    clippedDeadline: bridgeMethodDeadline(DefaultBridgeMethodPolicies, "loadContext", 1000, 1100),
    providerDeadline: bridgeMethodDeadline(DefaultBridgeMethodPolicies, "readFileAttachmentChunk", 1000, 1100),
    missingProviderDeadlineRejected,
  };
}

const rows = JSON.parse(await Bun.stdin.text()) as { role: string; env: Record<string, string> }[];
const results = rows.map(({ role, env }) => {
  const result = role === "provider-gateway" ? loadProviderGatewayConfigFromEnv(env)
    : role === "mcp-connector" ? loadMcpConnectorConfigFromEnv(env)
    : loadRuntimePodConfigFromEnv(env);
  if (!result.ok) return { role, ok: false };
  const config = result.config;
  return { role, ok: true, diagnostics: config.diagnostics,
    deploymentEnvironment: config.deploymentEnvironment, serviceVersion: config.serviceVersion,
    ...("databasePool" in config ? { databasePool: config.databasePool } : {}),
    ...("lifecycle" in config ? { lifecycle: config.lifecycle, bridgePolicy: bridgePolicyEvidence() } : {}),
    ...("drainTimeoutMs" in config ? { drainTimeoutMs: config.drainTimeoutMs, cancelJoinTimeoutMs: config.cancelJoinTimeoutMs } : {}) };
});
// Emit only configuration policy results. Fixture credentials never enter output.
process.stdout.write(JSON.stringify(results));
