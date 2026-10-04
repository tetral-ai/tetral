import { isIP } from "node:net";
import { PreviewLimits } from "./preview-protocol.js";

export const PreviewPublisherDefaults = Object.freeze({
  queueBytes: 4 * 1024 * 1024, queueFrames: 1024,
  batchBytes: 256 * 1024, batchFrames: 64,
  connectTimeoutMs: 1000, flushTimeoutMs: 1000,
  retryMaxMs: 5000, credentialPollMs: 250,
  pingIntervalMs: 1000, maxPingOut: 1,
});
export interface PreviewPublisherPolicy {
  readonly queueBytes: number; readonly queueFrames: number;
  readonly batchBytes: number; readonly batchFrames: number;
  readonly connectTimeoutMs: number; readonly flushTimeoutMs: number;
  readonly retryMaxMs: number; readonly credentialPollMs: number;
  readonly pingIntervalMs: number; readonly maxPingOut: number;
}
export interface PreviewNatsConfig {
  readonly servers: readonly string[];
  readonly userPath: string; readonly passwordPath: string;
  readonly tls?: { readonly caPath: string; readonly certPath: string; readonly keyPath: string };
  readonly policy: PreviewPublisherPolicy;
}
const policyKeys = {
  queueBytes: "TETRAL_GATEWAY_PREVIEW_QUEUE_BYTES", queueFrames: "TETRAL_GATEWAY_PREVIEW_QUEUE_FRAMES",
  batchBytes: "TETRAL_GATEWAY_PREVIEW_BATCH_BYTES", batchFrames: "TETRAL_GATEWAY_PREVIEW_BATCH_FRAMES",
  connectTimeoutMs: "TETRAL_NATS_CONNECT_TIMEOUT_MS", flushTimeoutMs: "TETRAL_GATEWAY_PREVIEW_FLUSH_TIMEOUT_MS",
  retryMaxMs: "TETRAL_NATS_RETRY_MAX_MS", credentialPollMs: "TETRAL_NATS_CREDENTIAL_POLL_MS",
  pingIntervalMs: "TETRAL_NATS_PING_INTERVAL_MS", maxPingOut: "TETRAL_NATS_MAX_PING_OUT",
} as const;
export const PreviewNatsEnvKeys = ["TETRAL_NATS_SERVERS", "TETRAL_NATS_USER_PATH", "TETRAL_NATS_PASSWORD_PATH", "TETRAL_NATS_TLS_CA_PATH", "TETRAL_NATS_TLS_CERT_PATH", "TETRAL_NATS_TLS_KEY_PATH", ...Object.values(policyKeys)] as const;
export function validPreviewPublisherPolicy(policy: PreviewPublisherPolicy): boolean {
  return Object.values(policy).every(value => Number.isSafeInteger(value) && value > 0) &&
    policy.batchBytes <= PreviewLimits.maxFrameBytes && policy.queueBytes >= policy.batchBytes * 2 &&
    policy.batchFrames <= policy.queueFrames && policy.connectTimeoutMs <= 60_000 &&
    policy.flushTimeoutMs <= 60_000 && policy.retryMaxMs <= 60_000 && policy.credentialPollMs <= 60_000 &&
    policy.pingIntervalMs <= 60_000 && policy.maxPingOut <= 16;
}
/** Unset wiring disables previews. Partial wiring is an error, never a plaintext fallback. */
export function parsePreviewNatsConfig(env: Record<string, string | undefined>): PreviewNatsConfig | undefined {
  if (PreviewNatsEnvKeys.every(key => env[key] === undefined)) return undefined;
  const serverText = env.TETRAL_NATS_SERVERS;
  const userPath = env.TETRAL_NATS_USER_PATH, passwordPath = env.TETRAL_NATS_PASSWORD_PATH;
  if (!serverText || serverText.length > 4096 || !userPath || !passwordPath) throw new Error("invalid preview configuration");
  const caPath = env.TETRAL_NATS_TLS_CA_PATH, certPath = env.TETRAL_NATS_TLS_CERT_PATH, keyPath = env.TETRAL_NATS_TLS_KEY_PATH;
  const protectedMode = [caPath, certPath, keyPath].some(value => value !== undefined);
  if (protectedMode && (!caPath || !certPath || !keyPath)) throw new Error("invalid preview TLS configuration");
  const servers = serverText.split(",");
  if (servers.length > 16 || new Set(servers).size !== servers.length) throw new Error("invalid preview servers");
  for (const address of servers) {
    const server = new URL(address);
    // URL.hostname preserves brackets around IPv6 literals; isIP expects the
    // address itself. Both IP families are excluded by the verified-DNS profile.
    const hostname = server.hostname.startsWith("[") && server.hostname.endsWith("]") ? server.hostname.slice(1, -1) : server.hostname;
    if (address !== address.trim() || server.username || server.password || server.search || server.hash || server.pathname !== "" ||
      server.protocol !== (protectedMode ? "tls:" : "nats:") || !hostname || (protectedMode && isIP(hostname) !== 0)) throw new Error("invalid preview server");
  }
  for (const path of [userPath, passwordPath, caPath, certPath, keyPath]) {
    if (path !== undefined && (path.length > 4096 || !path.startsWith("/") || /[\x00-\x1f\x7f]/.test(path))) throw new Error("invalid preview credential path");
  }
  const policy: { -readonly [K in keyof PreviewPublisherPolicy]: number } = { ...PreviewPublisherDefaults };
  for (const key of Object.keys(policyKeys) as (keyof PreviewPublisherPolicy)[]) {
    const value = env[policyKeys[key]];
    if (value !== undefined) {
      if (!/^[1-9][0-9]*$/.test(value)) throw new Error("invalid preview bound");
      policy[key] = Number(value);
    }
  }
  if (!validPreviewPublisherPolicy(policy)) throw new Error("invalid preview bounds");
  return { servers, userPath, passwordPath, policy, ...(protectedMode ? { tls: { caPath: caPath!, certPath: certPath!, keyPath: keyPath! } } : {}) };
}
