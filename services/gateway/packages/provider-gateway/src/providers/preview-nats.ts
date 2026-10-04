import type { ConnectionOptions, Status } from "@nats-io/transport-node";
import { open, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { createHash, createPrivateKey, X509Certificate } from "node:crypto";
import { semanticErrorFields } from "@tetral/ts-observability";
import type { GatewayLogger } from "../logger.js";
import type { PreviewNatsConfig } from "./preview-config.js";
import { PreviewPublisher } from "./preview-publisher.js";
import type { PreviewConnection, PreviewRequestProducer } from "./preview-publisher.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { connectOwnedPreviewTransport } from "./preview-transport.js";

const maxCredentialBytes = 4096;
const maxCertificateMaterialBytes = 64 * 1024;
interface Credentials {
  readonly user: string; readonly password: string; readonly fingerprint: string; readonly expires: number;
  readonly tls?: { readonly ca: string; readonly cert: string; readonly key: string; readonly handshakeFirst: true; readonly rejectUnauthorized: true };
}
async function readMaterial(path: string, limit: number): Promise<string> {
  const file = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    const info = await file.stat();
    if (!info.isFile() || info.size > limit) throw new Error("invalid preview credential file");
    const bytes = Buffer.alloc(limit + 1);
    let length = 0;
    while (length < bytes.length) {
      const result = await file.read(bytes, length, bytes.length - length, null);
      if (result.bytesRead === 0) break;
      length += result.bytesRead;
    }
    if (length > limit) throw new Error("preview credential material too large");
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length));
  } finally { await file.close(); }
}
async function readCredentials(config: PreviewNatsConfig): Promise<Credentials> {
  const paths = [config.userPath, config.passwordPath, ...(config.tls === undefined ? [] : [config.tls.caPath, config.tls.certPath, config.tls.keyPath])];
  const resolved = await Promise.all(paths.map(path => realpath(path)));
  const contents = await Promise.all(resolved.map((path, index) => readMaterial(path, index < 2 ? maxCredentialBytes : maxCertificateMaterialBytes)));
  const after = await Promise.all(paths.map(path => realpath(path)));
  if (resolved.some((path, index) => after[index] !== path)) throw new Error("preview credential generation changed while reading");
  const user = contents[0]!.trim(), password = contents[1]!.trim();
  if (!user || !password || /[\x00-\x1f\x7f]/.test(user + password)) throw new Error("invalid preview credentials");
  let expires = Infinity;
  let tls: Credentials["tls"];
  if (config.tls !== undefined) {
    const ca = certificateBlocks(contents[2]!);
    const cert = contents[3]!, key = contents[4]!;
    for (const block of ca) {
      const root = new X509Certificate(block);
      if (!root.ca) throw new Error("preview trust contains non-CA certificate");
      expires = Math.min(expires, validateValidity(root));
    }
    const chain = certificateBlocks(cert);
    const leaf = new X509Certificate(chain[0]!);
    for (const certificate of chain) expires = Math.min(expires, validateValidity(new X509Certificate(certificate)));
    if (leaf.ca || !leaf.checkPrivateKey(createPrivateKey(key))) throw new Error("preview certificate/key mismatch");
    tls = { ca: ca.join("\n"), cert, key, handshakeFirst: true, rejectUnauthorized: true };
  }
  return { user, password, expires, fingerprint: createHash("sha256").update(contents.map(value => `${Buffer.byteLength(value)}:${value}`).join("")).digest("hex"), ...(tls === undefined ? {} : { tls }) };
}
function certificateBlocks(pem: string): string[] {
  const blocks = pem.match(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g);
  if (!blocks?.length || pem.replace(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g, "").trim()) throw new Error("preview certificate bundle is malformed");
  return blocks;
}
function validateValidity(cert: X509Certificate): number {
  const until = Date.parse(cert.validTo);
  if (Date.now() < Date.parse(cert.validFrom) || Date.now() >= until) throw new Error("preview certificate is outside validity period");
  return until;
}
/** Native verified TLS-first transport. Each advertised DNS destination is verified by the official client. */
type NativePreviewConnection = PreviewConnection & { status(): AsyncIterable<Status> };
type NativePreviewConnect = (options: ConnectionOptions, signal?: AbortSignal) => Promise<NativePreviewConnection>;
async function connectGeneration(config: PreviewNatsConfig, credentials: Credentials, nativeConnect: NativePreviewConnect, signal: AbortSignal): Promise<PreviewConnection> {
  if (Date.now() >= credentials.expires) throw new Error("preview credentials expired");
  const connection = await nativeConnect({ servers: [...config.servers], user: credentials.user, pass: credentials.password,
    reconnect: false, timeout: config.policy.connectTimeoutMs, pingInterval: config.policy.pingIntervalMs, maxPingOut: config.policy.maxPingOut,
    ...(credentials.tls === undefined ? {} : { tls: credentials.tls }) }, signal);
  // Idle publishers must detect a vanished broker even when a TLS end callback
  // has not completed. The official heartbeat supplies that protocol signal.
  // Exit the iterator before joining close: native close also joins listeners.
  let staleClose: Promise<void> | undefined;
  const monitor = (async () => {
    for await (const status of connection.status()) {
      if (status.type === "staleConnection") { staleClose = connection.close(); break; }
    }
    await staleClose;
  })();
  void monitor.catch(() => undefined);
  return {
    publish: (subject, bytes) => connection.publish(subject, bytes),
    flush: () => connection.flush(),
    closed: () => connection.closed(),
    close: async () => {
      const outcomes = await Promise.allSettled([connection.close(), monitor]);
      const failed = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === "rejected");
      if (failed !== undefined) throw failed.reason;
    },
  };
}
/** Initial material is validated before startup; broker unavailability affects previews only. */
export interface NatsPreviewPublisher {
  start(): void;
  close(): Promise<void>;
  createProducer(request: ProviderRequest): PreviewRequestProducer;
  readonly metrics: PreviewPublisher["metrics"];
}
/** Dependency substitution used by real-transport lifecycle compositions. */
export interface NatsPreviewPublisherDependencies {
  readonly connectionFactory?: (nativeDial: () => Promise<PreviewConnection>) => Promise<PreviewConnection>;
  readonly nativeConnect?: NativePreviewConnect;
}
export async function createNatsPreviewPublisher(config: PreviewNatsConfig, logger?: GatewayLogger, dependencies: NatsPreviewPublisherDependencies = {}): Promise<NatsPreviewPublisher> {
  let credentials = await readCredentials(config);
  let stopped = false, poll: Promise<void> | undefined;
  let timer: ReturnType<typeof setInterval> | undefined;
  let malformed = false;
  const lifetime = new AbortController();
  const dial = (current: Credentials): Promise<PreviewConnection> => {
    const nativeDial = () => connectGeneration(config, current, dependencies.nativeConnect ?? connectOwnedPreviewTransport, lifetime.signal);
    return dependencies.connectionFactory?.(nativeDial) ?? nativeDial();
  };
  const publisher = new PreviewPublisher({ policy: config.policy, ...(logger === undefined ? {} : { logger }), connect: async () => {
    // Revalidate projected files for every fresh connection, including recovery.
    const current = await readCredentials(config);
    const connection = await dial(current);
    credentials = current;
    return connection;
  } });
  const diagnostic = (failed: boolean): void => {
    try {
      const record = { event: failed ? "preview.credential_reload_failed" : "preview.credential_reload_recovered", "event.kind": failed ? "preview.credential_reload_failed" : "preview.credential_reload_recovered", component: "gateway", operation: "preview.credentials", "transport.stage": "credential_reload", "transport.outcome": failed ? "invalid_generation" : "recovered" };
      if (failed) logger?.error({ ...record, ...semanticErrorFields({ errorClass: "preview_unavailable", errorCode: "credential_reload", messageSafe: "preview credential reload failed" }) });
      else logger?.info(record);
    } catch { /* Credential custody is independent of diagnostics. */ }
  };
  const observe = async (): Promise<void> => {
    let candidate: PreviewConnection | undefined;
    let replacementFailed = false;
    try {
      const current = await readCredentials(config);
      if (publisher.metrics.connected && current.fingerprint !== credentials.fingerprint) {
        // Syntactically valid replacement trust is now the intended policy.
        // If it rejects the broker, retaining the old client could preserve a
        // trust root that the operator deliberately removed.
        replacementFailed = true;
        candidate = await dial(current);
        let deadline: ReturnType<typeof setTimeout> | undefined;
        try { await Promise.race([candidate.flush(), new Promise<never>((_, reject) => { deadline = setTimeout(() => reject(new Error("preview replacement flush timed out")), config.policy.flushTimeoutMs); })]); }
        finally { if (deadline !== undefined) clearTimeout(deadline); }
        if (stopped) { await candidate.close(); return; }
        await publisher.replaceConnection(candidate); candidate = undefined; credentials = current;
        replacementFailed = false;
      }
      if (malformed) diagnostic(false);
      malformed = false;
    } catch {
      try { await candidate?.close(); } catch { publisher.metrics.failures++; }
      if (!stopped && !malformed) diagnostic(true);
      malformed = true;
      // Still-valid last-known-good may remain active; expiry cannot preserve it.
      if (replacementFailed || Date.now() >= credentials.expires) {
        try { await publisher.retireConnection("credential_reload"); } catch { publisher.metrics.failures++; }
      }
    }
  };
  const start = () => {
    if (timer !== undefined || stopped) return;
    publisher.start();
    timer = setInterval(() => {
      if (poll !== undefined || stopped) return;
      poll = observe().finally(() => { poll = undefined; });
    }, config.policy.credentialPollMs);
  };
  const close = async () => {
    stopped = true;
    lifetime.abort();
    if (timer !== undefined) clearInterval(timer);
    const outcomes = await Promise.allSettled([publisher.close(), poll]);
    const failed = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === "rejected");
    if (failed !== undefined) throw failed.reason;
  };
  return { start, close, metrics: publisher.metrics, createProducer: request => publisher.createProducer(request) };
}
