/**
 * Implements provider-failure classification and the in-memory platform key
 * pool used by the provider-gateway process.
 *
 * Active keys for one provider share a cache scope. Selection considers only
 * the available keys in the lowest numeric priority tier, applies a fixed
 * weight floor, and treats cooling and quarantine as disposable per-process
 * observations. The module neither persists credential state nor retries a
 * provider stream.
 *
 * `CachedPlatformCredentialPool` loads and decrypts row snapshots before
 * replacing the pool, while the credential resolver selects through its
 * boundary. Provider clients and `ProviderGatewayServiceShell` use the failure
 * classifier and `ProviderKeyFailureError`; this module delegates bounded error
 * normalization to gateway lowering and key decryption to the provider crypto
 * module.
 *
 * @packageDocumentation
 */
import { classifyOpenAIProviderError, normalizeProviderError } from "@tetral/gateway-lowering/src/errors.js";
import { decryptAES256GCM } from "./crypto.js";
import type { ProviderErrorInput } from "@tetral/gateway-lowering/src/errors.js";

/** Provider identifiers understood by provider-failure classification. */
export type GatewayCatalogProviderId = "anthropic" | "openai" | "deepseek" | "moonshotai" | "zai";

/** Providers eligible to use credentials from the platform-owned key pool. */
export type PlatformHostedProviderId = "anthropic" | "openai" | "deepseek";

/** Encrypted platform-key row read from durable credential storage. */
export interface EncryptedPlatformProviderKeyRow {
  readonly keyId: string;
  readonly providerId: PlatformHostedProviderId;
  readonly encryptedKey: Uint8Array;
  readonly weight: number;
  readonly priority: number;
  readonly cacheScope: string;
  readonly status: "active" | "disabled";
  readonly disabledReason?: string | undefined;
  readonly updatedAt: string;
}

/** Decrypted, active platform key held only for gateway request processing. */
export interface PlatformProviderKey {
  readonly keyId: string;
  readonly providerId: PlatformHostedProviderId;
  readonly key: string;
  readonly weight: number;
  readonly priority: number;
  readonly cacheScope: string;
}

/** Injectable clock, randomness, and quarantine observer for a key pool. */
export interface PlatformKeyPoolOptions {
  readonly now?: () => number;
  readonly random?: () => number;
  readonly onQuarantine?: (event: PlatformKeyPoolQuarantineEvent) => void;
}

/** Per-selection exclusions, normally keys already attempted by the current turn. */
export interface PlatformKeySelectionOptions {
  readonly excludeKeyIds?: ReadonlySet<string> | undefined;
}

/** Either one selected platform key or a bounded, retryable exhaustion error. */
export type PlatformKeySelection =
  | { readonly ok: true; readonly key: PlatformProviderKey }
  | { readonly ok: false; readonly error: ProviderErrorInput };

/** State transition or caller behavior produced by provider-failure classification. */
export type ProviderFailureAction = "cooling" | "quarantine" | "retryable" | "fail-fast";

/** A normalized provider error paired with its platform-key handling action. */
export interface ProviderFailureClassification {
  readonly action: ProviderFailureAction;
  readonly providerError: ProviderErrorInput;
  readonly cooldownMs?: number | undefined;
}

/** Carries a classified key-attempt failure from a provider client to orchestration. */
export class ProviderKeyFailureError extends Error {
  constructor(readonly classification: ProviderFailureClassification) {
    super(classification.providerError.message ?? "Provider key attempt failed.");
    this.name = "ProviderKeyFailureError";
  }
}

/** Notification emitted when this process quarantines a known platform key. */
export interface PlatformKeyPoolQuarantineEvent {
  readonly keyId: string;
  readonly providerId: PlatformHostedProviderId;
  readonly providerError: ProviderErrorInput;
}

// Frozen platform-key pool constants.
// PoolReadCacheTtlMs (30 s) bounds how long one whole-pool table read is reused,
//   so an operator key-row change (weight, priority, status='disabled') takes
//   effect within 30 s with no restart.
// DefaultCooldownMs (5 s) is the COOLING TTL used when a rate-limit carries no
//   retry-after/reset hint.
// CooldownCapMs (60 s) clamps any retry-after: a provider asking for a longer
//   cooldown still returns to ACTIVE within 60 s.
// MaxKeySwitchesPerTurn (3) caps pre-first-byte failover; the effective ceiling is
//   min(healthy keys, 3), enforced against platformAttempts in service.ts.
// KeySwitchDelayMs (0): no backoff between keys, because a DIFFERENT key is not
//   re-hitting the throttled endpoint that cooled the previous one.
// WeightFloor (10) is added to each key's configured weight in weightedPick, so a
//   weight-0 key still receives a share of traffic.
const PoolReadCacheTtlMs = 30_000;
const DefaultCooldownMs = 5_000;
const CooldownCapMs = 60_000;
const MaxKeySwitchesPerTurn = 3;
const KeySwitchDelayMs = 0;
const WeightFloor = 10;

/** Frozen timing, attempt, and weighting values shared with pool orchestration. */
export const PlatformKeyPoolConstants = {
  poolReadCacheTtlMs: PoolReadCacheTtlMs,
  defaultCooldownMs: DefaultCooldownMs,
  cooldownCapMs: CooldownCapMs,
  maxKeySwitchesPerTurn: MaxKeySwitchesPerTurn,
  keySwitchDelayMs: KeySwitchDelayMs,
  weightFloor: WeightFloor,
} as const;

/**
 * Validates the active-row cache scopes, decrypts active rows, and omits
 * disabled rows from the runtime pool.
 */
export async function decryptPlatformProviderKeyRows(
  rows: readonly EncryptedPlatformProviderKeyRow[],
  masterKeyHex: string,
): Promise<readonly PlatformProviderKey[]> {
  validatePlatformProviderKeyRows(rows);
  const output: PlatformProviderKey[] = [];
  for (const row of rows) {
    if (row.status !== "active") {
      continue;
    }
    const plaintext = await decryptAES256GCM(row.encryptedKey, masterKeyHex);
    output.push({
      keyId: row.keyId,
      providerId: row.providerId,
      key: new TextDecoder().decode(plaintext),
      weight: row.weight,
      priority: row.priority,
      cacheScope: row.cacheScope,
    });
  }
  return output;
}

/** Ensures all active rows for each provider name exactly one cache scope. */
export function validatePlatformProviderKeyRows(rows: readonly EncryptedPlatformProviderKeyRow[]): void {
  const activeScopes = new Map<PlatformHostedProviderId, string>();
  for (const row of rows) {
    if (row.status !== "active") {
      continue;
    }
    const existing = activeScopes.get(row.providerId);
    if (existing !== undefined && existing !== row.cacheScope) {
      throw new Error("platform provider keys for a provider must share one cache_scope");
    }
    activeScopes.set(row.providerId, row.cacheScope);
  }
}

// Per-key state machine, held per-replica and in-memory (a disposable local
// observation, never state of record). recordFailure is the only writer; select
// and availableKeys are the readers.
//
//   | state       | meaning                     | writers       | readers          | legal transitions            |
//   | ----------- | --------------------------- | ------------- | ---------------- | ---------------------------- |
//   | ACTIVE      | selectable                  | (initial /    | availableKeys,   | ACTIVE -> COOLING,           |
//   |             |                             | cooling expiry)| select           | ACTIVE -> QUARANTINED        |
//   | COOLING     | rate-limited, temporarily   | recordFailure | availableKeys    | COOLING -> ACTIVE on TTL     |
//   |             | unavailable (coolingUntilMs)|               |                  | expiry (retry-after clamped  |
//   |             |                             |               |                  | to CooldownCapMs)            |
//   | QUARANTINED | auth-dead or quota/billing  | recordFailure | availableKeys    | terminal for this process    |
//   |             | exhausted (401/403, etc.)   |               |                  | (no in-process exit)         |
//
// The sole DURABLE removal is an operator UPDATE status='disabled' on the row,
// which this gateway never writes; it is picked up on the next 30 s pool read.
/**
 * Selects platform keys and tracks cooling or quarantine state for one gateway
 * process. Durable key status remains owned by credential storage.
 */
export class PlatformKeyPool {
  private readonly keysByProvider = new Map<PlatformHostedProviderId, readonly PlatformProviderKey[]>();
  private readonly coolingUntilMs = new Map<string, number>();
  private readonly quarantined = new Set<string>();
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly onQuarantine: ((event: PlatformKeyPoolQuarantineEvent) => void) | undefined;

  constructor(keys: readonly PlatformProviderKey[], options: PlatformKeyPoolOptions = {}) {
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    this.onQuarantine = options.onQuarantine;
    this.replaceKeys(keys);
  }

  /** Replaces the loaded key snapshot after validating provider cache scopes. */
  replaceKeys(keys: readonly PlatformProviderKey[]): void {
    validatePlatformProviderKeys(keys);
    for (const providerId of ["anthropic", "openai", "deepseek"] as const) {
      this.keysByProvider.set(providerId, keys.filter((key) => key.providerId === providerId));
    }
  }

  /** Selects a weighted key from the first available priority tier. */
  select(providerId: PlatformHostedProviderId, options: PlatformKeySelectionOptions = {}): PlatformKeySelection {
    const now = this.now();
    const available = this.availableKeys(providerId, now, options.excludeKeyIds);
    if (available.length === 0) {
      return {
        ok: false,
        error: {
          code: "platform_keys_exhausted",
          message: "Platform provider keys are temporarily exhausted.",
          retryable: true,
          fatal: false,
          statusCode: 503,
          retryAfterMs: this.shortestCooldownMs(providerId, now),
        },
      };
    }
    return { ok: true, key: this.weightedPick(available) };
  }

  /** Applies a classified cooling or quarantine transition to one key. */
  recordFailure(keyId: string, classification: ProviderFailureClassification): void {
    if (classification.action === "cooling") {
      this.coolingUntilMs.set(keyId, this.now() + clampCooldown(classification.cooldownMs));
    }
    if (classification.action === "quarantine") {
      this.quarantined.add(keyId);
      const key = this.findKey(keyId);
      if (key !== undefined) {
        this.onQuarantine?.({ keyId, providerId: key.providerId, providerError: classification.providerError });
      }
    }
  }

  /** Reports whether this process has quarantined the key. */
  isQuarantined(keyId: string): boolean {
    return this.quarantined.has(keyId);
  }

  private availableKeys(providerId: PlatformHostedProviderId, now: number, excludeKeyIds: ReadonlySet<string> | undefined): readonly PlatformProviderKey[] {
    const keys = this.keysByProvider.get(providerId) ?? [];
    const healthy = keys.filter((key) =>
      !this.quarantined.has(key.keyId) &&
      !excludeKeyIds?.has(key.keyId) &&
      (this.coolingUntilMs.get(key.keyId) ?? 0) <= now
    );
    const highestPriority = Math.min(...healthy.map((key) => key.priority));
    return Number.isFinite(highestPriority) ? healthy.filter((key) => key.priority === highestPriority) : [];
  }

  private shortestCooldownMs(providerId: PlatformHostedProviderId, now: number): number {
    const keys = this.keysByProvider.get(providerId) ?? [];
    const remaining = keys
      .filter((key) => !this.quarantined.has(key.keyId))
      .map((key) => Math.max(0, (this.coolingUntilMs.get(key.keyId) ?? now) - now))
      .filter((value) => value > 0);
    return remaining.length === 0 ? DefaultCooldownMs : Math.min(...remaining);
  }

  private weightedPick(keys: readonly PlatformProviderKey[]): PlatformProviderKey {
    const weights = keys.map((key) => key.weight + WeightFloor);
    const total = weights.reduce((sum, weight) => sum + weight, 0);
    let cursor = this.random() * total;
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      const weight = weights[index];
      if (key === undefined || weight === undefined) {
        break;
      }
      if (cursor < weight) {
        return key;
      }
      cursor -= weight;
    }
    const fallback = keys.at(-1);
    if (fallback === undefined) {
      throw new Error("platform key selection invariant failed");
    }
    return fallback;
  }

  private findKey(keyId: string): PlatformProviderKey | undefined {
    for (const keys of this.keysByProvider.values()) {
      const key = keys.find((candidate) => candidate.keyId === keyId);
      if (key !== undefined) {
        return key;
      }
    }
    return undefined;
  }
}

/**
 * Classifies a provider attempt into a redacted Runtime-facing error and a
 * candidate platform-key action. Orchestration applies that action only to a
 * platform-owned attempt that has emitted no downstream event; session-owned
 * and post-event failures use the normalized error without changing key state.
 */
export function classifyProviderFailure(
  providerId: GatewayCatalogProviderId,
  input: { readonly statusCode?: number | undefined; readonly body?: unknown; readonly headers?: Readonly<Record<string, string | undefined>> | undefined; readonly networkError?: boolean | undefined; readonly timeout?: boolean | undefined },
): ProviderFailureClassification {
  const code = providerBodyCode(input.body);
  const bodyText = providerBodyText(input.body);
  if (isContextOverflow(input.statusCode, code, bodyText)) {
    return classification("fail-fast", "context_overflow", false, "Provider context window exceeded.", input.statusCode);
  }
  if (isOpenAICompatibleFamily(providerId) && input.statusCode === 404) {
    const openAIError = classifyOpenAIProviderError({ statusCode: input.statusCode, providerCode: code });
    if (openAIError !== undefined) {
      return providerFailureFromOpenAIError(code, openAIError);
    }
  }
  if (providerId === "openai") {
    const openAIError = classifyOpenAIProviderError({ statusCode: input.statusCode, providerCode: code });
    if (openAIError !== undefined) {
      return providerFailureFromOpenAIError(code, openAIError);
    }
  }
  if (input.timeout === true || input.networkError === true || (input.statusCode !== undefined && input.statusCode >= 500)) {
    return classification("retryable", "provider_stream_error", true, statusMessage(input.statusCode), input.statusCode);
  }
  if (input.statusCode === 400 || input.statusCode === 422) {
    return classification("fail-fast", "provider_request_invalid", false, "Provider rejected the request shape.", input.statusCode);
  }
  switch (providerId) {
    case "anthropic":
      if (isFallbackRateLimit(input.statusCode, bodyText)) {
        return cooling(rateLimitMessage(), retryAfterMs(input.headers?.["retry-after"]) ?? retryDelayFromTextMs(bodyText));
      }
      if (input.statusCode === 429 && code === "rate_limit_error") {
        return cooling(rateLimitMessage(), retryAfterMs(input.headers?.["retry-after"]));
      }
      if (input.statusCode === 401 || input.statusCode === 403 || (input.statusCode === 402 && code === "billing_error")) {
        return quarantine("provider_key_unavailable", "Provider key is not usable.", input.statusCode);
      }
      break;
    case "openai":
      if (input.statusCode === 429 && code === "rate_limit_exceeded") {
        return cooling(rateLimitMessage(), resetHeaderMs(input.headers));
      }
      if (input.statusCode === 401 && code === "invalid_api_key") {
        return quarantine("provider_key_unavailable", "Provider key is not usable.", input.statusCode);
      }
      break;
    case "deepseek":
      if (input.statusCode === 429) {
        return cooling(rateLimitMessage(), DefaultCooldownMs);
      }
      if (input.statusCode === 401 || input.statusCode === 402 || bodyText.includes("Insufficient Balance")) {
        return quarantine("provider_key_unavailable", "Provider key is not usable.", input.statusCode);
      }
      break;
    case "moonshotai":
      if (input.statusCode === 429 && (code === "rate_limit_reached_error" || code === "engine_overloaded_error")) {
        return cooling(rateLimitMessage(), DefaultCooldownMs);
      }
      if (input.statusCode === 401 && (code === "invalid_authentication_error" || code === "incorrect_api_key_error")) {
        return quarantine("provider_key_unavailable", "Provider key is not usable.", input.statusCode);
      }
      if (
        (input.statusCode === 429 && code === "exceeded_current_quota_error") ||
        (input.statusCode === 403 && bodyText.toLowerCase().includes("balance"))
      ) {
        return quarantine("provider_quota_exhausted", "Provider quota is exhausted.", input.statusCode);
      }
      break;
    case "zai":
      if (input.statusCode === 429 && ["1302", "1305", "1308", "1310", "1313"].includes(code)) {
        return cooling(rateLimitMessage(), retryDelayFromTextMs(bodyText));
      }
      if (input.statusCode === 401 && ["1000", "1001", "1003"].includes(code)) {
        return quarantine("provider_key_unavailable", "Provider key is not usable.", input.statusCode);
      }
      if (input.statusCode === 429 && (code === "1113" || ["1316", "1317", "1318", "1319", "1320", "1321"].includes(code))) {
        return quarantine("provider_quota_exhausted", "Provider quota is exhausted.", input.statusCode);
      }
      break;
  }
  if (isFallbackRateLimit(input.statusCode, bodyText)) {
    return cooling(rateLimitMessage(), retryAfterMs(input.headers?.["retry-after"]) ?? resetHeaderMs(input.headers) ?? retryDelayFromTextMs(bodyText));
  }
  return classification("fail-fast", "provider_request_failed", false, "Provider request failed.", input.statusCode);
}

function isOpenAICompatibleFamily(providerId: GatewayCatalogProviderId): boolean {
  return providerId === "openai" || providerId === "deepseek" || providerId === "zai";
}

function validatePlatformProviderKeys(keys: readonly PlatformProviderKey[]): void {
  const rows = keys.map((key) => ({
    keyId: key.keyId,
    providerId: key.providerId,
    encryptedKey: new Uint8Array([1]),
    weight: key.weight,
    priority: key.priority,
    cacheScope: key.cacheScope,
    status: "active" as const,
    updatedAt: "",
  }));
  validatePlatformProviderKeyRows(rows);
}

function cooling(message: string, cooldownMs: number | undefined): ProviderFailureClassification {
  const clamped = clampCooldown(cooldownMs);
  return {
    action: "cooling",
    cooldownMs: clamped,
    providerError: normalizeProviderError({
      code: "provider_rate_limited",
      message,
      retryable: true,
      fatal: false,
      statusCode: 429,
      retryAfterMs: clamped,
    }),
  };
}

function quarantine(code: string, message: string, statusCode: number | undefined): ProviderFailureClassification {
  return classification("quarantine", code, false, message, statusCode);
}

function providerFailureFromOpenAIError(providerCode: string, providerError: ProviderErrorInput): ProviderFailureClassification {
  const action: ProviderFailureAction = providerError.retryable === true
    ? "retryable"
    : providerCode === "usage_not_included" || providerCode === "insufficient_quota"
      ? "quarantine"
      : "fail-fast";
  return {
    action,
    providerError: normalizeProviderError(providerError),
  };
}

function classification(action: ProviderFailureAction, code: string, retryable: boolean, message: string, statusCode: number | undefined): ProviderFailureClassification {
  return {
    action,
    providerError: normalizeProviderError({
      code,
      message,
      retryable,
      fatal: !retryable,
      statusCode,
    }),
  };
}

function statusMessage(statusCode: number | undefined): string {
  return statusCode !== undefined && statusCode >= 500 ? "Provider returned a retryable server error." : "Provider request failed.";
}

function isContextOverflow(statusCode: number | undefined, code: string, bodyText: string): boolean {
  if (statusCode === 413 || code === "context_length_exceeded") {
    return true;
  }
  return /context[_ -]?(?:length|window).*(?:exceed|overflow|too large)|(?:maximum|max).*context|prompt is too long|input is too long|too many tokens/i.test(bodyText);
}

function isFallbackRateLimit(statusCode: number | undefined, bodyText: string): boolean {
  if (statusCode === 429) {
    return true;
  }
  return /rate[\s_-]*limit|too many requests|requests per (?:minute|second|day)|try again later|retry after/i.test(bodyText);
}

function rateLimitMessage(): string {
  return "Provider rate limit reached.";
}

function clampCooldown(value: number | undefined): number {
  if (value === undefined || !Number.isFinite(value) || value <= 0) {
    return DefaultCooldownMs;
  }
  return Math.min(Math.trunc(value), CooldownCapMs);
}

function retryAfterMs(value: string | undefined): number | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  const seconds = Number.parseFloat(value);
  return Number.isFinite(seconds) ? seconds * 1000 : undefined;
}

function resetHeaderMs(headers: Readonly<Record<string, string | undefined>> | undefined): number | undefined {
  const value = headers?.["x-ratelimit-reset-requests"] ?? headers?.["x-ratelimit-reset-tokens"];
  if (value === undefined) {
    return undefined;
  }

  if (/^[0-9]+$/.test(value)) {
    const milliseconds = Number(value);
    return Number.isFinite(milliseconds) ? milliseconds : undefined;
  }

  if (!/^(?:[0-9]+(?:\.[0-9]+)?(?:ms|h|m|s))+$/.test(value)) {
    return undefined;
  }

  const unitMilliseconds = { h: 3_600_000, m: 60_000, s: 1_000, ms: 1 } as const;
  let milliseconds = 0;
  for (const part of value.matchAll(/([0-9]+(?:\.[0-9]+)?)(ms|h|m|s)/g)) {
    const amount = Number(part[1]);
    const unit = part[2] as keyof typeof unitMilliseconds;
    milliseconds += amount * unitMilliseconds[unit];
  }
  return Number.isFinite(milliseconds) ? milliseconds : undefined;
}

function retryDelayFromTextMs(value: string): number | undefined {
  const seconds = /([0-9]+)\s*(?:s|sec|second|seconds)/i.exec(value)?.[1];
  return seconds !== undefined ? Number.parseInt(seconds, 10) * 1000 : undefined;
}

function providerBodyCode(body: unknown): string {
  const object = bodyObject(body);
  const value = object.code ?? nestedObject(object.error)?.code ?? nestedObject(object.error)?.type ?? object.type;
  return value === undefined ? "" : String(value);
}

function providerBodyText(body: unknown): string {
  if (typeof body === "string") {
    return body;
  }
  try {
    return JSON.stringify(body);
  } catch {
    return "";
  }
}

function bodyObject(body: unknown): Record<string, unknown> {
  if (typeof body === "string") {
    try {
      const parsed = JSON.parse(body) as unknown;
      return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed) ? parsed as Record<string, unknown> : {};
    } catch {
      return {};
    }
  }
  return typeof body === "object" && body !== null && !Array.isArray(body) ? body as Record<string, unknown> : {};
}

function nestedObject(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}
