import { readFile, realpath } from "node:fs/promises";
import { createHash, X509Certificate } from "node:crypto";
import { isIP } from "node:net";
import type { DatabasePoolConfig } from "./index.js";

/** Work stays inside the callback, including execution of lazy SQL queries and transaction commit. */
export interface SQLSource<S> {
  withSQL<T>(operation: (sql: S) => Promise<T>): Promise<T>;
}
export function fixedSQLSource<S>(sql: S): SQLSource<S> {
  return { withSQL: async (operation) => await operation(sql) };
}
export function asSQLSource<S>(input: S | SQLSource<S>): SQLSource<S> {
  return typeof input === "object" && input !== null && "withSQL" in input
    ? input as SQLSource<S> : fixedSQLSource(input as S);
}
export interface PostgresSQLOwner extends SQLSource<Bun.SQL> {
  close(options?: { readonly deadline?: Date }): Promise<void>;
}
/**
 * Bounded reason carried by reload_failed: the mounted bundle was unusable, or
 * a complete candidate pool built from it failed verification. A failure to
 * close a replaced generation carries no reason.
 */
export type SQLOwnerReloadFailure = "invalid_bundle" | "candidate_verification_failed";
export interface SQLOwnerObservation {
  readonly kind: "opened" | "closed" | "activated" | "reload_failed" | "reload_recovered";
  readonly pools: number;
  readonly failedCount?: number;
  readonly reason?: SQLOwnerReloadFailure;
}
export interface PostgresSQLOwnerOptions {
  readonly url: string;
  readonly pool: DatabasePoolConfig;
  readonly tls?: { readonly caPath: string; readonly serverName: string };
  readonly drainTimeoutSeconds?: number;
  readonly verify: (sql: Bun.SQL) => Promise<void>;
  readonly sqlFactory?: (options: Bun.SQL.PostgresOrMySQLOptions) => Bun.SQL;
  /** Bounded lifecycle observations carry no connection strings or material. */
  readonly observe?: (event: SQLOwnerObservation) => void;
}
interface Trust {
  /** Currently valid anchors only; this is the CA material given to Bun. */
  readonly pem: string;
  /** Identity of the whole mounted file, so any file change is observed. */
  readonly fingerprint: string;
  /** Latest expiry among retained anchors. */
  readonly expires: number;
  /** Retained anchors by DER SHA-256, with their expiry. */
  readonly anchors: ReadonlyMap<string, number>;
}
interface Generation { readonly sql: Bun.SQL; readonly trust?: Trust; users: number; idle: (() => void)[] }

const candidateRetryInitialMs = 250;
const candidateRetryMaxMs = 5_000;

async function readTrust(path: string): Promise<Trust> {
  const resolved = await realpath(path);
  const pem = await readFile(resolved, "utf8");
  if (await realpath(path) !== resolved) throw new Error("database trust generation changed while reading");
  const blocks = pem.match(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g);
  if (blocks === null || pem.replace(/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g, "").trim() !== "") throw new Error("database trust bundle is malformed");
  const now = Date.now();
  const retained: string[] = [];
  const anchors = new Map<string, number>();
  let expires = 0;
  for (const block of blocks) {
    const cert = new X509Certificate(block);
    const from = Date.parse(cert.validFrom), until = Date.parse(cert.validTo);
    if (!cert.ca) throw new Error("database trust bundle contains an invalid CA");
    // A not-yet-valid anchor keeps the update invalid, so the last-known-good
    // generation stays active and a later poll activates the bundle once the
    // anchor is valid. An expired anchor cannot validate any chain; leaving it
    // out keeps an old CA that expires during a planned overlap from disabling
    // the generation that still holds the current CA.
    if (now < from) throw new Error("database trust bundle contains a CA that is not yet valid");
    if (now >= until) continue;
    retained.push(block);
    const der = Buffer.from(block.replace(/-----(BEGIN|END) CERTIFICATE-----|\s/g, ""), "base64");
    anchors.set(createHash("sha256").update(der).digest("hex"), until);
    expires = Math.max(expires, until);
  }
  if (retained.length === 0) throw new Error("database trust bundle has no currently valid CA");
  return { pem: retained.join("\n"), expires, anchors, fingerprint: createHash("sha256").update(pem).digest("hex") };
}

/** True when the update removes an anchor of the active trust that is still valid. */
function narrows(active: Trust, next: Trust): boolean {
  const now = Date.now();
  for (const [anchor, until] of active.anchors) if (until > now && !next.anchors.has(anchor)) return true;
  return false;
}

/**
 * Process-owned Bun PostgreSQL pool generation owner. Owns at most two pools,
 * including candidate verification and old-pool drain; README.md states the
 * trust replacement, expiry and diagnostic contract.
 */
export async function openPostgresSQLOwner(options: PostgresSQLOwnerOptions): Promise<PostgresSQLOwner> {
  const drain = options.drainTimeoutSeconds ?? 20;
  if (!Number.isFinite(drain) || drain <= 0) throw new Error("database drain bound is invalid");
  if (options.tls !== undefined && (!options.tls.caPath || !options.tls.serverName || isIP(options.tls.serverName) !== 0)) throw new Error("database TLS trust and fixed DNS server name are required together");
  let stopped = false, pools = 0, closingDeadline: number | undefined;
  let rotation: Promise<void> | undefined, observation: Promise<void> | undefined;
  let pending = false;
  const generations = new Set<Generation>();
  const poolCloses = new Map<Generation, Promise<void>>();
  let stopDrain!: () => void;
  const drainingStopped = new Promise<void>((resolve) => { stopDrain = resolve; });
  let reloadFailures = 0, lastFailureSummary = 0, degraded = false;
  // A failing candidate is retried with exponential spacing; any new mounted
  // fingerprint or an activation resets the spacing.
  let failedFingerprint: string | undefined, failedAttempts = 0, retryAt = 0;
  const event = (kind: SQLOwnerObservation["kind"], failedCount?: number, reason?: SQLOwnerReloadFailure) => {
    // Diagnostics cannot alter SQL admission, activation or joined cleanup.
    try { options.observe?.({ kind, pools, ...(failedCount === undefined ? {} : { failedCount }), ...(reason === undefined ? {} : { reason }) }); } catch { /* observer owns its sink */ }
  };
  const reloadFailed = (reason?: SQLOwnerReloadFailure) => {
    reloadFailures++;
    const now = Date.now();
    if (!degraded || now - lastFailureSummary >= 30_000) {
      degraded = true; lastFailureSummary = now;
      event("reload_failed", reloadFailures, reason);
    }
  };
  const reloadRecovered = () => {
    if (degraded) event("reload_recovered", reloadFailures);
    degraded = false; reloadFailures = 0;
  };
  const remaining = (seconds: number) => Math.max(0, Math.min(seconds, closingDeadline === undefined ? seconds : (closingDeadline - Date.now()) / 1000));
  const closePool = (generation: Generation, seconds: number): Promise<void> => {
    const previous = poolCloses.get(generation);
    if (previous !== undefined) return previous;
    if (!generations.has(generation)) return Promise.resolve();
    const closing = Promise.resolve().then(async () => {
      try { await generation.sql.close({ timeout: remaining(seconds) }); }
      finally { generations.delete(generation); pools--; event("closed"); poolCloses.delete(generation); }
    });
    poolCloses.set(generation, closing);
    return closing;
  };
  const create = async (trust?: Trust): Promise<Generation> => {
    if (pools >= 2) throw new Error("database pool generation ceiling exceeded");
    const url = new URL(options.url);
    if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") throw new Error("PostgreSQL URL is required");
    if (trust !== undefined) {
      if (!url.hostname || decodeURIComponent(url.hostname).includes("/") || url.searchParams.has("host") || url.searchParams.has("hostaddr")) throw new Error("protected PostgreSQL requires one explicit TCP host");
      // URL mode cannot override the explicit verified TLS construction.
      for (const key of ["ssl", "sslmode", "sslrootcert", "sslcert", "sslkey", "sslpassword"]) url.searchParams.delete(key);
      url.searchParams.set("sslmode", "verify-full");
    }
    const config: Bun.SQL.PostgresOrMySQLOptions = {
      url: url.toString(), max: options.pool.max, idleTimeout: options.pool.idleTimeout,
      maxLifetime: options.pool.maxLifetime, connectionTimeout: options.pool.connectionTimeout,
      connection: { statement_timeout: options.pool.statementTimeoutMs },
      ...(trust === undefined ? {} : { tls: { ca: trust.pem, serverName: options.tls!.serverName, rejectUnauthorized: true } }),
    };
    const sql = options.sqlFactory?.(config) ?? new Bun.SQL(config);
    const generation: Generation = { sql, ...(trust === undefined ? {} : { trust }), users: 0, idle: [] };
    generations.add(generation); pools++; event("opened");
    try { await options.verify(sql); return generation; }
    catch { await closePool(generation, 1); throw new Error("database pool generation verification failed"); }
  };
  // Undefined only while a narrowed trust update has retired admission and no
  // candidate built from the mounted bundle has verified yet.
  let active: Generation | undefined = await create(options.tls === undefined ? undefined : await readTrust(options.tls.caPath));
  const drainOld = async (old: Generation) => {
    const deadline = Date.now() + drain * 1000;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let release: (() => void) | undefined;
    try {
      if (old.users !== 0) await Promise.race([
        new Promise<void>((resolve) => { release = resolve; old.idle.push(resolve); }),
        new Promise<void>((resolve) => { timer = setTimeout(resolve, remaining(drain) * 1000); }),
        drainingStopped,
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      if (release !== undefined) old.idle = old.idle.filter((entry) => entry !== release);
    }
    await closePool(old, stopped ? 5 : Math.max(0, (deadline - Date.now()) / 1000));
  };
  // Every update is validated as a complete candidate pool before it receives
  // new work. An update that only adds anchors, or keeps them, leaves the
  // active generation admitting until its candidate verifies. An update that
  // removes a still-valid anchor is authoritative trust removal: the active
  // generation stops admitting at once and drains, and admission resumes only
  // through a verified candidate built from the narrowed bundle. If the server
  // does not yet present a chain to the remaining trust, the store stays
  // unavailable until it does, rather than keeping the removed CA in use.
  const observe = async () => {
    if (stopped || options.tls === undefined) return;
    let trust: Trust;
    try { trust = await readTrust(options.tls.caPath); }
    catch { if (!stopped) reloadFailed("invalid_bundle"); return; }
    if (stopped) return;
    if (active !== undefined && active.trust?.fingerprint === trust.fingerprint) {
      failedFingerprint = undefined; failedAttempts = 0;
      reloadRecovered(); return;
    }
    if (trust.fingerprint === failedFingerprint && Date.now() < retryAt) return;
    if (rotation !== undefined) { pending = true; return; }
    rotation = (async () => {
      const previous = active;
      // The rotation joins this drain, so a retry candidate never adds a third
      // pool. The drain runs alongside the candidate and can settle first, so
      // its failure handler is attached when it starts.
      let retiring: Promise<void> | undefined;
      if (previous?.trust !== undefined && narrows(previous.trust, trust)) {
        active = undefined;
        retiring = drainOld(previous).catch(() => { if (!stopped) reloadFailed(); });
      }
      try {
        let candidate: Generation;
        try { candidate = await create(trust); }
        catch {
          if (!stopped) {
            failedAttempts = trust.fingerprint === failedFingerprint ? failedAttempts + 1 : 1;
            failedFingerprint = trust.fingerprint;
            retryAt = Date.now() + Math.min(candidateRetryMaxMs, candidateRetryInitialMs * 2 ** (failedAttempts - 1));
            reloadFailed("candidate_verification_failed");
          }
          return;
        }
        if (stopped) { await closePool(candidate, 5); return; }
        active = candidate; event("activated"); reloadRecovered();
        failedFingerprint = undefined; failedAttempts = 0;
        if (previous !== undefined && retiring === undefined) await drainOld(previous);
      } catch { if (!stopped) reloadFailed(); }
      finally {
        if (retiring !== undefined) await retiring;
      }
    })().finally(() => {
      rotation = undefined;
      // Re-read the mounted current generation; never retain queued credentials.
      if (pending && !stopped) { pending = false; void poll(); }
    });
  };
  const poll = async () => {
    if (observation !== undefined || stopped) return;
    observation = observe().finally(() => { observation = undefined; });
    await observation;
  };
  const timer = options.tls === undefined ? undefined : setInterval(() => { void poll(); }, 250);
  let closePromise: Promise<void> | undefined;
  return {
    withSQL: async (operation) => {
      if (stopped) throw new Error("database owner is closed");
      const generation = active;
      if (generation === undefined) throw new Error("database trust generation is retired pending a verified replacement");
      if (generation.trust !== undefined && Date.now() >= generation.trust.expires) throw new Error("database trust generation is expired");
      generation.users++;
      try { return await operation(generation.sql); }
      finally { generation.users--; if (generation.users === 0) for (const resolve of generation.idle.splice(0)) resolve(); }
    },
    close: (input) => {
      if (closePromise !== undefined) return closePromise;
      stopped = true; pending = false;
      stopDrain();
      closingDeadline = input?.deadline?.getTime() ?? Date.now() + 5000;
      if (timer !== undefined) clearInterval(timer);
      closePromise = (async () => {
        // Closing a validating candidate interrupts its connection/query; do
        // not wait for verification before asking that pool to stop.
        const results = await Promise.allSettled([...generations].map((generation) => closePool(generation, 5)));
        results.push(...await Promise.allSettled([observation, rotation]));
        results.push(...await Promise.allSettled([...generations].map((generation) => closePool(generation, 5))));
        const firstFailure = results.find((result): result is PromiseRejectedResult => result.status === "rejected");
        if (firstFailure !== undefined) throw firstFailure.reason;
      })();
      return closePromise;
    },
  };
}
