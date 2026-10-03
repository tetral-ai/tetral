/**
 * Common MCP SDK owner. Installed configuration binds one approved adapter and
 * Vault credential to each operation. A shared pending initializer connects and
 * completes all discovery pages before publishing SDK metadata/cache readiness.
 * Waiters keep finite deadlines/cancellation and inherit pending refresh costs;
 * execution shares one operation allowance with readiness. The SDK owns output
 * validation; transport classes alone establish authentication provenance.
 * Raw SDK/credential operations and close-once transports remain owned through
 * eviction and shutdown until their actual callbacks join.
 * @packageDocumentation
 */

import { DiscoverySDKClient, McpDiscoveryError, MCP_DISCOVERY_TIMEOUT_MS } from "./discovery.js";
import type { DiscoveryFailureReason } from "./discovery.js";
import { StreamableHTTPClientTransport, StreamableHTTPError } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { McpExecutionObservation } from "./logger.js";
import { mcpPhaseCompletedLogRecord } from "./logger.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import type { McpServerResolver, ResolvedMcpServer } from "./server-resolver.js";
import { validateAdapter } from "./adapters/registry.js";
import { UnauthorizedError } from "@modelcontextprotocol/sdk/client/auth.js";
import { McpConnectorError } from "./errors.js";
import type { McpCallToolResult } from "./formatter.js";
import type { McpCredentialResolution, McpCredentialResolver } from "./credential.js";
import {
  MCP_EXECUTION_TIMEOUT_MS, MCP_CALL_TIMEOUT_MS, MCP_SESSION_IDLE_TIMEOUT_MS,
  MCP_CONNECT_TIMEOUT_MS,
  MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
} from "./phase-budgets.js";
import type { McpClient, McpClientTool, McpConnectorLogger, McpLogRecord } from "./service.js";
import type { CallToolResult, CompatibilityCallToolResult, ListToolsResult } from "@modelcontextprotocol/sdk/types.js";

/** Defines the per-request timeout applied to MCP tool and discovery calls. */
export const MCP_CALL_TIMEOUT_SECONDS = MCP_CALL_TIMEOUT_MS / 1000;
/** Defines how long a cached connection remains unused before it closes. */
export const MCP_SESSION_IDLE_SECONDS = MCP_SESSION_IDLE_TIMEOUT_MS / 1000;
/** Lists the delays represented by the SDK transport's exponential reconnect settings. */
export const MCP_RECONNECT_DELAYS_MS = [1000, 4000, 16000] as const;
/** Defines the maximum number of automatic reconnect attempts for a dropped stream. */
export const MCP_RECONNECT_MAX_RETRIES = 3;
/** Bounds the number of `tools/list` pages one discovery follows. */
export { MCP_DISCOVERY_MAX_PAGES, MCP_DISCOVERY_MAX_TOOLS, MCP_DISCOVERY_MAX_BYTES } from "./discovery.js";
export { MCP_CONNECT_TIMEOUT_MS, MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS } from "./phase-budgets.js";

type McpIdentity = {
  readonly workspaceId: string;
  readonly sessionId: string;
  readonly mcpServerName: string;
};

type ToolsListChangedFailure = "refresh_failed" | "notify_failed";

/**
 * Describes the MCP SDK client operations used by the connection manager and
 * supplied by production clients or test doubles. The error callback carries
 * transport failures; the manager recognizes reconnect exhaustion there and
 * settles the affected tracked operations.
 */
export interface SDKClientLike {
	onerror?: ((error: Error) => void) | undefined;
  connect(transport: unknown, options?: { readonly timeout?: number; readonly signal?: AbortSignal }): Promise<void>;
  listTools(params?: unknown, options?: { readonly timeout?: number; readonly signal?: AbortSignal }): Promise<ListToolsResult>;
  callTool(
    params: { readonly name: string; readonly arguments?: Record<string, unknown> | undefined },
    resultSchema?: unknown,
    options?: { readonly timeout?: number; readonly signal?: AbortSignal },
  ): Promise<CallToolResult | CompatibilityCallToolResult>;
  close(): Promise<void>;
}

/**
 * Supplies the credential, manifest-change, logging, metrics, factory, timeout,
 * and timer collaborators used by {@link McpSDKClient}. The factory and timer
 * overrides expose the same lifecycle boundaries to focused tests.
 */
export interface McpSDKClientOptions {
  readonly credentialResolver: McpCredentialResolver;
  readonly serverResolver: McpServerResolver;
  readonly onConnectionReady?: ((identity: McpIdentity, tools: readonly McpClientTool[], options: { readonly signal?: AbortSignal; readonly timeoutMs?: number }) => Promise<void>) | undefined;
  readonly onToolsListChanged: (input: McpIdentity) => Promise<void>;
  readonly logger?: Pick<McpConnectorLogger, "error"> & Partial<Pick<McpConnectorLogger, "info">> | undefined;
  readonly createClient?: ((identity: McpIdentity) => SDKClientLike) | undefined;
  readonly createTransport?: ((input: { readonly url: URL; readonly token?: string | undefined; readonly requestHeaders?: Readonly<Record<string, string>> | undefined }) => unknown) | undefined;
  readonly monotonicNow?: (() => number) | undefined;
  readonly executionTimeoutMs?: number | undefined;
  readonly idleTimeoutMs?: number | undefined;
  readonly callTimeoutMs?: number | undefined;
  readonly credentialTimeoutMs?: number | undefined;
  readonly connectTimeoutMs?: number | undefined;
  readonly discoveryMaxPages?: number | undefined;
  readonly discoveryMaxTools?: number | undefined;
  readonly discoveryMaxBytes?: number | undefined;
  readonly discoveryTimeoutMs?: number | undefined;
  readonly setTimer?: ((callback: () => void, ms: number) => ReturnType<typeof setTimeout>) | undefined;
  readonly clearTimer?: ((timer: ReturnType<typeof setTimeout>) => void) | undefined;
}

type PhaseIdentity = McpIdentity & { readonly observers?: () => readonly (() => McpExecutionObservation)[] };
interface RefreshBudget { operationSpent: boolean; refreshTriggered: boolean; observer?: () => McpExecutionObservation; }
interface OpeningWaiter { deadline: number; budget: RefreshBudget; reject: (error: unknown) => void; }
interface ConnectionOpening { baseKey: string; key: string; endpoint: string; controller: AbortController; waiters: Set<OpeningWaiter>; handshakeSpent: boolean; operationSpent: boolean; promise: Promise<{entry: ConnectionEntry; tools: ListToolsResult}>; }
interface ConnectionEntry {
  readonly endpoint: string;
  readonly baseKey: string;
  readonly key: string;
  readonly tokenHash: string;
  readonly vaultId: string;
  readonly credentialId: string;
  readonly client: SDKClientLike;
	readonly inFlight: Set<InFlightCall>;
  idleTimer?: ReturnType<typeof setTimeout> | undefined;
	closing?: Promise<void> | undefined;
	closed: boolean;
}

interface InFlightCall {
	settled: boolean;
	reject: (error: McpConnectorError) => void;
}

type CredentialMaterial = Extract<McpCredentialResolution, { readonly ok: true; readonly mode: "bearer" }>;

/**
 * Pending → ready requires connect plus the complete SDK listing; failure/all
 * owners gone closes without cache publication. Ready → closed removes all
 * cache/index/timer ownership on expiry, replacement, exhaustion or shutdown.
 * Full listing definitions are operation-local; only SDK metadata survives.
 */
export class McpSDKClient implements McpClient {
  readonly #connections = new Map<string, ConnectionEntry>();
	readonly #clientEntries = new Map<SDKClientLike, ConnectionEntry>();
  readonly #lifetime = new AbortController();
  readonly #closing = new Set<Promise<void>>();
  readonly #rawOperations = new Set<Promise<unknown>>();
  readonly #openingClients = new Set<SDKClientLike>();
  readonly #clientClosures = new WeakMap<SDKClientLike, Promise<void>>();
  #closed: Promise<void> | undefined;
  readonly #connectionOpenings = new Map<string, ConnectionOpening>();
  readonly #idleTimeoutMs: number;
  readonly #callTimeoutMs: number;
  readonly #credentialTimeoutMs: number;
  readonly #connectTimeoutMs: number;
  readonly #setTimer: (callback: () => void, ms: number) => ReturnType<typeof setTimeout>;
  readonly #clearTimer: (timer: ReturnType<typeof setTimeout>) => void;

  /** Creates a client with production timeout and timer defaults unless overridden. */
  constructor(private readonly options: McpSDKClientOptions) {
    this.#idleTimeoutMs = options.idleTimeoutMs ?? MCP_SESSION_IDLE_SECONDS * 1000;
    this.#callTimeoutMs = options.callTimeoutMs ?? MCP_CALL_TIMEOUT_SECONDS * 1000;
    this.#credentialTimeoutMs = options.credentialTimeoutMs ?? MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS;
    this.#connectTimeoutMs = options.connectTimeoutMs ?? MCP_CONNECT_TIMEOUT_MS;
    this.#setTimer = options.setTimer ?? setTimeout;
    this.#clearTimer = options.clearTimer ?? clearTimeout;
  }

  /**
   * Lists tools through the current credential-bound connection and re-arms its
   * idle timer after success.
   *
   * Discovery follows opaque `nextCursor` values within this one listing until
   * absent, composing a single complete manifest. A repeated cursor, the page
   * bound, or the accumulated-tool/byte bound fails the listing: the
   * partial pages are never returned as a successful manifest. Each listing
   * starts cursorless on its own connection, so a re-list on a new MCP session
   * restarts pagination rather than reusing a cursor from the old session.
   */
  async listTools(input: McpIdentity, options?: { readonly signal?: AbortSignal; readonly timeoutMs?: number }): Promise<readonly McpClientTool[]> {
    return this.perform(input, options, true, async (entry, signal, timeout) =>
      this.runConnectionOperation(entry, () => entry.client.listTools(undefined, {signal, timeout}), signal)) as Promise<readonly McpClientTool[]>;
  }

  async callTool(input: McpIdentity & { readonly sessionThreadId: string; readonly toolName: string; readonly input: Record<string, unknown> }, options?: { readonly signal?: AbortSignal; readonly timeoutMs?: number; readonly executionObservation?: () => McpExecutionObservation }): Promise<McpCallToolResult> {
    return this.perform(input, options, false, async (entry, signal, timeout) => {
      const result = await this.runConnectionOperation(entry, () => entry.client.callTool({name: input.toolName, arguments: input.input}, undefined, {signal, timeout}), signal, true);
      return "toolResult" in result ? {structuredContent: result.toolResult} : result;
    }) as Promise<McpCallToolResult>;
  }

  private async perform(input: PhaseIdentity, options: { readonly signal?: AbortSignal; readonly timeoutMs?: number; readonly executionObservation?: () => McpExecutionObservation } | undefined, listing: boolean, operation: (entry: ConnectionEntry, signal: AbortSignal, timeout: number) => Promise<unknown>): Promise<unknown> {
    const ceiling = listing ? this.options.discoveryTimeoutMs ?? MCP_DISCOVERY_TIMEOUT_MS : this.options.executionTimeoutMs ?? MCP_EXECUTION_TIMEOUT_MS;
    const total = Math.min(options?.timeoutMs ?? ceiling, ceiling);
    const deadline = this.now() + total;
    const parent = options?.signal === undefined ? this.#lifetime.signal : AbortSignal.any([options.signal, this.#lifetime.signal]);
    const budget: RefreshBudget = {operationSpent: false, refreshTriggered: false, ...(options?.executionObservation===undefined?{}:{observer:options.executionObservation})};
    input = {...input, observers: () => budget.observer === undefined ? [] : [budget.observer]};
    try {
      return await withPhaseTimeout(async signal => {
        const server = await this.phase(input, "server_resolution", () => withPhaseTimeout(signal => this.ownOperation(this.options.serverResolver.resolve({...input, signal})), Math.min(this.#credentialTimeoutMs, this.remaining(deadline)), "MCP server resolution timed out.", signal));
        const credential = await this.resolveCredential(input, server, signal, deadline, budget);
        let ready = await this.connection(input, server, credential, budget, signal, deadline);
        for (;;) {
          signal.throwIfAborted();
          try {
            let result: unknown;
            if (listing && ready.tools !== undefined) result = ready.tools;
            else {
              if (!listing && ready.tools !== undefined && this.options.onConnectionReady !== undefined) {
                await this.phase(input, "manifest_preparation", () => this.ownOperation(this.options.onConnectionReady!(input, projectTools(ready.tools!), {signal, timeoutMs: this.remaining(deadline)})));
              }
              signal.throwIfAborted();
              result = await this.phase(input, listing ? "discovery" : "tool_call", () => operation(ready.entry, signal, Math.min(this.#callTimeoutMs, this.remaining(deadline))));
            }
            signal.throwIfAborted();
            this.touch(ready.entry);
            return listing ? projectTools(result as ListToolsResult) : {...result as McpCallToolResult, refreshTriggered: budget.refreshTriggered};
          } catch (error) {
            if (!isAuthFailureError(error)) throw error;
            if (budget.operationSpent) throw authFailure();
            budget.operationSpent = true;
            const refreshed = await this.refreshCredential(input, server, ready.entry, signal, deadline, budget);
            await this.closeConnection(ready.entry);
            ready = await this.connection(input, server, refreshed, budget, signal, deadline);
          }
        }
      }, total, "MCP operation timed out.", parent);
    } catch (error) {
      if (isTimeoutError(error) || error instanceof DOMException && error.name === "TimeoutError") throw new McpConnectorError("mcp_timeout", listing ? "MCP tool discovery timed out." : "MCP tool call timed out.");
      if (isAuthFailureError(error)) throw authFailure();
      if (isInvalidParamsError(error)) throw new McpConnectorError("mcp_invalid_input", "MCP server rejected the arguments.");
      if (error instanceof McpDiscoveryError) {
        try { this.options.logger?.error(mcpDiscoveryPaginationFailureLogRecord(input, error.reason, error.pages, error.toolCount)); } catch { /* diagnostic isolation */ }
      }
      throw mcpConnectionFailureError(error) ?? error;
    }
  }

  /** Cancels producers, closes pending and cached transports, and joins their actual operations. */
  closeAll(deadline = new Date(Date.now() + 5000)): Promise<void> {
    if (this.#closed !== undefined) return this.#closed;
    this.#lifetime.abort(new Error("MCP client shutting down"));
    this.#closed = (async () => {
      const joined = (async () => {
        await Promise.allSettled([
          ...[...this.#openingClients].map((client) =>
            this.closeClient(client),
          ),
          ...[...this.#connections.values()].map((entry) =>
            this.closeConnection(entry),
          ),
        ]);
        await Promise.allSettled([...this.#connectionOpenings.values()].map(opening => opening.promise));
        while (this.#rawOperations.size > 0 || this.#closing.size > 0) {
          await Promise.allSettled([...this.#rawOperations, ...this.#closing]);
        }
      })();
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          joined,
          new Promise<never>((_, reject) => {
            timer = setTimeout(
              () => reject(new Error("MCP client shutdown deadline exceeded")),
              Math.max(0, deadline.getTime() - Date.now()),
            );
          }),
        ]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
        // A deadline rejects the public close only after owned operations join.
        // Until then reusable commands must retain their required resources.
        await joined;
      }
    })();
    return this.#closed;
  }

  private ownOperation<T>(operation: Promise<T>): Promise<T> {
    this.#rawOperations.add(operation);
    void operation.then(
      () => this.#rawOperations.delete(operation),
      () => this.#rawOperations.delete(operation),
    );
    return operation;
  }

  private closeClient(client: SDKClientLike): Promise<void> {
    const existing = this.#clientClosures.get(client);
    if (existing !== undefined) return existing;
    const closing = Promise.resolve().then(() => client.close());
    this.#clientClosures.set(client, closing);
    this.#closing.add(closing);
    void closing.then(
      () => this.#closing.delete(closing),
      () => this.#closing.delete(closing),
    );
    return closing;
  }

  /** Returns the number of live cached connections, excluding pending openings. */
  connectionCount(): number {
    return this.#connections.size;
  }

  /** Observes actual pending ownership; aliases after credential rotation count once. */
  pendingWaiterCount(): number { return [...new Set(this.#connectionOpenings.values())].reduce((count, opening) => count + opening.waiters.size, 0); }

  private async resolveCredential(identity: McpIdentity, server: ResolvedMcpServer, signal: AbortSignal, deadline: number, budget: RefreshBudget): Promise<CredentialMaterial> {
    const material = await this.phase(identity, "credential_resolution", () => withPhaseTimeout(signal => this.ownOperation(this.options.credentialResolver.resolve({...identity, resolvedServer: server, signal})), Math.min(this.#credentialTimeoutMs, this.remaining(deadline)), "MCP credential resolution timed out.", signal));
    if (!material.ok || material.mode !== "bearer") throw credentialFailure(material);
    if (material.refreshTriggered) budget.refreshTriggered = true;
    return material;
  }

  private async refreshCredential(identity: McpIdentity, server: ResolvedMcpServer, previous: Pick<CredentialMaterial, "tokenHash" | "vaultId" | "credentialId">, signal: AbortSignal, deadline: number, budget: RefreshBudget): Promise<CredentialMaterial> {
    budget.refreshTriggered = true;
    const material = await this.phase(identity, "credential_refresh", () => withPhaseTimeout(signal => this.ownOperation(this.options.credentialResolver.refresh({...identity, resolvedServer: server, vaultId: previous.vaultId, credentialId: previous.credentialId, previousTokenHash: previous.tokenHash, force: true, signal})), Math.min(this.#credentialTimeoutMs, this.remaining(deadline)), "MCP credential refresh timed out.", signal));
    if (!material.ok || material.mode !== "bearer") throw credentialFailure(material);
    return material;
  }

  private async connection(identity: McpIdentity, server: ResolvedMcpServer, credential: CredentialMaterial, budget: RefreshBudget, signal: AbortSignal, deadline: number): Promise<{entry: ConnectionEntry; tools?: ListToolsResult}> {
    signal.throwIfAborted();
    const baseKey = connectionBaseKey(identity);
    const key = connectionCacheKey(baseKey, credential);
    // An alias after forced refresh belongs to the same opening, whose current
    // key may differ from the credential snapshot resolved by this waiter.
    const admitted = this.#connectionOpenings.get(key);
    this.retireStaleOpenings(baseKey, admitted?.controller.signal.aborted === false ? admitted.key : key);
    const cached = this.#connections.get(key);
    if (cached !== undefined) {
      if (cached.endpoint === server.endpoint) { this.touch(cached); return {entry: cached}; }
      await this.closeConnection(cached);
    }
    let opening = this.#connectionOpenings.get(key);
    if (opening?.controller.signal.aborted) opening = undefined;
    if (opening !== undefined && opening.endpoint !== server.endpoint) { opening.controller.abort(new Error("MCP installed endpoint changed.")); await opening.promise.catch(() => undefined); opening = undefined; }
    signal.throwIfAborted();
    if (opening === undefined) {
      const controller = new AbortController();
      opening = {baseKey, key, endpoint: server.endpoint, controller, waiters: new Set(), handshakeSpent: false, operationSpent: false, promise: undefined!};
      this.#connectionOpenings.set(key, opening);
      const ownedOpening = opening;
      // Start after the first waiter registers; initialization is owned by all remaining waiters.
      opening.promise = this.ownOperation(Promise.resolve().then(() => this.establishConnection(identity, server, credential, ownedOpening)));
      void opening.promise.finally(() => { for (const [openingKey, pending] of this.#connectionOpenings) if (pending === ownedOpening) this.#connectionOpenings.delete(openingKey); }).catch(() => undefined);
    }
    if (opening.operationSpent && budget.operationSpent) throw authFailure();
    if (opening.operationSpent) budget.operationSpent = true;
    const owned = opening;
    let waiter!: OpeningWaiter;
    let abort!: () => void;
    const detached = new Promise<never>((_, reject) => {
      waiter = {budget, reject, deadline};
      abort = () => reject(signal.reason);
      signal.addEventListener("abort", abort, {once: true});
      if (signal.aborted) abort();
      else owned.waiters.add(waiter);
    });
    try {
      const ready = await Promise.race([owned.promise, detached]);
      if (ready.entry.tokenHash !== credential.tokenHash) budget.refreshTriggered = true;
      if (owned.operationSpent) budget.operationSpent = true;
      return ready;
    } finally {
      signal.removeEventListener("abort", abort);
      owned.waiters.delete(waiter);
      if (owned.waiters.size === 0) owned.controller.abort(new Error("MCP readiness has no waiters."));
    }
  }

  private async establishConnection(identity: McpIdentity, server: ResolvedMcpServer, original: CredentialMaterial, opening: ConnectionOpening): Promise<{entry: ConnectionEntry; tools: ListToolsResult}> {
    const signal = AbortSignal.any([opening.controller.signal, this.#lifetime.signal]);
    // Caller timers detach independently; all-gone cancels the shared initializer.
    // A shared SDK request uses the finite opening/phase cap so a later waiter
    // cannot inherit an earlier caller's already-dispatched request deadline.
    const deadline = this.now() + (this.options.executionTimeoutMs ?? MCP_EXECUTION_TIMEOUT_MS);
    identity = {...identity, observers: () => [...opening.waiters].flatMap(waiter => waiter.budget.observer===undefined?[]:[waiter.budget.observer])} as PhaseIdentity;
    let credential = original;
    for (;;) {
      signal.throwIfAborted();
      this.requireOpeningWaiters(opening);
      const baseKey = connectionBaseKey(identity);
      const key = connectionCacheKey(baseKey, credential);
      await this.closeStaleTokenConnections(baseKey, key);
      const client = this.createClient(identity);
      this.#openingClients.add(client);
      let phase: "connect" | "list" = "connect";
      try {
        validateAdapter(server.adapter);
        const transport = this.createTransport({url: new URL(server.endpoint), token: credential.token, requestHeaders: server.adapter.requestHeaders});
        // Keep the finite shared owner cap: a current waiter's deadline must
        // not expire this SDK request before a later, longer waiter completes.
        const connectTimeout = Math.min(this.#connectTimeoutMs, this.remaining(deadline));
        await this.phase(identity, "connect", () => withPhaseTimeout(signal => this.ownOperation(client.connect(transport, {timeout: connectTimeout, signal})), connectTimeout, "MCP initialization timed out.", signal));
        this.requireOpeningWaiters(opening);
        phase = "list";
        // Readiness has the same shared ownership; caller timers detach
        // separately, and the last detachment aborts this phase.
        const readinessTimeout = Math.min(this.options.discoveryTimeoutMs ?? MCP_DISCOVERY_TIMEOUT_MS, this.remaining(deadline));
        const tools = await this.phase(identity, "readiness", () => withPhaseTimeout(signal => this.ownOperation(client.listTools(undefined, {timeout: readinessTimeout, signal})), readinessTimeout, "MCP readiness timed out.", signal));
        signal.throwIfAborted();
        const entry: ConnectionEntry = {endpoint: server.endpoint, baseKey, key, tokenHash: credential.tokenHash, vaultId: credential.vaultId, credentialId: credential.credentialId, client, inFlight: new Set(), closed: false};
        this.#connections.set(key, entry);
        this.#clientEntries.set(client, entry);
        client.onerror = error => this.handleClientError(client, error);
        this.touch(entry);
        return {entry, tools};
      } catch (error) {
        await this.closeClient(client).catch(() => undefined);
        if (!isAuthFailureError(error)) throw error;
        if (phase === "connect") {
          if (opening.handshakeSpent || opening.operationSpent) throw authFailure();
          for (const waiter of [...opening.waiters]) if (waiter.budget.operationSpent) { opening.waiters.delete(waiter); waiter.reject(authFailure()); }
          if (opening.waiters.size === 0) throw authFailure();
          opening.handshakeSpent = true;
        } else {
          if (opening.operationSpent) throw authFailure();
          for (const waiter of [...opening.waiters]) {
            if (waiter.budget.operationSpent) { opening.waiters.delete(waiter); waiter.reject(authFailure()); }
            else waiter.budget.operationSpent = true;
          }
          if (opening.waiters.size === 0) throw authFailure();
          opening.operationSpent = true;
        }
        this.requireOpeningWaiters(opening);
        const refreshBudget: RefreshBudget = {operationSpent: opening.operationSpent, refreshTriggered: true};
        credential = await this.refreshCredential(identity, server, credential, signal, deadline, refreshBudget);
        // A different credential admitted while refresh was in flight retires
        // this owner. Its late raw result cannot overwrite the replacement.
        signal.throwIfAborted();
        this.requireOpeningWaiters(opening);
        const replacementKey = connectionCacheKey(baseKey, credential);
        const replacementOpening = this.#connectionOpenings.get(replacementKey);
        if ((replacementOpening !== undefined && replacementOpening !== opening) || this.#connections.has(replacementKey)) {
          throw new McpConnectorError("mcp_connection_failed", "MCP credential-bound readiness was superseded.");
        }
        opening.key = replacementKey;
        this.retireStaleOpenings(baseKey, replacementKey);
        this.#connectionOpenings.set(replacementKey, opening);
        for (const waiter of opening.waiters) waiter.budget.refreshTriggered = true;
      } finally { this.#openingClients.delete(client); }
    }
  }

  private readonly phaseAttempts = new WeakMap<() => McpExecutionObservation, Map<string,number>>();
  private async phase<T>(identity: PhaseIdentity, phase: string, operation: () => Promise<T>): Promise<T> {
    const started = this.now(); let outcome = "failed";
    try { const value = await operation(); outcome = "completed"; return value; }
    catch (error) { outcome = isTimeoutError(error) ? "mcp_timeout" : isAuthFailureError(error) ? "mcp_authentication_failed" : "failed"; throw error; }
    finally {
      const observers = identity.observers?.() ?? [];
      if (observers.length===0) { try { this.options.logger?.info?.(mcpPhaseCompletedLogRecord({...identity,phase,outcome,durationMs:this.now()-started})); } catch {} }
      for (const observer of observers) { try {
        let attempts=this.phaseAttempts.get(observer);if(attempts===undefined){attempts=new Map();this.phaseAttempts.set(observer,attempts);}
        const attempt=(attempts.get(phase)??0)+1;attempts.set(phase,attempt);
        this.options.logger?.info?.(mcpPhaseCompletedLogRecord({...identity,...observer(),phase,outcome,durationMs:this.now()-started,attempt}));
      } catch { /* diagnostic isolation */ } }
    }
  }

  private now(): number { return (this.options.monotonicNow ?? (() => performance.now()))(); }
  private remaining(deadline: number): number { return remaining(deadline, this.now()); }
  private requireOpeningWaiters(opening: ConnectionOpening): void {
    for (const waiter of [...opening.waiters]) if (waiter.deadline <= this.now()) {
      opening.waiters.delete(waiter);
      waiter.reject(Object.assign(new Error("MCP deadline exceeded."), {code: ErrorCode.RequestTimeout}));
    }
    if (opening.waiters.size === 0) throw Object.assign(new Error("MCP deadline exceeded."), {code: ErrorCode.RequestTimeout});
  }

  private createClient(identity: McpIdentity): SDKClientLike {
    // Cached SDK callbacks own only routing scope. An operation's observers may
    // capture a pending opening and its complete list; neither belongs here.
    identity = Object.freeze({workspaceId: identity.workspaceId, sessionId: identity.sessionId, mcpServerName: identity.mcpServerName});
    if (this.options.createClient !== undefined) {
      return this.options.createClient(identity);
    }
    return new DiscoverySDKClient(
      {
        name: "tetral-mcp-connector",
        version: "0.1.0",
      },
      {
        listChanged: {
          tools: {
            // Tetral owns the one re-list per protocol notification. Disabling
            // SDK refresh and debounce preserves notification cardinality while
            // Bridge remains the sole durable manifest lifecycle owner.
            autoRefresh: false,
            debounceMs: 0,
            onChanged: (error) => {
              if (error !== undefined && error !== null) {
                try {
                  this.options.logger?.error(
                    mcpToolsListChangedFailureLogRecord(
                      identity,
                      "refresh_failed",
                    ),
                  );
                } catch {
                  /* diagnostic isolation */
                }
                return;
              }
              if (this.#lifetime.signal.aborted) return;
              void this.ownOperation(
                Promise.resolve().then(() =>
                  this.options.onToolsListChanged(identity),
                ),
              ).catch(() => {
                try {
                  this.options.logger?.error(
                    mcpToolsListChangedFailureLogRecord(
                      identity,
                      "notify_failed",
                    ),
                  );
                } catch {
                  /* diagnostic isolation */
                }
              });
            },
          },
        },
      },
      {
        ...(this.options.discoveryMaxPages === undefined
          ? {}
          : { maxPages: this.options.discoveryMaxPages }),
        ...(this.options.discoveryMaxTools === undefined
          ? {}
          : { maxTools: this.options.discoveryMaxTools }),
        ...(this.options.discoveryMaxBytes === undefined
          ? {}
          : { maxBytes: this.options.discoveryMaxBytes }),
      },
    );
  }

  private createTransport(input: { readonly url: URL; readonly token?: string | undefined; readonly requestHeaders?: Readonly<Record<string, string>> | undefined }): unknown {
    if (this.options.createTransport !== undefined) {
      return this.options.createTransport(input);
    }
    return new StreamableHTTPClientTransport(input.url, streamableHTTPTransportOptions(input));
  }

  private touch(entry: ConnectionEntry): void {
		if (entry.closed || this.#connections.get(entry.key) !== entry) {
			return;
		}
    if (entry.idleTimer !== undefined) {
      this.#clearTimer(entry.idleTimer);
    }
    entry.idleTimer = this.#setTimer(() => {
      void this.closeConnection(entry).catch(() => undefined);
    }, this.#idleTimeoutMs);
    if (typeof entry.idleTimer === "object" && entry.idleTimer !== null && "unref" in entry.idleTimer) {
      (entry.idleTimer as { unref: () => void }).unref();
    }
  }

  private retireStaleOpenings(baseKey: string, replacementKey: string): void {
    for (const pending of new Set(this.#connectionOpenings.values())) {
      if (pending.baseKey !== baseKey || pending.key === replacementKey) continue;
      const error = new McpConnectorError("mcp_connection_failed", "MCP credential-bound readiness was superseded.");
      pending.controller.abort(error);
      for (const waiter of pending.waiters) waiter.reject(error);
      // Remove admission aliases immediately; the raw opening/SDK/close
      // promises remain tracked until their actual callbacks join.
      for (const [key, owner] of this.#connectionOpenings) if (owner === pending) this.#connectionOpenings.delete(key);
    }
  }

  private async closeStaleTokenConnections(baseKey: string, replacementKey: string): Promise<void> {
    const stale = [...this.#connections.values()].filter((entry) => entry.baseKey === baseKey && entry.key !== replacementKey);
    await Promise.all(stale.map((entry) => this.closeConnection(entry)));
  }

  private async closeConnection(entry: ConnectionEntry): Promise<void> {
		if (entry.closing !== undefined) {
			return await entry.closing;
		}
		entry.closed = true;
    if (entry.idleTimer !== undefined) {
      this.#clearTimer(entry.idleTimer);
      entry.idleTimer = undefined;
    }
    this.#connections.delete(entry.key);
    this.#clientEntries.delete(entry.client);
    entry.closing = this.closeClient(entry.client);
    this.#closing.add(entry.closing);
    try {
      return await entry.closing;
    } finally {
      this.#closing.delete(entry.closing);
    }
  }

  private async runConnectionOperation<T>(
    entry: ConnectionEntry,
    operation: () => Promise<T>,
    signal?: AbortSignal,
    closeOnAbort = false,
  ): Promise<T> {
    let call!: InFlightCall;
    const exhausted = new Promise<T>((_resolve, reject) => {
      call = {
        settled: false,
        reject: (error) => {
          if (call.settled) {
            return;
          }
          call.settled = true;
          reject(error);
        },
      };
    });
    entry.inFlight.add(call);
    const aborted = () => {
      // Cancellation may close a transport only while this operation is its
      // sole owner. A later raw callback must not reconsider an earlier abort
      // after an unrelated call has completed and released its ownership.
      // Warm discovery owns a request, not the ready transport. Its SDK signal
      // stops pagination even if gRPC delivers cancellation after another call
      // has finished. Only a sole execution may retire its abandoned transport.
      if (closeOnAbort && entry.inFlight.size === 1) void this.closeConnection(entry).catch(() => undefined);
    };
    signal?.addEventListener("abort", aborted, {once: true});
    if (signal?.aborted) aborted();
    try {
      return await Promise.race([
        this.ownOperation(Promise.resolve().then(operation)),
        exhausted,
      ]);
    } finally {
      call.settled = true;
      entry.inFlight.delete(call);
      signal?.removeEventListener("abort", aborted);
    }
  }

	private handleClientError(client: SDKClientLike, error: Error): void {
		if (!isReconnectExhaustedError(error)) {
			return;
		}
		const entry = this.#clientEntries.get(client);
		if (entry === undefined || entry.closed) {
			return;
		}
		const failure = new McpConnectorError("mcp_connection_failed", "MCP connection retries exhausted.", "exhausted");
		for (const call of [...entry.inFlight]) {
			call.reject(failure);
		}
		void this.closeConnection(entry).catch(() => undefined);
	}
}

/**
 * Builds Streamable HTTP transport options with optional bearer authorization,
 * the adapter-owned service headers, and the connector's bounded exponential
 * reconnect schedule. Every newly created GitHub transport carries the
 * `X-MCP-Toolsets` header alongside `Authorization`, including connection
 * recreation after credential replacement.
 */
export function streamableHTTPTransportOptions(input: { readonly token?: string | undefined; readonly requestHeaders?: Readonly<Record<string, string>> | undefined }): {
  readonly requestInit: RequestInit;
  readonly reconnectionOptions: {
    readonly initialReconnectionDelay: number;
    readonly reconnectionDelayGrowFactor: number;
    readonly maxReconnectionDelay: number;
    readonly maxRetries: number;
  };
} {
  const headers: Record<string, string> = {};
  if (input.token !== undefined) {
    headers.Authorization = `Bearer ${input.token}`;
  }
  Object.assign(headers, input.requestHeaders ?? {});
  const requestInit: RequestInit = Object.keys(headers).length === 0 ? {} : { headers };
  return {
    requestInit,
    reconnectionOptions: {
      initialReconnectionDelay: MCP_RECONNECT_DELAYS_MS[0],
      reconnectionDelayGrowFactor: MCP_RECONNECT_DELAYS_MS[1] / MCP_RECONNECT_DELAYS_MS[0],
      maxReconnectionDelay: MCP_RECONNECT_DELAYS_MS[2],
      maxRetries: MCP_RECONNECT_MAX_RETRIES,
    },
  };
}

/** Builds a bounded structured error record for tool-list refresh or callback failure. */
export function mcpToolsListChangedFailureLogRecord(identity: McpIdentity, failure: ToolsListChangedFailure): McpLogRecord {
  const suffix = failure === "refresh_failed" ? "refresh_failed" : "notify_failed";
  return {
    event: `mcp_tools_list_changed_${suffix}`,
    "event.kind": `mcp_tools_list_changed_${suffix}`,
    operation: "mcp_manifest_refresh",
    component: "mcp-connector",
    "workspace.id": identity.workspaceId,
    "session.id": identity.sessionId,
    "mcp.server.name": identity.mcpServerName,
    ...semanticErrorFields({
      errorClass: "mcp_connection_failed",
      errorCode: "mcp_connection_failed",
      messageSafe: `mcp tools/list_changed ${failure === "refresh_failed" ? "refresh" : "notify"} failed`,
    }),
  };
}

/** Builds a bounded structured error record for a discovery pagination-invariant violation. */
export function mcpDiscoveryPaginationFailureLogRecord(
  identity: McpIdentity,
  reason: DiscoveryFailureReason,
  pages: number,
  toolCount: number,
): McpLogRecord {
  return {
    event: "mcp_discovery_pagination_failed",
    "event.kind": "mcp_discovery_pagination_failed",
    operation: "mcp_manifest_list",
    component: "mcp-connector",
    "workspace.id": identity.workspaceId,
    "session.id": identity.sessionId,
    "mcp.server.name": identity.mcpServerName,
    "mcp.discovery.failure_reason": reason,
    "mcp.discovery.pages": pages,
    "mcp.discovery.tool_count": toolCount,
    ...semanticErrorFields({
      errorClass: "mcp_connection_failed",
      errorCode: "mcp_connection_failed",
      messageSafe: `mcp discovery pagination failed: ${reason}`,
    }),
  };
}

function connectionBaseKey(identity: McpIdentity): string {
  return JSON.stringify([identity.workspaceId, identity.sessionId, identity.mcpServerName]);
}

function connectionCacheKey(
  baseKey: string,
  credential: Pick<CredentialMaterial, "vaultId" | "credentialId" | "tokenHash">,
): string {
  return JSON.stringify([baseKey, credential.vaultId, credential.credentialId, credential.tokenHash]);
}

function isTimeoutError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { readonly code?: unknown }).code === ErrorCode.RequestTimeout;
}

async function withPhaseTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
  message: string,
  parentSignal?: AbortSignal,
): Promise<T> {
  parentSignal?.throwIfAborted();
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let abort: (() => void) | undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    abort = () => { controller.abort(parentSignal?.reason); reject(parentSignal?.reason); };
    parentSignal?.addEventListener("abort", abort, { once: true });
  });
  const expired = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      const error = Object.assign(new Error(message), { code: ErrorCode.RequestTimeout });
      reject(error);
      controller.abort(error);
    }, timeoutMs);
    if (typeof timer === "object" && timer !== null && "unref" in timer) {
      (timer as { unref: () => void }).unref();
    }
  });
  try {
    return await Promise.race([operation(controller.signal), expired, cancelled]);
  } finally {
    if (abort !== undefined) parentSignal?.removeEventListener("abort", abort);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function isInvalidParamsError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { readonly code?: unknown }).code === ErrorCode.InvalidParams;
}

function mcpConnectionFailureError(error: unknown): McpConnectorError | undefined {
  const message = error instanceof Error ? error.message : String(error);
  if (isReconnectExhaustedError(error)) {
    return new McpConnectorError("mcp_connection_failed", "MCP connection retries exhausted.", "exhausted");
  }
  if (
    message.startsWith("Failed to reconnect SSE stream:")
    || message.startsWith("Failed to reconnect:")
    || message.startsWith("SSE stream disconnected:")
  ) {
    return new McpConnectorError("mcp_connection_failed", "MCP connection retrying.", "retrying");
  }
  return undefined;
}

function isReconnectExhaustedError(error: unknown): boolean {
	const message = error instanceof Error ? error.message : String(error);
	return /Maximum reconnection attempts \(\d+\) exceeded\./.test(message);
}

function isAuthFailureError(error: unknown): boolean {
  return error instanceof StreamableHTTPError && (error.code === 401 || error.code === 403) || error instanceof UnauthorizedError;
}
function remaining(deadline: number, now: number): number {
  const remaining = deadline - now;
  if (remaining <= 0) throw Object.assign(new Error("MCP deadline exceeded."), {code: ErrorCode.RequestTimeout});
  return Math.max(1, Math.ceil(remaining));
}
function projectTools(listed: ListToolsResult): readonly McpClientTool[] {
  return listed.tools.map(tool => ({name: tool.name, description: tool.description ?? "", inputSchema: tool.inputSchema, enabled: (tool as typeof tool & {enabled?: boolean}).enabled}));
}
function authFailure(): McpConnectorError { return new McpConnectorError("mcp_authentication_failed", "MCP authentication failed after refresh.", "terminal"); }
function credentialFailure(material: McpCredentialResolution): McpConnectorError {
  return !material.ok && material.error === "credential_required" ? new McpConnectorError("mcp_credential_required", "MCP server requires a configured credential.", "terminal") : !material.ok && material.error === "refresh_unavailable" ? new McpConnectorError("mcp_connection_failed", "MCP credential refresh is temporarily unavailable.", "terminal") : authFailure();
}
