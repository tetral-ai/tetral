/**
 * Owns the MCP SDK client boundary for tool discovery and tool execution. Each
 * operation resolves current bearer material, admits only cataloged servers,
 * and shares a connection by workspace, session, server, and token hash. The
 * module guards bounded authentication refresh, bounded SDK requests,
 * exactly-once settlement of tracked calls on reconnect exhaustion, cache
 * eviction, and idle connection closure. Credential resolution may perform a
 * proactive refresh; connection initialization and the later SDK operation
 * then keep separate one-refresh rejection budgets.
 *
 * Command assembly constructs {@link McpSDKClient}; the connector service calls
 * it for tool lists and tool results and receives successful tool-list change
 * callbacks. The client calls the credential resolver and MCP SDK, opens
 * Streamable HTTP transports, and reports callback failures through the
 * connector logger.
 *
 * @packageDocumentation
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { semanticErrorFields } from "@tetral/ts-observability";
import { ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { catalogEntryByName } from "./catalog.js";
import { McpConnectorError } from "./errors.js";
import type { McpCallToolResult } from "./formatter.js";
import type { GitHubMcpCredentialResolution, GitHubMcpCredentialResolver } from "./credential.js";
import {
  MCP_CONNECT_TIMEOUT_MS,
  MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
} from "./phase-budgets.js";
import type { McpClient, McpClientTool, McpConnectorLogger, McpLogRecord } from "./service.js";
import type { CallToolResult, CompatibilityCallToolResult, ListToolsResult } from "@modelcontextprotocol/sdk/types.js";

/** Defines the per-request timeout applied to MCP tool and discovery calls. */
export const MCP_CALL_TIMEOUT_SECONDS = 120;
/** Defines how long a cached connection remains unused before it closes. */
export const MCP_SESSION_IDLE_SECONDS = 1800;
/** Lists the delays represented by the SDK transport's exponential reconnect settings. */
export const MCP_RECONNECT_DELAYS_MS = [1000, 4000, 16000] as const;
/** Defines the maximum number of automatic reconnect attempts for a dropped stream. */
export const MCP_RECONNECT_MAX_RETRIES = 3;
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
  listTools(params?: unknown, options?: { readonly timeout?: number }): Promise<ListToolsResult>;
  callTool(
    params: { readonly name: string; readonly arguments?: Record<string, unknown> | undefined },
    resultSchema?: unknown,
    options?: { readonly timeout?: number },
  ): Promise<CallToolResult | CompatibilityCallToolResult>;
  close(): Promise<void>;
}

/**
 * Supplies the credential, manifest-change, logging, metrics, factory, timeout,
 * and timer collaborators used by {@link McpSDKClient}. The factory and timer
 * overrides expose the same lifecycle boundaries to focused tests.
 */
export interface McpSDKClientOptions {
  readonly credentialResolver: GitHubMcpCredentialResolver;
  readonly onToolsListChanged: (input: McpIdentity) => Promise<void>;
  readonly logger?: Pick<McpConnectorLogger, "error"> | undefined;
  readonly createClient?: ((identity: McpIdentity) => SDKClientLike) | undefined;
  readonly createTransport?: ((input: { readonly url: URL; readonly token?: string | undefined }) => unknown) | undefined;
  readonly idleTimeoutMs?: number | undefined;
  readonly callTimeoutMs?: number | undefined;
  readonly credentialTimeoutMs?: number | undefined;
  readonly connectTimeoutMs?: number | undefined;
  readonly setTimer?: ((callback: () => void, ms: number) => ReturnType<typeof setTimeout>) | undefined;
  readonly clearTimer?: ((timer: ReturnType<typeof setTimeout>) => void) | undefined;
}

interface ConnectionEntry {
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

type CredentialMaterial = Extract<GitHubMcpCredentialResolution, { readonly ok: true; readonly mode: "bearer" }>;

// Connection cache state machine.
//
// A cached connection is a ConnectionEntry keyed by
//   workspaceId \0 sessionId \0 mcpServerName \0 vaultId \0 credentialId \0 sha256(token)
// (base key from connectionBaseKey; tokenHash is sha256 of the token, produced
// by useToken in credential.ts). An identity or credential-material change
// therefore yields a new key, so a fresh client opens while the stale entry is
// closed by closeStaleTokenConnections with no drain or handoff.
//
// State   | Meaning                                   | Writers                          | Readers                        | Legal transitions
// --------|-------------------------------------------|----------------------------------|--------------------------------|--------------------------------------------------
// opening | establishConnection promise pending in    | openConnection (set/delete key), | openConnection                 | opening -> live (connect ok);
//         | #connectionOpenings; not yet in           | establishConnection              |                                | opening -> reopen (auth-failure refresh) | throw (no entry)
//         | #connections                              |                                  |                                |
// live    | entry in #connections and #clientEntries; | establishConnection (insert),    | connection, openConnection,    | live -> live (touch re-arms idle timer);
//         | idleTimer armed; client.onerror bound     | touch (idle timer),              | runConnectionOperation,        | live -> closed (idle fire | stale token |
//         |                                           | handleClientError (transition)   | closeStaleTokenConnections     |   onerror exhausted | refreshConnection | closeAll)
// closed  | closed=true, removed from both maps;      | closeConnection                  | touch (bails when closed or    | terminal
//         | client.close() tracked by entry.closing   |                                  | remapped)                      |
//
// The SDK transport retries a dropped stream MCP_RECONNECT_MAX_RETRIES (3) times
// at MCP_RECONNECT_DELAYS_MS (1s, 4s, 16s). The SDK fires onerror but never
// onclose, so handleClientError is the sole terminal signal for an exhausted
// connection: on isReconnectExhaustedError it synthesizes the
// mcp_connection_failed / "exhausted" classification, rejects every in-flight
// call exactly once (InFlightCall.settled guards the single settle), and
// closeConnection evicts the dead entry. This settles waiters immediately rather
// than letting them run out the MCP_CALL_TIMEOUT_SECONDS (120s) per-call timeout.
/**
 * Adapts the MCP SDK to the connector service's discovery and execution client.
 * It resolves credentials before every operation, shares same-identity
 * connection establishment, observes any proactive credential refresh,
 * permits one forced refresh during establishment and one after an
 * operation-level authentication rejection, and tracks live calls so reconnect
 * exhaustion can settle and evict the affected client before its request
 * timeout.
 */
export class McpSDKClient implements McpClient {
  readonly #connections = new Map<string, ConnectionEntry>();
	readonly #clientEntries = new Map<SDKClientLike, ConnectionEntry>();
  readonly #connectionOpenings = new Map<string, Promise<ConnectionEntry>>();
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

  /** Lists tools through the current credential-bound connection and re-arms its idle timer after success. */
  async listTools(input: McpIdentity): Promise<readonly McpClientTool[]> {
    const result = await this.withAuthRefreshRetry(input, async (connection) => {
      const listed = await this.runConnectionOperation(connection, () => connection.client.listTools(undefined, { timeout: this.#callTimeoutMs }));
      this.touch(connection);
      return listed;
    });
    return result.value.tools.map((tool) => ({
      name: tool.name,
      description: tool.description ?? "",
      inputSchema: tool.inputSchema,
      enabled: (tool as typeof tool & { readonly enabled?: boolean | undefined }).enabled,
    }));
  }

  /** Calls one MCP tool and reports whether credential refresh contributes to the successful attempt. */
  async callTool(input: McpIdentity & {
    readonly sessionThreadId: string;
    readonly toolName: string;
    readonly input: Record<string, unknown>;
  }): Promise<McpCallToolResult> {
    const result = await this.withAuthRefreshRetry(input, async (connection) => {
		const result = await this.runConnectionOperation(connection, () => connection.client.callTool(
        { name: input.toolName, arguments: input.input },
        undefined,
        { timeout: this.#callTimeoutMs },
		));
      this.touch(connection);
      if ("toolResult" in result) {
        return { structuredContent: result.toolResult };
      }
      return result;
    });
    return { ...result.value, refreshTriggered: result.refreshTriggered };
  }

  /** Closes and evicts every connection currently present in the live cache. */
  async closeAll(): Promise<void> {
    await Promise.all([...this.#connections.values()].map((entry) => this.closeConnection(entry)));
  }

  /** Returns the number of live cached connections, excluding pending openings. */
  connectionCount(): number {
    return this.#connections.size;
  }

  private async connection(identity: McpIdentity, markRefresh: () => void): Promise<ConnectionEntry> {
    const catalog = catalogEntryByName(identity.mcpServerName);
    if (catalog === undefined) {
      throw new McpConnectorError("mcp_invalid_input", "MCP server is outside the curated catalog.");
    }
    const credential = await withPhaseTimeout(
      (signal) => this.options.credentialResolver.resolve({ ...identity, signal }),
      this.#credentialTimeoutMs,
      "MCP credential resolution timed out.",
    );
    if (!credential.ok) {
      if (credential.error === "credential_required") {
        throw new McpConnectorError("mcp_credential_required", `MCP server ${identity.mcpServerName} requires a configured credential.`, "terminal");
      }
      throw new McpConnectorError("mcp_authentication_failed", "MCP credential is unavailable.", "terminal");
    }
    if (credential.mode !== "bearer") {
      throw new McpConnectorError("mcp_authentication_failed", "MCP requires bearer credential material.", "terminal");
    }
    if (credential.refreshTriggered === true) {
      markRefresh();
    }
    const baseKey = connectionBaseKey(identity);
    const key = connectionCacheKey(baseKey, credential);
    const existing = this.#connections.get(key);
    if (existing !== undefined) {
      this.touch(existing);
      return existing;
    }
    return await this.openConnection(identity, credential, true, markRefresh);
  }

  private async openConnection(identity: McpIdentity, credential: CredentialMaterial, allowAuthRefresh: boolean, markRefresh: () => void): Promise<ConnectionEntry> {
    const catalog = catalogEntryByName(identity.mcpServerName);
    if (catalog === undefined) {
      throw new McpConnectorError("mcp_invalid_input", "MCP server is outside the curated catalog.");
    }
    const baseKey = connectionBaseKey(identity);
    const key = connectionCacheKey(baseKey, credential);
    const existing = this.#connections.get(key);
    if (existing !== undefined) {
      this.touch(existing);
      return existing;
    }
    const opening = this.#connectionOpenings.get(key);
    if (opening !== undefined) {
      const entry = await opening;
      if (entry.tokenHash !== credential.tokenHash) {
        markRefresh();
      }
      this.touch(entry);
      return entry;
    }
    const started = this.establishConnection(identity, credential, allowAuthRefresh, markRefresh);
    this.#connectionOpenings.set(key, started);
    try {
      const entry = await started;
      if (entry.tokenHash !== credential.tokenHash) {
        markRefresh();
      }
      return entry;
    } finally {
      if (this.#connectionOpenings.get(key) === started) {
        this.#connectionOpenings.delete(key);
      }
    }
  }

  private async establishConnection(identity: McpIdentity, credential: CredentialMaterial, allowAuthRefresh: boolean, markRefresh: () => void): Promise<ConnectionEntry> {
    const catalog = catalogEntryByName(identity.mcpServerName);
    if (catalog === undefined) {
      throw new McpConnectorError("mcp_invalid_input", "MCP server is outside the curated catalog.");
    }
    const baseKey = connectionBaseKey(identity);
    const key = connectionCacheKey(baseKey, credential);
    await this.closeStaleTokenConnections(baseKey, key);
    const client = this.createClient(identity);
    const transport = this.createTransport({
      url: new URL(catalog.url),
      token: credential.token,
    });
    try {
      await withPhaseTimeout(
        (signal) => client.connect(transport, { timeout: this.#connectTimeoutMs, signal }),
        this.#connectTimeoutMs,
        "MCP connection initialization timed out.",
      );
    } catch (error) {
      await client.close().catch(() => undefined);
      if (allowAuthRefresh && credential.mode === "bearer" && isAuthFailureError(error)) {
        const refreshed = await this.refreshCredential(identity, credential.tokenHash, credential.vaultId, credential.credentialId, markRefresh);
        return await this.openConnection(identity, refreshed, false, markRefresh);
      }
      throw error;
    }
		const entry: ConnectionEntry = {
      baseKey,
      key,
      tokenHash: credential.tokenHash,
      vaultId: credential.vaultId,
      credentialId: credential.credentialId,
      client,
      inFlight: new Set(),
      closed: false,
    };
    this.#connections.set(key, entry);
		this.#clientEntries.set(client, entry);
		client.onerror = (error) => {
			this.handleClientError(client, error);
		};
    this.touch(entry);
    return entry;
  }

  private async withAuthRefreshRetry<T>(identity: McpIdentity, operation: (connection: ConnectionEntry) => Promise<T>): Promise<{ readonly value: T; readonly refreshTriggered: boolean }> {
    let refreshTriggered = false;
    const markRefresh = () => {
      refreshTriggered = true;
    };
    let connection: ConnectionEntry;
    try {
      connection = await this.connection(identity, markRefresh);
    } catch (error) {
      if (isTimeoutError(error)) {
        throw new McpConnectorError("mcp_timeout", "MCP tool call timed out.");
      }
      if (isInvalidParamsError(error)) {
        throw new McpConnectorError("mcp_invalid_input", "MCP server rejected the arguments.");
      }
      if (isAuthFailureError(error)) {
        throw new McpConnectorError("mcp_authentication_failed", "MCP authentication failed after refresh.", "terminal");
      }
      const connectionError = mcpConnectionFailureError(error);
      if (connectionError !== undefined) {
        throw connectionError;
      }
      throw error;
    }
    try {
      return { value: await operation(connection), refreshTriggered };
    } catch (error) {
      if (isTimeoutError(error)) {
        throw new McpConnectorError("mcp_timeout", "MCP tool call timed out.");
      }
      if (isInvalidParamsError(error)) {
        throw new McpConnectorError("mcp_invalid_input", "MCP server rejected the arguments.");
      }
      if (!isAuthFailureError(error)) {
        const connectionError = mcpConnectionFailureError(error);
        if (connectionError !== undefined) {
          throw connectionError;
        }
        throw error;
      }
      const refreshed = await this.refreshConnection(identity, connection, markRefresh);
      try {
        return { value: await operation(refreshed), refreshTriggered };
      } catch (retryError) {
        if (isTimeoutError(retryError)) {
          throw new McpConnectorError("mcp_timeout", "MCP tool call timed out.");
        }
        if (isInvalidParamsError(retryError)) {
          throw new McpConnectorError("mcp_invalid_input", "MCP server rejected the arguments.");
        }
        if (isAuthFailureError(retryError)) {
          throw new McpConnectorError("mcp_authentication_failed", "MCP authentication failed after refresh.", "terminal");
        }
        const connectionError = mcpConnectionFailureError(retryError);
        if (connectionError !== undefined) {
          throw connectionError;
        }
        throw retryError;
      }
    }
  }

  private async refreshConnection(identity: McpIdentity, previous: ConnectionEntry, markRefresh: () => void): Promise<ConnectionEntry> {
    const refreshed = await this.refreshCredential(identity, previous.tokenHash, previous.vaultId, previous.credentialId, markRefresh);
    await this.closeConnection(previous);
    return await this.openConnection(identity, refreshed, false, markRefresh);
  }

  private async refreshCredential(
    identity: McpIdentity,
    previousTokenHash: string,
    vaultId: string,
    credentialId: string,
    markRefresh: () => void,
  ): Promise<Extract<CredentialMaterial, { readonly mode: "bearer" }>> {
    markRefresh();
    let refreshed: GitHubMcpCredentialResolution;
    try {
      refreshed = await withPhaseTimeout(
        (signal) => this.options.credentialResolver.refresh({
          ...identity,
          vaultId,
          credentialId,
          previousTokenHash,
          force: true,
          signal,
        }),
        this.#credentialTimeoutMs,
        "MCP credential refresh timed out.",
      );
    } catch (error) {
      if (isTimeoutError(error)) {
        throw error;
      }
      throw new McpConnectorError("mcp_authentication_failed", "MCP credential refresh failed.", "terminal");
    }
    if (!refreshed.ok || refreshed.mode !== "bearer") {
      throw new McpConnectorError("mcp_authentication_failed", "MCP credential refresh failed.", "terminal");
    }
    return refreshed;
  }

  private createClient(identity: McpIdentity): SDKClientLike {
    if (this.options.createClient !== undefined) {
      return this.options.createClient(identity);
    }
    return new Client({
      name: "tetral-mcp-connector",
      version: "0.1.0",
    }, {
      listChanged: {
        tools: {
          // Tetral owns the one re-list per protocol notification. Disabling
          // SDK refresh and debounce preserves notification cardinality while
          // Bridge remains the sole durable manifest lifecycle owner.
          autoRefresh: false,
          debounceMs: 0,
          onChanged: (error) => {
            if (error !== undefined && error !== null) {
              this.options.logger?.error(mcpToolsListChangedFailureLogRecord(identity, "refresh_failed"));
              return;
            }
            void this.options.onToolsListChanged(identity).catch(() => {
              this.options.logger?.error(mcpToolsListChangedFailureLogRecord(identity, "notify_failed"));
            });
          },
        },
      },
    });
  }

  private createTransport(input: { readonly url: URL; readonly token?: string | undefined }): unknown {
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
      void this.closeConnection(entry);
    }, this.#idleTimeoutMs);
    if (typeof entry.idleTimer === "object" && entry.idleTimer !== null && "unref" in entry.idleTimer) {
      (entry.idleTimer as { unref: () => void }).unref();
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
		entry.closing = entry.client.close();
		return await entry.closing;
  }

	private async runConnectionOperation<T>(entry: ConnectionEntry, operation: () => Promise<T>): Promise<T> {
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
		try {
			return await Promise.race([operation(), exhausted]);
		} finally {
			call.settled = true;
			entry.inFlight.delete(call);
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
 * Builds Streamable HTTP transport options with optional bearer authorization
 * and the connector's bounded exponential reconnect schedule.
 */
export function streamableHTTPTransportOptions(input: { readonly token?: string | undefined }): {
  readonly requestInit: RequestInit;
  readonly reconnectionOptions: {
    readonly initialReconnectionDelay: number;
    readonly reconnectionDelayGrowFactor: number;
    readonly maxReconnectionDelay: number;
    readonly maxRetries: number;
  };
} {
  const requestInit: RequestInit = input.token === undefined
    ? {}
    : { headers: { Authorization: `Bearer ${input.token}` } };
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
    mcp_server_name: identity.mcpServerName,
    ...semanticErrorFields({
      errorClass: "mcp_connection_failed",
      errorCode: "mcp_connection_failed",
      messageSafe: `mcp tools/list_changed ${failure === "refresh_failed" ? "refresh" : "notify"} failed`,
    }),
  };
}

function connectionBaseKey(identity: McpIdentity): string {
  return `${identity.workspaceId}\u0000${identity.sessionId}\u0000${identity.mcpServerName}`;
}

function connectionCacheKey(
  baseKey: string,
  credential: Pick<CredentialMaterial, "vaultId" | "credentialId" | "tokenHash">,
): string {
  return `${baseKey}\u0000${credential.vaultId}\u0000${credential.credentialId}\u0000${credential.tokenHash}`;
}

function isTimeoutError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { readonly code?: unknown }).code === ErrorCode.RequestTimeout;
}

async function withPhaseTimeout<T>(
  operation: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
  message: string,
): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
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
    return await Promise.race([operation(controller.signal), expired]);
  } finally {
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
  if (typeof error !== "object" || error === null) {
    return false;
  }
  const code = (error as { readonly code?: unknown }).code;
  if (code === 401 || code === 403) {
    return true;
  }
  const name = (error as { readonly name?: unknown }).name;
  if (name === "UnauthorizedError") {
    return true;
  }
  const message = (error as { readonly message?: unknown }).message;
  return typeof message === "string" && /\b(?:401|403)\b/.test(message);
}
