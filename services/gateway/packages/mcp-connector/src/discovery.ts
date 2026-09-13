/**
 * Aggregates tools/list responses below SDK listTools so SDK metadata is
 * replaced only after all pages succeed. Discovery state is local to a single
 * request; tool execution and other protocol methods keep the SDK's behavior.
 * Bridge owns input retries and final manifest acceptance. These earlier bounds
 * limit raw accumulation, not the size of an individual HTTP body being parsed.
 * @packageDocumentation
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { RequestOptions } from "@modelcontextprotocol/sdk/shared/protocol.js";
import type { AnySchema, SchemaOutput } from "@modelcontextprotocol/sdk/server/zod-compat.js";
import { safeParse } from "@modelcontextprotocol/sdk/server/zod-compat.js";
import { ListToolsResultSchema } from "@modelcontextprotocol/sdk/types.js";

export const MCP_DISCOVERY_MAX_PAGES = 100;
export const MCP_DISCOVERY_MAX_TOOLS = 1024;
/** Raw definitions include SDK-only metadata; Bridge separately caps its projection at 256 KiB. */
export const MCP_DISCOVERY_MAX_BYTES = 1024 * 1024;
export const MCP_DISCOVERY_TIMEOUT_MS = 120_000;

export type DiscoveryFailureReason = "page_bound" | "tool_bound" | "byte_bound" | "repeated_cursor" | "invalid_response";

/** Safe discovery diagnostics, independent of tool-execution error enums. */
export class McpDiscoveryError extends Error {
  constructor(readonly reason: DiscoveryFailureReason, readonly pages = 0, readonly toolCount = 0) {
    super(`MCP tool discovery failed (${reason}).`);
    this.name = "McpDiscoveryError";
  }
}

export interface DiscoveryLimits {
  readonly maxPages?: number;
  readonly maxTools?: number;
  readonly maxBytes?: number;
}

/** Public request override leaves inherited listTools as the sole SDK cache writer. */
export class DiscoverySDKClient extends Client {
  constructor(info: ConstructorParameters<typeof Client>[0], options: ConstructorParameters<typeof Client>[1], private readonly limits: DiscoveryLimits = {}) {
    super(info, options);
  }

  override async request<T extends AnySchema>(request: Parameters<Client["request"]>[0], resultSchema: T, options?: RequestOptions): Promise<SchemaOutput<T>> {
    if (request.method !== "tools/list") {
      return super.request(request, resultSchema, options);
    }
    const tools: SchemaOutput<typeof ListToolsResultSchema>["tools"] = [];
    const cursors = new Set<string>();
    if (typeof request.params?.cursor === "string") cursors.add(request.params.cursor);
    let bytes = 2; // JSON array brackets; count UTF-8 bytes, not JS characters.
    const deadline = Date.now() + (options?.timeout ?? MCP_DISCOVERY_TIMEOUT_MS);
    for (let page = 1; ; page++) {
      options?.signal?.throwIfAborted();
      if (page > (this.limits.maxPages ?? MCP_DISCOVERY_MAX_PAGES)) throw new McpDiscoveryError("page_bound", page - 1, tools.length);
      if (Date.now() >= deadline) throw new DOMException("MCP discovery deadline exceeded", "TimeoutError");
      const listed: SchemaOutput<typeof ListToolsResultSchema> = await super.request(request, ListToolsResultSchema, { ...options, timeout: Math.max(1, deadline - Date.now()) });
      options?.signal?.throwIfAborted();
      if (tools.length + listed.tools.length > (this.limits.maxTools ?? MCP_DISCOVERY_MAX_TOOLS)) throw new McpDiscoveryError("tool_bound", page, tools.length + listed.tools.length);
      for (const tool of listed.tools) {
        bytes += Buffer.byteLength(JSON.stringify(tool), "utf8") + (tools.length === 0 ? 0 : 1);
        if (bytes > (this.limits.maxBytes ?? MCP_DISCOVERY_MAX_BYTES)) throw new McpDiscoveryError("byte_bound", page, tools.length);
        tools.push(tool);
      }
      if (listed.nextCursor === undefined) {
        // Preserve the caller's result-schema validation as well as per-page MCP validation.
        const parsed = safeParse(resultSchema, { ...listed, tools });
        if (!parsed.success) throw new McpDiscoveryError("invalid_response", page, tools.length);
        return parsed.data;
      }
      if (cursors.has(listed.nextCursor)) throw new McpDiscoveryError("repeated_cursor", page, tools.length);
      cursors.add(listed.nextCursor);
      request = { ...request, params: { ...request.params, cursor: listed.nextCursor } };
    }
  }
}
