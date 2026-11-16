import { expect, test } from "bun:test";
import { createServer } from "node:http";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { Server as McpServer } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { isInitializeRequest, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { McpSDKClient } from "../../src/client.js";
import type { McpCredentialResolver } from "../../src/credential.js";
import { fixtureServerResolver } from "../fixtures/registered-server.js";

test("an ordinary MCP HTTP 400 never consumes the authentication refresh budget", async () => {
	// A stand-in MCP server that initializes and lists one tool over the real
	// transport, and answers every tool call with a plain HTTP 400.
	let transport: StreamableHTTPServerTransport | undefined;
	let mcpServer: McpServer | undefined;
	let ordinaryBadRequests = 0;
	const httpServer = createServer(async (request, response) => {
		const chunks: Buffer[] = [];
		for await (const chunk of request) {
			chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
		}
		const encodedBody = Buffer.concat(chunks).toString("utf8");
		if (encodedBody.length === 0) {
			if (transport === undefined) {
				response.writeHead(400).end();
				return;
			}
			await transport.handleRequest(request, response);
			return;
		}
		const body = JSON.parse(encodedBody) as { readonly method?: string };
		if (body.method === "tools/call") {
			ordinaryBadRequests += 1;
			response.writeHead(400, { "content-type": "application/json" });
			response.end(JSON.stringify({ error: "ordinary request rejection" }));
			return;
		}
		if (transport === undefined && isInitializeRequest(body)) {
			transport = new StreamableHTTPServerTransport({
				sessionIdGenerator: () => "mcp-http-400-session",
				enableJsonResponse: true,
			});
			mcpServer = new McpServer({ name: "http-400-proof", version: "1.0.0" }, { capabilities: { tools: {} } });
			mcpServer.setRequestHandler(ListToolsRequestSchema, async () => ({
				tools: [{ name: "create_issue", inputSchema: { type: "object" } }],
			}));
			await mcpServer.connect(transport as unknown as Transport);
		}
		if (transport === undefined) {
			response.writeHead(400, { "content-type": "application/json" });
			response.end(JSON.stringify({ error: "missing MCP session" }));
			return;
		}
		await transport.handleRequest(request, response, body);
	});
	await new Promise<void>((resolve, reject) => {
		httpServer.once("error", reject);
		httpServer.listen(0, "127.0.0.1", () => resolve());
	});
	const address = httpServer.address();
	if (address === null || typeof address === "string") {
		throw new Error("the stand-in MCP server bound no TCP port");
	}
	const endpoint = new URL(`http://127.0.0.1:${address.port}/mcp`);

	let refreshes = 0;
	const credential = {
		ok: true,
		mode: "bearer",
		token: "fixture-token",
		tokenHash: "fixture-token-hash",
		vaultId: "vlt_http_400",
		credentialId: "cred_http_400",
	} as const;
	const credentialResolver: McpCredentialResolver = {
		resolve: async () => credential,
		refresh: async () => {
			refreshes += 1;
			return { ...credential, token: "unexpected-refreshed-token", tokenHash: "unexpected-refreshed-token-hash" };
		},
	};
	const client = new McpSDKClient({
		serverResolver: fixtureServerResolver,
		credentialResolver,
		onToolsListChanged: async () => undefined,
		createTransport: () => new StreamableHTTPClientTransport(endpoint),
		idleTimeoutMs: 60_000,
		callTimeoutMs: 2_000,
		connectTimeoutMs: 2_000,
	});

	let failure: { readonly code?: string | number; readonly retryStatus?: string } | undefined;
	try {
		await client.callTool({
			workspaceId: "wksp_http_400",
			sessionId: "sesn_http_400",
			sessionThreadId: "thrd_http_400",
			mcpServerName: "github",
			toolName: "create_issue",
			input: { title: "ordinary bad request" },
		});
	} catch (error) {
		failure = error as { readonly code?: string | number; readonly retryStatus?: string };
	} finally {
		await client.closeAll();
		await mcpServer?.close();
		await new Promise<void>((resolve, reject) =>
			httpServer.close((error) => (error === undefined ? resolve() : reject(error))),
		);
	}

	expect(ordinaryBadRequests).toBe(1);
	expect(refreshes).toBe(0);
	// 400 is the HTTP status, not the code of an authentication failure.
	expect(failure?.code).toBe(400);
	expect(failure?.retryStatus).toBeUndefined();
}, 10_000);
