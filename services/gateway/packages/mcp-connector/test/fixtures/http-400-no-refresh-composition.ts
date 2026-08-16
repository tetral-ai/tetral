import { createServer } from "node:http";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { Server as McpServer } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { McpSDKClient } from "../../src/client.js";
import type { GitHubMcpCredentialResolver } from "../../src/credential.js";

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
	const body = JSON.parse(encodedBody) as {
		readonly method?: string;
	};
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
		mcpServer = new McpServer(
			{ name: "http-400-proof", version: "1.0.0" },
			{ capabilities: { tools: {} } },
		);
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
	throw new Error("HTTP fixture did not bind a TCP port");
}
const endpoint = new URL(`http://127.0.0.1:${address.port}/mcp`);
let refreshes = 0;
const credentialResolver: GitHubMcpCredentialResolver = {
	resolve: async () => ({
		ok: true,
		mode: "bearer",
		token: "fixture-token",
		tokenHash: "fixture-token-hash",
		vaultId: "vlt_http_400",
		credentialId: "cred_http_400",
	}),
	refresh: async () => {
		refreshes += 1;
		return {
			ok: true,
			mode: "bearer",
			token: "unexpected-refreshed-token",
			tokenHash: "unexpected-refreshed-token-hash",
			vaultId: "vlt_http_400",
			credentialId: "cred_http_400",
		};
	},
};
const client = new McpSDKClient({
	credentialResolver,
	onToolsListChanged: async () => undefined,
	createTransport: () => new StreamableHTTPClientTransport(endpoint),
	idleTimeoutMs: 60_000,
	callTimeoutMs: 2_000,
	connectTimeoutMs: 2_000,
});

let errorCode: string | number | undefined;
let retryStatus: string | undefined;
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
	const typed = error as {
		readonly code?: string | number;
		readonly retryStatus?: string;
	};
	errorCode = typed.code;
	retryStatus = typed.retryStatus;
} finally {
	await client.closeAll();
	await mcpServer?.close();
	await new Promise<void>((resolve, reject) =>
		httpServer.close((error) => (error === undefined ? resolve() : reject(error))),
	);
}

process.stdout.write(
	`${JSON.stringify({
		ordinaryBadRequests,
		refreshes,
		errorCode,
		retryStatus,
		authClassified: errorCode === "mcp_authentication_failed",
	})}\n`,
);
