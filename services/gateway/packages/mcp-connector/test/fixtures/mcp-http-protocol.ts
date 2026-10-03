import type { Tool } from "@modelcontextprotocol/sdk/types.js";


export type FixtureAdapter = "github" | "slack";
export type FixtureResult = "valid" | "wrong-type" | "missing" | "plain-text" | "tool-error" | "error-malformed" | "jsonrpc-401" | "jsonrpc-403";
export interface ProtocolCounts { initialize: number; list: number; call: number; effects: number; cancelledCalls: number; notifications: number }
export interface ProtocolRequest { method: string; session: string; origin: string; protocolVersion?: string; accept: string; contentType: string; cursor?: string; nonce?: string; tool?: string; credentialLabel: string; toolset?: string }

/** Controlled protocol peer. The production SDK owns all HTTP parsing and validation. */
export class McpHTTPProtocolFixture {
	readonly counts: ProtocolCounts = { initialize: 0, list: 0, call: 0, effects: 0, cancelledCalls: 0, notifications: 0 };
	readonly requests: ProtocolRequest[] = [];
	readonly server: ReturnType<typeof Bun.serve>;
	tools: Tool[];
	result: FixtureResult = "valid";
	rejectUnknownTools = false;
	notificationsEnabled = true;
	effectBeforeHeldResponse = true;
	pages: { tools: Tool[]; nextCursor?: string }[] | undefined;
	readonly credentials = new Map<string, string>();
	readonly faults = new Map<string, number[]>();
	faultBody = "controlled protocol rejection";
	#sessionCounter = 0;
	#streams = new Set<ReadableStreamDefaultController<Uint8Array>>();
	#held = new Map<string, { entered: Barrier; release: Barrier }>();
	#encoder = new TextEncoder();
	constructor(readonly adapter: FixtureAdapter) {
		this.tools = fixtureTools();
		this.server = Bun.serve({ hostname: "127.0.0.1", port: 0, idleTimeout: 0, fetch: (request) => this.#handle(request) });
	}
	get url(): URL { return new URL("/mcp", this.server.url); }
	get pendingRequests(): number { return this.server.pendingRequests; }
	get notificationStreamCount(): number { return this.#streams.size; }
	resetCounts(): void { for (const key of Object.keys(this.counts) as (keyof ProtocolCounts)[]) this.counts[key] = 0; this.requests.length = 0; }
	hold(method: string, cursor?: string): { entered: Promise<void>; release: () => void } {
		const entered = barrier(), release = barrier();
		const key = cursor === undefined ? method : `${method}\0${cursor}`;
		this.#held.set(key, { entered, release });
		return { entered: entered.promise, release: () => { this.#held.delete(key); release.resolve(); } };
	}
	async notify(): Promise<void> {
		if (this.#streams.size === 0) throw new Error("SDK notification stream is not attached");
		this.counts.notifications += 1;
		for (const stream of this.#streams) stream.enqueue(this.#encoder.encode(`event: message\ndata: ${JSON.stringify({ jsonrpc: "2.0", method: "notifications/tools/list_changed" })}\n\n`));
	}
	async waitForNotificationStream(): Promise<void> { await until(() => this.#streams.size > 0); }
	async close(): Promise<void> {
		for (const hold of this.#held.values()) hold.release.resolve();
		this.#held.clear();
		for (const stream of this.#streams) { try { stream.close(); } catch {} }
		this.#streams.clear();
		// Let completed SSE response bodies leave the server before closing its
		// listener. Force-stopping while a stream is draining can retain a request.
		await until(() => this.server.pendingRequests === 0);
		await this.server.stop(true);
	}
	async #handle(request: Request): Promise<Response> {
		if (request.method === "GET") {
			if (!this.notificationsEnabled) return new Response(null, { status: 405 });
			let controller!: ReadableStreamDefaultController<Uint8Array>;
			const stream = new ReadableStream<Uint8Array>({
				start: (value) => { controller = value; this.#streams.add(value); value.enqueue(this.#encoder.encode(": ready\n\n")); request.signal.addEventListener("abort", () => { this.#streams.delete(value); try { value.close(); } catch {} }, { once: true }); },
				cancel: () => { this.#streams.delete(controller); },
			});
			return new Response(stream, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache" } });
		}
		if (request.method === "DELETE") return new Response(null, { status: 200 });
		if (request.method !== "POST") return new Response(null, { status: 405 });
		const message = await request.json() as { id?: string | number; method: string; params?: { cursor?: string; name?: string; arguments?: { nonce?: string } } };
		if (message.id === undefined) return new Response(null, { status: 202 });
		const method = message.method;
		const session = method === "initialize" ? `${this.adapter}-session-${++this.#sessionCounter}` : request.headers.get("mcp-session-id") ?? "missing-session";
		const authorization = request.headers.get("authorization") ?? "";
		const credentialLabel = this.credentials.get(authorization) ?? "unrecognized";
		const record: ProtocolRequest = { method, session, origin: request.headers.get("x-fixture-origin") ?? "direct-sdk", accept: request.headers.get("accept") ?? "", contentType: request.headers.get("content-type") ?? "", ...(request.headers.get("mcp-protocol-version") === null ? {} : { protocolVersion: request.headers.get("mcp-protocol-version")! }), credentialLabel, ...(message.params?.cursor === undefined ? {} : { cursor: message.params.cursor }), ...(message.params?.name === undefined ? {} : { tool: message.params.name }), ...(message.params?.arguments?.nonce === undefined ? {} : { nonce: message.params.arguments.nonce }), ...(request.headers.get("x-mcp-toolsets") === null ? {} : { toolset: request.headers.get("x-mcp-toolsets")! }) };
		this.requests.push(record);
		const countKey = method === "initialize" ? "initialize" : method === "tools/list" ? "list" : method === "tools/call" ? "call" : undefined;
		if (countKey !== undefined) this.counts[countKey] += 1;
		const fault = this.faults.get(`${method}\0${record.origin}`)?.shift() ?? this.faults.get(method)?.shift();
		if (fault !== undefined && fault !== 0) return new Response(this.faultBody, { status: fault });
		if (method === "tools/call" && this.rejectUnknownTools && !this.tools.some(tool => tool.name === message.params?.name)) return this.#json({ jsonrpc: "2.0", id: message.id, error: { code: -32602, message: "controlled tool name is unavailable" } }, session);
		if (method === "tools/call" && this.effectBeforeHeldResponse) this.counts.effects += 1;
		const hold = this.#held.get(`${method}\0${message.params?.cursor ?? ""}`) ?? this.#held.get(method);
		if (hold !== undefined) {
			hold.entered.resolve();
			let onAbort!: () => void;
			try { await Promise.race([hold.release.promise, new Promise<void>((resolve) => {
				onAbort = () => { if (method === "tools/call") this.counts.cancelledCalls += 1; resolve(); };
				if (request.signal.aborted) { onAbort(); return; }
				request.signal.addEventListener("abort", onAbort, { once: true });
			})]); } finally { request.signal.removeEventListener("abort", onAbort); }
			if (request.signal.aborted) return new Response(null, { status: 499 });
		}
		let result: unknown;
		if (method === "initialize") result = { protocolVersion: "2025-11-25", capabilities: { tools: { listChanged: true } }, serverInfo: { name: `${this.adapter}-fixture`, version: "1" } };
		else if (method === "tools/list") {
			if (this.pages === undefined) result = { tools: this.tools };
			else {
				const cursor = message.params?.cursor;
				const page = cursor === undefined ? this.pages[0] : this.pages[this.pages.findIndex((item) => item.nextCursor === cursor) + 1];
				if (page === undefined) throw new Error("unexpected fixture cursor");
				result = page;
			}
		}
		else if (method === "tools/call") {
			if (!this.effectBeforeHeldResponse) this.counts.effects += 1;
			if (this.result === "jsonrpc-401" || this.result === "jsonrpc-403") return this.#json({ jsonrpc: "2.0", id: message.id, error: { code: this.result === "jsonrpc-401" ? 401 : 403, message: "controlled application rejection" } }, session);
			const structuredContent = { ok: this.result === "wrong-type" || this.result === "error-malformed" ? "invalid" : true, source: `${this.adapter}-fixture`, nonce: message.params?.arguments?.nonce ?? "" };
			result = this.result === "missing" || this.result === "plain-text" ? { content: [{ type: "text", text: this.result === "missing" ? "missing structured result" : "plain tool output" }], isError: false } : this.result === "tool-error" ? { content: [{ type: "text", text: "controlled tool error" }], isError: true } : { content: [{ type: "text", text: JSON.stringify(structuredContent) }], structuredContent, ...(this.result === "error-malformed" ? { isError: true } : {}) };
		} else return this.#json({ jsonrpc: "2.0", id: message.id, error: { code: -32601, message: "unknown fixture method" } }, session);
		return this.#json({ jsonrpc: "2.0", id: message.id, result }, session);
	}
	#json(value: unknown, session: string): Response { return Response.json(value, { headers: { "mcp-session-id": session } }); }
}

export function fixtureTools(version: "v1" | "v2" | "removed" | "output-only" | "no-output" = "v1"): Tool[] {
	const outputSchema = { type: "object" as const, properties: { ok: { type: version === "output-only" ? "string" : "boolean" }, source: { type: "string" }, nonce: { type: "string" } }, required: ["ok", "source", "nonce"], additionalProperties: false };
	const echo: Tool = { name: version === "removed" ? "read_echo_v2" : "read_echo", description: "Echo the supplied nonce.", inputSchema: { type: "object", properties: { nonce: { type: "string" } }, required: ["nonce"], additionalProperties: false }, ...(version === "no-output" ? {} : { outputSchema }) };
	return version === "v2" ? [echo, { ...echo, name: "read_extra", description: "Read an extra fixture value." }] : [echo];
}
export interface Barrier { promise: Promise<void>; resolve: () => void }
export function barrier(): Barrier { let resolve!: () => void; const promise = new Promise<void>((done) => { resolve = done; }); return { promise, resolve }; }
export async function until(predicate: () => boolean, timeoutMs = 5_000): Promise<void> { const end = Date.now() + timeoutMs; while (!predicate()) { if (Date.now() >= end) throw new Error("fixture phase did not settle"); await Bun.sleep(1); } }
