/** The Go fixture copies this file beside a run-owned `sdk` symlink to the
 * immutable, runner-selected checkout. Both imports and every request/parser
 * therefore use that actual SDK; the Engine does not vendor or patch it. */
import Tetral from "./sdk/src/index";
import type { BetaManagedAgentsStreamSessionEvents } from "./sdk/src/resources/beta/sessions/events";
import type { BetaManagedAgentsStreamSessionThreadEvents } from "./sdk/src/resources/beta/sessions/threads/threads";
import type { BetaManagedAgentsDeltaType } from "./sdk/src/resources/beta/sessions/sessions";
import { readFile } from "node:fs/promises";
import { createInterface } from "node:readline";

type PublicEvent = BetaManagedAgentsStreamSessionEvents | BetaManagedAgentsStreamSessionThreadEvents;
interface Viewer {
	controller: AbortController;
	events: PublicEvent[];
	fieldNames: Set<string>;
	heartbeats: number;
	bytes: number;
	ended: boolean;
	error: string | null;
	joined: Promise<void>;
	wake: Set<() => void>;
}
type Command = Record<string, unknown> & { id: string; operation: string };
const bootstrap: unknown = JSON.parse(await readFile(process.argv[2]!, "utf8"));
function object(value: unknown): Record<string, unknown> {
	if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error("fixture object required");
	return value as Record<string, unknown>;
}
function text(value: unknown): string {
	if (typeof value !== "string" || value.length === 0) throw new Error("fixture string required");
	return value;
}
const config = object(bootstrap);
const ca = config.caPath === undefined ? undefined : await readFile(text(config.caPath), "utf8");
const viewers = new Map<string, Viewer>();
const requests: { path: string; query: [string, string][] }[] = [];
let opening: Viewer | undefined;

// Observe only SSE field names and comments. This never parses event bodies or
// changes bytes; the imported SDK remains the sole SSE/JSON event parser.
function observeFields(viewer: Viewer): TransformStream<Uint8Array, Uint8Array> {
	let prefix = "";
	let inValue = false;
	return new TransformStream({
		transform(chunk, controller) {
			for (const byte of chunk) {
				if (byte === 10) {
					prefix = "";
					inValue = false;
				} else if (!inValue) {
					if (byte === 58) {
						if (prefix === "") viewer.heartbeats += 1;
						else viewer.fieldNames.add(prefix);
						inValue = true;
					} else if (prefix.length < 32 && byte !== 13) {
						prefix += String.fromCharCode(byte);
					} else if (prefix.length >= 32) {
						throw new Error("fixture SSE field prefix exceeded");
					}
				}
			}
			controller.enqueue(chunk);
		},
	});
}
const client = new Tetral({
	baseURL: text(config.baseURL),
	apiKey: text(config.apiKey),
	maxRetries: 0,
	timeout: 90_000,
	fetch: async (input, init) => {
		const request = new Request(input, init);
		const url = new URL(request.url);
		requests.push({ path: url.pathname, query: [...url.searchParams] });
		const viewer = opening;
		const response = ca === undefined ? await fetch(request) : await Bun.fetch(request, { tls: { ca } });
		if (viewer === undefined || response.body === null || !url.pathname.endsWith("/stream")) return response;
		return new Response(response.body.pipeThrough(observeFields(viewer)), {
			status: response.status,
			statusText: response.statusText,
			headers: response.headers,
		});
	},
});

function viewerFor(command: Command): Viewer {
	const viewer = viewers.get(text(command.viewer));
	if (viewer === undefined) throw new Error("fixture viewer not found");
	return viewer;
}
function previewTypes(value: unknown): BetaManagedAgentsDeltaType[] | undefined {
	if (value === undefined) return undefined;
	if (!Array.isArray(value)) throw new Error("fixture preview types required");
	return value.map((entry: unknown) => {
		if (entry !== "agent.message" && entry !== "agent.thinking") throw new Error("unsupported fixture preview type");
		return entry;
	});
}
function record(viewer: Viewer, event: PublicEvent): void {
	// These typed accesses pin the exact SDK surface used by the composition.
	if (event.type === "event_start") {
		text(event.event.id);
		if (event.event.type !== "agent.message" && event.event.type !== "agent.thinking") throw new Error("unexpected SDK start type");
	} else if (event.type === "event_delta") {
		text(event.event_id);
		if (event.delta.type !== "content_delta" || event.delta.index !== 0 || event.delta.content.type !== "text") throw new Error("unexpected SDK delta shape");
	}
	viewer.bytes += Buffer.byteLength(JSON.stringify(event));
	if (viewer.events.length >= 8192 || viewer.bytes > 64 * 1024 * 1024) throw new Error("fixture capture budget exceeded");
	viewer.events.push(event);
	for (const wake of viewer.wake) wake();
}
async function consume(viewer: Viewer, stream: AsyncIterable<PublicEvent>): Promise<void> {
	try {
		for await (const event of stream) record(viewer, event);
	} catch {
		if (!viewer.controller.signal.aborted) viewer.error = "sdk_stream_failed";
	} finally {
		viewer.ended = true;
		for (const wake of viewer.wake) wake();
	}
}
function snapshot(viewer: Viewer) {
	return { events: viewer.events, fieldNames: [...viewer.fieldNames].sort(), heartbeats: viewer.heartbeats, ended: viewer.ended, error: viewer.error };
}
async function waitEvent(viewer: Viewer, eventType: string, count: number): Promise<void> {
	if (!Number.isSafeInteger(count) || count < 1) throw new Error("fixture wait count invalid");
	await new Promise<void>((resolve, reject) => {
		const deadline = setTimeout(() => finish(new Error("fixture event deadline exceeded")), 30_000);
		const check = () => {
			if (viewer.events.filter((event) => event.type === eventType).length >= count) finish();
			else if (viewer.ended) finish(new Error("fixture stream ended before expected event"));
		};
		const finish = (error?: Error) => {
			clearTimeout(deadline);
			viewer.wake.delete(check);
			if (error === undefined) resolve(); else reject(error);
		};
		viewer.wake.add(check);
		check();
	});
}
async function closeViewer(viewer: Viewer): Promise<void> {
	viewer.controller.abort();
	await viewer.joined;
}
async function execute(command: Command): Promise<unknown> {
	switch (command.operation) {
		case "open": {
			const name = text(command.viewer);
			if (viewers.has(name)) throw new Error("fixture viewer reused");
			const viewer: Viewer = { controller: new AbortController(), events: [], fieldNames: new Set(), heartbeats: 0, bytes: 0, ended: false, error: null, joined: Promise.resolve(), wake: new Set() };
			viewers.set(name, viewer);
			const before = requests.length;
			opening = viewer;
			try {
				if (command.threadId === undefined) {
					const types = previewTypes(command.eventDeltas);
					const response = await client.beta.sessions.events.stream(text(command.sessionId), types === undefined ? {} : { event_deltas: types }, { signal: viewer.controller.signal }).withResponse();
					viewer.joined = consume(viewer, response.data);
				} else {
					if (command.eventDeltas !== undefined) throw new Error("Thread SDK has no preview option");
					const response = await client.beta.sessions.threads.events.stream(text(command.threadId), { session_id: text(command.sessionId) }, { signal: viewer.controller.signal }).withResponse();
					viewer.joined = consume(viewer, response.data);
				}
			} finally {
				opening = undefined;
			}
			return { opened: true, requests: requests.slice(before) };
		}
		case "snapshot": return snapshot(viewerFor(command));
		case "wait_event": {
			const viewer = viewerFor(command);
			await waitEvent(viewer, text(command.eventType), command.count === undefined ? 1 : Number(command.count));
			return snapshot(viewer);
		}
		case "wait_count": {
			const viewer = viewerFor(command);
			const kind = text(command.eventType);
			await waitEvent(viewer, kind, Number(command.count));
			return { count: viewer.events.filter((event) => event.type === kind).length };
		}
		case "close_viewer": {
			const viewer = viewerFor(command);
			await closeViewer(viewer);
			return snapshot(viewer);
		}
		case "list": {
			const order = command.order === "desc" ? "desc" : "asc";
			const limit = command.limit === undefined ? 1 : Number(command.limit);
			const page = command.page === undefined ? {} : { page: text(command.page) };
			if (command.threadId === undefined) {
				const result = await client.beta.sessions.events.list(text(command.sessionId), { limit, order, ...page });
				return { data: result.data, next_page: result.next_page };
			}
			if (command.order !== undefined) throw new Error("Thread SDK list has no order parameter");
			const result = await client.beta.sessions.threads.events.list(text(command.threadId), { session_id: text(command.sessionId), limit, ...page });
			return { data: result.data, next_page: result.next_page };
		}
		case "send": return await client.beta.sessions.events.send(text(command.sessionId), { events: [{ type: "user.message", content: [{ type: "text", text: text(command.text) }] }] });
		case "confirm": {
			if (command.result !== "allow" && command.result !== "deny") throw new Error("fixture confirmation result invalid");
			return await client.beta.sessions.events.send(text(command.sessionId), { events: [{ type: "user.tool_confirmation", tool_use_id: text(command.toolUseEventId), result: command.result }] });
		}
		case "interrupt": return await client.beta.sessions.events.send(text(command.sessionId), { events: [{ type: "user.interrupt" }] });
		case "close": {
			await Promise.all([...viewers.values()].map(closeViewer));
			return { joined: true, viewers: viewers.size };
		}
		default: throw new Error("fixture operation invalid");
	}
}

const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
try {
	for await (const line of lines) {
		const parsed = object(JSON.parse(line) as unknown);
		const command: Command = { ...parsed, id: text(parsed.id), operation: text(parsed.operation) };
		try {
			const result = await execute(command);
			process.stdout.write(`${JSON.stringify({ id: command.id, ok: true, result })}\n`);
		} catch {
			// SDK errors may retain authenticated requests. Do not serialize them.
			process.stdout.write(`${JSON.stringify({ id: command.id, ok: false, error: "public_streaming_fixture_failed" })}\n`);
		}
		if (command.operation === "close") break;
	}
} finally {
	lines.close();
	await Promise.all([...viewers.values()].map(closeViewer));
}
