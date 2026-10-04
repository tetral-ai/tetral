/** Go-owned full-topology driver using the separately pinned public Tetral SDK. */
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { z } from "zod/v4";

const bootstrap = z.strictObject({
	baseURL: z.url(),
	apiKey: z.string().min(1),
}).parse(JSON.parse(await readFile(process.argv[2]!, "utf8")) as unknown);
const sdkRoot = process.env.TETRAL_ENGINE_SDK_ROOT;
if (sdkRoot === undefined || sdkRoot.length === 0) throw new Error("SDK root required");

// This structural port describes the actual imported SDK methods; every network
// operation below is performed by that pinned SDK, including multipart upload.
interface SDKClient {
	readonly beta: {
		readonly agents: { create(params: Readonly<Record<string, unknown>>): Promise<{ id: string; version: number }> };
		readonly files: { upload(params: { file: File }): Promise<unknown> };
		readonly sessions: {
			create(params: unknown): Promise<unknown>;
			retrieve(id: string): Promise<unknown>;
			readonly events: {
				send(id: string, params: unknown): Promise<unknown>;
				list(id: string, params: { limit: number }): AsyncIterable<unknown>;
			};
		};
	};
}
const module = await import(pathToFileURL(join(sdkRoot, "src/index.ts")).href) as {
	readonly default: new (options: { baseURL: string; apiKey: string; maxRetries: number; timeout: number }) => SDKClient;
};
const client = new module.default({ ...bootstrap, maxRetries: 0, timeout: 30_000 });
const identity = { id: z.string().min(1) };
const session = { ...identity, sessionId: z.string().min(1) };
const commandSchema = z.discriminatedUnion("operation", [
	z.strictObject({ ...identity, operation: z.literal("provision"), agent: z.record(z.string(), z.unknown()), environmentId: z.string().min(1), vaultIds: z.array(z.string().min(1)).optional() }),
	z.strictObject({ ...identity, operation: z.literal("upload_png") }),
	z.strictObject({ ...session, operation: z.literal("send"), text: z.string().min(1), fileId: z.string().min(1).optional() }),
	z.strictObject({ ...session, operation: z.literal("confirm"), toolUseEventId: z.string().min(1), result: z.enum(["allow", "deny"]), denyMessage: z.string().optional() }),
	z.strictObject({ ...session, operation: z.literal("events") }),
	z.strictObject({ ...session, operation: z.literal("interrupt") }),
	z.strictObject({ ...session, operation: z.literal("session") }),
	z.strictObject({ ...identity, operation: z.literal("close") }),
]);
// One fixed valid 1x1 PNG; the Go owner independently checks uploaded bytes.
const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==", "base64");
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
try {
	for await (const line of lines) {
		const command = commandSchema.parse(JSON.parse(line) as unknown);
		try {
			let result: unknown;
			switch (command.operation) {
				case "provision": {
					const agent = await client.beta.agents.create(command.agent);
					const createdSession = await client.beta.sessions.create({ agent: { type: "agent", id: agent.id, version: agent.version }, environment_id: command.environmentId, vault_ids: command.vaultIds ?? [] });
					result = { agent, session: createdSession };
					break;
				}
				case "upload_png": result = await client.beta.files.upload({ file: new File([png], "content-lifecycle-pixel.png", { type: "image/png" }) }); break;
				case "send": result = await client.beta.sessions.events.send(command.sessionId, { events: [{ type: "user.message", content: [{ type: "text", text: command.text }, ...(command.fileId === undefined ? [] : [{ type: "image", source: { type: "file", file_id: command.fileId } }])] }] }); break;
				case "confirm": {
					if (command.result === "allow" && command.denyMessage !== undefined) throw new Error("deny message requires deny");
					result = await client.beta.sessions.events.send(command.sessionId, { events: [{ type: "user.tool_confirmation", tool_use_id: command.toolUseEventId, result: command.result, ...(command.denyMessage === undefined ? {} : { deny_message: command.denyMessage }) }] });
					break;
				}
				case "interrupt": result = await client.beta.sessions.events.send(command.sessionId, { events: [{type:"user.interrupt"}] }); break;
				case "events": {
					const events: unknown[] = [];
					for await (const event of client.beta.sessions.events.list(command.sessionId, { limit: 100 })) {
						if (events.length >= 1000) throw new Error("fixture event census exceeded");
						events.push(event);
					}
					result = events;
					break;
				}
				case "session": result = await client.beta.sessions.retrieve(command.sessionId); break;
				case "close": result = { joined: true }; break;
			}
			process.stdout.write(`${JSON.stringify({ id: command.id, ok: true, result })}\n`);
		} catch {
			// SDK errors can retain authenticated request details; never serialize them.
			process.stdout.write(`${JSON.stringify({ id: command.id, ok: false, error: "sdk_operation_failed" })}\n`);
		}
		if (command.operation === "close") break;
	}
} finally {
	lines.close();
}
