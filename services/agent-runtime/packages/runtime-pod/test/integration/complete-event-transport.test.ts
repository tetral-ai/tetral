import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { Metadata, status } from "@grpc/grpc-js";
import {
	ProviderFinishReason,
	ProviderRequest,
	ProviderRequestKind,
	ProviderStreamEvent,
	ProviderStreamEventType,
	ProviderThreadRole,
	ProviderThreadVisibility,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import { MaxProviderResponseFrameBytes } from "@tetral/gateway-protocol/src/bounds.js";
import { createLLMService } from "../../../core/src/llm/llm-service.js";
import type { LLMServiceStreamObserver } from "../../../core/src/llm/llm-service.js";
import { gatewayGrpcChannelOptions } from "../../src/bounds.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";
import {
	retiredProviderFrame,
	sizedProviderFrame,
	startDirectContentWireGateway,
} from "../harness/complete-event-wire-harness.js";
import type { DirectContentWireScenario } from "../harness/complete-event-wire-harness.js";

const textEventId = "evt_00000000000000000000000000000001";
const thinkingEventId = "evt_00000000000000000000000000000002";

function request(kind = ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST): ProviderRequest {
	const reviewer = kind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER || kind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION;
	return ProviderRequest.fromPartial({
		requestId: "req_complete_wire", modelRequestId: "mreq_complete_wire",
		workspaceId: "wksp_complete_wire", sessionId: "sesn_complete_wire", sessionThreadId: "thr_complete_wire",
		bindingId: "bind_complete_wire", bindingGeneration: 1, runtimeBindingToken: "wire-fixture-token", runtimeProcessId: "process_complete_wire",
		outputContractVersion: 2, modelRequestStartEventId: "evt_00000000000000000000000000000003",
		threadRole: reviewer ? ProviderThreadRole.PROVIDER_THREAD_ROLE_APPROVAL_REVIEWER : ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,
		threadVisibility: reviewer ? ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_INTERNAL : ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC,
		requestKind: kind, model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
		limits: { maxOutputTokens: 128, timeoutMs: 10_000 },
		...(kind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER ? { outputSchemaJson: '{"type":"object"}' } : {}),
	});
}

function textFrame(sequence = 1, text = "alpha", providerPartId = "shared-provider-id", eventId = textEventId): ProviderStreamEvent {
	return ProviderStreamEvent.fromPartial({
		frameSequence: sequence, type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_COMPLETE,
		textComplete: { providerPartId, eventId, text },
	});
}

function thinkingFrame(sequence = 1): ProviderStreamEvent {
	return ProviderStreamEvent.fromPartial({
		frameSequence: sequence, type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_THINKING_STARTED,
		thinkingStarted: { providerPartId: "shared-provider-id", eventId: thinkingEventId },
	});
}

function reasoningFrame(sequence = 2, eventId = thinkingEventId): ProviderStreamEvent {
	return ProviderStreamEvent.fromPartial({
		frameSequence: sequence, type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_COMPLETE,
		reasoningComplete: { providerPartId: "shared-provider-id", thinkingEventId: eventId, text: "reason-before-text", providerMetadataJson: '{"anthropic":{"signature":"fixture-signature-text"}}' },
	});
}

function toolFrame(sequence = 4, inputJson = '{"file_path":"/workspace/note.txt"}'): ProviderStreamEvent {
	return ProviderStreamEvent.fromPartial({
		frameSequence: sequence, type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL_COMPLETE,
		toolCallComplete: { modelToolCallId: "call-read-note", name: "Read", inputJson, providerMetadataJson: "{}" },
	});
}

function finishFrame(sequence = 2): ProviderStreamEvent {
	return ProviderStreamEvent.fromPartial({
		frameSequence: sequence, type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
		finish: { reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP, metadataJson: "{}", contextWindowTokens: 200_000, outputTokenLimit: 128,
			usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, providerUsageJson: "{}" } },
	});
}

function encode(frame: ProviderStreamEvent): Buffer {
	return Buffer.from(ProviderStreamEvent.encode(frame).finish());
}

async function withWire<T>(scenario: DirectContentWireScenario, run: (client: RuntimePodGatewayClient) => Promise<T>): Promise<T> {
	const server = await startDirectContentWireGateway(scenario);
	const client = new RuntimePodGatewayClient({
		address: server.address, tokenPath: "/unused", channelOptions: gatewayGrpcChannelOptions(), metadataFactory: async () => new Metadata(),
	});
	try {
		return await run(client);
	} finally {
		try { await client.close(); } finally { await server.close(); }
	}
}

const consumerKinds = [
	["ordinary", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST],
	["reviewer", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER],
	["compaction", ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY],
	["reviewer compaction", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION],
] as const;

describe("complete content over actual Gateway wire", () => {
	for (const [name, kind] of consumerKinds) {
		test(`${name} validates complete content and preserves distinct block-kind identity`, async () => {
			const frames = [thinkingFrame(), reasoningFrame(), textFrame(3), toolFrame(), finishFrame(5)].map(encode);
			const events = await withWire({ frames }, async (client) => Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request(kind))))));
			expect(events.map((event) => event.type)).toEqual(["thinking-started", "reasoning-complete", "text-complete", "tool-call-complete", "finish"]);
			expect(events[1]).toMatchObject({ text: "reason-before-text", thinkingEventId, providerMetadata: { anthropic: { signature: "fixture-signature-text" } } });
			expect(events[2]).toMatchObject({ eventId: textEventId, text: "alpha" });
			expect(events[3]).toMatchObject({ id: "call-read-note", input: { file_path: "/workspace/note.txt" } });
		}, 15_000);

		for (let retiredType = 1; retiredType <= 10; retiredType++) {
			test(`${name} rejects retired content enum ${retiredType} on the protobuf wire`, async () => {
				const events = await withWire({ frames: [retiredProviderFrame(retiredType), encode(finishFrame())] }, async (client) => Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request(kind))))));
				expect(events).toHaveLength(1);
				expect(events[0]).toMatchObject({ type: "provider-error", error: { code: "gateway_protocol_error", retryable: false, fatal: true } });
			}, 15_000);
		}
	}

	for (const [name, frames] of [
		["zero sequence", [textFrame(0), finishFrame(1)]],
		["sequence gap", [textFrame(2), finishFrame(3)]],
		["duplicate sequence", [textFrame(), textFrame(1, "beta", "second", "evt_00000000000000000000000000000004"), finishFrame(2)]],
		["content after terminal", [finishFrame(1), textFrame(2)]],
		["duplicate terminal", [finishFrame(1), finishFrame(2)]],
		["missing terminal", [textFrame()]],
		["reasoning without start", [reasoningFrame(1), finishFrame(2)]],
		["wrong thinking identity", [thinkingFrame(), reasoningFrame(2, textEventId), finishFrame(3)]],
		["duplicate thinking block", [thinkingFrame(), thinkingFrame(2), finishFrame(3)]],
		["cross-kind event identity collision", [thinkingFrame(), reasoningFrame(), textFrame(3, "alpha", "text", thinkingEventId), finishFrame(4)]],
		["unfinished reasoning", [thinkingFrame(), finishFrame(2)]],
		["duplicate text identity", [textFrame(), textFrame(2, "beta", "second"), finishFrame(3)]],
		["duplicate text block", [textFrame(), textFrame(2, "beta", "shared-provider-id", "evt_00000000000000000000000000000004"), finishFrame(3)]],
		["empty complete text", [textFrame(1, ""), finishFrame(2)]],
		["malformed content identity", [textFrame(1, "alpha", "text", "not-an-event-id"), finishFrame(2)]],
		["malformed reasoning metadata", [thinkingFrame(), ProviderStreamEvent.fromPartial({ ...reasoningFrame(), reasoningComplete: { ...reasoningFrame().reasoningComplete!, providerMetadataJson: "{" } }), finishFrame(3)]],
		["malformed tool JSON", [toolFrame(1, "{"), finishFrame(2)]],
		["repeated complete Tool call", [toolFrame(1), toolFrame(2), finishFrame(3)]],
	] as const) {
		test(`rejects ${name} without successful completion`, async () => {
			const events = await withWire({ frames: frames.map(encode) }, async (client) => Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request())))));
			expect(events.at(-1)).toMatchObject({ type: "provider-error", error: { code: name === "missing terminal" ? "gateway_stream_error" : "gateway_protocol_error" } });
			expect(events.some((event) => event.type === "finish")).toBe(false);
		}, 15_000);
	}

	test("Finish observed before a transport error is not successful EOF", async () => {
		let releaseTerminal: (() => void) | undefined;
		const terminalObserved = new Promise<void>((resolve) => { releaseTerminal = resolve; });
		let observed = false;
		const observer: LLMServiceStreamObserver = { terminalObserved() { observed = true; releaseTerminal?.(); } };
		try {
			const failure = await withWire({ frames: [encode(textFrame()), encode(finishFrame())], afterFrames: () => terminalObserved, terminalError: { code: status.UNAVAILABLE, details: "controlled post-terminal disconnect" } }, async (client) => Effect.runPromise(Stream.runCollect(createLLMService(client, observer).stream(request())).pipe(Effect.flip)));
			expect(observed).toBe(true);
			expect(failure).toMatchObject({ type: "llm-service", error: { code: "gateway_stream_error" } });
		} finally {
			releaseTerminal?.();
		}
	}, 15_000);
});

// The Gateway's own tests send at this protocol bound through its server.
describe("encoded complete-frame transport fuse", () => {
	const fuse = MaxProviderResponseFrameBytes;
	for (const delta of [-1, 0, 1]) {
		test(`the Runtime client enforces the encoded stream-event bound ${delta}`, async () => {
			const vector = sizedProviderFrame(fuse + delta);
			expect(vector.frame.length).toBe(fuse + delta);
			const server = await startDirectContentWireGateway({ frames: [vector.frame] }, { "grpc.max_send_message_length": fuse * 2 });
			const client = new RuntimePodGatewayClient({ address: server.address, tokenPath: "/unused", channelOptions: gatewayGrpcChannelOptions(), metadataFactory: async () => new Metadata() });
			try {
				const handle = await client.streamProviderRequest(request());
				const received = Array.from(await Effect.runPromise(Stream.runCollect(handle.events)));
				const result = await handle.completion;
				if (delta > 0) expect(result).toMatchObject({ outcome: "transport_failure", error: { type: "gateway-client", code: "gateway_protocol_error", statusCode: status.RESOURCE_EXHAUSTED, retryable: false, fatal: true } });
				else {
					expect(result.outcome).toBe("eof");
					expect(received[0]?.textComplete?.text).toBe("x".repeat(vector.textBytes));
				}
				expect(received).toHaveLength(delta > 0 ? 0 : 1);
			} finally {
				try { await client.close(); } finally { await server.close(); }
			}
		}, 60_000);
	}
});

// Frames and final status the Gateway pins for each provider outcome (see
// testdata/README.md at the repository root), replayed to the Runtime.
const streamEventVectors = new URL("../../../../../../testdata/provider-stream-events/", import.meta.url);

type ServedVector = { readonly events: readonly unknown[]; readonly code: number; readonly details: string };

function replay<A>(name: string, kind: ProviderRequestKind, run: (stream: ReturnType<ReturnType<typeof createLLMService>["stream"]>) => Promise<A>): Promise<A> {
	const served = JSON.parse(readFileSync(new URL(`${name}.json`, streamEventVectors), "utf8")) as ServedVector;
	return withWire({
		frames: served.events.map((event) => encode(ProviderStreamEvent.fromJSON(event))),
		...(served.code === status.OK ? {} : { terminalError: { code: served.code, details: served.details } }),
	}, (client) => run(createLLMService(client).stream(request(kind))));
}

function events(name: string, kind = ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST) {
	return replay(name, kind, async (stream) => Array.from(await Effect.runPromise(Stream.runCollect(stream))));
}

function failure(name: string) {
	return replay(name, ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, (stream) => Effect.runPromise(Stream.runCollect(stream).pipe(Effect.flip)));
}

const streamEventRows: Record<string, () => Promise<void>> = {
	"sdk-interleaved": async () => {
		for (const [, kind] of consumerKinds) {
			const received = await events("sdk-interleaved", kind);
			expect(received.map((event) => event.type)).toEqual(["thinking-started", "reasoning-complete", "text-complete", "text-complete", "tool-call-complete", "finish"]);
			expect(received[1]).toMatchObject({ text: "private R1", providerMetadata: { anthropic: { signature: "fixture_signature" } } });
			expect(received[2]).toMatchObject({ text: "alpha" });
			expect(received[3]).toMatchObject({ text: "beta" });
			expect(received[4]).toMatchObject({ id: "call_fixture_1", toolName: "Read", input: { file_path: "/workspace/note.txt" } });
		}
	},
	"sdk-empty-text": async () => {
		for (const [, kind] of consumerKinds) expect((await events("sdk-empty-text", kind)).map((event) => event.type)).toEqual(["finish"]);
	},
	"sdk-empty-then-valid": async () => {
		for (const [, kind] of consumerKinds) {
			const received = await events("sdk-empty-then-valid", kind);
			expect(received.map((event) => event.type)).toEqual(["text-complete", "finish"]);
			expect(received[0]).toMatchObject({ text: "alpha" });
		}
	},
	"sdk-truncated": async () => {
		expect((await events("sdk-truncated")).map((event) => event.type)).toEqual(["provider-error"]);
	},
	"sdk-error-after-partial": async () => {
		expect((await events("sdk-error-after-partial")).map((event) => event.type)).toEqual(["provider-error"]);
	},
	"large-tool-input": async () => {
		const received = await events("large-tool-input");
		expect(received[0]).toMatchObject({
			type: "tool-call-complete",
			id: "call_large_memory_0",
			toolName: "memory",
			input: { action: "create", path: "notes/large.md", content: `CREATE_HEAD${"\u0001".repeat(9_000)}CREATE_TAIL` },
		});
		expect(received[1]).toMatchObject({
			type: "tool-call-complete",
			id: "call_large_memory_1",
			toolName: "memory",
			input: {
				action: "replace",
				path: "notes/large.md",
				old_text: `OLD_HEAD${"<".repeat(5_000)}OLD_TAIL`,
				new_text: `NEW_HEAD${"\\".repeat(5_000)}NEW_TAIL`,
			},
		});
		expect(received[2]).toMatchObject({ type: "finish", finishReason: "tool-calls" });
	},
	"recorded-glm": async () => {
		const received = await events("recorded-glm");
		expect(received.map((event) => event.type)).toEqual(["thinking-started", "reasoning-complete", "tool-call-complete", "text-complete", "finish"]);
		const reasoning = received[1];
		if (reasoning?.type !== "reasoning-complete") throw new Error("recorded reasoning missing");
		// Independently derived from the 66 recorded reasoning_content SSE deltas using Python.
		expect(Buffer.byteLength(reasoning.text, "utf8")).toBe(209);
		expect(createHash("sha256").update(reasoning.text).digest("hex")).toBe("2433ebc0071a6fcfeb780ea1efd0b72b91bc5e1be63044b1fda3379bb9574a5a");
		expect(received[0]).toMatchObject({ eventId: reasoning.thinkingEventId });
		expect(received[2]).toMatchObject({ toolName: "Search", input: { query: "tetral" } });
		expect(received[3]).toMatchObject({ text: "ok" });
		expect(received[4]).toMatchObject({ type: "finish", finishReason: "tool-calls" });
	},
	"zero-retry-delay": async () => {
		const terminal = (await events("zero-retry-delay")).at(-1);
		if (terminal?.type !== "provider-error") throw new Error("provider error missing");
		expect(terminal.error).toMatchObject({ type: "provider", code: "provider_unavailable", retryable: true, fatal: false });
		expect(terminal.error).not.toHaveProperty("retryAfterMs");
	},
	"pre-event-invalid-argument": async () => {
		expect(await failure("pre-event-invalid-argument")).toMatchObject({ type: "llm-service", error: { code: "gateway_protocol_error", retryable: false, fatal: true } });
	},
	"pre-event-unavailable": async () => {
		expect(await failure("pre-event-unavailable")).toMatchObject({ type: "llm-service", error: { code: "gateway_stream_error", retryable: true, fatal: false } });
	},
};

describe("Gateway stream-event vectors", () => {
	test("every vector has a row and every row a vector", () => {
		expect(readdirSync(streamEventVectors).filter((name) => name.endsWith(".json")).map((name) => name.slice(0, -".json".length)).sort()).toEqual(Object.keys(streamEventRows).sort());
	});
	for (const [name, row] of Object.entries(streamEventRows)) test(`the Runtime reads ${name}`, row, 30_000);

	// The Gateway's tests send these sizes through the actual SDK path.
	for (const [textCodeUnits, canonicalBytes, digest] of [
		[8_388_608, 8_388_610, "0c77bc0a0795a93612d45256897456d0fcb24f151c44c150d07ecd03f4ef5168"],
		[16_777_214, 16_777_216, "5e01c49110c2e2b22e70986eff62ca694e6f3dd681bab492099ff95c4fc6e96b"],
	] as const) {
		test(`the Runtime keeps ${canonicalBytes} canonical text bytes`, async () => {
			const received = await withWire({ frames: [encode(textFrame(1, "x".repeat(textCodeUnits))), encode(finishFrame(2))] }, async (client) => Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request())))));
			expect(received.map((event) => event.type)).toEqual(["text-complete", "finish"]);
			const text = received[0];
			if (text?.type !== "text-complete") throw new Error("complete text missing");
			expect(text.text.length).toBe(textCodeUnits);
			expect(Buffer.byteLength(JSON.stringify(text.text))).toBe(canonicalBytes);
			expect(createHash("sha256").update(text.text).digest("hex")).toBe(digest);
		}, 60_000);
	}
});
