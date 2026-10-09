import { describe, expect, test } from "bun:test";
import { Metadata, credentials, status } from "@grpc/grpc-js";
import {
	ProviderFinishReason,
	ProviderGatewayServiceClient,
	ProviderRequest,
	ProviderRequestKind,
	ProviderStreamEvent,
	ProviderStreamEventType,
	ProviderThreadRole,
	ProviderThreadVisibility,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import { grpcServerOptions as gatewayServerOptions } from "../../../../../gateway/packages/provider-gateway/src/bounds.js";
import { createLLMService } from "../../../core/src/llm/llm-service.js";
import type { LLMServiceStreamObserver } from "../../../core/src/llm/llm-service.js";
import { gatewayGrpcChannelOptions, MaxGatewayStreamEventGrpcMessageBytes } from "../../src/bounds.js";
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

describe("encoded complete-frame transport fuses", () => {
	const fuse = 32 * 1024 * 1024;
	for (const direction of ["Gateway sender", "Runtime receiver"] as const) {
		for (const delta of [-1, 0, 1]) {
			test(`${direction} enforces the exact 32 MiB encoded boundary ${delta}`, async () => {
				expect(MaxGatewayStreamEventGrpcMessageBytes).toBe(fuse);
				expect(gatewayServerOptions()["grpc.max_send_message_length"]).toBe(fuse);
				const vector = sizedProviderFrame(fuse + delta);
				expect(vector.frame.length).toBe(fuse + delta);
				const server = await startDirectContentWireGateway({ frames: [vector.frame] }, {
					...gatewayServerOptions(),
					...(direction === "Runtime receiver" ? { "grpc.max_send_message_length": fuse * 2 } : {}),
				});
				if (direction === "Gateway sender") {
					// Raise only the receiver to isolate the actual production
					// server fuse. No semantic validator runs in this raw fixture.
					const client = new ProviderGatewayServiceClient(server.address, credentials.createInsecure(), { "grpc.max_receive_message_length": fuse * 2 });
					try {
						const received: ProviderStreamEvent[] = [];
						const result = await new Promise<number>((resolve) => {
							const call = client.streamProviderRequest(request(), new Metadata(), { deadline: Date.now() + 30_000 });
							call.on("data", (frame: ProviderStreamEvent) => received.push(frame));
							call.on("error", () => { /* Final status is the independent transport oracle. */ });
							call.on("status", (result) => resolve(result.code));
						});
						expect(result).toBe(delta > 0 ? status.RESOURCE_EXHAUSTED : status.OK);
						expect(received).toHaveLength(delta > 0 ? 0 : 1);
						if (delta <= 0) expect(received[0]?.textComplete?.text).toBe("x".repeat(vector.textBytes));
					} finally {
						client.close();
						await server.close();
					}
				} else {
					const client = new RuntimePodGatewayClient({ address: server.address, tokenPath: "/unused", channelOptions: gatewayGrpcChannelOptions(), metadataFactory: async () => new Metadata() });
					try {
						const handle = await client.streamProviderRequest(request());
						const received = Array.from(await Effect.runPromise(Stream.runCollect(handle.events)));
						const result = await handle.completion;
						if (delta > 0) expect(result).toMatchObject({ outcome: "transport_failure", error: { code: "gateway_protocol_error", statusCode: status.RESOURCE_EXHAUSTED } });
						else {
							expect(result.outcome).toBe("eof");
							expect(received[0]?.textComplete?.text).toBe("x".repeat(vector.textBytes));
						}
						expect(received).toHaveLength(delta > 0 ? 0 : 1);
					} finally {
						try { await client.close(); } finally { await server.close(); }
					}
				}
			}, 300_000);
		}
	}
});
