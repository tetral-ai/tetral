import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import {
	ProviderContextRole,
	ProviderFinishReason,
	ProviderRequest,
	ProviderStreamEvent,
	ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { MaxProviderContextTextJsonBytes, MaxProviderRequestMessageBytes } from "@tetral/gateway-protocol/src/bounds.js";
import { Effect, Stream } from "effect";
import { gatewayGrpcChannelOptions } from "../../src/bounds.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";
import {
	CapacityTargetTokens,
	capacityVectors,
	maximumReadRequest,
	RequiredHeadroomRatio,
	SystemWireMarkers,
} from "../harness/capacity-vectors.js";
import { startDirectContentWireGateway } from "../harness/complete-event-wire-harness.js";

// The Gateway's own tests receive and lower requests at this protocol bound.
const bound = MaxProviderRequestMessageBytes;

describe("Runtime provider request capacity", () => {
	test("the maximum-context requests fit the request bound with headroom", async () => {
		const vectors = await capacityVectors();
		expect(vectors).toHaveLength(5);
		for (const vector of vectors) {
			expect(vector.estimatedTokens, vector.name).toBeGreaterThanOrEqual(CapacityTargetTokens - 2);
			expect(vector.estimatedTokens, vector.name).toBeLessThanOrEqual(CapacityTargetTokens);
			const encoded = Buffer.from(ProviderRequest.encode(vector.request).finish());
			expect(encoded.byteLength, vector.name).toBeLessThanOrEqual(bound * (1 - RequiredHeadroomRatio));
			for (const marker of [...SystemWireMarkers, ...vector.wireMarkers]) expect(encoded.includes(marker), `${vector.name} ${marker}`).toBe(true);
		}
		const coldHistory = vectors.find((vector) => vector.name === "escape_dense_output_history");
		expect(coldHistory?.loadedContextBytes).toBeGreaterThan(20 * 1024 * 1024);
		expect(coldHistory?.loadedContextBytes).toBeLessThan(bound * (1 - RequiredHeadroomRatio));
	}, 60_000);

	test("carries an exact maximum Read result into the provider request", async () => {
		const measurement = await maximumReadRequest();
		expect(measurement.envelopeBytes).toBe(200_000);
		expect(measurement.projectedOutputBytes).toBeLessThanOrEqual(512 * 1024);
		const encoded = Buffer.from(ProviderRequest.encode(measurement.request).finish());
		expect(encoded.byteLength).toBeLessThan(bound);
		expect(encoded.includes("TETRAL_MAXIMUM_READ_HEAD")).toBe(true);
		expect(encoded.includes("TETRAL_MAXIMUM_READ_TAIL")).toBe(true);
		expect(measurement.outputPreserved).toBe(true);
		expect(measurement.truncated).toBe(false);
	}, 30_000);

	test("the Runtime client sends a request at the bound byte for byte", async () => {
		const request = boundedRequest(bound);
		const sent = Buffer.from(ProviderRequest.encode(request).finish());
		expect(sent.byteLength).toBe(bound);
		const server = await startDirectContentWireGateway({ frames: [finishFrame()] }, { "grpc.max_receive_message_length": bound * 2 });
		const client = new RuntimePodGatewayClient({ address: server.address, tokenPath: "/unused", channelOptions: gatewayGrpcChannelOptions(), metadataFactory: async () => new Metadata() });
		try {
			const handle = await client.streamProviderRequest(request);
			await Effect.runPromise(Stream.runCollect(handle.events));
			expect((await handle.completion).outcome).toBe("eof");
			expect(server.requests).toHaveLength(1);
			expect(Buffer.from(ProviderRequest.encode(server.requests[0] as ProviderRequest).finish()).equals(sent)).toBe(true);
		} finally {
			try { await client.close(); } finally { await server.close(); }
		}
	}, 60_000);

	test("the Runtime client refuses a request a byte over the bound before sending", async () => {
		const server = await startDirectContentWireGateway({ frames: [finishFrame()] }, { "grpc.max_receive_message_length": bound * 2 });
		const client = new RuntimePodGatewayClient({ address: server.address, tokenPath: "/unused", channelOptions: gatewayGrpcChannelOptions(), metadataFactory: async () => new Metadata() });
		try {
			await expect(client.streamProviderRequest(boundedRequest(bound + 1))).rejects.toMatchObject({
				type: "gateway-client",
				code: "gateway_protocol_error",
				retryable: false,
				fatal: true,
				statusCode: status.RESOURCE_EXHAUSTED,
			});
			expect(server.requests).toEqual([]);
		} finally {
			try { await client.close(); } finally { await server.close(); }
		}
	}, 60_000);
});

// boundedRequest fills a request with text items at their own bound until it
// encodes to exactly encodedBytes.
function boundedRequest(encodedBytes: number): ProviderRequest {
	const build = (filler: number): ProviderRequest => {
		const itemLength = MaxProviderContextTextJsonBytes - 2;
		const context = [];
		for (let left = filler; left > 0; left -= itemLength) {
			context.push({ role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER, content: [{ text: { text: "x".repeat(Math.min(left, itemLength)) } }] });
		}
		return ProviderRequest.fromPartial({
			requestId: "req_request_bound", modelRequestId: "mreq_request_bound",
			workspaceId: "wksp_request_bound", sessionId: "sesn_request_bound", sessionThreadId: "thr_request_bound",
			bindingId: "bind_request_bound", bindingGeneration: 1, runtimeBindingToken: "request-bound-token", runtimeProcessId: "process_request_bound",
			outputContractVersion: 2, modelRequestStartEventId: "evt_00000000000000000000000000000001",
			model: { providerId: "anthropic", modelId: "claude-opus-4-8" },
			limits: { maxOutputTokens: 128, timeoutMs: 10_000 },
			context,
		});
	};
	const encoded = (filler: number) => ProviderRequest.encode(build(filler)).finish().byteLength;
	let filler = encodedBytes - encoded(0);
	for (let attempt = 0; attempt < 8 && encoded(filler) !== encodedBytes; attempt++) filler += encodedBytes - encoded(filler);
	return build(filler);
}

function finishFrame(): Buffer {
	return Buffer.from(ProviderStreamEvent.encode(ProviderStreamEvent.fromPartial({
		frameSequence: 1, type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
		finish: { reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP, metadataJson: "{}", contextWindowTokens: 200_000, outputTokenLimit: 128,
			usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, providerUsageJson: "{}" } },
	})).finish());
}
