import {
	Server,
	ServerCredentials,
	status,
} from "@grpc/grpc-js";
import type { ServerOptions, ServerWritableStream } from "@grpc/grpc-js";
import {
	ProviderGatewayServiceService,
	ProviderRequest,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

export interface DirectContentWireScenario {
	readonly frames: readonly Buffer[];
	/** A test-owned observation gate, interrupted when the caller cancels. */
	readonly afterFrames?: () => Promise<void>;
	readonly terminalError?: { readonly code: status; readonly details: string };
}

/** A raw protobuf endpoint only for malformed-frame and isolated fuse tests.
 * It deliberately bypasses Provider normalization and is never SDK evidence. */
export async function startDirectContentWireGateway(
	scenario: DirectContentWireScenario,
	options: ServerOptions = {},
): Promise<{
	readonly address: string;
	readonly requests: ProviderRequest[];
	readonly close: () => Promise<void>;
}> {
	const server = new Server(options);
	const requests: ProviderRequest[] = [];
	const tasks = new Set<Promise<void>>();
	const failures: unknown[] = [];
	server.addService(
		{
			streamProviderRequest: {
				...ProviderGatewayServiceService.streamProviderRequest,
				responseSerialize: (value: Buffer) => value,
				responseDeserialize: (value: Buffer) => value,
			},
		},
		{
			streamProviderRequest(call: ServerWritableStream<ProviderRequest, Buffer>) {
				requests.push(call.request);
				const task = writeScenario(call, scenario).catch((error: unknown) => {
					failures.push(error);
					if (!call.cancelled) {
						call.emit("error", { code: status.INTERNAL, details: "wire fixture failed" });
					}
				});
				tasks.add(task);
				void task.then(() => tasks.delete(task));
			},
		},
	);
	const port = await new Promise<number>((resolve, reject) => {
		server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
			if (error !== null) {
				server.forceShutdown();
				reject(error);
			} else resolve(boundPort);
		});
	});
	return {
		address: `127.0.0.1:${port}`,
		requests,
		async close() {
			await new Promise<void>((resolve, reject) => {
				const timer = setTimeout(() => {
					server.forceShutdown();
					reject(new Error("direct content wire server did not join"));
				}, 10_000);
				server.tryShutdown((error) => {
					clearTimeout(timer);
					if (error !== undefined) reject(error);
					else resolve();
				});
			});
			await Promise.all(tasks);
			if (failures.length > 0) throw new AggregateError(failures, "direct content wire fixture failed");
		},
	};
}

async function writeScenario(
	call: ServerWritableStream<ProviderRequest, Buffer>,
	scenario: DirectContentWireScenario,
): Promise<void> {
	let cancelled = call.cancelled;
	let resolveCancelled: (() => void) | undefined;
	const cancellation = new Promise<void>((resolve) => { resolveCancelled = resolve; });
	const onCancelled = () => { cancelled = true; resolveCancelled?.(); };
	call.once("cancelled", onCancelled);
	try {
		for (const frame of scenario.frames) {
			if (cancelled) return;
			await Promise.race([
				new Promise<void>((resolve, reject) => {
					call.write(frame, (error: Error | null | undefined) => {
						if (error != null && !cancelled) reject(error);
						else resolve();
					});
				}),
				cancellation,
			]);
		}
		if (scenario.afterFrames !== undefined) {
			await Promise.race([scenario.afterFrames(), cancellation]);
		}
		if (cancelled) return;
		if (scenario.terminalError !== undefined) call.emit("error", scenario.terminalError);
		else call.end();
	} finally {
		call.off("cancelled", onCancelled);
	}
}

/** Independent encoding of the retired schema, including a v2 sequence, so
 * rejection cannot be explained solely by a missing sequence number. */
export function retiredProviderFrame(type: number): Buffer {
	const payloadField = type <= 3 ? 4 : type <= 6 ? 5 : type <= 9 ? 6 : 7;
	const fields = [protobufString(1, "provider-part")];
	if (payloadField <= 5) fields.push(protobufString(2, "fragment"), protobufString(3, "{}"));
	else fields.push(protobufString(2, "Read"), protobufString(3, type === 10 ? '{"file_path":"/workspace/note.txt"}' : "{}"), protobufString(4, "{}"));
	const payload = Buffer.concat(fields);
	return Buffer.concat([Buffer.from([0x18, type]), protobufBytes(payloadField, payload), Buffer.from([0x58, 1])]);
}

/** Builds a schema-valid frame of exactly the requested encoded length using
 * independent protobuf field encoding. Its text intentionally exceeds the
 * domain limit: this fixture isolates the transport fuse from semantic checks. */
export function sizedProviderFrame(encodedBytes: number): { readonly frame: Buffer; readonly textBytes: number } {
	const identity = Buffer.concat([
		protobufString(1, "wire-size"),
		protobufString(2, "evt_00000000000000000000000000000001"),
	]);
	let textBytes = encodedBytes - identity.length - 16;
	for (let attempt = 0; attempt < 3; attempt++) {
		const payload = Buffer.concat([identity, protobufBytes(3, Buffer.alloc(textBytes, 0x78))]);
		const frame = Buffer.concat([Buffer.from([0x18, 15, 0x58, 1]), protobufBytes(13, payload)]);
		if (frame.length === encodedBytes) return { frame, textBytes };
		textBytes += encodedBytes - frame.length;
	}
	throw new Error("exact transport vector could not be constructed");
}

function protobufString(field: number, value: string): Buffer {
	return protobufBytes(field, Buffer.from(value));
}

function protobufBytes(field: number, value: Buffer): Buffer {
	const size: number[] = [];
	let remaining = value.length;
	do {
		const next = remaining & 0x7f;
		remaining >>>= 7;
		size.push(next | (remaining > 0 ? 0x80 : 0));
	} while (remaining > 0);
	return Buffer.concat([Buffer.from([(field << 3) | 2, ...size]), value]);
}
