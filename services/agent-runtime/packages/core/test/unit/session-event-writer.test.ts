import { describe, expect, test } from "bun:test";
import type {
	SessionEventEnvelope,
	SessionEventWriterAppendResult,
} from "../../src/contracts/runtime.js";
import { SessionEventWriterRetryPolicy } from "../../src/contracts/runtime.js";
import { createSessionEventWriter } from "../../src/runtime/session-event-writer.js";

function envelope(writeId = "write_1"): SessionEventEnvelope {
	return {
		requestId: "request_1",
		workspaceId: "workspace_1",
		sessionId: "session_1",
		sessionThreadId: "thread_1",
		bindingId: "binding_1",
		bindingGeneration: 1,
		targetPodUid: "pod_1",
		writeId,
		modelRequestId: "model_request_1",
		event: {
			type: "agent.message",
			content: [{ type: "text", text: "hello" }],
		},
		assistantContextAppend: {
			parts: [{ type: "text", text: "hello", truncated: false }],
		},
	};
}

describe("SessionEventWriter boundary", () => {
	test("accepts an operation-specific result that does not echo the request", async () => {
		const requests: SessionEventEnvelope[] = [];
		const writer = createSessionEventWriter({
			append: async (request) => {
				requests.push(request);
				return { ok: true, type: "committed", eventId: "event_1" };
			},
			sleep: async () => true,
		});

		expect(await writer.append(envelope())).toEqual({
			ok: true,
			type: "committed",
			eventId: "event_1",
		});
		expect(requests).toEqual([envelope()]);
		expect(
			JSON.stringify(await writer.append(envelope("write_2"))),
		).not.toContain("write_2");
	});

	test("closed result union rejects unrelated and caller-echo fields", async () => {
		const writer = createSessionEventWriter({
			append: async () => ({
				ok: true,
				type: "committed",
				eventId: "event_1",
				writeId: "forbidden_echo",
			}),
			sleep: async () => true,
		});

		expect(await writer.append(envelope())).toEqual({
			ok: false,
			error: expect.objectContaining({
				code: "schema_mismatch",
				writeId: "write_1",
			}),
		});
	});

	test("retries retryable failures with the stable write identity", async () => {
		const writeIds: string[] = [];
		const writer = createSessionEventWriter({
			append: async (request): Promise<SessionEventWriterAppendResult> => {
				writeIds.push(request.writeId);
				if (writeIds.length === 1) {
					return {
						ok: false,
						error: {
							type: "session-event-writer",
							code: "unavailable",
							message: "try later",
							retryable: true,
							fatal: false,
							writeId: request.writeId,
						},
					};
				}
				return { ok: true, type: "duplicate", eventId: "event_2" };
			},
			sleep: async () => true,
		});

		expect(await writer.append(envelope("stable_write"))).toEqual({
			ok: true,
			type: "duplicate",
			eventId: "event_2",
		});
		expect(writeIds).toEqual(["stable_write", "stable_write"]);
	});

	test("invalid envelopes fail before transport", async () => {
		const requests: unknown[] = [];
		const writer = createSessionEventWriter({
			append: async (request) => {
				requests.push(request);
				return { ok: true, type: "committed", eventId: "event_1" };
			},
			sleep: async () => true,
		});

		await expect(
			writer.append({ ...envelope(), sessionId: "" }),
		).rejects.toThrow();
		expect(requests).toEqual([]);
	});

	test("retry policy remains fixed at the Runtime writer boundary", () => {
		expect(SessionEventWriterRetryPolicy).toEqual({
			attempts: 3,
			timeoutPerAttemptMs: 3000,
			backoffMs: [100, 300],
		});
	});
});
