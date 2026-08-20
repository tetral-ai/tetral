import { describe, expect, jest, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { CallOptions } from "@grpc/grpc-js";
import { status as GrpcStatus, Metadata } from "@grpc/grpc-js";
import type {
	RuntimeContextEntry,
	RuntimeJsonValue,
	RuntimeOpenRequestDraft,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import {
	RuntimeInternalToolRepairStore,
	SessionEventWriterRetryPolicy,
} from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { toGatewayProviderContext } from "@tetral/agent-runtime-core/src/runtime/context-projection.js";
import { stableRuntimeID } from "@tetral/agent-runtime-core/src/runtime/runtime-identity.js";
import type {
	ThreadToolRouteView,
	ThreadTurnCheckpoint,
	ThreadTurnLoadFacts,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import {
	extractColdThreadToolRouteView,
	extractThreadTurnCheckpoint,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-checkpoint.js";
import { deriveThreadTurnDecision } from "@tetral/agent-runtime-core/src/thread-loop/thread-turn-reducer.js";
import type { RuntimeToolExecutionRequest } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import { runtimeToolSettlement } from "@tetral/agent-runtime-core/src/thread-loop/tool-execution.js";
import type { ToolEntry } from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import {
	createToolCatalog,
	lookupToolEntry,
} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import type {
	AcceptSandboxExecutionRequest,
	AcceptSandboxExecutionResponse,
	AdmitChildInterruptRequest,
	AgentRuntimeBridgeServiceClient,
	AwaitChildInterruptRequest,
	AwaitSandboxExecutionRequest,
	AwaitSandboxExecutionResponse,
	CancelCommandRequest,
	CloseChildControlRequest,
	CreateSubagentThreadRequest,
	DeliverInterAgentMailRequest,
	ListChildThreadsRequest,
	MarkChildThreadActiveRequest,
	ReadCommandResultRequest,
	ReadCommandResultResponse,
	ResolveChildThreadRequest,
	RunMemoryRequest,
	RunMemoryResponse,
	SendCommandInputRequest,
	SendCommandInputResponse,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import {
	ChildControlAction,
	ChildInterruptOutcome,
	ChildLifecycleDisposition,
} from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { MaxProviderRequestToolOutputJsonBytes } from "@tetral/gateway-protocol/src/bounds.js";
import type {
	McpConnectorServiceClient,
	ProviderGatewayServiceClient,
	RunMcpToolRequest,
	RunMcpToolResponse,
	RunWebRequest,
	RunWebResponse,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {
	McpErrorKind,
	McpRetryStatus,
	RunMcpToolStatus,
	RunWebStatus,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { canonicalRunToolJSON } from "@tetral/gateway-protocol/src/run-tool-canonical-json.js";
import { Stream } from "effect";
import type {
	RuntimeCoreHostsOptions,
	RuntimeSubAgentRunHost,
} from "../../src/core-hosts.js";
import {
	buildRuntimeCoreHosts,
	validateClosedThreadResumeCheckpoint,
} from "../../src/core-hosts.js";
import { RuntimePodToolRunner } from "../../src/tool-runner.js";

function userMessage(
	_sessionId: string,
	_id: string,
	messageSequence: number,
	text: string,
): RuntimeContextEntry {
	return {
		messageSequence,
		contextKind: "user",
		parts: [{ type: "text", text }],
	};
}

function assistantRunningToolMessage(
	_sessionId: string,
	_id: string,
	messageSequence: number,
	modelToolCallId: string,
	toolName: string,
	_toolUseEventId: string,
	input: RuntimeJsonValue,
	modelRequestId = "mreq_resume_checkpoint",
): RuntimeOpenRequestDraft {
	return {
		modelRequestId,
		messageSequence,
		parts: [
			{ type: "tool_call", modelToolCallId, toolName, canonicalInput: input },
		],
	};
}

function completedToolMessage(
	toolName: string,
	input: RuntimeJsonValue,
	output: string,
	messageSequence = 1,
): RuntimeContextEntry {
	const modelToolCallId = `tool_${toolName.toLowerCase()}_completed`;
	return {
		messageSequence,
		contextKind: "assistant",
		parts: [
			{
				type: "tool_call",
				modelToolCallId,
				toolName,
				canonicalInput: input,
			},
			{
				type: "tool_result",
				modelToolCallId,
				result: {
					type: "completed",
					output: { text: output },
				},
			},
		],
	};
}

function runtimeTextMessage(
	_id: string,
	role: "user" | "assistant",
	origin: "user" | "agent" | "runtime",
	sequence: number,
	text: string,
): RuntimeContextEntry {
	return {
		messageSequence: sequence + 1,
		contextKind:
			role === "assistant"
				? "assistant"
				: origin === "runtime"
					? "runtime_notification"
					: "user",
		parts: [{ type: "text", text }],
	};
}

describe("RuntimePodToolRunner", () => {
	test("shares exact raw JSON canonical vectors with Bridge", () => {
		const vectors = JSON.parse(
			readFileSync(
				resolve(
					import.meta.dir,
					"../../../../../../testdata/run-tool-canonical-vectors.json",
				),
				"utf8",
			),
		) as Array<{
			name: string;
			inputs: string[];
			canonical: string;
		}>;
		for (const vector of vectors) {
			for (const input of vector.inputs) {
				expect(canonicalRunToolJSON(input), vector.name).toBe(vector.canonical);
			}
		}
		expect(() =>
			canonicalRunToolJSON(`${"[".repeat(257)}0${"]".repeat(257)}`),
		).toThrow("nesting exceeds");
		expect(() => canonicalRunToolJSON(`{"unterminated":`)).toThrow();
	});

	test("accepts one durable sandbox execution before independently awaiting its result", async () => {
		const startedAt = Date.now();
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				summary: "created notes/a.txt",
				payload_path:
					"/tmp/tetral-runtime/tool-payloads/sevt_tool_1/payload.json",
				provider_command_id: "provider-secret",
				sandbox_process_id: "pid-secret",
			},
		});
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: {
				text: "status: success\nsummary: created notes/a.txt",
				truncated: false,
			},
		});
		expect(JSON.stringify(result)).not.toContain("payload");
		expect(JSON.stringify(result)).not.toContain("provider-secret");
		expect(JSON.stringify(result)).not.toContain("sandbox_process");
		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.acceptSandboxExecutionOptions).toHaveLength(1);
		expect(bridge.acceptSandboxExecutionOptions[0]?.deadline).toBeNumber();
		expect(
			bridge.acceptSandboxExecutionOptions[0]?.deadline as number,
		).toBeGreaterThanOrEqual(
			startedAt + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
		);
		expect(
			bridge.acceptSandboxExecutionOptions[0]?.deadline as number,
		).toBeLessThanOrEqual(
			Date.now() + SessionEventWriterRetryPolicy.timeoutPerAttemptMs,
		);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests[0]).toEqual(
			bridge.acceptSandboxExecutionRequests[0],
		);
		expect(bridge.acceptSandboxExecutionRequests[0]).toEqual({
			toolUseEventId: "sevt_tool_1",
			scope: {
				workspaceId: "wksp_1",
				sessionId: "sesn_1",
				sessionThreadId: "thrd_1",
				binding: {
					bindingId: "bind_1",
					bindingGeneration: 42,
					targetPodUid: "pod_1",
				},
			},
		});
	});

	test("preserves explicit non-retryable sandbox failures", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "runtime_error",
			error_code: "projection_refresh_failed",
			message: "memory projection failed",
			retryable: false,
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				code: "runtime_invalid_sequence",
				retryable: false,
			},
		});
	});

	test("maps activation exhaustion to one generic error across sandbox tool families", async () => {
		for (const testCase of [
			{ toolName: "exec_command", input: { cmd: "true" } },
			{
				toolName: "Write",
				input: { content: "hello", file_path: "notes/a.txt" },
			},
			{ toolName: "view_image", input: { path: "plot.png" } },
			{
				toolName: "write_stdin",
				input: { session_id: "task_1", chars: "continue" },
				commandRoute: "send",
			},
			{
				toolName: "write_stdin",
				input: { session_id: "task_1", chars: "" },
				commandRoute: "poll",
			},
		]) {
			const bridge = new RecordingBridgeClient();
			const activationResult = JSON.stringify({
				status: "error",
				error: {
					kind: "sandbox_activation_attempts_exhausted",
					message: "sandbox activation could not be completed",
				},
			});
			bridge.awaitSandboxExecutionResultJson = activationResult;
			if (testCase.commandRoute === "send")
				bridge.sendCommandInputResultJson = activationResult;
			if (testCase.commandRoute === "poll")
				bridge.readCommandResultResultJson = activationResult;

			const result = await makeRunner({ bridge }).runTool(
				toolRequest(testCase.toolName, testCase.input),
			);

			expect(result).toEqual({
				type: "error",
				error: {
					type: "runtime",
					code: "runtime_invalid_sequence",
					message: "The requested operation could not be completed.",
					retryable: false,
					fatal: false,
					sessionId: "sesn_1",
				},
			});
			const projected = JSON.stringify(result);
			expect(projected).not.toContain("Partial result");
			expect(projected).not.toContain("sandbox_activation_attempts_exhausted");
			expect(projected).not.toContain("quota_exceeded");
			expect(projected).not.toContain("sandbox activation");
			if (result.type !== "error") {
				throw new Error(
					"activation exhaustion did not produce a Runtime error",
				);
			}
			expect(runtimeToolSettlement(result)).toEqual({
				type: "error",
				error: expect.objectContaining({
					type: "runtime",
					code: "runtime_invalid_sequence",
					message: "The requested operation could not be completed.",
				}),
			});
		}
	});

	test("rejects non-boolean sandbox retryability instead of deriving it from status", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "runtime_error",
			message: "bad retry marker",
			retryable: "yes",
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				message: "Tool route returned malformed retryability.",
				retryable: false,
			},
		});
	});

	test("retries the exact durable acceptance before beginning the result wait", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.acceptSandboxExecutionErrors.push(
			Object.assign(new Error("connection reset"), {
				code: GrpcStatus.UNAVAILABLE,
			}),
		);
		const sleep = new ControlledSleep();
		const runner = makeRunner({ bridge, sleep: sleep.sleep });

		const pending = runner.runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);
		await Bun.sleep(0);

		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(0);
		expect(sleep.calls).toHaveLength(1);
		sleep.releaseNext();
		const result = await pending;

		expect(result.type).toBe("completed");
		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(2);
		expect(bridge.acceptSandboxExecutionRequests[1]!).toEqual(
			bridge.acceptSandboxExecutionRequests[0]!,
		);
		expect(bridge.awaitSandboxExecutionRequests).toEqual([
			bridge.acceptSandboxExecutionRequests[0]!,
		]);
	});

	test("does not turn an acceptance authentication failure into a tool result", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.acceptSandboxExecutionErrors.push(
			Object.assign(new Error("token rejected"), {
				code: GrpcStatus.UNAUTHENTICATED,
			}),
		);
		const sleep = new ControlledSleep();
		const pending = makeRunner({ bridge, sleep: sleep.sleep }).runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);
		await Bun.sleep(0);

		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(0);
		expect(sleep.calls).toHaveLength(1);
		sleep.releaseNext();

		expect((await pending).type).toBe("completed");
		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(2);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(1);
	});

	test("retries only the result wait after durable sandbox acceptance", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionErrors.push(
			Object.assign(new Error("connection reset"), {
				code: GrpcStatus.UNAVAILABLE,
			}),
		);
		const sleep = new ControlledSleep();
		const pending = makeRunner({ bridge, sleep: sleep.sleep }).runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);
		await Bun.sleep(0);

		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(1);
		expect(sleep.calls).toHaveLength(1);
		sleep.releaseNext();

		expect((await pending).type).toBe("completed");
		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(2);
		expect(bridge.awaitSandboxExecutionRequests[1]).toEqual(
			bridge.awaitSandboxExecutionRequests[0],
		);
	});

	test("returns stale custody when Bridge permanently rejects the result wait", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionErrors.push(
			Object.assign(new Error("binding moved"), {
				code: GrpcStatus.FAILED_PRECONDITION,
			}),
		);
		const sleep = new ControlledSleep();
		const result = await makeRunner({ bridge, sleep: sleep.sleep }).runTool(
			toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
		);

		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(1);
		expect(sleep.calls).toHaveLength(0);
		expect(result).toEqual({ type: "stale_custody" });
	});

	test("maps each typed stale executor result to stale custody before Tool settlement", async () => {
		const acceptanceBridge = new RecordingBridgeClient();
		acceptanceBridge.acceptSandboxExecutionResponses.push({ stale: {} });
		expect(
			await makeRunner({ bridge: acceptanceBridge }).runTool(
				toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
			),
		).toEqual({ type: "stale_custody" });
		expect(acceptanceBridge.awaitSandboxExecutionRequests).toEqual([]);

		const waitBridge = new RecordingBridgeClient();
		waitBridge.awaitSandboxExecutionResponses.push({ stale: {} });
		expect(
			await makeRunner({ bridge: waitBridge }).runTool(
				toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
			),
		).toEqual({ type: "stale_custody" });

		const readBridge = new RecordingBridgeClient();
		readBridge.readCommandResultResponses.push({ stale: {} });
		expect(
			await makeRunner({ bridge: readBridge }).runTool(
				toolRequest("write_stdin", { session_id: "task_1", chars: "" }),
			),
		).toEqual({ type: "stale_custody" });

		const inputBridge = new RecordingBridgeClient();
		inputBridge.sendCommandInputResponses.push({ stale: {} });
		expect(
			await makeRunner({ bridge: inputBridge }).runTool(
				toolRequest("write_stdin", { session_id: "task_1", chars: "hello" }),
			),
		).toEqual({ type: "stale_custody" });

		const memoryBridge = new RecordingBridgeClient();
		memoryBridge.runMemoryResponses.push({ stale: {} });
		expect(
			await makeRunner({ bridge: memoryBridge }).runTool(
				toolRequest("memory", {
					action: "create",
					path: "notes/todo.md",
					content: "one",
				}),
			),
		).toEqual({ type: "stale_custody" });
	});

	test("accepts every legal duplicate executor result arm", async () => {
		const sandboxBridge = new RecordingBridgeClient();
		sandboxBridge.acceptSandboxExecutionResponses.push({ duplicate: {} });
		expect(
			(
				await makeRunner({ bridge: sandboxBridge }).runTool(
					toolRequest("Write", { content: "hello", file_path: "notes/a.txt" }),
				)
			).type,
		).toBe("completed");

		const inputBridge = new RecordingBridgeClient();
		inputBridge.sendCommandInputResponses.push({
			duplicate: { resultJson: '{"status":"accepted"}' },
		});
		expect(
			(
				await makeRunner({ bridge: inputBridge }).runTool(
					toolRequest("write_stdin", { session_id: "task_1", chars: "hello" }),
				)
			).type,
		).toBe("completed");

		const memoryBridge = new RecordingBridgeClient();
		memoryBridge.runMemoryResponses.push({
			duplicate: { resultJson: '{"status":"completed","summary":"created"}' },
		});
		expect(
			(
				await makeRunner({ bridge: memoryBridge }).runTool(
					toolRequest("memory", {
						action: "create",
						path: "notes/todo.md",
						content: "one",
					}),
				)
			).type,
		).toBe("completed");
	});

	test("rejects zero-arm and multi-arm executor responses instead of selecting by precedence", async () => {
		const cases: Array<{
			readonly name: string;
			readonly run: () => Promise<unknown>;
		}> = [];

		for (const response of [{}, { committed: {}, duplicate: {} }]) {
			const bridge = new RecordingBridgeClient();
			bridge.acceptSandboxExecutionResponses.push(response);
			cases.push({
				name: "sandbox acceptance",
				run: async () =>
					await makeRunner({ bridge }).runTool(
						toolRequest("Write", {
							content: "hello",
							file_path: "notes/a.txt",
						}),
					),
			});
		}
		for (const response of [
			{},
			{
				completed: { resultJson: '{"status":"success"}', taskId: "" },
				stale: {},
			},
		]) {
			const bridge = new RecordingBridgeClient();
			bridge.awaitSandboxExecutionResponses.push(response);
			cases.push({
				name: "sandbox wait",
				run: async () =>
					await makeRunner({ bridge }).runTool(
						toolRequest("Write", {
							content: "hello",
							file_path: "notes/a.txt",
						}),
					),
			});
		}
		for (const response of [
			{},
			{ completed: { resultJson: '{"status":"running"}' }, stale: {} },
		]) {
			const bridge = new RecordingBridgeClient();
			bridge.readCommandResultResponses.push(response);
			cases.push({
				name: "command read",
				run: async () =>
					await makeRunner({ bridge }).runTool(
						toolRequest("write_stdin", { session_id: "task_1", chars: "" }),
					),
			});
		}
		for (const response of [
			{},
			{ committed: { resultJson: '{"status":"accepted"}' }, stale: {} },
		]) {
			const bridge = new RecordingBridgeClient();
			bridge.sendCommandInputResponses.push(response);
			cases.push({
				name: "command input",
				run: async () =>
					await makeRunner({ bridge }).runTool(
						toolRequest("write_stdin", {
							session_id: "task_1",
							chars: "hello",
						}),
					),
			});
		}
		for (const response of [
			{},
			{
				committed: { resultJson: '{"status":"completed"}' },
				duplicate: { resultJson: '{"status":"completed"}' },
			},
		]) {
			const bridge = new RecordingBridgeClient();
			bridge.runMemoryResponses.push(response);
			cases.push({
				name: "memory",
				run: async () =>
					await makeRunner({ bridge }).runTool(
						toolRequest("memory", {
							action: "create",
							path: "notes/todo.md",
							content: "one",
						}),
					),
			});
		}

		for (const testCase of cases) {
			expect(await testCase.run(), testCase.name).toMatchObject({
				type: "error",
				error: { retryable: true },
			});
		}
	});

	test("numbers only the requested Read range with true file line numbers", async () => {
		const allLines = Array.from(
			{ length: 20 },
			(_, index) => `line ${index + 1}`,
		);
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: `${allLines.slice(9, 14).join("\n")}\n`,
				start_line: 10,
				returned_lines: 5,
				total_lines: 20,
				truncated: false,
				line_truncations: 0,
			},
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Read", {
				file_path: "notes/a.txt",
				offset: 10,
				limit: 5,
			}),
		);

		expect(result.type).toBe("completed");
		if (result.type !== "completed")
			throw new Error("expected completed result");
		expect(result.output.text).toContain(
			`content:\n${allLines
				.slice(9, 14)
				.map((line, index) => `${String(index + 10).padStart(6, " ")}\t${line}`)
				.join("\n")}`,
		);
		expect(result.output.text).not.toMatch(/(?:^|\n) {5}9\t/u);
		expect(result.output.text).not.toMatch(/(?:^|\n) {4}15\t/u);
	});

	test("uses the helper start_line when a Read request offset is zero", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: "first\nsecond\n",
				start_line: 1,
				returned_lines: 2,
				total_lines: 2,
				truncated: false,
				line_truncations: 0,
			},
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Read", {
				file_path: "notes/a.txt",
				offset: 0,
			}),
		);

		expect(result.type).toBe("completed");
		if (result.type !== "completed")
			throw new Error("expected completed result");
		expect(result.output.text).toContain(
			"content:\n     1\tfirst\n     2\tsecond",
		);
		expect(result.output.text).not.toContain("     0\t");
	});

	test("does not number a spurious empty line for trailing-newline Read content", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: "one\n\nthree\n",
				start_line: 4,
				returned_lines: 3,
				total_lines: 6,
				truncated: false,
				line_truncations: 0,
			},
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Read", { file_path: "notes/a.txt" }),
		);

		expect(result.type).toBe("completed");
		if (result.type !== "completed")
			throw new Error("expected completed result");
		expect(result.output.text).toContain(
			"     4\tone\n     5\t\n     6\tthree",
		);
		expect(result.output.text).not.toMatch(/(?:^|\n) {5}7\t(?:\n|$)/u);

		const blankBridge = new RecordingBridgeClient();
		blankBridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: "\n",
				start_line: 9,
				returned_lines: 1,
				total_lines: 9,
				truncated: false,
				line_truncations: 0,
			},
		});
		const blankResult = await makeRunner({ bridge: blankBridge }).runTool(
			toolRequest("Read", { file_path: "notes/blank.txt" }),
		);
		expect(blankResult.type).toBe("completed");
		if (blankResult.type !== "completed")
			throw new Error("expected completed result");
		expect(blankResult.output.text).toContain("content:\n     9\t");
	});

	test("keeps cat-n prefixes exclusive to Read results", async () => {
		const cases = [
			{
				tool: "Write",
				input: { file_path: "notes/a.txt", content: "alpha\nbeta\n" },
			},
			{ tool: "Grep", input: { pattern: "alpha", path: "notes" } },
			{
				tool: "apply_patch",
				input: { patch: "*** Begin Patch\n*** End Patch\n" },
			},
		] as const;

		for (const testCase of cases) {
			const bridge = new RecordingBridgeClient();
			bridge.awaitSandboxExecutionResultJson = JSON.stringify({
				status: "success",
				result: { content: "alpha\nbeta\n", start_line: 7 },
			});

			const result = await makeRunner({ bridge }).runTool(
				toolRequest(testCase.tool, testCase.input),
			);

			expect(result.type, testCase.tool).toBe("completed");
			if (result.type !== "completed")
				throw new Error("expected completed result");
			expect(result.output.text, testCase.tool).toContain(
				"content: alpha\nbeta\n",
			);
			expect(result.output.text, testCase.tool).not.toMatch(
				/(?:^|\n)\s*\d+\t/u,
			);
		}
	});

	test("renders truncation without hiding result bodies and never rebuilds expected guards", async () => {
		const truncatedReadBridge = new RecordingBridgeClient();
		truncatedReadBridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: "long line… [line truncated]\n",
				start_line: 1,
				returned_lines: 1,
				truncated: true,
				line_truncations: 1,
			},
		});
		const truncatedRead = await makeRunner({
			bridge: truncatedReadBridge,
		}).runTool(toolRequest("Read", { file_path: "notes/a.txt" }));
		expect(truncatedRead.type).toBe("completed");
		if (truncatedRead.type !== "completed")
			throw new Error("expected completed result");
		expect(truncatedRead.output.text).toContain("truncated: true");
		expect(truncatedRead.output.text).toContain("line_truncations: 1");
		expect(truncatedRead.output.truncated).toBe(true);

		const completeReadBridge = new RecordingBridgeClient();
		completeReadBridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: "complete\n",
				start_line: 1,
				returned_lines: 1,
				total_lines: 1,
				truncated: false,
				line_truncations: 0,
			},
		});
		const completeRead = await makeRunner({
			bridge: completeReadBridge,
		}).runTool(toolRequest("Read", { file_path: "notes/a.txt" }));
		expect(completeRead.type).toBe("completed");
		if (completeRead.type !== "completed")
			throw new Error("expected completed result");
		expect(completeRead.output.text).not.toContain("truncated:");
		expect(completeRead.output.text).not.toContain("line_truncations:");
		expect(completeRead.output.truncated).toBe(false);

		const readMessage = completedToolMessage(
			"Read",
			{ file_path: "notes/a.txt" },
			truncatedRead.output.text,
		);
		const editBridge = new RecordingBridgeClient();
		editBridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: { replacements: 1 },
		});
		await makeRunner({ bridge: editBridge }).runTool({
			...toolRequest("Edit", {
				file_path: "notes/a.txt",
				old_string: "line",
				new_string: "row",
			}),
		});
		expect(editBridge.acceptSandboxExecutionRequests[0]).toMatchObject({
			toolUseEventId: "sevt_tool_1",
		});

		const writeBridge = new RecordingBridgeClient();
		writeBridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			truncated: true,
			result: { created: true, bytes_written: 12 },
		});
		const writeResult = await makeRunner({ bridge: writeBridge }).runTool(
			toolRequest("Write", {
				file_path: "notes/a.txt",
				content: "hello world\n",
			}),
		);
		expect(writeResult.type).toBe("completed");
		if (writeResult.type !== "completed")
			throw new Error("expected completed result");
		expect(writeResult.output.text).toContain('"created": true');
		expect(writeResult.output.text).toContain('"bytes_written": 12');
		expect(writeResult.output.text).toContain("truncated: true");
		expect(writeResult.output.truncated).toBe(true);
	});

	test("keeps maximal numbered Read parts below the fatal context byte bound at deep line numbers", async () => {
		const content = `${Array.from({ length: 2000 }, () => '\\"'.repeat(24)).join("\n")}\n`;
		for (const startLine of [
			1,
			1_000_000,
			1_000_000_000,
			Number.MAX_SAFE_INTEGER,
		]) {
			const bridge = new RecordingBridgeClient();
			bridge.awaitSandboxExecutionResultJson = JSON.stringify({
				status: "success",
				result: {
					content,
					start_line: startLine,
					returned_lines: 2000,
					truncated: true,
					line_truncations: 0,
				},
			});
			expect(
				Buffer.byteLength(bridge.awaitSandboxExecutionResultJson, "utf8"),
			).toBeGreaterThan(190_000);
			expect(
				Buffer.byteLength(bridge.awaitSandboxExecutionResultJson, "utf8"),
			).toBeLessThanOrEqual(200_000);

			const result = await makeRunner({ bridge }).runTool(
				toolRequest("Read", {
					file_path: "notes/deep.txt",
					offset: startLine,
					limit: 2000,
				}),
			);

			expect(result.type, String(startLine)).toBe("completed");
			if (result.type !== "completed")
				throw new Error("expected completed result");
			expect(result.output.text, String(startLine)).toContain(
				`${String(startLine).padStart(6, " ")}\t`,
			);
			if (startLine === Number.MAX_SAFE_INTEGER) {
				expect(result.output.text).toContain(
					`${(BigInt(startLine) + 1n).toString()}\t`,
				);
			}
			const projected = toGatewayProviderContext([
				completedToolMessage(
					"Read",
					{ file_path: "notes/deep.txt", offset: startLine, limit: 2000 },
					result.output.text,
				),
			]);
			expect(projected.ok, String(startLine)).toBe(true);
			if (!projected.ok) throw new Error("expected projected Read result");
			const outputJson =
				projected.context[0]?.content.find(
					(item) => item.toolResult !== undefined,
				)?.toolResult?.completed?.outputJson ?? "";
			expect(
				Buffer.byteLength(outputJson, "utf8"),
				String(startLine),
			).toBeLessThanOrEqual(MaxProviderRequestToolOutputJsonBytes);
			expect(JSON.parse(outputJson).text, String(startLine)).toBe(
				result.output.text,
			);
		}

		const unsafeStartBridge = new RecordingBridgeClient();
		unsafeStartBridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				content: "alpha\nbeta\n",
				start_line: Number.MAX_SAFE_INTEGER + 1,
				returned_lines: 2,
				truncated: false,
				line_truncations: 0,
			},
		});
		const unsafeStartResult = await makeRunner({
			bridge: unsafeStartBridge,
		}).runTool(toolRequest("Read", { file_path: "notes/deep.txt" }));
		expect(unsafeStartResult.type).toBe("completed");
		if (unsafeStartResult.type !== "completed")
			throw new Error("expected completed result");
		expect(unsafeStartResult.output.text).toContain("content:\nalpha\nbeta");
		expect(unsafeStartResult.output.text).not.toMatch(/(?:^|\n)\s*\d+\t/u);
	});

	test("formats failed Read envelopes without content or start_line", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "tool_error",
			error: { kind: "not_found", message: "path does not exist" },
			result: {},
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Read", { file_path: "missing.txt" }),
		);

		expect(result.type).toBe("error");
		expect(JSON.stringify(result)).toContain("path does not exist");
		expect(JSON.stringify(result)).not.toContain("NaN");
	});

	test("never adds expected to runtime-built sandbox requests", async () => {
		const readMessage = completedToolMessage(
			"Read",
			{ file_path: "notes/a.txt" },
			"status: success\ncontent: hello",
		);
		const cases = [
			{ tool: "Bash", input: { command: "true" } },
			{ tool: "Read", input: { file_path: "notes/a.txt" } },
			{
				tool: "Write",
				input: { content: "hello again", file_path: "notes/a.txt" },
			},
			{
				tool: "Edit",
				input: {
					file_path: "notes/a.txt",
					old_string: "hello",
					new_string: "there",
				},
			},
			{ tool: "Glob", input: { pattern: "**/*" } },
			{ tool: "Grep", input: { pattern: "hello", path: "notes" } },
			{ tool: "exec_command", input: { cmd: "true" } },
			{ tool: "view_image", input: { path: "image.png" } },
			{
				tool: "apply_patch",
				input: { patch: "*** Begin Patch\n*** End Patch\n" },
			},
		] as const;

		for (const testCase of cases) {
			const bridge = new RecordingBridgeClient();
			bridge.awaitSandboxExecutionResultJson = JSON.stringify({
				status: "success",
				result: {},
			});
			await makeRunner({ bridge }).runTool({
				...toolRequest(testCase.tool, testCase.input),
			});
			expect(bridge.acceptSandboxExecutionRequests[0], testCase.tool).toEqual(
				expect.objectContaining({ toolUseEventId: "sevt_tool_1" }),
			);
		}

		const stdinBridge = new RecordingBridgeClient();
		await makeRunner({ bridge: stdinBridge }).runTool(
			toolRequest("write_stdin", {
				session_id: "task_1",
				chars: "hello",
			}),
		);
		expect(JSON.stringify(stdinBridge.sendCommandInputRequests)).not.toContain(
			"expected",
		);
	});

	test("returns background task metadata for sandbox running results", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "running",
			result: {
				task_id: "task_bridge_1",
				stdout: { text: "started", truncated: false },
			},
		});
		bridge.awaitSandboxExecutionTaskId = "task_bridge_1";
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("exec_command", { cmd: "sleep 10" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: {
				text: "status: running\nsession_id: task_bridge_1\nstdout:\nstarted",
				truncated: false,
			},
			backgroundTask: { taskId: "task_bridge_1" },
		});
	});

	test("provider Bash background execution reaches detached helper after privilege drop", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "running",
			result: { task_id: "sevt_tool_1" },
		});
		bridge.awaitSandboxExecutionTaskId = "sevt_tool_1";

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Bash", {
				command: "sleep 60",
				cwd: "/workspace",
				timeout: 120_000,
				run_in_background: true,
			}),
		);

		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.acceptSandboxExecutionRequests[0]).toMatchObject({
			toolUseEventId: "sevt_tool_1",
		});
		expect(result).toMatchObject({
			type: "completed",
			backgroundTask: { taskId: "sevt_tool_1" },
			output: { text: "status: running\nsession_id: sevt_tool_1" },
		});
	});

	test("turns Bridge view_image attachment refs into pending provider attachments", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				mime: "image/png",
				size_bytes: 3,
				attachment_ref: "att_bridge_view_image",
				filename: "plot.png",
				source_tool_use_event_id: "sevt_tool_1",
				source_path: "plot.png",
				page_range: "",
				detail: "auto",
				data_base64: "AAEC",
			},
		});
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("view_image", { path: "plot.png" }),
		);

		expect(result).toMatchObject({
			type: "completed",
			output: {
				text: "status: success\nmime: image/png\nsize_bytes: 3\nattachment: plot.png",
				truncated: false,
			},
			attachments: [
				{
					transient: {
						attachmentRef: "att_bridge_view_image",
						sourcePath: "plot.png",
						pageRange: "",
						detail: "auto",
					},
					fileBacked: undefined,
					mime: "image/png",
					filename: "plot.png",
				},
			],
		});
		expect(JSON.stringify(result)).not.toContain("AAEC");
		expect(JSON.stringify(result)).not.toContain("data_base64");
		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
	});

	test("turns Bridge Read PDF attachment refs into pending provider attachments with page range", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				mime: "application/pdf",
				size_bytes: 5,
				attachment_ref: "att_bridge_pdf",
				filename: "report.pdf",
				source_tool_use_event_id: "sevt_tool_1",
				source_path: "docs/report.pdf",
				page_range: "2-6",
				detail: "auto",
				data_base64: "JVBERi0=",
			},
		});
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("Read", { file_path: "docs/report.pdf", page_range: "2-6" }),
		);

		expect(result).toMatchObject({
			type: "completed",
			output: {
				text: "status: success\nmime: application/pdf\nsize_bytes: 5\npage_range: 2-6\nattachment: report.pdf",
				truncated: false,
			},
			attachments: [
				{
					transient: {
						attachmentRef: "att_bridge_pdf",
						sourcePath: "docs/report.pdf",
						pageRange: "2-6",
						detail: "auto",
					},
					fileBacked: undefined,
					mime: "application/pdf",
					filename: "report.pdf",
				},
			],
		});
		expect(JSON.stringify(result)).not.toContain("data_base64");
		expect(JSON.stringify(result)).not.toContain("JVBERi0=");
	});

	test("does not re-derive missing PDF page range from the original tool input", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				mime: "application/pdf",
				size_bytes: 5,
				attachment_ref: "att_bridge_pdf_legacy",
				filename: "report.pdf",
				source_tool_use_event_id: "sevt_tool_1",
				source_path: "docs/report.pdf",
				detail: "auto",
			},
		});
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("Read", { file_path: "docs/report.pdf", page_range: "4-8" }),
		);

		expect(result).toMatchObject({
			type: "completed",
			attachments: [{ transient: { pageRange: "" } }],
		});
	});

	test("rejects raw sandbox media bytes at the Runtime boundary", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				mime: "image/png",
				size_bytes: 3,
				data_base64: "AAEC",
			},
		});
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("view_image", { path: "plot.png" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				retryable: true,
				message:
					"view_image returned raw media payload after Bridge attachment boundary.",
			},
		});
		expect(JSON.stringify(result)).not.toContain("AAEC");
	});

	test("filters transport-only base64 fields out of visible non-media sandbox tool JSON", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				mime: "image/png",
				size_bytes: 3,
				data_base64: "AAEC",
			},
		});
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("Write", { file_path: "plot.png", content: "ok" }),
		);

		expect(result).toMatchObject({
			type: "completed",
			output: {
				text: '{\n  "mime": "image/png",\n  "size_bytes": 3\n}',
				truncated: false,
			},
		});
		expect(JSON.stringify(result)).not.toContain("data_base64");
		expect(JSON.stringify(result)).not.toContain("AAEC");
	});

	test("routes write_stdin send and poll through the Bridge command RPCs", async () => {
		const bridge = new RecordingBridgeClient();
		const runner = makeRunner({ bridge });

		await runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "hello", max_output_tokens: 123 },
				"sevt_tool_send",
			),
		);
		await runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "", max_output_tokens: 123 },
				"sevt_tool_poll",
			),
		);

		expect(bridge.sendCommandInputRequests).toEqual([
			expect.objectContaining({
				operationId: stableTestId("req", "command-followup:sevt_tool_send"),
				taskId: "task_1",
				maxOutputTokens: 123,
				toolUseEventId: "sevt_tool_send",
			}),
		]);
		expect(bridge.readCommandResultRequests).toEqual([
			expect.objectContaining({
				operationId: stableTestId("req", "command-followup:sevt_tool_poll"),
				taskId: "task_1",
				maxOutputTokens: 123,
				toolUseEventId: "sevt_tool_poll",
			}),
		]);
	});

	test("rejoins write_stdin send and poll after transport loss with the same request identity", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.sendCommandInputErrors.push(new Error("send disconnected"));
		bridge.readCommandResultErrors.push(new Error("poll disconnected"));
		const sleep = new ControlledSleep();
		const runner = makeRunner({ bridge, sleep: sleep.sleep });

		const sendPending = runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "hello" },
				"sevt_tool_send",
			),
		);
		await waitForCondition(
			() => sleep.calls.length === 1,
			"command send rejoin wait",
		);
		sleep.releaseNext();
		expect((await sendPending).type).toBe("completed");

		const pollPending = runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "" },
				"sevt_tool_poll",
			),
		);
		await waitForCondition(
			() => sleep.calls.length === 2,
			"command poll rejoin wait",
		);
		sleep.releaseNext();
		expect((await pollPending).type).toBe("completed");

		expect(bridge.sendCommandInputRequests).toHaveLength(2);
		expect(bridge.sendCommandInputRequests[1]).toEqual(
			bridge.sendCommandInputRequests[0],
		);
		expect(bridge.readCommandResultRequests).toHaveLength(2);
		expect(bridge.readCommandResultRequests[1]).toEqual(
			bridge.readCommandResultRequests[0],
		);
	});

	test("write_stdin ignores undeclared task and camel-case token aliases", async () => {
		const bridge = new RecordingBridgeClient();
		const runner = makeRunner({ bridge });

		const legacyTask = await runner.runTool(
			toolRequest("write_stdin", { task_id: "task_1", chars: "hello" }),
		);
		await runner.runTool(
			toolRequest("write_stdin", {
				session_id: "task_1",
				chars: "hello",
				maxOutputTokens: 123,
			}),
		);

		expect(legacyTask).toMatchObject({
			type: "error",
			error: {
				retryable: false,
				message: "write_stdin requires a session_id task handle.",
			},
		});
		expect(bridge.sendCommandInputRequests).toHaveLength(1);
		expect(bridge.sendCommandInputRequests[0]?.maxOutputTokens).toBe(0);
	});

	test("uses distinct explicit operation ids for multiple stdin writes in one runtime input", async () => {
		const bridge = new RecordingBridgeClient();
		const runner = makeRunner({ bridge });

		await runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "first" },
				"sevt_tool_send_1",
			),
		);
		await runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "second" },
				"sevt_tool_send_2",
			),
		);

		expect(
			bridge.sendCommandInputRequests.map((request) => request.operationId),
		).toEqual([
			stableTestId("req", "command-followup:sevt_tool_send_1"),
			stableTestId("req", "command-followup:sevt_tool_send_2"),
		]);
		expect(bridge.sendCommandInputRequests).toHaveLength(2);
		expect(
			bridge.sendCommandInputRequests.every(
				(request) => !("stdinWriteSequence" in request),
			),
		).toBe(true);
	});

	test("cancels only the local command wait when its tool fiber aborts", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.deferReadCommandResult = true;
		const runner = makeRunner({ bridge });
		const abortController = new AbortController();

		const resultPromise = runner.runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "" },
				"sevt_tool_poll",
				abortController.signal,
			),
		);
		await waitForCondition(
			() => bridge.readCommandResultRequests.length === 1,
			"read command request",
		);
		abortController.abort();
		const result = await resultPromise;

		expect(result.type).toBe("cancelled");
		expect(bridge.cancelCommandRequests).toEqual([]);
	});

	test.each([
		["missing task", GrpcStatus.NOT_FOUND],
		["stale task", GrpcStatus.FAILED_PRECONDITION],
	])("does not retry typed command rejection: %s", async (_name, code) => {
		const bridge = new RecordingBridgeClient();
		bridge.readCommandResultErrors.push(
			Object.assign(new Error("typed rejection"), { code }),
		);
		const sleep = new ControlledSleep();
		const result = await makeRunner({ bridge, sleep: sleep.sleep }).runTool(
			toolRequest(
				"write_stdin",
				{ session_id: "task_1", chars: "" },
				"sevt_tool_poll",
			),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				retryable: false,
				message: "Bridge rejected the command operation.",
			},
		});
		expect(bridge.readCommandResultRequests).toHaveLength(1);
		expect(sleep.calls).toHaveLength(0);
	});

	test("routes memory through Bridge RunMemory", async () => {
		const bridge = new RecordingBridgeClient();
		const runner = makeRunner({ bridge });

		const result = await runner.runTool(
			toolRequest("memory", {
				action: "create",
				path: "notes/todo.md",
				content: "one",
			}),
		);

		expect(result.type).toBe("completed");
		expect(bridge.runMemoryRequests).toEqual([
			{
				toolUseEventId: "sevt_tool_1",
				scope: {
					workspaceId: "wksp_1",
					sessionId: "sesn_1",
					sessionThreadId: "thrd_1",
					binding: {
						bindingId: "bind_1",
						bindingGeneration: 42,
						targetPodUid: "pod_1",
					},
				},
			},
		]);
	});

	test("rejoins Memory after transport loss with the same request identity", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.runMemoryErrors.push(new Error("bridge unavailable"));
		const sleep = new ControlledSleep();

		const pending = makeRunner({
			bridge,
			sleep: sleep.sleep,
		}).runTool(
			toolRequest("memory", {
				action: "create",
				path: "notes/todo.md",
				content: "one",
			}),
		);
		await waitForCondition(
			() => sleep.calls.length === 1,
			"Memory transport rejoin wait",
		);
		sleep.releaseNext();
		const result = await pending;

		expect(result.type).toBe("completed");
		expect(bridge.runMemoryRequests).toHaveLength(2);
		expect(bridge.runMemoryRequests[1]).toEqual(bridge.runMemoryRequests[0]);
	});

	test("settles a typed Memory rejection without consuming the transport retry loop", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.runMemoryErrors.push(
			Object.assign(new Error("memory identity rejected"), {
				code: GrpcStatus.FAILED_PRECONDITION,
			}),
		);
		const sleep = new ControlledSleep();

		const result = await makeRunner({
			bridge,
			sleep: sleep.sleep,
		}).runTool(
			toolRequest("memory", {
				action: "create",
				path: "notes/todo.md",
				content: "one",
			}),
		);

		expect(result).toMatchObject({
			type: "error",
			error: { retryable: false },
		});
		expect(bridge.runMemoryRequests).toHaveLength(1);
		expect(sleep.calls).toHaveLength(0);
	});

	test("preserves model-visible Memory stale and refresh signals", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.runMemoryResultJson = JSON.stringify({
			status: "tool_error",
			error_code: "path_exists",
			message: "The requested path conflicts with existing memory.",
			reread_required: true,
			projection_refreshed: true,
			retryable: false,
			conflicting_paths: ["notes", "notes/todo.md"],
			memory_store_id: "mem_internal",
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("memory", { action: "create", path: "notes" }),
		);

		expect(result.type).toBe("error");
		expect(JSON.stringify(result)).toContain("error_code: path_exists");
		expect(JSON.stringify(result)).toContain("reread_required: true");
		expect(JSON.stringify(result)).toContain("projection_refreshed: true");
		expect(JSON.stringify(result)).toContain(
			'conflicting_paths: [\\"notes\\",\\"notes/todo.md\\"]',
		);
		expect(JSON.stringify(result)).not.toContain("mem_internal");
	});

	test("recursively removes formatter-declared forbidden result fields", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: {
				raw_bytes: "top-secret-bytes",
				nested: { rawBytes: "nested-secret-bytes", summary: "visible" },
			},
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Read", { file_path: "notes/a.txt" }),
		);

		expect(JSON.stringify(result)).not.toContain("secret-bytes");
		expect(JSON.stringify(result)).toContain("visible");
	});

	test("does not replace a route-bounded Read result with a universal text prefix", async () => {
		const bridge = new RecordingBridgeClient();
		const content = "x".repeat(60 * 1024);
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: { content, next_offset: 61440, total_lines: 2000 },
		});

		const result = await makeRunner({ bridge }).runTool(
			toolRequest("Read", { file_path: "notes/a.txt" }),
		);

		expect(result.type).toBe("completed");
		if (result.type !== "completed")
			throw new Error("expected completed result");
		expect(result.output.text).toContain(content);
		expect(result.output.text).toContain("next_offset: 61440");
		expect(result.output.truncated).toBe(false);
	});

	test("routes web through Gateway RunWeb with Runtime binding token", async () => {
		const gateway = new RecordingGatewayClient();
		const runner = makeRunner({ gateway });

		const result = await runner.runTool(
			toolRequest("web", {
				search_query: [{ q: "tetral", domains: ["example.com"] }],
			}),
		);

		expect(result).toEqual({
			type: "completed",
			output: { text: "web result", truncated: false },
			serverToolUse: { webSearchRequests: 2, webFetchRequests: 1 },
		});
		expect(gateway.runWebRequests).toEqual([
			expect.objectContaining({
				workspaceId: "wksp_1",
				sessionId: "sesn_1",
				sessionThreadId: "thrd_1",
				toolUseEventId: "sevt_tool_1",
				bindingId: "bind_1",
				bindingGeneration: 42,
				runtimeBindingToken: "binding-token",
				input: {
					searchQuery: [{ q: "tetral", domains: ["example.com"] }],
					open: [],
					find: [],
				},
			}),
		]);
	});

	test("uses the Web connector's model-visible text without encoding the response twice", async () => {
		const gateway = new RecordingGatewayClient();
		gateway.runWebResponse = {
			...gateway.runWebResponse,
			resultText: 'quoted \\"window\\" with \\\\slashes',
			refs: [{ refId: "ref_1", url: "https://example.com", title: "Example" }],
			windowTruncated: true,
			nextLineno: 42,
		};

		const result = await makeRunner({ gateway }).runTool(
			toolRequest("web", { open: [{ ref_id: "ref_1" }] }),
		);

		expect(result).toMatchObject({
			type: "completed",
			output: { text: gateway.runWebResponse.resultText, truncated: false },
		});
		expect(JSON.stringify(result)).not.toContain("window_truncated");
	});

	test("settles a genuine Web result bound violation as a deterministic tool failure", async () => {
		const gateway = new RecordingGatewayClient();
		gateway.runWebResponse = {
			...gateway.runWebResponse,
			resultText: "\u0001".repeat(87_380),
		};

		const result = await makeRunner({ gateway }).runTool(
			toolRequest("web", { open: [{ ref_id: "ref_1" }] }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				message: "Tool result exceeds the 512 KiB model-visible output limit.",
				retryable: false,
			},
		});
	});

	test("rejects Web requests outside the shared semantic envelope before transport", async () => {
		const gateway = new RecordingGatewayClient();
		const runner = makeRunner({ gateway });
		const cases = [
			{
				input: {
					search_query: Array.from({ length: 9 }, () => ({ q: "tetral" })),
				},
				message: "web accepts at most 8 operations.",
			},
			{
				input: { search_query: [{ q: "x".repeat(64 * 1024 + 1) }] },
				message: "web search_query contains an invalid operation.",
			},
			{
				input: {
					search_query: [
						{
							q: "tetral",
							domains: Array.from({ length: 5 }, () => "example.com"),
						},
					],
				},
				message: "web search_query contains an invalid operation.",
			},
			{
				input: { search_query: [{ q: "tetral", domains: "example.com" }] },
				message: "web search_query contains an invalid operation.",
			},
			{
				input: { open: [{ url: "https://example.com", ref_id: "ref_1" }] },
				message: "web open contains an invalid operation.",
			},
			{
				input: {
					find: [{ ref_id: "ref_1", pattern: "x".repeat(64 * 1024 + 1) }],
				},
				message: "web find contains an invalid operation.",
			},
		];

		for (const testCase of cases) {
			const result = await runner.runTool(toolRequest("web", testCase.input));
			expect(result).toMatchObject({
				type: "error",
				error: { retryable: false, message: testCase.message },
			});
		}
		expect(gateway.runWebRequests).toHaveLength(0);
	});

	test("accepts exactly eight Web operations before transport", async () => {
		const gateway = new RecordingGatewayClient();
		const result = await makeRunner({ gateway }).runTool(
			toolRequest("web", {
				search_query: Array.from({ length: 8 }, (_, index) => ({
					q: `query-${index}-${"q".repeat(64 * 1024 - `query-${index}-`.length)}`,
				})),
			}),
		);

		expect(result.type).toBe("completed");
		expect(gateway.runWebRequests[0]?.input?.searchQuery).toHaveLength(8);
		expect(
			gateway.runWebRequests[0]?.input?.searchQuery.every(
				(query) => query.q.length === 64 * 1024,
			),
		).toBe(true);
	});

	test("web ignores the undeclared refId alias", async () => {
		const gateway = new RecordingGatewayClient();
		const result = await makeRunner({ gateway }).runTool(
			toolRequest("web", {
				open: [{ refId: "ref_legacy" }],
			}),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				retryable: false,
				message: "web open contains an invalid operation.",
			},
		});
		expect(gateway.runWebRequests).toHaveLength(0);
	});

	test("carries web usage on a durable tool-error result without inventing it for transport failures", async () => {
		const gateway = new RecordingGatewayClient();
		gateway.runWebResponse = {
			...gateway.runWebResponse,
			status: RunWebStatus.RUN_WEB_STATUS_TOOL_ERROR,
			resultText: "target rejected",
			usage: {
				operation: "open",
				backendTokens: 0,
				targetHttpStatus: 422,
				storedBytes: 0,
				durationMs: 8,
				webSearchRequests: 0,
				webFetchRequests: 1,
			},
		};
		const result = await makeRunner({ gateway }).runTool(
			toolRequest("web", { open: [{ ref_id: "https://example.invalid" }] }),
		);
		expect(result).toMatchObject({
			type: "error",
			serverToolUse: { webSearchRequests: 0, webFetchRequests: 1 },
		});

		gateway.runWebResponse = { ...gateway.runWebResponse, usage: undefined };
		const malformed = await makeRunner({ gateway }).runTool(
			toolRequest("web", { open: [{ ref_id: "https://example.invalid" }] }),
		);
		expect(malformed).toMatchObject({
			type: "error",
			error: { retryable: true },
		});
		expect(malformed).not.toHaveProperty("serverToolUse");
	});

	test("accepts the per-call web usage maxima and rejects counters above them", async () => {
		const gateway = new RecordingGatewayClient();
		gateway.runWebResponse = {
			...gateway.runWebResponse,
			usage: {
				...gateway.runWebResponse.usage!,
				webSearchRequests: 32,
				webFetchRequests: 8,
			},
		};
		const accepted = await makeRunner({ gateway }).runTool(
			toolRequest("web", { search_query: [{ q: "tetral" }] }),
		);
		expect(accepted).toMatchObject({
			type: "completed",
			serverToolUse: { webSearchRequests: 32, webFetchRequests: 8 },
		});

		gateway.runWebResponse = {
			...gateway.runWebResponse,
			usage: { ...gateway.runWebResponse.usage!, webSearchRequests: 33 },
		};
		const tooManySearches = await makeRunner({ gateway }).runTool(
			toolRequest("web", { search_query: [{ q: "tetral" }] }),
		);
		expect(tooManySearches).toMatchObject({
			type: "error",
			error: { retryable: true },
		});
		expect(tooManySearches).not.toHaveProperty("serverToolUse");

		gateway.runWebResponse = {
			...gateway.runWebResponse,
			usage: {
				...gateway.runWebResponse.usage!,
				webSearchRequests: 0,
				webFetchRequests: 9,
			},
		};
		const tooManyFetches = await makeRunner({ gateway }).runTool(
			toolRequest("web", { open: [{ ref_id: "https://example.com" }] }),
		);
		expect(tooManyFetches).toMatchObject({
			type: "error",
			error: { retryable: true },
		});
		expect(tooManyFetches).not.toHaveProperty("serverToolUse");
	});

	test("routes MCP tools through the MCP connector with Runtime binding token", async () => {
		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
			resultText: "mcp result",
			attachments: [],
			errorKind: undefined,
		};
		const runner = makeRunner({ mcp });

		const result = await runner.runTool(mcpToolRequest({ query: "issues" }));

		expect(result).toEqual({
			type: "completed",
			output: { text: "mcp result", truncated: false },
		});
		expect(mcp.runMcpToolRequests).toEqual([
			{
				workspaceId: "wksp_1",
				sessionId: "sesn_1",
				sessionThreadId: "thrd_1",
				toolUseEventId: "sevt_tool_1",
				bindingId: "bind_1",
				bindingGeneration: 42,
				runtimeBindingToken: "binding-token",
			},
		]);
	});

	test("accepts a completed MCP result without a cross-service result alias", async () => {
		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
			resultText: "must not reach the model",
			attachments: [],
			errorKind: undefined,
		};

		await expect(
			makeRunner({ mcp }).runTool(mcpToolRequest({ query: "issues" })),
		).resolves.toMatchObject({ type: "completed" });
		expect(mcp.runMcpToolRequests).toHaveLength(1);
	});

	test("accepts the true 50 KiB escape-dense MCP result without a line-cap shortcut", async () => {
		const mcp = new RecordingMcpConnectorClient();
		const resultText = "\u0001".repeat(50 * 1024);
		mcp.runMcpToolResponse = {
			...mcp.runMcpToolResponse,
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
			resultText,
		};

		const result = await makeRunner({ mcp }).runTool(
			mcpToolRequest({ query: "issues" }),
		);

		expect(result).toMatchObject({
			type: "completed",
			output: { text: resultText, truncated: false },
		});
		expect(resultText).not.toContain("\n");
		expect(
			Buffer.byteLength(JSON.stringify({ text: resultText }), "utf8"),
		).toBeLessThanOrEqual(MaxProviderRequestToolOutputJsonBytes);
	});

	test("uses one model-visible output contract for Web, MCP, and command results", async () => {
		const web = new RecordingGatewayClient();
		web.runWebResponse = {
			...web.runWebResponse,
			resultText: "\u0001".repeat(87_379),
		};
		const webResult = await makeRunner({ gateway: web }).runTool(
			toolRequest("web", { open: [{ ref_id: "ref_1" }] }),
		);

		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			...mcp.runMcpToolResponse,
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
			resultText: "\u0001".repeat(50 * 1024),
		};
		const mcpResult = await makeRunner({ mcp }).runTool(
			mcpToolRequest({ query: "issues" }),
		);

		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "running",
			result: {
				task_id: "task_contract",
				stdout: {
					text: `COMMAND_HEAD${"x".repeat(20_000)}COMMAND_TAIL`,
					truncated: false,
				},
			},
		});
		bridge.awaitSandboxExecutionTaskId = "task_contract";
		const commandResult = await makeRunner({ bridge }).runTool(
			toolRequest("exec_command", { cmd: "build" }),
		);

		for (const [toolName, input, result] of [
			["web", { open: [{ ref_id: "ref_1" }] }, webResult],
			["mcp", { query: "issues" }, mcpResult],
			["exec_command", { cmd: "build" }, commandResult],
		] as const) {
			expect(result.type, toolName).toBe("completed");
			if (result.type !== "completed")
				throw new Error(`expected completed ${toolName} result`);
			const projected = toGatewayProviderContext([
				completedToolMessage(toolName, input, result.output.text),
			]);
			expect(projected.ok, toolName).toBe(true);
			if (!projected.ok)
				throw new Error(`expected projected ${toolName} result`);
			const outputJson =
				projected.context[0]?.content.find(
					(item) => item.toolResult !== undefined,
				)?.toolResult?.completed?.outputJson ?? "";
			expect(outputJson, toolName).toBe(
				JSON.stringify({ text: result.output.text }),
			);
			expect(
				Buffer.byteLength(outputJson, "utf8"),
				toolName,
			).toBeLessThanOrEqual(MaxProviderRequestToolOutputJsonBytes);
		}
	});

	test("does not rejoin the sandbox result wait after a deterministic output bound violation", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.awaitSandboxExecutionResultJson = JSON.stringify({
			status: "success",
			result: { content: "\u0001".repeat(87_380) },
		});
		const sleep = new ControlledSleep();

		const result = await makeRunner({ bridge, sleep: sleep.sleep }).runTool(
			toolRequest("Read", { file_path: "notes/a.txt" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				message: "Tool result exceeds the 512 KiB model-visible output limit.",
				retryable: false,
			},
		});
		expect(bridge.acceptSandboxExecutionRequests).toHaveLength(1);
		expect(bridge.awaitSandboxExecutionRequests).toHaveLength(1);
		expect(sleep.calls).toHaveLength(0);
	});

	test("maps MCP tool_error to a model-visible tool error", async () => {
		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,
			resultText: "invalid repository",
			attachments: [],
			errorKind: McpErrorKind.MCP_ERROR_KIND_TOOL_ERROR,
		};
		const runner = makeRunner({ mcp });

		const result = await runner.runTool(mcpToolRequest({ repo: "" }));

		expect(result).toMatchObject({
			type: "error",
			error: {
				retryable: false,
				message: "invalid repository",
			},
		});
	});

	test("uses MCP tool-error attachment refs without a Runtime-side byte handoff", async () => {
		const bridge = new RecordingBridgeClient();
		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_TOOL_ERROR,
			resultText: "tool returned an image error",
			attachments: [
				{
					attachmentRef: "att_mcp_error",
					mime: "image/png",
					sizeBytes: 3,
					suggestedFilename: "error.png",
				},
			],
			errorKind: McpErrorKind.MCP_ERROR_KIND_TOOL_ERROR,
		};
		const runner = makeRunner({ bridge, mcp });

		const result = await runner.runTool(mcpToolRequest({ repo: "" }));

		expect(result).toMatchObject({
			type: "error",
			error: { retryable: false, message: "tool returned an image error" },
			attachments: [
				{
					transient: {
						attachmentRef: "att_mcp_error",
						sourcePath: "mcp:github/error.png",
					},
					fileBacked: undefined,
					mime: "image/png",
					filename: "error.png",
				},
			],
		});
	});

	test("settles MCP infrastructure failures as one generic non-retryable Tool error", async () => {
		for (const response of [
			{
				errorKind: McpErrorKind.MCP_ERROR_KIND_AUTHENTICATION_FAILED,
				retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
			},
			{
				errorKind: McpErrorKind.MCP_ERROR_KIND_CREDENTIAL_REQUIRED,
				retryStatus: McpRetryStatus.MCP_RETRY_STATUS_TERMINAL,
			},
			{
				errorKind: McpErrorKind.MCP_ERROR_KIND_CONNECTION_FAILED,
				retryStatus: McpRetryStatus.MCP_RETRY_STATUS_EXHAUSTED,
			},
		]) {
			const mcp = new RecordingMcpConnectorClient();
			mcp.runMcpToolResponse = {
				status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
				resultText: "credential and provider response must not escape",
				attachments: [],
				errorKind: response.errorKind,
				retryStatus: response.retryStatus,
			};
			const runner = makeRunner({ mcp });

			const result = await runner.runTool(mcpToolRequest({ repo: "tetral" }));

			expect(result).toMatchObject({
				type: "error",
				error: expect.objectContaining({
					retryable: false,
					message: "MCP tool execution is unavailable.",
				}),
			});
		}
	});

	test("projects pre-commit MCP uncertainty without a discard loop", async () => {
		for (const testCase of [
			{
				errorKind: McpErrorKind.MCP_ERROR_KIND_IN_FLIGHT,
				message:
					"The MCP tool execution is still in progress. Check the external service before retrying.",
			},
			{
				errorKind: McpErrorKind.MCP_ERROR_KIND_COMMIT_FAILED,
				message:
					"The MCP tool may have completed, but its result could not be confirmed. Check the external service before retrying.",
			},
			{
				errorKind: McpErrorKind.MCP_ERROR_KIND_CLAIM_CONFLICT,
				message:
					"The MCP tool request conflicts with an existing execution. Check the external service before retrying.",
			},
		]) {
			const mcp = new RecordingMcpConnectorClient();
			mcp.runMcpToolResponse = {
				status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
				resultText: "connector details must not escape",
				attachments: [],
				errorKind: testCase.errorKind,
				retryStatus: undefined,
			};
			const runner = makeRunner({ mcp });

			const first = await runner.runTool(mcpToolRequest({ repo: "tetral" }));
			const repeated = await runner.runTool(mcpToolRequest({ repo: "tetral" }));

			expect(first).toMatchObject({
				type: "error",
				error: {
					retryable: false,
					message: testCase.message,
				},
			});
			expect(repeated).toEqual(first);
			expect(mcp.runMcpToolRequests).toHaveLength(2);
		}
	});

	test("returns stale custody without projecting a model-visible MCP failure", async () => {
		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_RUNTIME_ERROR,
			resultText: "MCP tool execution lost runtime custody.",
			attachments: [],
			errorKind: McpErrorKind.MCP_ERROR_KIND_CUSTODY_LOST,
			retryStatus: undefined,
		};
		const runner = makeRunner({ mcp });

		await expect(
			runner.runTool(mcpToolRequest({ repo: "tetral" })),
		).resolves.toEqual({
			type: "stale_custody",
		});
	});

	test("returns MCP attachment refs without a Runtime-side byte handoff", async () => {
		const bridge = new RecordingBridgeClient();
		const mcp = new RecordingMcpConnectorClient();
		mcp.runMcpToolResponse = {
			status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
			resultText: "image",
			attachments: [
				{
					attachmentRef: "att_mcp_plot",
					mime: "image/png",
					sizeBytes: 3,
					suggestedFilename: "plot.png",
				},
			],
			errorKind: undefined,
		};
		const runner = makeRunner({ bridge, mcp });

		const result = await runner.runTool(mcpToolRequest({ repo: "tetral" }));

		expect(result).toMatchObject({
			type: "completed",
			output: { text: "image", truncated: false },
			attachments: [
				{
					transient: {
						attachmentRef: "att_mcp_plot",
						sourcePath: "mcp:github/plot.png",
						pageRange: "",
						detail: "auto",
					},
					fileBacked: undefined,
					mime: "image/png",
					filename: "plot.png",
				},
			],
		});
	});

	test("spawns a sub-agent by atomically declaring the child and its opening input", async () => {
		const bridge = new RecordingBridgeClient();
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("spawn_agent", {
				task_name: "researcher",
				prompt: "work on this",
				agent_type: "research",
				fork_turns: "none",
			}),
		);

		expect(result.type).toBe("completed");
		expect(bridge.createSubagentThreadRequests).toEqual([
			expect.objectContaining({
				taskName: "researcher",
				agentType: "research",
				sourceToolUseEventId: "sevt_tool_1",
				forkTurns: "none",
				initialPrompt: "work on this",
			}),
		]);
		expect(bridge.deliverInterAgentMailRequests).toEqual([]);
		expect(subAgentHost.actions).toEqual(["preload"]);
		expect(subAgentHost.preloaded).toEqual([
			expect.objectContaining({
				sessionThreadId: "thr_child_1",
				thread: expect.objectContaining({
					parentThreadId: "thrd_1",
					role: "subagent",
					taskName: "researcher",
				}),
			}),
		]);
		expect(subAgentHost.enqueued).toEqual([]);
		expect(JSON.stringify(result)).not.toContain("delivery");
		expect(JSON.stringify(result)).not.toContain("binding-token");
	});

	test("rejects legacy camelCase sub-agent tool inputs", async () => {
		const runner = makeRunner({
			bridge: new RecordingBridgeClient(),
			subAgentHost: new RecordingSubAgentHost(),
		});

		const spawn = await runner.runTool(
			toolRequest("spawn_agent", {
				taskName: "researcher",
				prompt: "work on this",
				agentType: "research",
				forkTurns: "none",
			}),
		);
		const wait = await runner.runTool(
			toolRequest("wait_agent", { taskName: "researcher", timeoutMs: 25 }),
		);

		expect(spawn).toMatchObject({
			type: "error",
			error: {
				retryable: false,
				message: expect.stringContaining("task_name"),
			},
		});
		expect(wait).toMatchObject({
			type: "error",
			error: {
				retryable: false,
				message: expect.stringContaining("task_name"),
			},
		});
	});

	test("spawn_agent and send_message ignore undeclared text aliases", async () => {
		const runner = makeRunner({
			bridge: new RecordingBridgeClient(),
			subAgentHost: new RecordingSubAgentHost(),
		});

		const spawn = await runner.runTool(
			toolRequest("spawn_agent", { task_name: "worker", message: "legacy" }),
		);
		const send = await runner.runTool(
			toolRequest("send_message", { task_name: "worker", prompt: "legacy" }),
		);

		expect(spawn).toMatchObject({
			type: "error",
			error: { retryable: false, message: expect.stringContaining("prompt") },
		});
		expect(send).toMatchObject({
			type: "error",
			error: { retryable: false, message: expect.stringContaining("message") },
		});
	});

	test("rejects oversized task names and fork counts before any durable actor mutation", async () => {
		const bridge = new RecordingBridgeClient();
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const oversizedName = await runner.runTool(
			toolRequest("spawn_agent", {
				task_name: "é".repeat(65),
				prompt: "work",
			}),
		);
		const oversizedFork = await runner.runTool(
			toolRequest("spawn_agent", {
				task_name: "worker",
				prompt: "work",
				fork_turns: "1001",
			}),
		);

		expect(oversizedName).toMatchObject({
			type: "error",
			error: { retryable: false },
		});
		expect(oversizedFork).toMatchObject({
			type: "error",
			error: { retryable: false },
		});
		expect(bridge.createSubagentThreadRequests).toEqual([]);
		expect(bridge.deliverInterAgentMailRequests).toEqual([]);
	});

	test("rejects zero-arm and contradictory actor responses at every consumed oneof", async () => {
		const createBridge = new RecordingBridgeClient();
		createBridge.createSubagentThreadResponse = {
			committed: { childThreadId: "thr_child_1" },
			duplicate: { childThreadId: "thr_child_1" },
		};
		expect(
			await makeRunner({
				bridge: createBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(
				toolRequest("spawn_agent", { task_name: "worker", prompt: "work" }),
			),
		).toMatchObject({ type: "error", error: { retryable: true } });

		const deliveryBridge = new RecordingBridgeClient();
		deliveryBridge.deliverInterAgentMailResponse = {
			committed: {},
			duplicate: {},
		};
		expect(
			await makeRunner({
				bridge: deliveryBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(
				toolRequest("send_message", { task_name: "worker", message: "work" }),
			),
		).toMatchObject({ type: "error" });

		const listBridge = new RecordingBridgeClient();
		listBridge.listChildThreadsResponse = {};
		expect(
			await makeRunner({
				bridge: listBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(toolRequest("wait_agent", { task_name: "worker" })),
		).toMatchObject({
			type: "error",
			error: {
				retryable: true,
				message: "Bridge returned malformed child-thread facts.",
			},
		});

		const resolveBridge = new RecordingBridgeClient();
		resolveBridge.resolveChildThreadResponse = {};
		expect(
			await makeRunner({
				bridge: resolveBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(toolRequest("wait_agent", { task_name: "worker" })),
		).toMatchObject({
			type: "error",
			error: {
				retryable: true,
				message: "Bridge returned malformed child-thread facts.",
			},
		});

		const interruptBridge = new RecordingBridgeClient();
		interruptBridge.admitChildInterruptResponse = {
			committed: { controlOperationId: "ctrl_1" },
			duplicate: { controlOperationId: "ctrl_1" },
		};
		expect(
			await makeRunner({
				bridge: interruptBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(toolRequest("interrupt_agent", { task_name: "worker" })),
		).toMatchObject({ type: "error" });

		const awaitBridge = new RecordingBridgeClient();
		awaitBridge.awaitChildInterruptResponse = {};
		expect(
			await makeRunner({
				bridge: awaitBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(toolRequest("interrupt_agent", { task_name: "worker" })),
		).toMatchObject({ type: "error" });

		const closeBridge = new RecordingBridgeClient();
		closeBridge.closeChildControlResponse = {
			committed: { children: [] },
			duplicate: { children: [] },
		};
		expect(
			await makeRunner({
				bridge: closeBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(toolRequest("close_agent", { task_name: "worker" })),
		).toMatchObject({ type: "error" });

		const resumeBridge = new RecordingBridgeClient();
		resumeBridge.markChildThreadActiveResponse = {
			committed: {},
			duplicate: {},
		};
		expect(
			await makeRunner({
				bridge: resumeBridge,
				subAgentHost: new RecordingSubAgentHost(),
			}).runTool(toolRequest("resume_agent", { task_name: "worker" })),
		).toMatchObject({ type: "error" });
	});

	test("atomic sub-agent creation returns delivered without enqueueing child input in process", async () => {
		const bridge = new RecordingBridgeClient();
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("spawn_agent", {
				task_name: "researcher",
				prompt: "work on this",
				agent_type: "research",
				fork_turns: "none",
			}),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: delivered"),
			}),
		});
		expect(bridge.createSubagentThreadRequests[0]?.initialPrompt).toBe(
			"work on this",
		);
		expect(bridge.deliverInterAgentMailRequests).toHaveLength(0);
		expect(subAgentHost.enqueued).toEqual([]);
	});

	test("replays ambiguous atomic child creation with an identical declaration", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.createSubagentThreadErrors.push(grpcError(GrpcStatus.UNAVAILABLE));
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const result = await runner.runTool(
			toolRequest("spawn_agent", {
				task_name: "researcher",
				prompt: "work on this",
				fork_turns: "none",
			}),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(bridge.createSubagentThreadRequests).toHaveLength(2);
		expect(bridge.createSubagentThreadRequests[1]).toEqual(
			bridge.createSubagentThreadRequests[0],
		);
		expect(bridge.deliverInterAgentMailRequests).toHaveLength(0);
	});

	test("replays ambiguous child interrupt admission and await with identical operation identities", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.admitChildInterruptErrors.push(grpcError(GrpcStatus.UNKNOWN));
		bridge.awaitChildInterruptErrors.push(grpcError(GrpcStatus.UNAVAILABLE));
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const result = await runner.runTool(
			toolRequest("interrupt_agent", { task_name: "worker" }),
		);

		expect(result).toMatchObject({ type: "completed" });
		expect(bridge.admitChildInterruptRequests).toHaveLength(2);
		expect(bridge.admitChildInterruptRequests[0]).toEqual(
			expect.objectContaining({
				targetChildThreadId: "thr_child_1",
				action: ChildControlAction.CHILD_CONTROL_ACTION_INTERRUPT,
			}),
		);
		expect(bridge.admitChildInterruptRequests[1]).toEqual(
			bridge.admitChildInterruptRequests[0],
		);
		expect(bridge.awaitChildInterruptRequests).toHaveLength(2);
		expect(bridge.awaitChildInterruptRequests[1]).toEqual(
			bridge.awaitChildInterruptRequests[0],
		);
	});

	test("replays ambiguous child close and resume without changing their immutable source identity", async () => {
		const closeBridge = new RecordingBridgeClient();
		closeBridge.closeChildControlErrors.push(grpcError(GrpcStatus.INTERNAL));
		const close = await makeRunner({
			bridge: closeBridge,
			subAgentHost: new RecordingSubAgentHost(),
		}).runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_close"),
		);
		expect(close).toMatchObject({ type: "completed" });
		expect(closeBridge.admitChildInterruptRequests[0]).toEqual(
			expect.objectContaining({
				targetChildThreadId: "thr_child_1",
				action: ChildControlAction.CHILD_CONTROL_ACTION_CLOSE,
			}),
		);
		expect(closeBridge.closeChildControlRequests).toHaveLength(2);
		expect(closeBridge.closeChildControlRequests[1]).toEqual(
			closeBridge.closeChildControlRequests[0],
		);

		const resumeBridge = new RecordingBridgeClient();
		resumeBridge.childStatus = "closed_for_runtime";
		resumeBridge.markChildThreadActiveErrors.push(
			grpcError(GrpcStatus.ABORTED),
		);
		const resume = await makeRunner({
			bridge: resumeBridge,
			subAgentHost: new RecordingSubAgentHost(),
		}).runTool(
			toolRequest("resume_agent", { task_name: "worker" }, "sevt_resume"),
		);
		expect(resume).toMatchObject({ type: "completed" });
		expect(resumeBridge.markChildThreadActiveRequests).toHaveLength(2);
		expect(resumeBridge.markChildThreadActiveRequests[0]).toEqual(
			expect.objectContaining({ targetChildThreadId: "thr_child_1" }),
		);
		expect(resumeBridge.markChildThreadActiveRequests[1]).toEqual(
			resumeBridge.markChildThreadActiveRequests[0],
		);
	});

	test("send_message keeps atomic durable admission authoritative after caller cancellation", async () => {
		const bridge = new RecordingBridgeClient();
		const abort = new AbortController();
		bridge.afterDeliverInterAgentMailResponse = () => abort.abort();
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const result = await runner.runTool(
			toolRequest(
				"send_message",
				{ task_name: "worker", message: "keep going" },
				"sevt_tool_send_cancel_after_sent",
				abort.signal,
			),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: delivered"),
			}),
		});
		expect(bridge.deliverInterAgentMailRequests).toEqual([
			expect.objectContaining({
				targetThreadId: "thr_child_1",
				sourceToolUseEventId: "sevt_tool_send_cancel_after_sent",
				content: "keep going",
			}),
		]);
	});

	test("duplicate spawn_agent task names return a non-retryable tool error", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.createSubagentThreadErrorCode = GrpcStatus.ALREADY_EXISTS;
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const result = await runner.runTool(
			toolRequest("spawn_agent", {
				task_name: "researcher",
				prompt: "work on this",
				agent_type: "research",
				fork_turns: "none",
			}),
		);

		expect(result).toEqual({
			type: "error",
			error: expect.objectContaining({
				message: expect.stringContaining("already exists"),
				retryable: false,
			}),
		});
	});

	test("send_message serializes delivery resolution before a concurrent close releases hot state", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.deferDeliverInterAgentMail = true;
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const send = runner.runTool(
			toolRequest(
				"send_message",
				{
					task_name: "worker",
					message: "keep going",
				},
				"sevt_tool_send",
			),
		);
		await waitForCondition(
			() => bridge.deliverInterAgentMailRequests.length === 1,
			"send delivery resolution",
		);

		const close = runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(bridge.listChildThreadsRequests).toHaveLength(1);
		expect(bridge.closeChildControlRequests).toEqual([]);

		bridge.completeDeferredDeliverInterAgentMail();
		expect(await send).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: delivered"),
			}),
		});
		expect(await close).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: closed_for_runtime"),
			}),
		});
		expect(subAgentHost.actions).toEqual(["preload", "close"]);
	});

	test("close_agent commits durable closed projection before releasing hot child state", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.closeReceiptTargetIds = ["thr_child_1", "thr_grandchild_1"];
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.onClose = () => {
			expect(bridge.closeChildControlRequests).toHaveLength(1);
		};
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: closed_for_runtime"),
			}),
		});
		expect(subAgentHost.actions).toEqual(["close", "close"]);
		expect(subAgentHost.closedThreadIds).toEqual([
			"thr_child_1",
			"thr_grandchild_1",
		]);
		expect(bridge.closeChildControlRequests).toHaveLength(1);
	});

	test("close_agent preserves the root terminal status while releasing every stamped hot subtree target", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "failed";
		bridge.closeReceiptTargetIds = [
			"thr_child_1",
			"thr_terminated_1",
			"thr_running_1",
		];
		bridge.closeReceiptDispositions.set(
			"thr_child_1",
			ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED,
		);
		bridge.closeReceiptDispositions.set(
			"thr_terminated_1",
			ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED,
		);
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: failed"),
			}),
		});
		expect(subAgentHost.closedThreadIds).toEqual([
			"thr_child_1",
			"thr_terminated_1",
			"thr_running_1",
		]);
	});

	test("close_agent rejects a lifecycle receipt from another Runtime binding", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childLifecycleObservedBindingId = "bind_other";
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);
		const replay = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);

		expect(result).toEqual({ type: "stale_custody" });
		expect(replay).toEqual({ type: "stale_custody" });
		const firstCloseRequest = bridge.closeChildControlRequests[0];
		expect(firstCloseRequest).toBeDefined();
		expect(bridge.closeChildControlRequests.map((request) => request)).toEqual([
			firstCloseRequest!,
			firstCloseRequest!,
		]);
		expect(subAgentHost.actions).toEqual([]);
	});

	test("close_agent returns stale custody without releasing hot child state", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.closeChildControlStale = true;
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);

		expect(result).toEqual({ type: "stale_custody" });
		expect(bridge.closeChildControlRequests).toHaveLength(1);
		expect(subAgentHost.actions).toEqual([]);
	});

	test("close_agent reports retryable failure when hot close fails after durable close", async () => {
		const bridge = new RecordingBridgeClient();
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.closeResults.push({
			ok: false,
			sessionId: "sesn_1",
			sessionThreadId: "thr_child_1",
			reason: "thread_busy",
		});
		const runner = makeRunner({ bridge, subAgentHost });

		const first = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);
		await Bun.sleep(5);
		const second = await runner.runTool(
			toolRequest("close_agent", { task_name: "worker" }, "sevt_tool_close"),
		);

		expect(first).toEqual({
			type: "error",
			error: expect.objectContaining({
				message: expect.stringContaining("thread_busy"),
				retryable: true,
			}),
		});
		expect(second).toMatchObject({ type: "completed" });
		expect(subAgentHost.actions).toEqual(["close", "close"]);
		expect(bridge.closeChildControlRequests).toHaveLength(2);
		expect(bridge.closeChildControlRequests[1]?.controlOperationId).toEqual(
			bridge.closeChildControlRequests[0]?.controlOperationId,
		);
	});

	test("send_message keeps same-child inputs in model order when the first child lookup is slow", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.deferFirstListChildThreads = true;
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const first = runner.runTool(
			toolRequest(
				"send_message",
				{
					task_name: "worker",
					message: "first",
				},
				"sevt_tool_send_1",
				new AbortController().signal,
				0,
			),
		);
		await waitForCondition(
			() => bridge.listChildThreadsRequests.length === 1,
			"first send child lookup",
		);

		const second = runner.runTool(
			toolRequest(
				"send_message",
				{
					task_name: "worker",
					message: "second",
				},
				"sevt_tool_send_2",
				new AbortController().signal,
				1,
			),
		);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(bridge.listChildThreadsRequests).toHaveLength(1);
		expect(subAgentHost.enqueued).toHaveLength(0);

		bridge.completeDeferredListChildThreads();
		await Promise.all([first, second]);

		expect(subAgentHost.enqueued).toEqual([]);
		expect(
			bridge.deliverInterAgentMailRequests.map(
				(request) => request.sourceToolUseEventId,
			),
		).toEqual(["sevt_tool_send_1", "sevt_tool_send_2"]);
	});

	test("send_message drops an aborted operation while it is waiting in the same-task queue", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.deferFirstListChildThreads = true;
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const first = runner.runTool(
			toolRequest(
				"send_message",
				{
					task_name: "worker",
					message: "first",
				},
				"sevt_tool_send_1",
				new AbortController().signal,
				0,
			),
		);
		await waitForCondition(
			() => bridge.listChildThreadsRequests.length === 1,
			"first send child lookup",
		);

		const secondController = new AbortController();
		const second = runner.runTool(
			toolRequest(
				"send_message",
				{
					task_name: "worker",
					message: "cancelled",
				},
				"sevt_tool_send_2",
				secondController.signal,
				1,
			),
		);
		secondController.abort();

		expect(await second).toEqual({
			type: "cancelled",
			error: expect.objectContaining({
				code: "runtime_invalid_sequence",
				message: "Sub-agent send was cancelled.",
				retryable: false,
			}),
		});
		expect(bridge.listChildThreadsRequests).toHaveLength(1);

		bridge.completeDeferredListChildThreads();
		await first;

		expect(subAgentHost.enqueued).toHaveLength(0);
		expect(
			bridge.deliverInterAgentMailRequests.map(
				(request) => request.sourceToolUseEventId,
			),
		).toEqual(["sevt_tool_send_1"]);
	});

	test("fork_turns counts complete user-led turns without splitting tool results", async () => {
		const bridge = new RecordingBridgeClient();
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});
		const request = {
			...toolRequest("spawn_agent", {
				task_name: "recent-turn",
				prompt: "work on the latest turn",
				fork_turns: "1",
			}),
		} satisfies RuntimeToolExecutionRequest;

		const result = await runner.runTool(request);

		expect(result.type).toBe("completed");
		expect(bridge.createSubagentThreadRequests[0]).toEqual(
			expect.objectContaining({
				sourceToolUseEventId: request.toolUseEventId,
				forkTurns: "1",
			}),
		);
	});

	test("wait_agent uses the hot child wait signal instead of treating timeout_ms as an automatic timeout", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.waitResult = {
			ok: true,
			sessionId: "sesn_1",
			sessionThreadId: "thr_child_1",
			observed: true,
			status: "idle",
			timedOut: false,
		};
		subAgentHost.pulledAgentMail = {
			deliveryId: "delivery_child_complete",
			finalMessage: "completed child output",
		};
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("wait_agent", { task_name: "worker", timeout_ms: 25 }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("timed_out: false"),
			}),
		});
		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("final_message:\ncompleted child output"),
			}),
		});
		expect(subAgentHost.pullAgentMailCalls).toEqual([
			{
				sessionThreadId: "thrd_1",
				sourceThreadId: "thr_child_1",
			},
		]);
	});

	test("wait_agent settles an oversized child final message as a non-retryable tool failure", async () => {
		const bridge = new RecordingBridgeClient();
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.pulledAgentMail = {
			deliveryId: "delivery_child_oversized",
			finalMessage: "\u0001".repeat(90_000),
		};
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("wait_agent", { task_name: "worker" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				message: "Tool result exceeds the 512 KiB model-visible output limit.",
				retryable: false,
			},
		});
	});

	test("wait_agent surfaces observed hot wait timeouts as completed results", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.waitResult = {
			ok: true,
			sessionId: "sesn_1",
			sessionThreadId: "thr_child_1",
			observed: true,
			status: "running",
			timedOut: true,
		};
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("wait_agent", { task_name: "worker", timeout_ms: 25 }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("timed_out: true"),
			}),
		});
		expect(subAgentHost.pullAgentMailCalls).toEqual([]);
	});

	test("wait_agent refuses completion pulls until hot or cold child status is settled", async () => {
		for (const statusValue of [
			"running",
			"rescheduling",
			"requires_action",
		] as const) {
			const bridge = new RecordingBridgeClient();
			bridge.childStatus = statusValue;
			const subAgentHost = new RecordingSubAgentHost();
			subAgentHost.waitResult = {
				ok: true,
				sessionId: "sesn_1",
				sessionThreadId: "thr_child_1",
				observed: statusValue === "requires_action",
				...(statusValue === "requires_action" ? { status: statusValue } : {}),
				timedOut: false,
			};
			subAgentHost.pulledAgentMail = {
				deliveryId: `delivery_stale_${statusValue}`,
				finalMessage: "stale completion",
			};
			const runner = makeRunner({ bridge, subAgentHost });

			const result = await runner.runTool(
				toolRequest("wait_agent", { task_name: "worker" }),
			);

			expect(result).toEqual({
				type: "completed",
				output: expect.objectContaining({
					text: expect.stringContaining(`status: ${statusValue}`),
				}),
			});
			expect(result).toEqual({
				type: "completed",
				output: expect.not.objectContaining({
					text: expect.stringContaining("final_message:"),
				}),
			});
			expect(subAgentHost.pullAgentMailCalls).toEqual([]);
		}
	});

	test("wait_agent pulls completion mail for every hot or cold settled child status", async () => {
		for (const statusValue of [
			"idle",
			"failed",
			"terminated",
			"closed_for_runtime",
		] as const) {
			const hotObserved =
				statusValue === "terminated" || statusValue === "closed_for_runtime";
			const bridge = new RecordingBridgeClient();
			bridge.childStatus = statusValue;
			const subAgentHost = new RecordingSubAgentHost();
			subAgentHost.waitResult = {
				ok: true,
				sessionId: "sesn_1",
				sessionThreadId: "thr_child_1",
				observed: hotObserved,
				...(hotObserved ? { status: statusValue } : {}),
				timedOut: false,
			};
			subAgentHost.pulledAgentMail = {
				deliveryId: `delivery_settled_${statusValue}`,
				finalMessage: `completion for ${statusValue}`,
			};
			const runner = makeRunner({ bridge, subAgentHost });

			const result = await runner.runTool(
				toolRequest("wait_agent", { task_name: "worker" }),
			);

			expect(result).toEqual({
				type: "completed",
				output: expect.objectContaining({
					text: expect.stringContaining(
						`final_message:\ncompletion for ${statusValue}`,
					),
				}),
			});
			expect(subAgentHost.pullAgentMailCalls).toEqual([
				{
					sessionThreadId: "thrd_1",
					sourceThreadId: "thr_child_1",
				},
			]);
		}
	});

	test("wait_agent presents the same enveloped body when a receipt ensure replays", async () => {
		const bridge = new RecordingBridgeClient();
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.pulledAgentMail = {
			deliveryId: "delivery_child_duplicate",
			finalMessage:
				"Message Type: FINAL_ANSWER\nTask name: main\nSender: worker\nPayload:\ncompleted",
		};
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("wait_agent", { task_name: "worker" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining(
					"final_message:\nMessage Type: FINAL_ANSWER",
				),
			}),
		});
	});

	test("wait_agent returns a truncated errored completion envelope without rewriting it", async () => {
		const bridge = new RecordingBridgeClient();
		const subAgentHost = new RecordingSubAgentHost();
		const envelope =
			"Message Type: FINAL_ANSWER\nTask name: main\nSender: worker\nPayload:\nAgent errored: head…42 tokens truncated…tail";
		subAgentHost.pulledAgentMail = {
			deliveryId: "delivery_child_errored",
			finalMessage: envelope,
		};
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("wait_agent", {
				task_name: "worker",
				timeout_ms: 1_000,
			}),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining(`final_message:\n${envelope}`),
			}),
		});
	});

	test("interrupt_agent preserves a child that became durably failed before control completion", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "failed";
		bridge.childInterruptOutcome =
			ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_PRESERVED_FAILED;
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("interrupt_agent", { task_name: "worker" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("interrupted: false"),
			}),
		});
		if (result.type === "completed") {
			expect(result.output.text).toContain("status: failed");
		}
		expect(subAgentHost.actions).toEqual([]);
	});

	test("interrupt_agent settles a durable control delivery failure without retrying", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		bridge.childInterruptOutcome =
			ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_DELIVERY_FAILED;
		bridge.childInterruptErrorCode = "child_interrupt_delivery_failed";
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const result = await runner.runTool(
			toolRequest("interrupt_agent", { task_name: "worker" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				message: "child_interrupt_delivery_failed",
				retryable: false,
			},
		});
		expect(bridge.admitChildInterruptRequests).toHaveLength(1);
		expect(bridge.awaitChildInterruptRequests).toHaveLength(1);
	});

	test("rejects an oversized child task name as a retryable Bridge boundary contract failure", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		bridge.childTaskName = "\u0001".repeat(90_000);
		const runner = makeRunner({
			bridge,
			subAgentHost: new RecordingSubAgentHost(),
		});

		const result = await runner.runTool(
			toolRequest("interrupt_agent", { task_name: "worker" }),
		);

		expect(result).toMatchObject({
			type: "error",
			error: {
				message: "Bridge returned malformed child-thread facts.",
				retryable: true,
			},
		});
	});

	test("wait_agent cancellation detaches the hot waiter without a late result", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.waitUntilAbort = true;
		const runner = makeRunner({ bridge, subAgentHost });
		const controller = new AbortController();
		const waiting = runner.runTool({
			...toolRequest("wait_agent", { task_name: "worker" }),
			abortSignal: controller.signal,
		});
		await waitForCondition(() => subAgentHost.waitStarted, "hot wait start");

		controller.abort();

		await expect(waiting).resolves.toMatchObject({ type: "cancelled" });
		expect(subAgentHost.waitDetached).toBe(true);
	});

	test("resume_agent admits only a quiescent closed checkpoint before durable activation", async () => {
		const closedThread = {
			parentThreadId: "thrd_1",
			role: "subagent" as const,
			visibility: "public" as const,
			taskName: "worker",
			agentType: "worker" as const,
			status: "closed_for_runtime" as const,
		};
		const pendingContextEntries = [
			userMessage("sesn_1", "user-resume-checkpoint", 1, "hello"),
		];
		const pendingOpenRequestDraft = assistantRunningToolMessage(
			"sesn_1",
			"assistant-resume-checkpoint",
			2,
			"tool-resume-checkpoint",
			"Read",
			"sevt_tool_resume_checkpoint",
			{ file_path: "a.txt" },
		);
		const pendingFacts = resumeTurnFactsForPendingTool({
			modelRequestId: "mreq_resume_checkpoint",
			toolUseEventId: "sevt_tool_resume_checkpoint",
			modelToolCallId: "tool-resume-checkpoint",
			toolName: "Read",
		});
		const pendingInput = userMessage(
			"sesn_1",
			"user-pending-resume",
			1,
			"pending",
		);
		// Disjunct coverage for validateClosedThreadResumeCheckpoint (core-hosts.ts).
		// Every row declares the exact set of validator disjuncts its durable
		// fixture trips, and the loop asserts that set against the reconstructed
		// checkpoint, so a fixture can never silently stop covering a disjunct.
		// A disjunct is isolated when some row trips it alone: deleting that one
		// disjunct then makes the row stop throwing.
		//
		// Isolated by a durable row: interruptEventId ("unresolved interrupt"),
		// terminalCloseout ("terminal closeout"). Isolated by the argument-level
		// guard table below: routes, pendingToolUses, pendingSandboxExecutions.
		//
		// Co-trips the durable data model forces, so no row can split them:
		//  - executionRunId and an open request each imply a non-idle decision
		//    (ready_to_finish / request_open), so both keep {stateNotIdle,
		//    actionNotAwaitInput}. Those two are themselves inseparable: the reducer
		//    returns await_input only together with the idle state, so stateNotIdle
		//    always implies actionNotAwaitInput. The one decision that pairs the
		//    idle state with another action (close_interrupted) needs both an open
		//    execution run and an unresolved interrupt, which trip two more
		//    disjuncts, so actionNotAwaitInput cannot stand alone either.
		//  - a pending Tool Use or Sandbox execution must name an incomplete public
		//    member of the newest request (extractColdThreadToolRouteView throws
		//    otherwise) and that member always yields a route, so pendingToolUses,
		//    pendingSandboxExecutions and routes each co-trip with incompleteToolUse
		//    and with the resulting non-idle decision. A "complete Tool Use with a
		//    Sandbox route" fixture is rejected by the same extractor check, so the
		//    incompleteToolUse co-trip on the Sandbox row cannot be removed either.
		//  - an incomplete member with no route is only extractable while an
		//    interrupt is unresolved, so incompleteToolUse keeps interruptEventId as
		//    its single remaining co-trip.
		const cases = [
			{
				// Restores an opened execution run with nothing else pending.
				name: "execution run open",
				trips: ["executionRunId", "stateNotIdle", "actionNotAwaitInput"],
				context: {
					contextEntries: [],
					thread: closedThread,
					pendingToolUses: [],
					pendingSandboxExecutions: [],
					turnFacts: {
						events: [
							{
								eventId: "sevt_resume_running",
								eventSequence: 1,
								type: "session.status_running" as const,
							},
						],
						internalRepairs: [],
					},
				},
			},
			{
				name: "open request",
				trips: ["openRequest", "stateNotIdle", "actionNotAwaitInput"],
				context: {
					contextEntries: [],
					thread: closedThread,
					pendingToolUses: [],
					pendingSandboxExecutions: [],
					turnFacts: {
						events: [
							{
								eventId: "sevt_resume_start",
								eventSequence: 1,
								type: "span.model_request_start" as const,
								modelRequestId: "mreq_resume_open",
								requestStart: {
									requestKind: "agent_provider_request" as const,
									contextThroughMessageSequence: 0,
								},
							},
						],
						internalRepairs: [],
					},
				},
			},
			{
				name: "pending tool route",
				trips: [
					"incompleteToolUse",
					"pendingToolUses",
					"routes",
					"stateNotIdle",
					"actionNotAwaitInput",
				],
				context: {
					contextEntries: pendingContextEntries,
					openRequestDraft: pendingOpenRequestDraft,
					thread: closedThread,
					turnFacts: pendingFacts,
					pendingToolUses: [
						{
							toolUseEventId: "sevt_tool_resume_checkpoint",
							modelRequestId: "mreq_resume_checkpoint",
							modelToolCallId: "tool-resume-checkpoint",
							toolName: "Read",
							input: { file_path: "a.txt" },
							status: "pending" as const,
						},
					],
					pendingSandboxExecutions: [],
				},
			},
			{
				name: "unfinished sandbox route",
				trips: [
					"incompleteToolUse",
					"pendingSandboxExecutions",
					"routes",
					"stateNotIdle",
					"actionNotAwaitInput",
				],
				context: {
					contextEntries: pendingContextEntries,
					openRequestDraft: pendingOpenRequestDraft,
					thread: closedThread,
					turnFacts: pendingFacts,
					pendingToolUses: [],
					pendingSandboxExecutions: [
						{
							toolUseEventId: "sevt_tool_resume_checkpoint",
							modelRequestId: "mreq_resume_checkpoint",
							modelToolCallId: "tool-resume-checkpoint",
							toolName: "Read",
							input: { file_path: "a.txt" },
							executionState: "running" as const,
						},
					],
				},
			},
			{
				name: "unresolved interrupt",
				trips: ["interruptEventId"],
				context: {
					contextEntries: [],
					thread: closedThread,
					pendingToolUses: [],
					pendingSandboxExecutions: [],
					turnFacts: {
						events: [
							{
								eventId: "sevt_resume_interrupt",
								eventSequence: 1,
								type: "agent.thread_interrupt_requested" as const,
							},
						],
						internalRepairs: [],
					},
				},
			},
			{
				// An unresolved interrupt is the only durable state that lets a sealed
				// incomplete Tool Use survive without a route, which is what shrinks
				// incompleteToolUse's co-trip set to the interrupt alone: the decision
				// stays idle/await_input because no execution run is open.
				name: "interrupted incomplete Tool Use without a route",
				trips: ["interruptEventId", "incompleteToolUse"],
				context: {
					contextEntries: pendingContextEntries,
					openRequestDraft: pendingOpenRequestDraft,
					thread: closedThread,
					pendingToolUses: [],
					pendingSandboxExecutions: [],
					turnFacts: {
						...pendingFacts,
						events: [
							...pendingFacts.events,
							{
								eventId: "sevt_resume_interrupt_route",
								eventSequence: 5,
								type: "agent.thread_interrupt_requested" as const,
							},
						],
					},
				},
			},
			{
				// A terminated run with its paired failure fact: the reducer reports
				// idle/await_input and the extractor drops both the request and every
				// route, so terminalCloseout is the only disjunct left standing.
				name: "terminal closeout",
				trips: ["terminalCloseout"],
				context: {
					contextEntries: [],
					thread: closedThread,
					pendingToolUses: [],
					pendingSandboxExecutions: [],
					turnFacts: {
						events: [
							{
								eventId: "sevt_resume_failure",
								eventSequence: 1,
								type: "session.error" as const,
								failure: {
									errorType: "provider_unavailable",
									retryStatus: "terminal" as const,
								},
							},
							{
								eventId: "sevt_resume_terminated",
								eventSequence: 2,
								type: "session.status_terminated" as const,
							},
						],
						internalRepairs: [],
					},
				},
			},
			{
				name: "reducer has pending input",
				trips: ["stateNotIdle", "actionNotAwaitInput"],
				context: {
					contextEntries: [pendingInput],
					thread: closedThread,
					pendingToolUses: [],
					pendingSandboxExecutions: [],
					turnFacts: resumeTurnFactsFor([pendingInput]),
				},
			},
		];
		for (const testCase of cases) {
			const checkpoint = extractThreadTurnCheckpoint({
				contextEntries: testCase.context.contextEntries,
				facts: testCase.context.turnFacts,
			});
			const routeView = extractColdThreadToolRouteView({
				checkpoint,
				pendingToolUses: testCase.context.pendingToolUses,
				pendingSandboxExecutions: testCase.context.pendingSandboxExecutions,
			});
			expect(
				resumeCheckpointTrippedDisjuncts(
					checkpoint,
					routeView,
					testCase.context.pendingToolUses,
					testCase.context.pendingSandboxExecutions,
				),
				testCase.name,
			).toEqual(testCase.trips);
			expect(
				() =>
					validateClosedThreadResumeCheckpoint(
						checkpoint,
						routeView,
						testCase.context.pendingToolUses,
						testCase.context.pendingSandboxExecutions,
						false,
					),
				testCase.name,
			).toThrow("closed Thread resume requires a quiescent durable checkpoint");
			const hosts = await buildResumeTestHosts(async () => ({
				...testCase.context,
				runtimeBindingToken: "runtime-binding-token-resume-checkpoint",
			}));
			const bridge = new RecordingBridgeClient();
			bridge.childStatus = "closed_for_runtime";
			try {
				const result = await makeRunner({
					bridge,
					subAgentHost: hosts.subAgentRunHost,
				}).runTool(toolRequest("resume_agent", { task_name: "worker" }));
				expect(result, testCase.name).toMatchObject({
					type: "error",
					error: { message: expect.stringContaining("context preload failed") },
				});
				expect(
					bridge.markChildThreadActiveRequests,
					testCase.name,
				).toHaveLength(0);
				expect(
					await hosts.subAgentRunHost.inspectThread(resumeChildControl()),
					testCase.name,
				).toMatchObject({
					ok: true,
					observed: false,
				});
			} finally {
				await hosts.close();
			}
		}

		// Argument-level guards. The cold extractor demands exactly one route per
		// incomplete member, so it rejects every combination below before the
		// validator sees it and no durable fixture can reach these rows. They hold
		// the validator's own boundary — it receives the route view and both
		// pending sets as independent arguments from the context loader — and they
		// are what isolates routes, pendingToolUses and pendingSandboxExecutions.
		const quiescentCheckpoint = extractThreadTurnCheckpoint({
			contextEntries: [],
			facts: emptyResumeTurnFacts,
		});
		expect(() =>
			validateClosedThreadResumeCheckpoint(
				quiescentCheckpoint,
				{ routes: [] },
				[],
				[],
				false,
			),
		).not.toThrow();
		const argumentGuards = [
			{
				name: "pending attachment",
				routeView: { routes: [] },
				pendingToolUses: [],
				pendingSandboxExecutions: [],
				hasPendingAttachments: true,
			},
			{
				name: "route view retains a route",
				routeView: {
					routes: [
						{
							toolUseEventId: "sevt_guard_route",
							disposition: "hot_execution" as const,
						},
					],
				},
				pendingToolUses: [],
				pendingSandboxExecutions: [],
				hasPendingAttachments: false,
			},
			{
				name: "pending Tool Use outside the route view",
				routeView: { routes: [] },
				pendingToolUses: [{ toolUseEventId: "sevt_guard_pending_tool" }],
				pendingSandboxExecutions: [],
				hasPendingAttachments: false,
			},
			{
				name: "pending Sandbox execution outside the route view",
				routeView: { routes: [] },
				pendingToolUses: [],
				pendingSandboxExecutions: [
					{ toolUseEventId: "sevt_guard_pending_sandbox" },
				],
				hasPendingAttachments: false,
			},
		];
		for (const guard of argumentGuards) {
			expect(
				() =>
					validateClosedThreadResumeCheckpoint(
						quiescentCheckpoint,
						guard.routeView,
						guard.pendingToolUses,
						guard.pendingSandboxExecutions,
						guard.hasPendingAttachments,
					),
				guard.name,
			).toThrow("closed Thread resume requires a quiescent durable checkpoint");
		}

		const pendingAttachmentHosts = await buildResumeTestHosts(async () => ({
			contextEntries: [],
			thread: closedThread,
			pendingToolUses: [],
			pendingSandboxExecutions: [],
			pendingAttachments: [
				{
					fileBacked: {
						sourceEventId: "sevt_closed_resume_attachment",
						fileId: "file_closed_resume_attachment",
					},
					mime: "image/png",
					filename: "resume.png",
				},
			],
			turnFacts: emptyResumeTurnFacts,
			runtimeBindingToken: "runtime-binding-token-pending-attachment",
		}));
		const pendingAttachmentBridge = new RecordingBridgeClient();
		pendingAttachmentBridge.childStatus = "closed_for_runtime";
		try {
			expect(
				await makeRunner({
					bridge: pendingAttachmentBridge,
					subAgentHost: pendingAttachmentHosts.subAgentRunHost,
				}).runTool(toolRequest("resume_agent", { task_name: "worker" })),
			).toMatchObject({
				type: "error",
				error: { message: expect.stringContaining("context preload failed") },
			});
			expect(pendingAttachmentBridge.markChildThreadActiveRequests).toHaveLength(
				0,
			);
			expect(
				await pendingAttachmentHosts.subAgentRunHost.inspectThread(
					resumeChildControl(),
				),
			).toMatchObject({ ok: true, observed: false });
		} finally {
			await pendingAttachmentHosts.close();
		}

		const hosts = await buildResumeTestHosts(async () => ({
			contextEntries: [],
			thread: closedThread,
			turnFacts: emptyResumeTurnFacts,
			runtimeBindingToken: "runtime-binding-token-quiescent-resume",
		}));
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "closed_for_runtime";
		try {
			expect(
				await makeRunner({
					bridge,
					subAgentHost: hosts.subAgentRunHost,
				}).runTool(toolRequest("resume_agent", { task_name: "worker" })),
			).toMatchObject({ type: "completed" });
			expect(bridge.markChildThreadActiveRequests).toHaveLength(1);
			expect(
				await hosts.subAgentRunHost.inspectThread(resumeChildControl()),
			).toMatchObject({
				ok: true,
				observed: true,
			});
		} finally {
			await hosts.close();
		}
	});

	test("resume_agent marks a closed child active and preloads context without enqueueing new input", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "closed_for_runtime";
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);

		expect(result.type).toBe("completed");
		expect(bridge.markChildThreadActiveRequests).toEqual([
			expect.objectContaining({
				sourceToolUseEventId: "sevt_tool_1",
				targetChildThreadId: "thr_child_1",
			}),
		]);
		expect(subAgentHost.preloaded).toEqual([
			expect.objectContaining({
				sessionThreadId: "thr_child_1",
				thread: expect.objectContaining({
					status: "closed_for_runtime",
					taskName: "worker",
				}),
			}),
		]);
		expect(subAgentHost.enqueued).toEqual([]);
	});

	test("resume_agent consumes a preserved-terminal Bridge disposition without marking stale hot state active", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "closed_for_runtime";
		bridge.markChildThreadActiveResponse = {
			committed: {
				disposition:
					ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED,
			},
		};
		const subAgentHost = new RecordingSubAgentHost();
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: failed"),
			}),
		});
		expect(subAgentHost.actions).toEqual(["preload", "close"]);
		expect(subAgentHost.actions).not.toContain("resume");
		expect(subAgentHost.actions).not.toContain("inspect");
	});

	test("resume_agent records an already-active declaration without reloading the observed child", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.inspectObserved = true;
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: idle"),
			}),
		});
		expect(bridge.markChildThreadActiveRequests).toEqual([
			expect.objectContaining({ sourceToolUseEventId: "sevt_tool_1" }),
		]);
		expect(subAgentHost.actions).toEqual(["inspect"]);
		expect(subAgentHost.preloaded).toEqual([]);
		expect(subAgentHost.enqueued).toEqual([]);
	});

	test("resume_agent rechecks hot residency after an already-active receipt before reporting success", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "running";
		bridge.deferMarkChildThreadActive = true;
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.inspectObserved = true;
		const runner = makeRunner({ bridge, subAgentHost });

		const resuming = runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);
		await waitForCondition(
			() => bridge.markChildThreadActiveRequests.length === 1,
			"durable already-active declaration",
		);
		subAgentHost.inspectObserved = false;
		bridge.completeDeferredMarkChildThreadActive();

		expect(await resuming).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: running"),
			}),
		});
		expect(subAgentHost.actions).toEqual(["inspect"]);
		expect(subAgentHost.preloaded).toHaveLength(0);
	});

	test("resume_agent succeeds when a requeued notification starts the child before hot projection", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "closed_for_runtime";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.activeResults.push({
			ok: false,
			sessionId: "sesn_1",
			sessionThreadId: "thr_child_1",
			reason: "thread_busy",
		});
		subAgentHost.inspectObserved = true;
		subAgentHost.inspectStatus = "running";
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: running"),
			}),
		});
		expect(subAgentHost.actions).toEqual(["preload", "resume", "inspect"]);
		expect(subAgentHost.closedThreadIds).toEqual([]);
		expect(bridge.markChildThreadActiveRequests).toHaveLength(1);
	});

	test("resume_agent keeps a durable resume successful when hot projection disappears", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "closed_for_runtime";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.activeError = new Error("hot entry disappeared");
		subAgentHost.inspectError = new Error("hot entry unavailable");
		const runner = makeRunner({ bridge, subAgentHost });

		const result = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);

		expect(result).toEqual({
			type: "completed",
			output: expect.objectContaining({
				text: expect.stringContaining("status: idle"),
			}),
		});
		expect(subAgentHost.actions).toEqual(["preload", "resume", "inspect"]);
		expect(subAgentHost.closedThreadIds).toEqual([]);
		expect(bridge.markChildThreadActiveRequests).toHaveLength(1);
	});

	test("resume_agent retries closed preload without a durable reopen after the first preload fails", async () => {
		const bridge = new RecordingBridgeClient();
		bridge.childStatus = "closed_for_runtime";
		const subAgentHost = new RecordingSubAgentHost();
		subAgentHost.preloadResults.push(
			{
				ok: false,
				sessionId: "sesn_1",
				sessionThreadId: "thr_child_1",
				reason: "context_load_failed",
			},
			{
				ok: true,
				sessionId: "sesn_1",
				sessionThreadId: "thr_child_1",
				applied: true,
			},
		);
		const runner = makeRunner({ bridge, subAgentHost });

		const first = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);
		await Bun.sleep(5);
		const second = await runner.runTool(
			toolRequest("resume_agent", { task_name: "worker" }),
		);

		expect(first).toMatchObject({ type: "error" });
		expect(second).toMatchObject({ type: "completed" });
		expect(bridge.markChildThreadActiveRequests).toHaveLength(1);
		expect(subAgentHost.preloaded).toHaveLength(2);
		expect(
			subAgentHost.actions.filter((action) => action === "resume"),
		).toHaveLength(1);
		expect(bridge.activeReceiptDispositions).toEqual([
			ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_RESUMED,
		]);
	});

	for (const testCase of [
		{
			status: "failed",
			disposition:
				ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED,
		},
		{
			status: "terminated",
			disposition:
				ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED,
		},
	]) {
		test(`resume_agent preserves a cold ${testCase.status} child without loading context`, async () => {
			const bridge = new RecordingBridgeClient();
			bridge.childStatus = testCase.status;
			bridge.activeReceiptDisposition = testCase.disposition;
			const subAgentHost = new RecordingSubAgentHost();
			const runner = makeRunner({ bridge, subAgentHost });

			const result = await runner.runTool(
				toolRequest("resume_agent", { task_name: "worker" }),
			);

			expect(result).toEqual({
				type: "completed",
				output: expect.objectContaining({
					text: expect.stringContaining(`status: ${testCase.status}`),
				}),
			});
			expect(bridge.markChildThreadActiveRequests).toHaveLength(0);
			expect(subAgentHost.actions).toEqual([]);
			expect(subAgentHost.preloaded).toEqual([]);
		});
	}
});

const emptyResumeTurnFacts: ThreadTurnLoadFacts = {
	events: [],
	internalRepairs: [],
};

/**
 * Mirrors the disjunct list of validateClosedThreadResumeCheckpoint
 * (core-hosts.ts) in source order so every resume-rejection row can pin the
 * exact set of non-quiescent facts its durable fixture produces.
 */
function resumeCheckpointTrippedDisjuncts(
	checkpoint: ThreadTurnCheckpoint,
	routeView: ThreadToolRouteView,
	pendingToolUses: readonly unknown[],
	pendingSandboxExecutions: readonly unknown[],
): readonly string[] {
	const decision = deriveThreadTurnDecision(checkpoint, routeView, [], {
		hasPendingAttachments: false,
	});
	const incompleteToolUse =
		checkpoint.request?.toolMembers.some(
			(member) =>
				member.memberKind === "public_tool_use" &&
				member.terminalResult === undefined,
		) ?? false;
	const disjuncts: readonly (readonly [string, boolean])[] = [
		["executionRunId", checkpoint.executionRunId !== undefined],
		["interruptEventId", checkpoint.interruptEventId !== undefined],
		["terminalCloseout", checkpoint.terminalCloseout !== undefined],
		[
			"openRequest",
			checkpoint.request !== undefined &&
				checkpoint.request.requestEnd === undefined,
		],
		["incompleteToolUse", incompleteToolUse],
		["pendingToolUses", pendingToolUses.length !== 0],
		["pendingSandboxExecutions", pendingSandboxExecutions.length !== 0],
		["routes", routeView.routes.length !== 0],
		["stateNotIdle", decision.state.state !== "idle"],
		["actionNotAwaitInput", decision.action.action !== "await_input"],
	];
	return disjuncts.flatMap(([name, tripped]) => (tripped ? [name] : []));
}

function resumeTurnFactsFor(
	contextEntries: readonly RuntimeContextEntry[],
): ThreadTurnLoadFacts {
	void contextEntries;
	return { events: [], internalRepairs: [] };
}

function resumeTurnFactsForPendingTool(input: {
	readonly modelRequestId: string;
	readonly toolUseEventId: string;
	readonly modelToolCallId: string;
	readonly toolName: string;
}): ThreadTurnLoadFacts {
	return {
		events: [
			{
				eventId: `start_${input.modelRequestId}`,
				eventSequence: 1,
				type: "span.model_request_start",
				modelRequestId: input.modelRequestId,
				requestStart: {
					requestKind: "agent_provider_request",
					contextThroughMessageSequence: 1,
				},
			},
			{
				eventId: input.toolUseEventId,
				eventSequence: 2,
				type: "agent.tool_use",
				modelRequestId: input.modelRequestId,
				toolUse: {
					modelToolCallId: input.modelToolCallId,
					toolName: input.toolName,
				},
			},
			{
				eventId: `end_${input.modelRequestId}`,
				eventSequence: 3,
				type: "span.model_request_end",
				modelRequestId: input.modelRequestId,
				requestEnd: {
					requestStartEventId: `start_${input.modelRequestId}`,
					isError: false,
				},
			},
		],
		internalRepairs: [],
	};
}

function resumeChildControl() {
	return {
		requestId: "req_resume_inspect",
		workspaceId: "wksp_1",
		sessionId: "sesn_1",
		sessionThreadId: "thr_child_1",
		bindingId: "bind_1",
		bindingGeneration: 42,
		targetPodUid: "pod_1",
		runtimeInputId: "rin_resume_inspect",
		eventIds: [],
		sequenceFrom: 0,
		sequenceTo: 0,
	};
}

async function buildResumeTestHosts(
	loadThreadContext: NonNullable<
		RuntimeCoreHostsOptions["contextLoader"]["loadThreadContext"]
	>,
) {
	return await buildRuntimeCoreHosts({
		maxLocalSessions: 4,
		now: () => "2026-06-16T00:00:00.000Z",
		contextLoader: {
			loadThreadContext,
			commitAcceptedInput: async () => ({
				type: "committed",
				assignedContextSequences: [1],
				pendingAttachments: [],
				interruptToolResults: [],
			}),
		},
		threadLoop: {
			internalToolRepairStore: new ResumeTestRepairStore(),
			sessionEventWriter: {
				settleToolResult: async () => ({
					ok: true,
					result: { type: "committed" },
				}),
				append: async () => {
					throw new Error("resume checkpoint test does not append events");
				},
				writeRequestEnd: async () => {
					throw new Error("resume checkpoint test does not end requests");
				},
				finishIdle: async () => {
					throw new Error("resume checkpoint test does not finish turns");
				},
			},
			runtime: {
				now: () => "2026-06-16T00:00:00.000Z",
				monotonicMs: () => 0,
				createId: (prefix) => `${prefix}_resume_test`,
				sleep: async () => true,
			},
			llmService: { stream: () => Stream.empty },
			storeOperationTimeoutMs: 100,
			runtimeModel: () => ({ providerId: "fake", modelId: "fake-chat" }),
			runtimePolicy: () => ({
				toolCatalog: createToolCatalog({ family: "claude" }),
			}),
		},
	});
}

class ResumeTestRepairStore extends RuntimeInternalToolRepairStore {
	protected async commitInternalToolRepairRecord(): Promise<never> {
		throw new Error("resume checkpoint test does not repair internal Tools");
	}
}

function makeRunner(options: {
	readonly bridge?: RecordingBridgeClient;
	readonly gateway?: RecordingGatewayClient;
	readonly mcp?: RecordingMcpConnectorClient;
	readonly subAgentHost?: RuntimeSubAgentRunHost;
	readonly metadataFactory?: () => Promise<Metadata>;
	readonly sleep?: (delayMs: number, abortSignal: AbortSignal) => Promise<void>;
}): RuntimePodToolRunner {
	return new RuntimePodToolRunner({
		bridgeAddress: "bridge.test:9090",
		webAddress: "gateway.test:9090",
		mcpConnectorAddress: "gateway.test:9091",
		tokenPath: "/var/run/token",
		bridgeClient: (options.bridge ?? new RecordingBridgeClient()).client(),
		webClient: (options.gateway ?? new RecordingGatewayClient()).client(),
		mcpConnectorClient: (
			options.mcp ?? new RecordingMcpConnectorClient()
		).client(),
		metadataFactory: options.metadataFactory ?? (async () => new Metadata()),
		...(options.sleep !== undefined ? { sleep: options.sleep } : {}),
		subAgentRunHost: () => options.subAgentHost,
	});
}

function toolRequest(
	toolName: string,
	input: RuntimeJsonValue,
	toolUseEventId = "sevt_tool_1",
	abortSignal: AbortSignal = new AbortController().signal,
	modelOrder = 0,
): RuntimeToolExecutionRequest {
	const gptNames = new Set([
		"exec_command",
		"write_stdin",
		"view_image",
		"apply_patch",
	]);
	const catalog = createToolCatalog({
		family: gptNames.has(toolName) ? "gpt" : "claude",
	});
	const entry = lookupToolEntry(catalog, toolName);
	if (entry === undefined) {
		throw new Error(`missing test tool ${toolName}`);
	}
	return {
		workspaceId: "wksp_1",
		sessionId: "sesn_1",
		sessionThreadId: "thrd_1",
		bindingId: "bind_1",
		bindingGeneration: 42,
		runtimeBindingToken: "binding-token",
		targetPodUid: "pod_1",
		modelRequestId: "mreq_1",
		modelToolCallId: "tool_call_1",
		modelOrder,
		toolUseEventId,
		entry,
		input,
		currentModel: { providerId: "openai", modelId: "gpt-5.5" },
		abortSignal,
	};
}

function mcpToolRequest(input: RuntimeJsonValue): RuntimeToolExecutionRequest {
	return {
		...toolRequest("web", input),
		entry: mcpToolEntry(),
	};
}

function mcpToolEntry(): ToolEntry {
	return {
		name: "github_search",
		definition: {
			kind: "function",
			name: "github_search",
			description: "Search GitHub through MCP.",
			inputSchema: {
				type: "object",
				properties: { query: { type: "string" } },
			},
		},
		inputContract: { kind: "json_object" },
		route: {
			kind: "gateway",
			operation: "RunMcpTool",
			mcpServerName: "github",
		},
		formatter: {
			successShape: "MCP formatted text.",
			errorShape: "MCP formatted error.",
			forbiddenFields: ["authorization", "token", "credential"],
		},
		defaultPermissionPolicy: "always_allow",
		required: false,
	};
}

function sha256(input: string): string {
	return createHash("sha256").update(input).digest("hex");
}

const immediateSleep = async (): Promise<void> => undefined;

class ControlledSleep {
	readonly calls: Array<{
		readonly delayMs: number;
		readonly abortSignal: AbortSignal;
	}> = [];
	private readonly releases: Array<() => void> = [];

	readonly sleep = (
		delayMs: number,
		abortSignal: AbortSignal,
	): Promise<void> => {
		this.calls.push({ delayMs, abortSignal });
		return new Promise<void>((resolve, reject) => {
			let settled = false;
			const cleanup = (): void => {
				abortSignal.removeEventListener("abort", abort);
			};
			const abort = (): void => {
				if (settled) {
					return;
				}
				settled = true;
				cleanup();
				reject(new DOMException("aborted", "AbortError"));
			};
			abortSignal.addEventListener("abort", abort, { once: true });
			this.releases.push(() => {
				if (settled) {
					return;
				}
				settled = true;
				cleanup();
				resolve();
			});
			if (abortSignal.aborted) {
				abort();
			}
		});
	};

	releaseNext(): void {
		const release = this.releases.shift();
		if (release === undefined) {
			throw new Error("no controlled sleep is parked");
		}
		release();
	}
}

function stableTestId(prefix: string, seed: string): string {
	return `${prefix}_${sha256(seed).slice(0, 32)}`;
}

class RecordingBridgeClient {
	readonly acceptSandboxExecutionRequests: AcceptSandboxExecutionRequest[] = [];
	readonly acceptSandboxExecutionOptions: CallOptions[] = [];
	readonly acceptSandboxExecutionErrors: Error[] = [];
	readonly acceptSandboxExecutionResponses: AcceptSandboxExecutionResponse[] =
		[];
	readonly awaitSandboxExecutionRequests: AwaitSandboxExecutionRequest[] = [];
	readonly awaitSandboxExecutionErrors: Error[] = [];
	readonly awaitSandboxExecutionResponses: AwaitSandboxExecutionResponse[] = [];
	readonly runMemoryRequests: RunMemoryRequest[] = [];
	readonly runMemoryResponses: RunMemoryResponse[] = [];
	readonly runMemoryMetadata: Metadata[] = [];
	readonly sendCommandInputRequests: SendCommandInputRequest[] = [];
	readonly sendCommandInputResponses: SendCommandInputResponse[] = [];
	readonly readCommandResultRequests: ReadCommandResultRequest[] = [];
	readonly readCommandResultResponses: ReadCommandResultResponse[] = [];
	readonly cancelCommandRequests: CancelCommandRequest[] = [];
	readonly createSubagentThreadRequests: CreateSubagentThreadRequest[] = [];
	readonly resolveChildThreadRequests: ResolveChildThreadRequest[] = [];
	readonly listChildThreadsRequests: ListChildThreadsRequest[] = [];
	readonly deliverInterAgentMailRequests: DeliverInterAgentMailRequest[] = [];
	readonly admitChildInterruptRequests: AdmitChildInterruptRequest[] = [];
	readonly awaitChildInterruptRequests: AwaitChildInterruptRequest[] = [];
	readonly closeChildControlRequests: CloseChildControlRequest[] = [];
	readonly markChildThreadActiveRequests: MarkChildThreadActiveRequest[] = [];
	private deferredListChildThreads: ((response: unknown) => void) | undefined;
	private deferredDeliverInterAgentMail:
		| ((response: unknown) => void)
		| undefined;
	private deferredRunMemory: (() => void) | undefined;
	private deferredMarkChildThreadActive: (() => void) | undefined;
	deferReadCommandResult = false;
	deferFirstListChildThreads = false;
	deferDeliverInterAgentMail = false;
	createSubagentThreadErrorCode: GrpcStatus | undefined;
	readonly createSubagentThreadErrors: Error[] = [];
	createSubagentThreadResponse: unknown = {
		committed: { childThreadId: "thr_child_1" },
	};
	resolveChildThreadResponse: unknown | undefined;
	listChildThreadsResponse: unknown | undefined;
	deliverInterAgentMailResponse: unknown = { committed: {} };
	readonly deliverInterAgentMailErrors: Error[] = [];
	admitChildInterruptResponse: unknown | undefined;
	readonly admitChildInterruptErrors: Error[] = [];
	awaitChildInterruptResponse: unknown | undefined;
	readonly awaitChildInterruptErrors: Error[] = [];
	closeChildControlResponse: unknown | undefined;
	readonly closeChildControlErrors: Error[] = [];
	markChildThreadActiveResponse: unknown | undefined;
	readonly markChildThreadActiveErrors: Error[] = [];
	childTaskName = "worker";
	childStatus = "idle";
	childInterruptOutcome =
		ChildInterruptOutcome.CHILD_INTERRUPT_OUTCOME_COMPLETED;
	childInterruptErrorCode: string | undefined;
	awaitSandboxExecutionResultJson =
		'{"status":"success","result":{"text":"ok"}}';
	awaitSandboxExecutionTaskId = "";
	sendCommandInputResultJson = '{"status":"accepted"}';
	readCommandResultResultJson = '{"status":"running","task_id":"task_1"}';
	runMemoryResultJson = '{"status":"completed","summary":"created"}';
	runMemoryResultJsons: string[] = [];
	readonly runMemoryErrors: Error[] = [];
	readonly sendCommandInputErrors: Error[] = [];
	readonly readCommandResultErrors: Error[] = [];
	deferRunMemoryCall = 0;
	afterRunMemoryResponse: ((callNumber: number) => void) | undefined;
	closeChildControlStale = false;
	closeReceiptTargetIds: string[] = [];
	readonly closeReceiptDispositions = new Map<
		string,
		ChildLifecycleDisposition
	>();
	activeReceiptDisposition: ChildLifecycleDisposition | undefined;
	readonly activeReceiptDispositions: ChildLifecycleDisposition[] = [];
	deferMarkChildThreadActive = false;
	private readonly activeReceiptDispositionByOperationId = new Map<
		string,
		ChildLifecycleDisposition
	>();
	childLifecycleObservedBindingId: string | undefined;
	afterDeliverInterAgentMailResponse: (() => void) | undefined;

	client(): Pick<
		AgentRuntimeBridgeServiceClient,
		| "acceptSandboxExecution"
		| "awaitSandboxExecution"
		| "runMemory"
		| "sendCommandInput"
		| "readCommandResult"
		| "cancelCommand"
		| "createSubagentThread"
		| "resolveChildThread"
		| "listChildThreads"
		| "deliverInterAgentMail"
		| "admitChildInterrupt"
		| "awaitChildInterrupt"
		| "closeChildControl"
		| "markChildThreadActive"
	> {
		return {
			acceptSandboxExecution: this.acceptSandboxExecution.bind(this),
			awaitSandboxExecution: this.awaitSandboxExecution.bind(this),
			runMemory: this.runMemory.bind(this),
			sendCommandInput: this.sendCommandInput.bind(this),
			readCommandResult: this.readCommandResult.bind(this),
			cancelCommand: this.cancelCommand.bind(this),
			createSubagentThread: this.createSubagentThread.bind(this),
			resolveChildThread: this.resolveChildThread.bind(this),
			listChildThreads: this.listChildThreads.bind(this),
			deliverInterAgentMail: this.deliverInterAgentMail.bind(this),
			admitChildInterrupt: this.admitChildInterrupt.bind(this),
			awaitChildInterrupt: this.awaitChildInterrupt.bind(this),
			closeChildControl: this.closeChildControl.bind(this),
			markChildThreadActive: this.markChildThreadActive.bind(this),
		} as unknown as Pick<
			AgentRuntimeBridgeServiceClient,
			| "acceptSandboxExecution"
			| "awaitSandboxExecution"
			| "runMemory"
			| "sendCommandInput"
			| "readCommandResult"
			| "cancelCommand"
			| "createSubagentThread"
			| "resolveChildThread"
			| "listChildThreads"
			| "deliverInterAgentMail"
			| "admitChildInterrupt"
			| "awaitChildInterrupt"
			| "closeChildControl"
			| "markChildThreadActive"
		>;
	}

	private acceptSandboxExecution(
		request: AcceptSandboxExecutionRequest,
		_metadata: Metadata,
		options: CallOptions,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.acceptSandboxExecutionRequests.push(request);
		this.acceptSandboxExecutionOptions.push(options);
		const error = this.acceptSandboxExecutionErrors.shift();
		if (error !== undefined) {
			callback(error, undefined);
			return grpcCall();
		}
		callback(
			null,
			this.acceptSandboxExecutionResponses.shift() ?? { committed: {} },
		);
		return grpcCall();
	}

	private awaitSandboxExecution(
		request: AwaitSandboxExecutionRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.awaitSandboxExecutionRequests.push(request);
		const error = this.awaitSandboxExecutionErrors.shift();
		if (error !== undefined) {
			callback(error, undefined);
			return grpcCall();
		}
		callback(
			null,
			this.awaitSandboxExecutionResponses.shift() ?? {
				completed: {
					resultJson: this.awaitSandboxExecutionResultJson,
					taskId: this.awaitSandboxExecutionTaskId,
				},
			},
		);
		return grpcCall();
	}

	private runMemory(
		request: RunMemoryRequest,
		metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.runMemoryRequests.push(request);
		this.runMemoryMetadata.push(metadata);
		const error = this.runMemoryErrors.shift();
		if (error !== undefined) {
			callback(error, undefined);
			return grpcCall();
		}
		const resultJson =
			this.runMemoryResultJsons.shift() ?? this.runMemoryResultJson;
		const respond = (): void => {
			callback(
				null,
				this.runMemoryResponses.shift() ?? {
					committed: { resultJson },
				},
			);
			this.afterRunMemoryResponse?.(this.runMemoryRequests.length);
		};
		if (this.deferRunMemoryCall === this.runMemoryRequests.length) {
			this.deferredRunMemory = respond;
			return grpcCall();
		}
		respond();
		return grpcCall();
	}

	completeDeferredRunMemory(): void {
		const complete = this.deferredRunMemory;
		if (complete === undefined) {
			throw new Error("no deferred runMemory call");
		}
		this.deferredRunMemory = undefined;
		complete();
	}

	private sendCommandInput(
		request: SendCommandInputRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.sendCommandInputRequests.push(request);
		const error = this.sendCommandInputErrors.shift();
		if (error !== undefined) {
			callback(error, undefined);
			return grpcCall();
		}
		callback(
			null,
			this.sendCommandInputResponses.shift() ?? {
				committed: { resultJson: this.sendCommandInputResultJson },
			},
		);
		return grpcCall();
	}

	private readCommandResult(
		request: ReadCommandResultRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.readCommandResultRequests.push(request);
		const error = this.readCommandResultErrors.shift();
		if (error !== undefined) {
			callback(error, undefined);
			return grpcCall();
		}
		if (this.deferReadCommandResult) {
			return grpcCall();
		}
		callback(
			null,
			this.readCommandResultResponses.shift() ?? {
				completed: { resultJson: this.readCommandResultResultJson },
			},
		);
		return grpcCall();
	}

	private cancelCommand(
		request: CancelCommandRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.cancelCommandRequests.push(request);
		callback(null, {
			committed: { resultJson: '{"status":"cancelled"}' },
		});
		return grpcCall();
	}

	private createSubagentThread(
		request: CreateSubagentThreadRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.createSubagentThreadRequests.push(request);
		const transportError = this.createSubagentThreadErrors.shift();
		if (transportError !== undefined) {
			callback(transportError, undefined);
			return grpcCall();
		}
		if (this.createSubagentThreadErrorCode !== undefined) {
			callback(
				Object.assign(new Error("create child failed"), {
					code: this.createSubagentThreadErrorCode,
				}),
				undefined,
			);
			return grpcCall();
		}
		callback(null, this.createSubagentThreadResponse);
		return grpcCall();
	}

	private resolveChildThread(
		request: ResolveChildThreadRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.resolveChildThreadRequests.push(request);
		callback(
			null,
			this.resolveChildThreadResponse ?? {
				resolved: {
					child: childThreadFact(this.childTaskName, this.childStatus),
				},
			},
		);
		return grpcCall();
	}

	private listChildThreads(
		request: ListChildThreadsRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.listChildThreadsRequests.push(request);
		const response = this.listChildThreadsResponse ?? {
			completed: {
				children: [childThreadFact(this.childTaskName, this.childStatus)],
			},
		};
		if (
			this.deferFirstListChildThreads &&
			this.listChildThreadsRequests.length === 1
		) {
			this.deferredListChildThreads = (deferredResponse) =>
				callback(null, deferredResponse);
			return grpcCall();
		}
		callback(null, response);
		return grpcCall();
	}

	completeDeferredListChildThreads(): void {
		const complete = this.deferredListChildThreads;
		if (complete === undefined)
			throw new Error("no deferred listChildThreads call");
		this.deferFirstListChildThreads = false;
		this.deferredListChildThreads = undefined;
		complete({
			completed: {
				children: [childThreadFact(this.childTaskName, this.childStatus)],
			},
		});
	}

	private deliverInterAgentMail(
		request: DeliverInterAgentMailRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.deliverInterAgentMailRequests.push(request);
		const transportError = this.deliverInterAgentMailErrors.shift();
		if (transportError !== undefined) {
			callback(transportError, undefined);
			return grpcCall();
		}
		const response = this.deliverInterAgentMailResponse;
		if (this.deferDeliverInterAgentMail) {
			this.deferredDeliverInterAgentMail = (deferredResponse) =>
				callback(null, deferredResponse);
			return grpcCall();
		}
		callback(null, response);
		this.afterDeliverInterAgentMailResponse?.();
		return grpcCall();
	}

	completeDeferredDeliverInterAgentMail(): void {
		const complete = this.deferredDeliverInterAgentMail;
		if (complete === undefined)
			throw new Error("no deferred deliverInterAgentMail call");
		this.deferDeliverInterAgentMail = false;
		this.deferredDeliverInterAgentMail = undefined;
		complete({ committed: {} });
	}

	private readonly interruptTargetByOperationId = new Map<string, string>();

	private admitChildInterrupt(
		request: AdmitChildInterruptRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.admitChildInterruptRequests.push(request);
		const transportError = this.admitChildInterruptErrors.shift();
		if (transportError !== undefined) {
			callback(transportError, undefined);
			return grpcCall();
		}
		const controlOperationId = `ctrl_${request.sourceToolUseEventId}`;
		this.interruptTargetByOperationId.set(controlOperationId, "thr_child_1");
		callback(
			null,
			this.admitChildInterruptResponse ?? { committed: { controlOperationId } },
		);
		return grpcCall();
	}

	private awaitChildInterrupt(
		request: AwaitChildInterruptRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.awaitChildInterruptRequests.push(request);
		const transportError = this.awaitChildInterruptErrors.shift();
		if (transportError !== undefined) {
			callback(transportError, undefined);
			return grpcCall();
		}
		const childThreadId =
			this.interruptTargetByOperationId.get(request.controlOperationId) ??
			"thr_child_1";
		callback(
			null,
			this.awaitChildInterruptResponse ?? {
				completed: {
					targets: [
						{
							childThreadId,
							outcome: this.childInterruptOutcome,
							...(this.childInterruptErrorCode === undefined
								? {}
								: { errorCode: this.childInterruptErrorCode }),
						},
					],
				},
			},
		);
		return grpcCall();
	}

	private closeChildControl(
		request: CloseChildControlRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.closeChildControlRequests.push(request);
		const transportError = this.closeChildControlErrors.shift();
		if (transportError !== undefined) {
			callback(transportError, undefined);
			return grpcCall();
		}
		if (
			this.closeChildControlStale ||
			(this.childLifecycleObservedBindingId !== undefined &&
				this.childLifecycleObservedBindingId !==
					request.scope?.binding?.bindingId)
		) {
			callback(null, { stale: {} });
			return grpcCall();
		}
		const rootThreadId =
			this.interruptTargetByOperationId.get(request.controlOperationId ?? "") ??
			"thr_child_1";
		const targetIds =
			this.closeReceiptTargetIds.length > 0
				? this.closeReceiptTargetIds
				: [rootThreadId];
		callback(
			null,
			this.closeChildControlResponse ?? {
				committed: {
					children: targetIds.map((targetId) => ({
						childThreadId: targetId,
						disposition:
							this.closeReceiptDispositions.get(targetId) ??
							ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_CLOSED,
					})),
				},
			},
		);
		return grpcCall();
	}

	private markChildThreadActive(
		request: MarkChildThreadActiveRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.markChildThreadActiveRequests.push(request);
		const transportError = this.markChildThreadActiveErrors.shift();
		if (transportError !== undefined) {
			callback(transportError, undefined);
			return grpcCall();
		}
		const operationId = request.sourceToolUseEventId;
		const disposition =
			this.activeReceiptDispositionByOperationId.get(operationId) ??
			this.activeReceiptDisposition ??
			(this.childStatus === "closed_for_runtime"
				? ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_RESUMED
				: ChildLifecycleDisposition.CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE);
		this.activeReceiptDispositionByOperationId.set(operationId, disposition);
		this.activeReceiptDispositions.push(disposition);
		const respond = (): void =>
			callback(
				null,
				this.markChildThreadActiveResponse ?? { committed: { disposition } },
			);
		if (this.deferMarkChildThreadActive) {
			this.deferredMarkChildThreadActive = respond;
			return grpcCall();
		}
		respond();
		return grpcCall();
	}

	completeDeferredMarkChildThreadActive(): void {
		const complete = this.deferredMarkChildThreadActive;
		if (complete === undefined) {
			throw new Error("no deferred markChildThreadActive call");
		}
		this.deferMarkChildThreadActive = false;
		this.deferredMarkChildThreadActive = undefined;
		complete();
	}
}

class RecordingSubAgentHost implements RuntimeSubAgentRunHost {
	readonly actions: string[] = [];
	readonly closedThreadIds: string[] = [];
	readonly enqueued: Parameters<
		RuntimeSubAgentRunHost["enqueueThreadInput"]
	>[0][] = [];
	readonly preloaded: Parameters<RuntimeSubAgentRunHost["preloadThread"]>[0][] =
		[];
	waitResult:
		| Awaited<ReturnType<RuntimeSubAgentRunHost["waitThread"]>>
		| undefined;
	pulledAgentMail:
		| Awaited<ReturnType<NonNullable<RuntimeSubAgentRunHost["pullAgentMail"]>>>
		| undefined;
	readonly pullAgentMailCalls: Array<{
		readonly sessionThreadId: string;
		readonly sourceThreadId: string;
	}> = [];
	readonly closeResults: Array<
		Awaited<ReturnType<RuntimeSubAgentRunHost["markThreadClosed"]>>
	> = [];
	onClose: (() => void) | undefined;
	waitUntilAbort = false;
	waitStarted = false;
	waitDetached = false;
	inspectObserved = false;
	inspectStatus: "idle" | "running" = "idle";
	activeError: Error | undefined;
	inspectError: Error | undefined;
	readonly preloadResults: Array<
		Awaited<ReturnType<RuntimeSubAgentRunHost["preloadThread"]>>
	> = [];
	readonly activeResults: Array<
		Awaited<ReturnType<RuntimeSubAgentRunHost["markThreadActive"]>>
	> = [];

	async enqueueThreadInput(
		input: Parameters<RuntimeSubAgentRunHost["enqueueThreadInput"]>[0],
	) {
		this.actions.push("enqueue");
		this.enqueued.push(input);
		return {
			ok: true as const,
			sessionId: input.sessionId,
			created: false,
			started: true,
			...(input.kind === "approval_review"
				? {
						reviewerExecutionToken: {
							reviewId: input.reviewId,
							reviewerThreadId: input.sessionThreadId,
							runId: 1,
						},
					}
				: {}),
		};
	}

	async preloadThread(
		input: Parameters<RuntimeSubAgentRunHost["preloadThread"]>[0],
	) {
		this.actions.push("preload");
		this.preloaded.push(input);
		return (
			this.preloadResults.shift() ?? {
				ok: true as const,
				sessionId: input.sessionId,
				sessionThreadId: input.sessionThreadId,
				applied: true,
			}
		);
	}

	async evictReviewerExecution(
		command: Parameters<RuntimeSubAgentRunHost["evictReviewerExecution"]>[0],
	) {
		this.actions.push("evict-reviewer");
		return {
			ok: true as const,
			sessionId: command.sessionId,
			sessionThreadId: command.sessionThreadId,
			applied: true,
			terminal: true,
		};
	}

	async interruptReviewerExecution(
		command: Parameters<
			RuntimeSubAgentRunHost["interruptReviewerExecution"]
		>[0],
	) {
		this.actions.push("interrupt-reviewer");
		return {
			ok: true as const,
			sessionId: command.sessionId,
			sessionThreadId: command.sessionThreadId,
			applied: true,
			terminal: true,
		};
	}

	async releaseReviewerExecution(
		command: Parameters<RuntimeSubAgentRunHost["releaseReviewerExecution"]>[0],
	) {
		this.actions.push("release-reviewer");
		return {
			ok: true as const,
			sessionId: command.sessionId,
			sessionThreadId: command.sessionThreadId,
			applied: true,
			terminal: true,
		};
	}

	async markThreadClosed(
		command: Parameters<RuntimeSubAgentRunHost["markThreadClosed"]>[0],
	) {
		this.actions.push("close");
		this.closedThreadIds.push(command.sessionThreadId);
		this.onClose?.();
		return (
			this.closeResults.shift() ?? {
				ok: true as const,
				sessionId: command.sessionId,
				sessionThreadId: command.sessionThreadId,
				applied: true,
			}
		);
	}

	async markThreadActive(
		command: Parameters<RuntimeSubAgentRunHost["markThreadActive"]>[0],
	) {
		this.actions.push("resume");
		if (this.activeError !== undefined) {
			throw this.activeError;
		}
		return (
			this.activeResults.shift() ?? {
				ok: true as const,
				sessionId: command.sessionId,
				sessionThreadId: command.sessionThreadId,
				applied: true,
			}
		);
	}

	async waitThread(
		command: Parameters<RuntimeSubAgentRunHost["waitThread"]>[0],
		_timeoutMs: number | undefined,
		abortSignal?: AbortSignal,
	) {
		this.waitStarted = true;
		if (this.waitUntilAbort) {
			await new Promise<void>((_resolve, reject) => {
				const onAbort = () => {
					abortSignal?.removeEventListener("abort", onAbort);
					this.waitDetached = true;
					reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
				};
				abortSignal?.addEventListener("abort", onAbort, { once: true });
				if (abortSignal?.aborted === true) {
					onAbort();
				}
			});
		}
		return (
			this.waitResult ?? {
				ok: true as const,
				sessionId: command.sessionId,
				sessionThreadId: command.sessionThreadId,
				observed: false,
				timedOut: false,
			}
		);
	}

	async pullAgentMail(
		command: Parameters<
			NonNullable<RuntimeSubAgentRunHost["pullAgentMail"]>
		>[0],
		sourceThreadId: string,
	) {
		this.pullAgentMailCalls.push({
			sessionThreadId: command.sessionThreadId,
			sourceThreadId,
		});
		return this.pulledAgentMail;
	}

	async waitReviewerExecution(
		command: Parameters<RuntimeSubAgentRunHost["waitReviewerExecution"]>[0],
		_token: Parameters<RuntimeSubAgentRunHost["waitReviewerExecution"]>[1],
		timeoutMs: number | undefined,
		abortSignal?: AbortSignal,
	) {
		const result = await this.waitThread(command, timeoutMs, abortSignal);
		if (!result.ok) {
			return {
				ok: false as const,
				sessionId: command.sessionId,
				sessionThreadId: command.sessionThreadId,
				reason: "reviewer_execution_mismatch" as const,
			};
		}
		return {
			ok: true as const,
			sessionId: command.sessionId,
			sessionThreadId: command.sessionThreadId,
			status: result.status ?? ("idle" as const),
			terminal: !result.timedOut,
			timedOut: result.timedOut,
		};
	}

	async inspectThread(
		command: Parameters<RuntimeSubAgentRunHost["inspectThread"]>[0],
	) {
		this.actions.push("inspect");
		if (this.inspectError !== undefined) {
			throw this.inspectError;
		}
		return {
			ok: true as const,
			sessionId: command.sessionId,
			sessionThreadId: command.sessionThreadId,
			observed: this.inspectObserved,
			...(this.inspectObserved ? { status: this.inspectStatus } : {}),
			entries: [],
		};
	}

	async inspectReviewerExecution(
		command: Parameters<RuntimeSubAgentRunHost["inspectReviewerExecution"]>[0],
	) {
		this.actions.push("inspect-reviewer");
		return {
			ok: false as const,
			sessionId: command.sessionId,
			sessionThreadId: command.sessionThreadId,
			reason: "reviewer_execution_mismatch" as const,
		};
	}

	async commitApprovalReviewDecision(
		command: Parameters<
			RuntimeSubAgentRunHost["commitApprovalReviewDecision"]
		>[0],
	) {
		this.actions.push("commit-approval-review");
		return {
			ok: true as const,
			type: "committed" as const,
			eventId: "evt_decision",
			eventSequence: 1,
		};
	}

	async commitApprovalReviewFailure(
		command: Parameters<
			RuntimeSubAgentRunHost["commitApprovalReviewFailure"]
		>[0],
	) {
		this.actions.push("commit-approval-review-failure");
		return {
			ok: true as const,
			type: "committed" as const,
			eventId: "evt_failure",
			eventSequence: 1,
		};
	}
}

function childThreadFact(taskName: string, status = "idle") {
	return {
		childThreadId: "thr_child_1",
		parentThreadId: "thrd_1",
		role: "subagent",
		visibility: "public",
		status,
		taskName,
		agentType: "worker",
	};
}

class RecordingGatewayClient {
	readonly runWebRequests: RunWebRequest[] = [];
	runWebResponse: RunWebResponse = {
		status: RunWebStatus.RUN_WEB_STATUS_COMPLETED,
		resultText: "web result",
		refs: [],
		windowTruncated: false,
		sourceIncomplete: false,
		usage: {
			operation: "search",
			backendTokens: 4,
			targetHttpStatus: 200,
			storedBytes: 0,
			durationMs: 12,
			webSearchRequests: 2,
			webFetchRequests: 1,
		},
	};

	client(): Pick<ProviderGatewayServiceClient, "runWeb"> {
		return {
			runWeb: this.runWeb.bind(this),
		} as unknown as Pick<ProviderGatewayServiceClient, "runWeb">;
	}

	private runWeb(
		request: RunWebRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: unknown) => void,
	): unknown {
		this.runWebRequests.push(request);
		callback(null, this.runWebResponse);
		return grpcCall();
	}
}

class RecordingMcpConnectorClient {
	readonly runMcpToolRequests: RunMcpToolRequest[] = [];
	runMcpToolResponse: RunMcpToolResponse = {
		status: RunMcpToolStatus.RUN_MCP_TOOL_STATUS_COMPLETED,
		resultText: "mcp result",
		attachments: [],
		errorKind: undefined,
		retryStatus: undefined,
	};

	client(): Pick<McpConnectorServiceClient, "runMcpTool"> {
		return {
			runMcpTool: this.runMcpTool.bind(this),
		} as unknown as Pick<McpConnectorServiceClient, "runMcpTool">;
	}

	private runMcpTool(
		request: RunMcpToolRequest,
		_metadata: Metadata,
		callback: (error: Error | null, response: RunMcpToolResponse) => void,
	): unknown {
		this.runMcpToolRequests.push(request);
		callback(null, this.runMcpToolResponse);
		return grpcCall();
	}
}

function grpcCall(): { readonly cancel: () => void } {
	return { cancel: () => undefined };
}

function grpcError(code: GrpcStatus): Error {
	return Object.assign(new Error("transport unavailable"), { code });
}

async function waitForCondition(
	condition: () => boolean,
	description: string,
): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (condition()) {
			return;
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
	throw new Error(`timed out waiting for ${description}`);
}

async function flushMicrotasks(): Promise<void> {
	for (let attempt = 0; attempt < 20; attempt += 1) {
		await Promise.resolve();
	}
}
