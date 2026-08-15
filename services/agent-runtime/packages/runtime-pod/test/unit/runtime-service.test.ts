import { describe, expect, test } from "bun:test";
import { Metadata, status } from "@grpc/grpc-js";
import type {
	AcceptAgentMailRequest,
	AcceptInputRequest,
	AcceptTaskNotificationRequest,
	ApplyRuntimeConfigRequest,
	CleanupSessionRequest,
	InterruptRequest,
	ResolveToolConfirmationRequest,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import {
	AcceptInputFailure,
	AcceptInputRejectionReason,
	ApplyRuntimeConfigFailure,
	CleanupSessionReason,
	InterruptOrigin,
	ToolConfirmationDecision,
} from "@tetral/agent-runtime-protocol/src/gen/tetral/agent_runtime/v1/agent_runtime.js";
import type { RuntimePodLogRecord } from "../../src/logger.js";
import type {
	RuntimeAuthenticator,
	RuntimeCleanupController,
	RuntimeControlInputCommitter,
	RuntimeSessionRunHost,
} from "../../src/runtime-service.js";
import {
	GrpcStatusError,
	RuntimeControlService,
} from "../../src/runtime-service.js";

describe("RuntimeControlService method-specific ingress", () => {
	test("authorizes before validating or applying a request", async () => {
		const fixture = makeFixture({ deny: true });
		await expectGrpcCode(
			fixture.service.acceptInput(
				{ ...acceptInput(), sessionId: "" },
				metadata(),
			),
			status.PERMISSION_DENIED,
		);
		expect(fixture.host.inputs).toEqual([]);
		expect(fixture.logger.records).toEqual([
			expect.objectContaining({
				event: "runtime_command_rejected",
				phase: "authentication",
				reason: "authentication_rejected",
			}),
		]);
		expect(fixture.logger.records[0]).not.toHaveProperty("session.id");
	});

	test("authenticates config requests before bounds, parsing, or memo observation", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.applyRuntimeConfig(mcpConfig(), metadata()),
		).toEqual({ applied: {} });
		fixture.authenticator.deny = true;

		await expectGrpcCode(
			fixture.service.applyRuntimeConfig(
				mcpConfig({
					mcpManifest: {
						mcpServerName: "docs",
						generation: 3,
						contentJson: "{",
					},
				}),
				metadata(),
			),
			status.PERMISSION_DENIED,
		);
		await expectGrpcCode(
			fixture.service.applyRuntimeConfig(
				mcpConfig({
					mcpManifest: {
						mcpServerName: "docs",
						generation: 3,
						contentJson: "x".repeat(2 * 1024 * 1024 + 1),
					},
				}),
				metadata(),
			),
			status.PERMISSION_DENIED,
		);
		await expectGrpcCode(
			fixture.service.applyRuntimeConfig(
				mcpConfig({
					mcpManifest: {
						mcpServerName: "new",
						generation: 4,
						contentJson: "{",
					},
				}),
				metadata(),
			),
			status.PERMISSION_DENIED,
		);

		expect(fixture.host.configs).toHaveLength(1);
		expect(fixture.logger.records.slice(1)).toHaveLength(3);
		expect(
			fixture.logger.records
				.slice(1)
				.every((record) => record.phase === "authentication"),
		).toBe(true);
	});

	test("checks selected Pod and active binding before config parsing or memo lookup", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.applyRuntimeConfig(mcpConfig(), metadata()),
		).toEqual({ applied: {} });
		expect(
			await fixture.service.applyRuntimeConfig(
				mcpConfig({
					targetPodUid: "another-pod",
					mcpManifest: {
						mcpServerName: "docs",
						generation: 3,
						contentJson: "{",
					},
				}),
				metadata(),
			),
		).toEqual({
			rejected: {
				reason:
					ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_SELECTED_POD_MISMATCH,
				retryable: true,
			},
		});
		expect(
			await fixture.service.applyRuntimeConfig(
				mcpConfig({
					bindingId: "bind_2",
					mcpManifest: {
						mcpServerName: "docs",
						generation: 3,
						contentJson: "{",
					},
				}),
				metadata(),
			),
		).toEqual({
			rejected: {
				reason:
					ApplyRuntimeConfigFailure.APPLY_RUNTIME_CONFIG_FAILURE_BINDING_MISMATCH,
				retryable: true,
			},
		});
		expect(fixture.host.configs).toHaveLength(1);
		expect(
			fixture.logger.records
				.slice(-2)
				.map((record) => [record.phase, record.reason]),
		).toEqual([
			["selected_pod", "selected_pod_mismatch"],
			["binding", "binding_mismatch"],
		]);
	});

	test("accepts messages using only the input order and method-owned content", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.acceptInput(acceptInput(), metadata()),
		).toEqual({ accepted: {} });
		expect(fixture.host.inputs).toEqual([
			{
				...inputScope(),
				inputOrder: 9,
				kind: "messages",
				contentJson: '{"messages":[]}',
			},
		]);
		expect(fixture.logger.records.at(-1)).toMatchObject({
			operation: "AcceptInput",
			"grpc.method":
				"/tetral.agent_runtime.v1.AgentRuntimePodService/AcceptInput",
			"operation.id": "rin_1",
		});
	});

	test("accepts a bounded rejection without rejected content", async () => {
		const fixture = makeFixture();
		const request = acceptInput({
			messagesJson: undefined,
			rejection: {
				reason:
					AcceptInputRejectionReason.ACCEPT_INPUT_REJECTION_REASON_PAYLOAD_TOO_LARGE,
			},
		});
		expect(await fixture.service.acceptInput(request, metadata())).toEqual({
			accepted: {},
		});
		expect(fixture.host.inputs[0]).toEqual({
			...inputScope(),
			inputOrder: 9,
			kind: "rejection",
			reasonCode: "runtime_command_payload_too_large",
		});
	});

	test("returns an operation-specific selected-pod failure", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.acceptInput(
				acceptInput({ targetPodUid: "another-pod" }),
				metadata(),
			),
		).toEqual({
			rejected: {
				reason: AcceptInputFailure.ACCEPT_INPUT_FAILURE_SELECTED_POD_MISMATCH,
				retryable: true,
			},
		});
	});

	test("delivers agent mail without an input-order or event-range echo", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.acceptAgentMail(agentMail(), metadata()),
		).toEqual({ accepted: {} });
		expect(fixture.host.mail).toHaveLength(1);
		expect(fixture.host.mail[0]).toEqual({
			...inputScope({ runtimeInputId: "agent_mail:delivery_1" }),
			kind: "inter_agent_message",
			deliveryId: "delivery_1",
			content: "child completion",
		});
		expect(fixture.host.mail[0]).not.toHaveProperty("inputOrder");
		expect(fixture.host.mail[0]).not.toHaveProperty("eventIds");
	});

	test("routes task, interrupt, and confirmation fields to their dedicated hosts", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.acceptTaskNotification(
				taskNotification(),
				metadata(),
			),
		).toEqual({ accepted: {} });
		expect(await fixture.service.interrupt(interrupt(), metadata())).toEqual({
			accepted: {},
		});
		expect(
			await fixture.service.resolveToolConfirmation(
				toolConfirmation(),
				metadata(),
			),
		).toEqual({ accepted: {} });
		expect(fixture.host.tasks[0]?.command).toMatchObject({
			inputOrder: 0,
			taskId: "task_1",
			status: "completed",
		});
		expect(fixture.host.interrupts[0]?.command).toMatchObject({
			inputOrder: 10,
			origin: "user",
		});
		expect(fixture.host.confirmations[0]?.command).toMatchObject({
			toolUseEventId: "sevt_tool",
			decision: "deny",
			denyMessage: "no",
		});
		expect(fixture.committer.commits.map((input) => input.inputKind)).toEqual([
			"interrupt_control",
			"tool_confirmation",
		]);
		expect(fixture.committer.commits[0]?.scope).toEqual(
			inputScope({ runtimeInputId: "rin_interrupt" }),
		);
	});

	test("applies session and MCP config through explicit config identities", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.applyRuntimeConfig(sessionConfig(), metadata()),
		).toEqual({ applied: {} });
		expect(
			await fixture.service.applyRuntimeConfig(mcpConfig(), metadata()),
		).toEqual({ applied: {} });
		expect(
			fixture.host.configs.map(({ command }) => command.configIdentity),
		).toEqual(["session:7", "mcp:docs:3"]);
		expect(fixture.host.configs[0]?.command).not.toHaveProperty(
			"runtimeInputId",
		);
		expect(fixture.host.configs[1]?.command).toMatchObject({
			mcpServerName: "docs",
			manifestETag: "etag-1",
			manifestReadiness: "ready",
		});
	});

	test("retains only the current runtime-config identity for each active family", async () => {
		const fixture = makeFixture();
		expect(
			await fixture.service.applyRuntimeConfig(sessionConfig(), metadata()),
		).toEqual({ applied: {} });
		expect(
			await fixture.service.applyRuntimeConfig(
				sessionConfig({
					sessionConfig: { generation: 8, contentJson: '{"provider":"next"}' },
				}),
				metadata(),
			),
		).toEqual({ applied: {} });

		// Generation 8 superseded generation 7, so an old identity no longer has a
		// process-local conflict memo. The owning host decides that stale generation.
		expect(
			await fixture.service.applyRuntimeConfig(
				sessionConfig({
					sessionConfig: {
						generation: 7,
						contentJson: '{"provider":"changed"}',
					},
				}),
				metadata(),
			),
		).toEqual({ applied: {} });
		expect(fixture.host.configs).toHaveLength(3);
	});

	test("retires runtime-config memo state when no residency exists and after cleanup", async () => {
		const fixture = makeFixture();
		fixture.host.configNoResidency = true;
		expect(
			await fixture.service.applyRuntimeConfig(sessionConfig(), metadata()),
		).toEqual({ noResidency: {} });
		fixture.host.configNoResidency = false;
		expect(
			await fixture.service.applyRuntimeConfig(
				sessionConfig({
					sessionConfig: {
						generation: 7,
						contentJson: '{"provider":"after-no-residency"}',
					},
				}),
				metadata(),
			),
		).toEqual({ applied: {} });

		expect(await fixture.service.cleanupSession(cleanup(), metadata())).toEqual(
			{ completed: {} },
		);
		expect(
			await fixture.service.applyRuntimeConfig(
				sessionConfig({
					sessionConfig: {
						generation: 7,
						contentJson: '{"provider":"after-cleanup"}',
					},
				}),
				metadata(),
			),
		).toEqual({ applied: {} });
		expect(fixture.host.configs).toHaveLength(3);
	});

	test("cleans a session with a session-scoped operation identity", async () => {
		const fixture = makeFixture();
		expect(await fixture.service.cleanupSession(cleanup(), metadata())).toEqual(
			{ completed: {} },
		);
		expect(fixture.cleanup.requests).toEqual([
			{
				scope: { ...sessionScope(), cleanupOperationId: "cleanup_1" },
				reason: "expired",
			},
		]);
	});

	test("rejects invalid operation content before host effects", async () => {
		const fixture = makeFixture();
		await expectGrpcCode(
			fixture.service.acceptTaskNotification(
				taskNotification({ notificationJson: "{}" }),
				metadata(),
			),
			status.INVALID_ARGUMENT,
		);
		await expectGrpcCode(
			fixture.service.acceptAgentMail(agentMail({ content: "" }), metadata()),
			status.INVALID_ARGUMENT,
		);
		expect(fixture.host.tasks).toEqual([]);
		expect(fixture.host.mail).toEqual([]);
		expect(
			fixture.logger.records.map((record) => [record.phase, record.reason]),
		).toEqual([
			["content_parse", "invalid_content"],
			["request_validation", "invalid_request"],
		]);
	});

	test("records bounded diagnostics for validation, selected-Pod, binding, and identity rejection", async () => {
		const fixture = makeFixture();
		await expectGrpcCode(
			fixture.service.acceptInput(
				acceptInput({ sessionId: "raw-secret-content".repeat(20) }),
				metadata(),
			),
			status.INVALID_ARGUMENT,
		);
		expect(
			await fixture.service.acceptInput(
				acceptInput({ targetPodUid: "another-pod" }),
				metadata(),
			),
		).toEqual({
			rejected: {
				reason: AcceptInputFailure.ACCEPT_INPUT_FAILURE_SELECTED_POD_MISMATCH,
				retryable: true,
			},
		});
		expect(
			await fixture.service.acceptInput(acceptInput(), metadata()),
		).toEqual({ accepted: {} });
		expect(
			await fixture.service.acceptInput(
				acceptInput({ runtimeInputId: "rin_2", bindingId: "bind_2" }),
				metadata(),
			),
		).toEqual({
			rejected: {
				reason: AcceptInputFailure.ACCEPT_INPUT_FAILURE_BINDING_MISMATCH,
				retryable: true,
			},
		});

		const gate = deferred();
		fixture.host.acceptInputGate = gate.promise;
		const first = fixture.service.acceptInput(
			acceptInput({ runtimeInputId: "rin_3" }),
			metadata(),
		);
		await waitFor(() => fixture.host.inputs.length === 2);
		expect(
			await fixture.service.acceptInput(
				acceptInput({
					runtimeInputId: "rin_3",
					messagesJson: '{"messages":["different"]}',
				}),
				metadata(),
			),
		).toEqual({
			rejected: {
				reason: AcceptInputFailure.ACCEPT_INPUT_FAILURE_IDENTITY_CONFLICT,
				retryable: false,
			},
		});
		gate.resolve();
		expect(await first).toEqual({ accepted: {} });

		expect(
			fixture.logger.records
				.filter((record) => record.event === "runtime_command_rejected")
				.map((record) => [record.phase, record.reason]),
		).toEqual([
			["request_validation", "invalid_request"],
			["selected_pod", "selected_pod_mismatch"],
			["binding", "binding_mismatch"],
			["identity", "identity_conflict"],
		]);
		expect(JSON.stringify(fixture.logger.records)).not.toContain(
			"raw-secret-content",
		);
		expect(JSON.stringify(fixture.logger.records)).not.toContain("different");
	});

	test("keeps applied and duplicate outcomes consistent when the logger sink throws", async () => {
		const fixture = makeFixture();
		fixture.logger.throwOnWrite = true;
		const gate = deferred();
		fixture.host.acceptInputGate = gate.promise;
		const applied = fixture.service.acceptInput(acceptInput(), metadata());
		await waitFor(() => fixture.host.inputs.length === 1);
		const duplicate = fixture.service.acceptInput(acceptInput(), metadata());
		gate.resolve();

		expect(await Promise.all([applied, duplicate])).toEqual([
			{ accepted: {} },
			{ duplicate: {} },
		]);
		expect(fixture.host.inputs).toHaveLength(1);
	});
});

function sessionScope() {
	return {
		workspaceId: "wksp_1",
		sessionId: "sesn_1",
		bindingId: "bind_1",
		bindingGeneration: 42,
		targetPodUid: "uid-a",
	};
}

function inputScope(
	overrides: Partial<ReturnType<typeof inputScopeBase>> = {},
) {
	return { ...inputScopeBase(), ...overrides };
}

function inputScopeBase() {
	return {
		...sessionScope(),
		sessionThreadId: "thrd_1",
		runtimeInputId: "rin_1",
	};
}

function acceptInput(
	overrides: Partial<AcceptInputRequest> = {},
): AcceptInputRequest {
	return {
		...inputScope(),
		inputOrder: 9,
		messagesJson: '{"messages":[]}',
		...overrides,
	};
}

function agentMail(
	overrides: Partial<AcceptAgentMailRequest> = {},
): AcceptAgentMailRequest {
	return {
		...inputScope({ runtimeInputId: "agent_mail:delivery_1" }),
		deliveryId: "delivery_1",
		content: "child completion",
		...overrides,
	};
}

function taskNotification(
	overrides: Partial<AcceptTaskNotificationRequest> = {},
): AcceptTaskNotificationRequest {
	return {
		...inputScope({ runtimeInputId: "task:task_1" }),
		inputOrder: 0,
		notificationJson: JSON.stringify({
			task_id: "task_1",
			source_tool_use_event_id: "sevt_task_tool",
			status: "completed",
			stdout: { text: "done", truncated: false },
			stderr: { text: "", truncated: false },
		}),
		...overrides,
	};
}

function interrupt(
	overrides: Partial<InterruptRequest> = {},
): InterruptRequest {
	return {
		...inputScope({ runtimeInputId: "rin_interrupt" }),
		inputOrder: 10,
		origin: InterruptOrigin.INTERRUPT_ORIGIN_USER,
		...overrides,
	};
}

function toolConfirmation(
	overrides: Partial<ResolveToolConfirmationRequest> = {},
): ResolveToolConfirmationRequest {
	return {
		...inputScope({ runtimeInputId: "rin_confirmation" }),
		toolUseEventId: "sevt_tool",
		decision: ToolConfirmationDecision.TOOL_CONFIRMATION_DECISION_DENY,
		denyMessage: "no",
		...overrides,
	};
}

function sessionConfig(
	overrides: Partial<ApplyRuntimeConfigRequest> = {},
): ApplyRuntimeConfigRequest {
	return {
		...sessionScope(),
		sessionConfig: { generation: 7, contentJson: '{"provider":"test"}' },
		...overrides,
	};
}

function mcpConfig(
	overrides: Partial<ApplyRuntimeConfigRequest> = {},
): ApplyRuntimeConfigRequest {
	return {
		...sessionScope(),
		mcpManifest: {
			mcpServerName: "docs",
			generation: 3,
			contentJson: '{"readiness":"ready","manifest_etag":"etag-1"}',
		},
		...overrides,
	};
}

function cleanup(
	overrides: Partial<CleanupSessionRequest> = {},
): CleanupSessionRequest {
	return {
		...sessionScope(),
		cleanupOperationId: "cleanup_1",
		reason: CleanupSessionReason.CLEANUP_SESSION_REASON_EXPIRED,
		...overrides,
	};
}

function metadata(): Metadata {
	const value = new Metadata();
	value.set("authorization", "bearer token");
	return value;
}

async function expectGrpcCode(
	promise: Promise<unknown>,
	code: status,
): Promise<void> {
	try {
		await promise;
		throw new Error(`expected gRPC code ${code}`);
	} catch (error) {
		if (error instanceof GrpcStatusError) {
			expect(error.code).toBe(code);
			return;
		}
		throw error;
	}
}

function makeFixture(options: { readonly deny?: boolean } = {}) {
	const host = new RecordingRunHost();
	const cleanup = new RecordingCleanupController();
	const committer = new RecordingCommitter();
	const logger = new RecordingLogger();
	const authenticator = new FixedAuthenticator(options.deny === true);
	const service = new RuntimeControlService({
		ownPod: {
			namespace: "engine",
			name: "runtime-pod-a",
			uid: "uid-a",
			ip: "10.0.0.1",
		},
		allowedBridge: { namespace: "engine", name: "bridge" },
		authenticator,
		runHost: host,
		cleanupController: cleanup,
		controlInputCommitter: committer,
		logger,
		ready: () => true,
	});
	return { service, host, cleanup, committer, logger, authenticator };
}

class FixedAuthenticator implements RuntimeAuthenticator {
	constructor(public deny: boolean) {}
	async authenticate() {
		return this.deny
			? {
					ok: false as const,
					code: "PermissionDenied" as const,
					message: "denied",
				}
			: {
					ok: true as const,
					serviceAccount: { namespace: "engine", name: "bridge" },
				};
	}
}

class RecordingRunHost implements RuntimeSessionRunHost {
	readonly inputs: Array<
		Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]
	> = [];
	readonly mail: Array<
		Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0]
	> = [];
	readonly tasks: Array<{
		sessionId: string;
		command: Parameters<RuntimeSessionRunHost["handleTaskNotification"]>[1];
	}> = [];
	readonly interrupts: Array<{
		sessionId: string;
		command: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[1];
	}> = [];
	readonly confirmations: Array<{
		sessionId: string;
		command: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[1];
	}> = [];
	readonly configs: Array<{
		sessionId: string;
		command: Parameters<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>[1];
	}> = [];
	acceptInputGate: Promise<void> | undefined;
	configNoResidency = false;

	async handleAcceptInput(
		command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0],
	) {
		this.inputs.push(command);
		await this.acceptInputGate;
		return {
			ok: true as const,
			sessionId: command.sessionId,
			created: true,
			started: true,
		};
	}
	async handleAgentMail(
		command: Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0],
	) {
		this.mail.push(command);
		return { ok: true as const, sessionId: command.sessionId, applied: true };
	}
	async handleTaskNotification(
		sessionId: string,
		command: Parameters<RuntimeSessionRunHost["handleTaskNotification"]>[1],
	) {
		this.tasks.push({ sessionId, command });
		return { ok: true as const, sessionId, created: false, applied: true };
	}
	async handleInterruptControl(
		sessionId: string,
		command: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[1],
		commit: Parameters<RuntimeSessionRunHost["handleInterruptControl"]>[2],
	) {
		this.interrupts.push({ sessionId, command });
		await commit({ inputKind: "interrupt" });
		return {
			ok: true as const,
			sessionId,
			created: false,
			interrupted: true,
			idleInterrupt: false,
		};
	}
	async handleToolConfirmation(
		sessionId: string,
		command: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[1],
		commit: Parameters<RuntimeSessionRunHost["handleToolConfirmation"]>[2],
	) {
		this.confirmations.push({ sessionId, command });
		await commit({ inputKind: "tool_confirmation" });
		return { ok: true as const, sessionId, created: false, applied: true };
	}
	async handleRuntimeConfigPatch(
		sessionId: string,
		command: Parameters<RuntimeSessionRunHost["handleRuntimeConfigPatch"]>[1],
	) {
		this.configs.push({ sessionId, command });
		return {
			ok: true as const,
			sessionId,
			created: false,
			applied: true,
			...(this.configNoResidency ? { noResidency: true as const } : {}),
		};
	}
}

class RecordingCommitter implements RuntimeControlInputCommitter {
	readonly commits: Array<
		Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0]
	> = [];
	async commitControlInput(
		input: Parameters<RuntimeControlInputCommitter["commitControlInput"]>[0],
	) {
		this.commits.push(input);
		return { ok: true as const, type: "stale" as const };
	}
}

class RecordingCleanupController implements RuntimeCleanupController {
	readonly requests: Array<{
		scope: Parameters<RuntimeCleanupController["startCleanup"]>[0];
		reason: Parameters<RuntimeCleanupController["startCleanup"]>[1];
	}> = [];
	async startCleanup(
		scope: Parameters<RuntimeCleanupController["startCleanup"]>[0],
		reason: Parameters<RuntimeCleanupController["startCleanup"]>[1],
	) {
		this.requests.push({ scope, reason });
		return {
			ok: true as const,
			sessionId: scope.sessionId,
			completion: Promise.resolve({
				ok: true as const,
				sessionId: scope.sessionId,
				cleaned: true,
			}),
		};
	}
}

class RecordingLogger {
	readonly records: RuntimePodLogRecord[] = [];
	throwOnWrite = false;
	info(record: RuntimePodLogRecord): void {
		this.records.push(record);
		if (this.throwOnWrite) throw new Error("logger failed");
	}
	error(record: RuntimePodLogRecord): void {
		this.records.push(record);
		if (this.throwOnWrite) throw new Error("logger failed");
	}
}

function deferred(): {
	readonly promise: Promise<void>;
	readonly resolve: () => void;
} {
	let resolve: () => void = () => undefined;
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

async function waitFor(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (predicate()) return;
		await Promise.resolve();
	}
	throw new Error("condition was not observed");
}
