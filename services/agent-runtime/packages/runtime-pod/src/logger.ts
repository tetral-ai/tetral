/**
 * Defines the Runtime Pod's structured log vocabulary and record factories. The process command
 * creates the JSON logger, while lifecycle and command handling emit records through it; this module
 * delegates serialization and common fields to the shared observability package. Startup adapter
 * failures use a fixed public-safe message, and the record factories add no raw request, token,
 * provider, or dependency payloads.
 */

import type {
	RuntimeCloseoutEvent,
	RuntimeMCPManifestUpdateEvent,
} from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type {
	RuntimeAcceptedInputCommitObservation,
	RuntimeProviderRescheduleObservation,
	RuntimeProviderToolDeclarationRejectionObservation,
	RuntimeTerminalSettlementObservation,
} from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import type {
	TetralJsonLogger,
	TetralLogRecord,
} from "@tetral/ts-observability";
import {
	createTetralJsonLogger,
	semanticErrorFields,
} from "@tetral/ts-observability";

/** Structured JSON logger accepted by Runtime Pod composition and runtime services. */
export type RuntimePodLogger = TetralJsonLogger<RuntimePodLogRecord>;

/** Stable phases for strict parsing of one Bridge cold-context response. */
export type RuntimeContextLoadParsePhase =
	| "context_json_parse"
	| "context_envelope_parse"
	| "durable_context_parse"
	| "open_request_draft_parse"
	| "turn_facts_parse"
	| "thread_context_prefix_parse"
	| "thread_metadata_parse"
	| "runtime_config_parse"
	| "mcp_manifests_parse"
	| "pending_tool_uses_parse"
	| "pending_sandbox_executions_parse"
	| "pending_attachments_parse"
	| "pending_agent_mail_parse";

/** Stable, payload-free reasons paired with Runtime cold-context parse phases. */
export type RuntimeContextLoadParseReason =
	| "invalid_context_json"
	| "invalid_context_envelope_shape"
	| "invalid_durable_context_shape"
	| "invalid_open_request_draft_shape"
	| "invalid_turn_facts_shape"
	| "invalid_thread_context_prefix_shape"
	| "invalid_thread_metadata_shape"
	| "invalid_runtime_config_shape"
	| "invalid_mcp_manifests_shape"
	| "invalid_pending_tool_uses_shape"
	| "invalid_pending_sandbox_executions_shape"
	| "invalid_pending_attachments_shape"
	| "invalid_pending_agent_mail_shape";

/** Closed diagnostic phases for the authenticated Bridge-to-Runtime ingress. */
export type RuntimeIngressRejectionPhase =
	| "authentication"
	| "lifecycle"
	| "request_validation"
	| "selected_pod"
	| "binding"
	| "identity"
	| "content_parse"
	| "application";

/** Payload-free reasons emitted when one Runtime ingress request is rejected. */
export type RuntimeIngressRejectionReason =
	| "authentication_rejected"
	| "runtime_not_accepting"
	| "invalid_request"
	| "selected_pod_mismatch"
	| "binding_mismatch"
	| "identity_conflict"
	| "invalid_content"
	| "operation_rejected"
	| "resource_exhausted"
	| "operation_unavailable"
	| "internal_failure";

/** Enumerates Runtime Pod-specific fields layered on the shared structured log record. */
export type RuntimePodLogRecord = TetralLogRecord & {
	readonly event: string;
	readonly kind?: "config_error" | "startup_error" | "shutdown_error";
	readonly "grpc.method"?: string;
	readonly "grpc.code"?: string;
	readonly "caller.service_account"?: string;
	readonly "runtime_input.id"?: string;
	readonly "runtime_input.kind"?: string;
	readonly "model_request.id"?: string;
	readonly "terminal.disposition"?: "terminated";
	readonly "retry.status"?: "terminal";
	readonly "settlement.outcome"?: string;
	readonly "closeout.active_count"?: number;
	readonly "closeout.error_code"?: RuntimeCloseoutEvent["errorCode"];
	readonly "startup.cause_class"?: string;
	readonly "startup.cause_category"?: RuntimeStartupCauseCategory;
	readonly "mcp.server.name"?: string;
	readonly "mcp.manifest.disposition"?: RuntimeMCPManifestUpdateEvent["disposition"];
	readonly "mcp.manifest.source"?: RuntimeMCPManifestUpdateEvent["source"];
	readonly "mcp.manifest.received_generation"?: number;
	readonly "mcp.manifest.current_generation"?: number;
	readonly "mcp.tool_catalog.eligible"?: boolean;
	readonly "reconstruction.phase"?: "cold_checkpoint";
	readonly "failure.kind"?: "invalid_durable_facts";
	readonly phase?: RuntimeContextLoadParsePhase | RuntimeIngressRejectionPhase;
	readonly reason?:
		| RuntimeContextLoadParseReason
		| RuntimeIngressRejectionReason;
};

/** Closed, non-sensitive Runtime Pod startup stages. */
export type RuntimeStartupCauseCategory =
	| "configuration"
	| "listener"
	| "dependency_readiness"
	| "unknown";

/** Emits bounded identity-only evidence when durable facts cannot form a cold Thread checkpoint. */
export function recordCheckpointReconstructionFailure(
	logger: RuntimePodLogger | undefined,
	identity: {
		readonly workspaceId: string;
		readonly sessionId: string;
		readonly sessionThreadId: string;
	},
): void {
	try {
		logger?.error({
			event: "runtime_checkpoint_reconstruction_failed",
			"event.kind": "runtime_checkpoint_reconstruction_failed",
			component: "agent-runtime",
			message: "durable Thread facts could not be reconstructed",
			"workspace.id": identity.workspaceId,
			"session.id": identity.sessionId,
			"thread.id": identity.sessionThreadId,
			"reconstruction.phase": "cold_checkpoint",
			"failure.kind": "invalid_durable_facts",
		});
	} catch {
		// Durable context handling is authoritative; diagnostics are fail-open.
	}
}

/** Emits identity-only evidence when a strict cold-context parse phase rejects Bridge data. */
export function recordContextLoadParseFailure(
	logger: RuntimePodLogger | undefined,
	identity: {
		readonly workspaceId: string;
		readonly sessionId: string;
		readonly sessionThreadId: string;
	},
	phase: RuntimeContextLoadParsePhase,
	reason: RuntimeContextLoadParseReason,
): void {
	try {
		logger?.error({
			event: "runtime_context_load_parse_failed",
			"event.kind": "runtime_context_load_parse_failed",
			component: "agent-runtime",
			message: "durable Runtime context projection was rejected",
			operation: "load_context",
			phase,
			reason,
			"workspace.id": identity.workspaceId,
			"session.id": identity.sessionId,
			"thread.id": identity.sessionThreadId,
		});
	} catch {
		// Durable context rejection is authoritative; diagnostics are fail-open.
	}
}

/** Configures the JSON sink and optional service resource attributes for Runtime Pod logs. */
export interface JsonLoggerOptions {
	readonly write: (line: string) => void;
	readonly serviceName?: string;
	readonly deploymentEnvironment?: string;
	readonly serviceVersion?: string;
	readonly clock?: (() => Date) | undefined;
}

/** Creates a Runtime Pod JSON logger backed by the shared observability serializer. */
export function createJsonLogger(options: JsonLoggerOptions): RuntimePodLogger {
	return createTetralJsonLogger<RuntimePodLogRecord>({
		write: options.write,
		serviceName: options.serviceName ?? "agent-runtime",
		deploymentEnvironment: options.deploymentEnvironment,
		serviceVersion: options.serviceVersion,
		clock: options.clock,
	});
}

/**
 * Builds a startup failure record by retaining the supplied configuration message and collapsing
 * adapter startup failures to a fixed message.
 */
export function startupFailureLogRecord(input: {
	readonly kind: "config_error" | "startup_error";
	readonly message: string;
	readonly cause?: unknown;
	readonly causeCategory?: RuntimeStartupCauseCategory;
}): RuntimePodLogRecord {
	const safeMessage =
		input.kind === "config_error"
			? input.message
			: "runtime pod startup failed";
	const causeCategory =
		input.kind === "config_error"
			? "configuration"
			: (input.causeCategory ?? "unknown");
	return {
		event: "startup_failed",
		"event.kind": "startup_failed",
		operation: "startup",
		component: "agent-runtime",
		kind: input.kind,
		message: safeMessage,
		"startup.cause_category": causeCategory,
		...(input.kind === "startup_error"
			? { "startup.cause_class": startupCauseClass(input.cause) }
			: {}),
		...semanticErrorFields({
			errorClass: input.kind,
			errorCode: input.kind,
			messageSafe: safeMessage,
		}),
	};
}

/** Builds the lifecycle record emitted after Runtime Pod listeners are ready. */
export function workloadStartedLogRecord(): RuntimePodLogRecord {
	return {
		event: "workload.started",
		"event.kind": "started",
		operation: "workload.lifecycle",
		component: "workload",
		"listener.transport": "tcp",
		"readiness.state": "ready",
	};
}

/** Builds the payload-free record for one declaration rejected before a provider request opens. */
export function providerToolDeclarationRejectedLogRecord(
	event: RuntimeProviderToolDeclarationRejectionObservation,
): RuntimePodLogRecord {
	return {
		event: "provider_tool_declaration_rejected",
		"event.kind": "provider_tool_declaration_rejected",
		operation: "assemble_provider_request",
		component: "agent-runtime",
		message: "provider tool declaration rejected",
		"workspace.id": event.workspaceId,
		"session.id": event.sessionId,
		"thread.id": event.sessionThreadId,
		"request.id": event.requestId,
		"model_request.id": event.modelRequestId,
		"tool.declaration.kind": event.declarationKind,
		"tool.family": event.family,
		"validation.member": event.validationMember,
		...semanticErrorFields({
			errorClass: "provider_tool_declaration",
			errorCode: "invalid_tool_definition",
			messageSafe: "provider tool declaration rejected",
		}),
	};
}

/** Builds the bounded fact emitted only after durable provider reschedule admission. */
export function providerRescheduleSelectedLogRecord(
	event: RuntimeProviderRescheduleObservation,
): RuntimePodLogRecord {
	return {
		event: "provider_reschedule_selected",
		"event.kind": "provider_reschedule_selected",
		operation: "provider_reschedule",
		component: "agent-runtime",
		message: "provider request reschedule selected",
		"workspace.id": event.workspaceId,
		"session.id": event.sessionId,
		"thread.id": event.sessionThreadId,
		"request.id": event.requestId,
		"model_request.id": event.modelRequestId,
		"retry.attempt": event.attempt,
		"delay.ms": event.delayMs,
		"delay.source": event.delaySource,
		"provider.failure.code": event.failureCode,
		retryable: true,
	};
}

/** Builds the payload-free record for one reducer-owned accepted-input commit observation. */
export function acceptedInputCommitLogRecord(
	event: RuntimeAcceptedInputCommitObservation,
): RuntimePodLogRecord {
	return {
		event: "runtime_accepted_input_commit",
		"event.kind": "runtime_accepted_input_commit",
		operation: "commit_accepted_input",
		component: "agent-runtime",
		message: "accepted input commit observed",
		"workspace.id": event.workspaceId,
		"session.id": event.sessionId,
		"thread.id": event.sessionThreadId,
		"runtime_input.id": event.runtimeInputId,
		"runtime_input.kind": event.inputKind,
		"retry.attempt": event.attempt,
		"duration.ms": event.durationMs,
		outcome: event.outcome,
		...(event.failureClass === undefined
			? {}
			: { "failure.class": event.failureClass }),
	};
}

/** Emits the started record without allowing a logging sink to change process readiness. */
export function logWorkloadStarted(logger: RuntimePodLogger): void {
	try {
		logger.info(workloadStartedLogRecord());
	} catch {
		// Listener readiness, not observability delivery, determines startup success.
	}
}

function startupCauseClass(cause: unknown): string {
	try {
		if (
			(typeof cause !== "object" || cause === null) &&
			typeof cause !== "function"
		) {
			return "unknown";
		}
		const constructor = Reflect.get(cause, "constructor") as unknown;
		if (
			(typeof constructor !== "object" || constructor === null) &&
			typeof constructor !== "function"
		) {
			return "unknown";
		}
		const name = Reflect.get(constructor, "name") as unknown;
		if (
			typeof name === "string" &&
			name.length <= 64 &&
			/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(name)
		) {
			return name;
		}
	} catch {
		// A thrown value may be a revoked Proxy or expose throwing accessors.
	}
	return "unknown";
}

/** Builds a typed shutdown failure record for active-run settlement or drain timeout failure. */
export function shutdownFailureLogRecord(input: {
	readonly event:
		| "shutdown_active_run_settlement_failed"
		| "shutdown_drain_timeout";
	readonly message: string;
}): RuntimePodLogRecord {
	return {
		event: input.event,
		"event.kind": input.event,
		operation: "shutdown",
		component: "agent-runtime",
		kind: "shutdown_error",
		message: input.message,
		...semanticErrorFields({
			errorClass: "shutdown_error",
			errorCode: input.event,
			messageSafe: input.message,
		}),
	};
}

/** Builds one bounded closeout alarm, recovery, or unrepairable-release record. */
export function runtimeCloseoutLogRecord(
	input: RuntimeCloseoutEvent,
): RuntimePodLogRecord {
	return {
		event: input.event,
		"event.kind": input.event,
		operation: "runtime_closeout",
		component: "agent-runtime",
		message: input.event,
		"closeout.active_count": input.activeCloseouts,
		...(input.errorCode !== undefined
			? { "closeout.error_code": input.errorCode }
			: {}),
		...semanticErrorFields({
			errorClass: "runtime_closeout",
			errorCode: input.errorCode ?? input.event,
			messageSafe: input.event,
		}),
	};
}

/** Builds the bounded operator record for a durably settled Runtime semantic failure. */
export function runtimeTerminalSettlementLogRecord(
	input: RuntimeTerminalSettlementObservation,
): RuntimePodLogRecord {
	return {
		event: "runtime_terminal_settlement",
		"event.kind": "runtime_terminal_settlement",
		operation: "commit_runtime_termination",
		component: "agent-runtime",
		message: "runtime contract validation terminated the Thread",
		"workspace.id": input.workspaceId,
		"session.id": input.sessionId,
		"thread.id": input.sessionThreadId,
		...(input.modelRequestId !== undefined
			? { "model_request.id": input.modelRequestId }
			: {}),
		terminal: true,
		"terminal.disposition": "terminated",
		"retry.status": "terminal",
		"settlement.outcome": input.settlementOutcome,
		...semanticErrorFields({
			errorClass: "runtime_contract_validation",
			errorCode: "runtime_semantic_error",
			messageSafe: "runtime contract validation failed",
		}),
	};
}

/** Safe effective-state record emitted only after the Session configuration gate decides. */
export function runtimeMCPManifestUpdateLogRecord(
	input: RuntimeMCPManifestUpdateEvent,
): RuntimePodLogRecord {
	return {
		event: "runtime_mcp_manifest_update",
		"event.kind": "runtime_mcp_manifest_update",
		operation: "runtime_config_apply",
		component: "agent-runtime",
		message: "runtime MCP manifest generation observed",
		"workspace.id": input.workspaceId,
		"session.id": input.sessionId,
		"mcp.server.name": input.mcpServerName,
		"mcp.manifest.disposition": input.disposition,
		"mcp.manifest.source": input.source,
		"mcp.manifest.received_generation": input.receivedGeneration,
		"mcp.manifest.current_generation": input.currentGeneration,
		"mcp.tool_catalog.eligible": input.toolCatalogEligible,
	};
}
