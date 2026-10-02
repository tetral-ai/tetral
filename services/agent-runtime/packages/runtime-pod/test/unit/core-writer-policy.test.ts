import { expect, test } from "bun:test";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { ServerUnaryCall, sendUnaryData } from "@grpc/grpc-js";
import { AgentRuntimeBridgeServiceService } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { CommitRuntimeTerminationRequest, CommitRuntimeTerminationResponse, WriteEventRequest, WriteEventResponse, CommitInputsRequest, CommitInputsResponse } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { normalizeRuntimeFailure } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { commitRuntimeTerminationWithRetry, closeFailedRunDurably, createFailedRunCloseoutMemo } from "@tetral/agent-runtime-core/src/thread-loop/closeout.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import { commitAcceptedInputWithRetry } from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { acceptedInput, approvalReviewAcceptedInput, testRunCustody } from "@tetral/agent-runtime-core/test/unit/thread-loop/thread-loop-test-support.js";
import type { RuntimeCoreHostsOptions } from "../../src/core-hosts.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { buildRuntimePodCommandDependencies } from "../../src/command.js";
import { loadRuntimePodConfig } from "../../src/config.js";
const scope = {
    workspaceId: "wksp_test", sessionId: "sesn_1", sessionThreadId: "thrd_1", bindingId: "bind_1", bindingGeneration: 1, targetPodUid: "pod_1", runtimeProcessId: "process-test"
};
const termination = {
    ...scope, writeId: "termination_original", failure: normalizeRuntimeFailure({
        type: "runtime", code: "runtime_invalid_sequence", sessionId: scope.sessionId, retryable: false, fatal: true
    })
};
test("failed-run memo observation expires independently and rejoins configured transport", async () => {
    const server = new Server();
    let active = 0, cancelled = 0, appends = 0, idle = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        writeEvent: (call: ServerUnaryCall<WriteEventRequest, WriteEventResponse>, callback: sendUnaryData<WriteEventResponse>) => {
            appends++;
            active++;
            expect(Number(call.getDeadline()) - Date.now()).toBeGreaterThan(6500);
            call.once("cancelled", () => {
                if (active)
                    cancelled++;
            });
            setTimeout(() => {
                active--;
                callback(null, {
                    committed: {
                        eventId: "memo_failure"
                    }
                });
            }, 4000);
        },
        finishIdle: (_call: unknown, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            idle++;
            callback(null, {
                committed: {
                    idleEventId: "memo_idle"
                }
            });
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_WRITE_EVENT_TIMEOUT_MS: "7000"
    });
    try {
        const session = new ThreadRuntime(scope.sessionId);
        session.state.installThreadTurn({
            executionRunId: "memo_turn", pendingInputContextSequences: []
        }, {
            routes: []
        });
        const memo = createFailedRunCloseoutMemo("memo_write", "memo_turn");
        const observe = () => closeFailedRunDurably(assembled.options.threadLoop, session, memo, testRunCustody(), (writeId, failure) => assembled.options.threadLoop.sessionEventWriter.append({
            ...scope, writeId, event: {
                type: "session.error", error: failure
            }
        }));
        expect(await observe()).toMatchObject({
            type: "retry", error: {
                code: "timeout"
            }
        });
        expect(active).toBe(1);
        expect(cancelled).toBe(0);
        expect(appends).toBe(1);
        expect(await observe()).toMatchObject({
            type: "landed"
        });
        expect(appends).toBe(1);
        expect(idle).toBe(1);
        expect(active).toBe(0);
        expect(cancelled).toBe(0);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
}, 15000);
test("parsed writer policies reach actual Core termination and reviewer failure assembly", async () => {
    let active = 0;
    const calls: Array<{
        method: string;
        id: string;
        budget: number;
    }> = [];
    const server = new Server();
    const finish = <Request, Response>(method: string, id: string, call: ServerUnaryCall<Request, Response>, callback: sendUnaryData<Response>, response: Response) => {
        active++;
        calls.push({
            method, id, budget: Number(call.getDeadline()) - Date.now()
        });
        setTimeout(() => {
            active--;
            callback(null, response);
        }, 4000);
    };
    server.addService(AgentRuntimeBridgeServiceService, {
        commitRuntimeTermination: (call: ServerUnaryCall<CommitRuntimeTerminationRequest, CommitRuntimeTerminationResponse>, callback: sendUnaryData<CommitRuntimeTerminationResponse>) => finish("termination", call.request.runtimeWriteId, call, callback, {
            committed: {
                failureEventId: "evt_failure", closeoutEventId: "evt_closeout"
            }
        }),
        writeEvent: (call: ServerUnaryCall<WriteEventRequest, WriteEventResponse>, callback: sendUnaryData<WriteEventResponse>) => finish("reviewer", call.request.runtimeWriteId, call, callback, {
            committed: {
                eventId: "evt_review_failure"
            }
        }),
    });
    const address = await bind(server);
    const assembled = await assembly(address, {
        TETRAL_BRIDGE_COMMIT_RUNTIME_TERMINATION_TIMEOUT_MS: "7000", TETRAL_BRIDGE_WRITE_EVENT_TIMEOUT_MS: "7000"
    });
    try {
        const started = performance.now();
        const [closed, reviewer] = await Promise.all([
            commitRuntimeTerminationWithRetry(assembled.options.threadLoop, termination),
            assembled.dependencies.coreHosts.subAgentRunHost.commitApprovalReviewFailure({
                ...approvalReviewAcceptedInput(), ...scope
            }, {
                type: "approval_review.failure", review_id: "review_original", parent_thread_id: "parent_original", target_model_tool_call_id: "call_original", target_tool_name: "Write", failure_kind: "runtime_failure", message: "review failed"
            }),
        ]);
        expect(closed).toMatchObject({
            ok: true, failureEventId: "evt_failure"
        });
        expect(reviewer).toMatchObject({
            ok: true, eventId: "evt_review_failure"
        });
        expect(performance.now() - started).toBeGreaterThan(3900);
        expect(performance.now() - started).toBeLessThan(7000);
        expect(active).toBe(0);
        expect(calls.map(call => [call.method, call.id]).sort()).toEqual([["reviewer", "rwrite_review_original_failure"], ["termination", "termination_original"]]);
        for (const call of calls)
            expect(call.budget).toBeGreaterThan(6500);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
}, 15000);
test("earlier phase cancels actual termination RPC and joins before retry completion", async () => {
    const server = new Server();
    let active = 0, cancelled = 0;
    const admissions: number[] = [];
    server.addService(AgentRuntimeBridgeServiceService, {
        commitRuntimeTermination: (call: ServerUnaryCall<CommitRuntimeTerminationRequest, CommitRuntimeTerminationResponse>) => {
            active++;
            admissions.push(Number(call.getDeadline()) - Date.now());
            call.once("cancelled", () => {
                active--;
                cancelled++;
            });
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_COMMIT_RUNTIME_TERMINATION_TIMEOUT_MS: "7000"
    });
    try {
        const writer = assembled.options.threadLoop.sessionEventWriter;
        const started = performance.now();
        (writer as unknown as {
            beginDrain(deadline: number): void;
        }).beginDrain(Date.now() + 100);
        const result = await commitRuntimeTerminationWithRetry(assembled.options.threadLoop, termination);
        expect(result).toMatchObject({
            ok: false, error: {
                retryable: true
            }
        });
        await waitFor(() => cancelled === 1 && active === 0);
        expect(admissions).toHaveLength(1);
        expect(admissions[0]).toBeLessThanOrEqual(100);
        expect(performance.now() - started).toBeLessThan(1000);
        expect(active).toBe(0);
        console.info(JSON.stringify({
            event: "termination_phase_probe", phase_ms: 100, completion_ms: performance.now() - started, admissions, active, cancelled
        }));
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
});
test("accepted-input preparation precedes the actual configured RPC deadline", async () => {
    const server = new Server();
    let active = 0, calls = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        commitInputs: (call: ServerUnaryCall<CommitInputsRequest, CommitInputsResponse>, callback: sendUnaryData<CommitInputsResponse>) => {
            calls++;
            active++;
            expect(call.request.runtimeInputId).toBe("rin_prepared");
            expect(Number(call.getDeadline()) - Date.now()).toBeGreaterThan(100);
            setTimeout(() => {
                active--;
                callback(null, {
                    committed: {
                        context: {
                            assignedContextSequences: [7], pendingAttachmentJson: []
                        }
                    }
                });
            }, 120);
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_COMMIT_INPUTS_TIMEOUT_MS: "150"
    }, 75);
    try {
        const result = await commitAcceptedInputWithRetry(assembled.options.contextLoader, acceptedInput("rin_prepared"), assembled.options.threadLoop, new AbortController().signal);
        expect(result).toMatchObject({
            ok: true, result: {
                type: "committed", assignedContextSequences: [7]
            }
        });
        expect(calls).toBe(1);
        expect(active).toBe(0);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
});
test("actual CommitInputs deadline exhaustion preserves three joined attempts and stable identity", async () => {
    const server = new Server();
    const admitted: string[] = [], joined: string[] = [];
    server.addService(AgentRuntimeBridgeServiceService, {
        commitInputs: (call: ServerUnaryCall<CommitInputsRequest, CommitInputsResponse>) => {
            admitted.push(call.request.runtimeInputId);
            call.once("cancelled", () => joined.push(call.request.runtimeInputId));
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_COMMIT_INPUTS_TIMEOUT_MS: "50"
    });
    try {
        const backoffs: number[] = [];
        const runtime = assembled.options.threadLoop.runtime;
        const result = await commitAcceptedInputWithRetry(assembled.options.contextLoader, acceptedInput("rin_hung"), {
            ...assembled.options.threadLoop, runtime: {
                ...runtime, sleep: async (duration, signal) => {
                    backoffs.push(duration);
                    return await runtime.sleep(duration, signal);
                }
            }
        }, new AbortController().signal);
        expect(backoffs).toEqual([100, 300]);
        expect(result).toMatchObject({
            ok: false, exhausted: true
        });
        await waitFor(() => joined.length === 3);
        expect(admitted).toEqual(["rin_hung", "rin_hung", "rin_hung"]);
        expect(joined).toEqual(admitted);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
});
test("nonretryable termination status does not authorize another raw call", async () => {
    const server = new Server();
    let calls = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        commitRuntimeTermination: (_call: unknown, callback: sendUnaryData<CommitRuntimeTerminationResponse>) => {
            calls++;
            callback({
                code: status.INVALID_ARGUMENT, message: "invalid operation"
            });
        }
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_COMMIT_RUNTIME_TERMINATION_TIMEOUT_MS: "7000"
    });
    try {
        expect(await commitRuntimeTerminationWithRetry(assembled.options.threadLoop, termination)).toMatchObject({
            ok: false, error: {
                retryable: false
            }
        });
        expect(calls).toBe(1);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
});
async function assembly(address: string, extra: Record<string, string>, preparationMs = 0) {
    const parsed = loadRuntimePodConfig({
        TETRAL_RUNTIME_POD_NAMESPACE: "engine", TETRAL_RUNTIME_POD_NAME: "runtime", TETRAL_RUNTIME_POD_UID: "pod_1", TETRAL_RUNTIME_POD_IP: "127.0.0.1", TETRAL_RUNTIME_POD_GRPC_PORT: "19090", TETRAL_RUNTIME_POD_HTTP_ADDR: "127.0.0.1:0", TETRAL_DEPLOYMENT_ENVIRONMENT: "test", TETRAL_SERVICE_VERSION: "test", TETRAL_RUNTIME_POD_GRPC_AUDIENCE: "tetral-internal-grpc", TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS: "engine/job-runner", KUBERNETES_API_SERVER_URL: "https://kubernetes.default.svc", KUBERNETES_API_CA_CERT_PATH: "/unused/ca", KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH: "/unused/token", TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH: "/unused/token", TETRAL_BRIDGE_API_GRPC_ADDR: address, TETRAL_GATEWAY_GRPC_ADDR: address, TETRAL_MCP_CONNECTOR_GRPC_ADDR: address, TETRAL_WEB_CONNECTOR_GRPC_ADDR: address, TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL: "anthropic/claude-opus-4-8", TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES: "32768", ...extra
    });
    if (!parsed.ok)
        throw new Error("invalid fixture config");
    let options!: RuntimeCoreHostsOptions;
    const dependencies = await buildRuntimePodCommandDependencies({
        config: parsed.config, logger: {
            info: () => undefined, error: () => undefined
        }, builderOptions: {
            outboundMetadataFactory: async () => {
                if (preparationMs)
                    await Bun.sleep(preparationMs);
                return new Metadata();
            }, coreHostsFactory: async (value) => {
                options = value;
                return await buildRuntimeCoreHosts(value);
            }
        }
    });
    return {
        dependencies, options
    };
}
async function bind(server: Server) {
    const port = await new Promise<number>((resolve, reject) => server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => error ? reject(error) : resolve(port)));
    return `127.0.0.1:${port}`;
}
async function stop(server: Server) {
    await new Promise<void>(resolve => server.tryShutdown(() => resolve()));
}
async function waitFor(predicate: () => boolean) {
    const end = Date.now() + 1000;
    while (!predicate() && Date.now() < end)
        await Bun.sleep(5);
    expect(predicate()).toBe(true);
}
