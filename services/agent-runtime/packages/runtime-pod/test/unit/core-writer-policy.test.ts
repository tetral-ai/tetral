import { expect, test } from "bun:test";
import { Effect, Fiber } from "effect";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { RecordingContextLoader, runtimeThreadLoopLayer } from "@tetral/agent-runtime-core/test/unit/thread-loop/thread-loop-test-support.js";
import { Metadata, Server, ServerCredentials, status } from "@grpc/grpc-js";
import type { ServerUnaryCall, sendUnaryData } from "@grpc/grpc-js";
import { AgentRuntimeBridgeServiceService } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import type { CommitRuntimeTerminationRequest, CommitRuntimeTerminationResponse, WriteEventRequest, WriteEventResponse, CommitInputsRequest, CommitInputsResponse } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import { normalizeRuntimeFailure } from "@tetral/agent-runtime-core/src/contracts/runtime.js";
import { commitRuntimeTerminationWithRetry, closeFailedRunDurably, createFailedRunCloseoutMemo, finishIdleWithRetry } from "@tetral/agent-runtime-core/src/thread-loop/closeout.js";
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
            beginDrain(phase: { currentStepDeadline: number; settlementDeadline: number; settlementAttemptTimeoutMs: number }): void;
        }).beginDrain({ currentStepDeadline: Date.now(), settlementDeadline: Date.now() + 100, settlementAttemptTimeoutMs: 5000 });
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
test("actual Core rejoins three configured FinishIdle wait expiries before late readiness", async () => {
    const server = new Server();
    let active = 0, maxActive = 0, cancelled = 0;
    const requests: unknown[] = [], backoffs: number[] = [];
    server.addService(AgentRuntimeBridgeServiceService, {
        finishIdle: (call: ServerUnaryCall<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleRequest, import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            requests.push(structuredClone(call.request));
            expect(Number(call.getDeadline()) - Date.now()).toBeLessThanOrEqual(80);
            active++;
            maxActive = Math.max(maxActive, active);
            if (requests.length <= 3)
                call.once("cancelled", () => {
                    active--;
                    cancelled++;
                });
            else {
                active--;
                callback(null, {
                    committed: {
                        idleEventId: "idle_original"
                    }
                });
            }
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_FINISH_IDLE_TIMEOUT_MS: "80"
    });
    const session = new ThreadRuntime(scope.sessionId);
    session.state.installThreadTurn({
        executionRunId: "turn_original", pendingInputContextSequences: []
    }, {
        routes: []
    });
    try {
        const runtime = assembled.options.threadLoop.runtime;
        const result = await Effect.runPromise(Effect.gen(function* () {
            return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
        }).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
            type: "empty"
        }), {
            runtimeModel: () => undefined,
            writer: assembled.options.threadLoop.sessionEventWriter,
            runtime: {
                ...runtime, sleep: async (ms, signal) => {
                    backoffs.push(ms);
                    return runtime.sleep(ms, signal);
                }
            },
        }))));
        expect(result).toEqual({
            type: "completed", modelMessageCount: 0
        });
        expect(requests).toHaveLength(4);
        expect(requests.every(request => JSON.stringify(request) === JSON.stringify(requests[0]))).toBe(true);
        expect(backoffs).toEqual([100, 300, 300]);
        expect(cancelled).toBe(3);
        expect(active).toBe(0);
        expect(maxActive).toBe(1);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
}, 5000);

test("actual command quiesce preserves current budgets then caps final and retained Bridge calls", async () => {
    const server = new Server();
    const admitted: Array<{
        id: string;
        budget: number;
        cancelledAt?: number;
    }> = [];
    let active = 0, cancelled = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        commitRuntimeTermination: (call: ServerUnaryCall<CommitRuntimeTerminationRequest, CommitRuntimeTerminationResponse>, callback: sendUnaryData<CommitRuntimeTerminationResponse>) => {
            admitted.push({
                id: call.request.runtimeWriteId, budget: Number(call.getDeadline()) - Date.now()
            });
            active++;
            if (call.request.runtimeWriteId === "healthy_current")
                setTimeout(() => {
                    active--;
                    callback(null, {
                        committed: {
                            failureEventId: "failure_current", closeoutEventId: "closeout_current"
                        }
                    });
                }, 4000);
            else
                call.once("cancelled", () => {
                    active--;
                    cancelled++;
                    admitted.find(record => record.id === call.request.runtimeWriteId)!.cancelledAt = Date.now();
                });
        },
        finishIdle: (call: ServerUnaryCall<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleRequest, import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            admitted.push({
                id: "final_idle", budget: Number(call.getDeadline()) - Date.now()
            });
            active++;
            call.once("cancelled", () => {
                active--;
                cancelled++;
            });
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "4100",
        TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS: "300",
        TETRAL_RUNTIME_SETTLEMENT_ATTEMPT_TIMEOUT_MS: "80",
        TETRAL_BRIDGE_COMMIT_RUNTIME_TERMINATION_TIMEOUT_MS: "7000",
    }, 0, async (phase, options) => {
        const healthy = commitRuntimeTerminationWithRetry(options.threadLoop, {
            ...termination, writeId: "healthy_current"
        });
        let retainedCompletedAt: number | undefined;
        const retained = options.threadLoop.sessionEventWriter.commitRuntimeTermination!({
            ...termination, writeId: "retained_current"
        }).then(result => {
            retainedCompletedAt = Date.now();
            return result;
        });
        expect(await healthy).toMatchObject({
            ok: true
        });
        await Bun.sleep(Math.max(0, phase.currentStepDeadline - Date.now()) + 5);
        expect(await retained).toMatchObject({
            ok: false, error: {
                code: "timeout"
            }
        });
        expect(retainedCompletedAt!).toBeLessThanOrEqual(phase.currentStepDeadline + 80 + 100);
        await waitFor(() => admitted.find(record => record.id === "retained_current")?.cancelledAt !== undefined);
        expect(admitted.find(record => record.id === "retained_current")!.cancelledAt!).toBeLessThanOrEqual(phase.currentStepDeadline + 80 + 100);
        const final = await finishIdleWithRetry(options.threadLoop, {
            ...scope, durableTurnId: "turn_final", stopReason: {
                type: "end_turn"
            }
        });
        expect(final).toMatchObject({
            ok: false, error: {
                retryable: false
            }
        });
        expect(await retained).toMatchObject({
            ok: false, error: {
                code: "timeout"
            }
        });
    });
    try {
        await assembled.dependencies.app.shutdown();
        await waitFor(() => active === 0);
        expect(admitted.filter(call => call.id === "healthy_current")).toHaveLength(1);
        expect(admitted.filter(call => call.id === "retained_current")).toHaveLength(1);
        for (const call of admitted.filter(call => call.id.endsWith("current")))
            expect(call.budget).toBeGreaterThan(4000);
        const final = admitted.filter(call => call.id === "final_idle");
        expect(final.length).toBeGreaterThanOrEqual(1);
        expect(final.every(call => call.budget > 0 && call.budget <= 80)).toBe(true);
        expect(cancelled).toBe(1 + final.length);
        const before = admitted.length;
        expect(await assembled.options.threadLoop.sessionEventWriter.finishIdle!({
            ...scope, durableTurnId: "after_terminal", stopReason: {
                type: "end_turn"
            }
        })).toMatchObject({
            ok: false, error: {
                retryable: false
            }
        });
        expect(admitted).toHaveLength(before);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
}, 15000);

test("FinishIdle owning caller cancellation and earlier deadline join without rejoin", async () => {
    for (const mode of ["cancel", "deadline"] as const) {
        const server = new Server();
        let calls = 0, active = 0;
        let entered!: () => void;
        const admission = new Promise<void>(resolve => {
            entered = resolve;
        });
        server.addService(AgentRuntimeBridgeServiceService, {
            finishIdle: (call: {
                once(name: string, handler: () => void): void;
            }) => {
                calls++;
                active++;
                call.once("cancelled", () => {
                    active--;
                });
                entered();
            },
        });
        const assembled = await assembly(await bind(server), {
            TETRAL_BRIDGE_FINISH_IDLE_TIMEOUT_MS: "7000"
        });
        const controller = new AbortController();
        try {
            const result = finishIdleWithRetry(assembled.options.threadLoop, {
                ...scope, durableTurnId: "original_caller", stopReason: {
                    type: "end_turn"
                },
            }, mode === "cancel" ? {
                signal: controller.signal
            } : {
                deadlineEpochMs: Date.now() + 100
            });
            await admission;
            if (mode === "cancel")
                controller.abort();
            expect(await result).toMatchObject({
                ok: false, error: {
                    retryable: false
                }
            });
            await waitFor(() => active === 0);
            expect(calls).toBe(1);
        }
        finally {
            await assembled.dependencies.app.shutdown();
            await assembled.dependencies.coreHosts.close();
            await stop(server);
        }
    }
});

test("FinishIdle expected waits do not spend ordinary failures or change their backoffs", async () => {
    const assembled = await assembly("127.0.0.1:1", {});
    const backoffs: number[] = [];
    let calls = 0;
    try {
        const runtime = assembled.options.threadLoop.runtime;
        const result = await finishIdleWithRetry({
            ...assembled.options.threadLoop,
            runtime: {
                ...runtime, sleep: async (ms) => {
                    backoffs.push(ms);
                    return true;
                }
            },
            sessionEventWriter: {
                ...assembled.options.threadLoop.sessionEventWriter,
                finishIdle: async () => ({
                    ok: false,
                    error: {
                        type: "session-event-writer", code: ++calls <= 2 ? "timeout" : "unavailable", message: "Operation failed.", retryable: true, fatal: false
                    },
                }),
            },
        }, {
            ...scope, durableTurnId: "mixed_original", stopReason: {
                type: "end_turn"
            }
        });
        expect(result).toMatchObject({
            ok: false, error: {
                code: "unavailable"
            }
        });
        expect(calls).toBe(5);
        expect(backoffs).toEqual([100, 300, 100, 300]);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
    }
});

test("failed-run memo observation retains the separately owned actual FinishIdle", async () => {
    const server = new Server();
    let calls = 0, active = 0, cancelled = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        writeEvent: (_call: unknown, callback: sendUnaryData<WriteEventResponse>) => callback(null, {
            committed: {
                eventId: "failure_memo_idle"
            }
        }),
        finishIdle: (call: {
            once(name: string, handler: () => void): void;
        }, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            calls++;
            active++;
            call.once("cancelled", () => {
                if (active > 0)
                    cancelled++;
            });
            setTimeout(() => {
                active--;
                callback(null, {
                    committed: {
                        idleEventId: "idle_memo_original"
                    }
                });
            }, 4000);
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_BRIDGE_FINISH_IDLE_TIMEOUT_MS: "7000"
    });
    const session = new ThreadRuntime(scope.sessionId);
    session.state.installThreadTurn({
        executionRunId: "turn_memo_idle", pendingInputContextSequences: []
    }, {
        routes: []
    });
    const memo = createFailedRunCloseoutMemo("failure_memo_idle", "turn_memo_idle");
    const observe = () => closeFailedRunDurably(assembled.options.threadLoop, session, memo, testRunCustody(), (writeId, failure) => assembled.options.threadLoop.sessionEventWriter.append({
        ...scope, writeId, event: {
            type: "session.error", error: failure
        }
    }));
    try {
        expect(await observe()).toMatchObject({
            type: "retry", error: {
                code: "timeout"
            }
        });
        expect(calls).toBe(1);
        expect(active).toBe(1);
        expect(cancelled).toBe(0);
        expect(await observe()).toMatchObject({
            type: "landed"
        });
        expect(calls).toBe(1);
        expect(active).toBe(0);
        expect(cancelled).toBe(0);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
}, 15000);

test("Core interruption cannot release an owned FinishIdle before its raw callback", async () => {
    const server = new Server();
    let entered!: () => void, complete!: () => void;
    const admission = new Promise<void>(resolve => {
        entered = resolve;
    });
    let active = 0, calls = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        finishIdle: (_call: unknown, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            calls++;
            active++;
            complete = () => {
                active--;
                callback(null, {
                    committed: {
                        idleEventId: "idle_uninterruptible_original"
                    }
                });
            };
            entered();
        },
    });
    const assembled = await assembly(await bind(server), {});
    const session = new ThreadRuntime(scope.sessionId);
    session.state.installThreadTurn({
        executionRunId: "turn_owned", pendingInputContextSequences: []
    }, {
        routes: []
    });
    const fiber = Effect.runFork(Effect.gen(function* () {
        return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
        type: "empty"
    }), {
        runtimeModel: () => undefined,
        writer: assembled.options.threadLoop.sessionEventWriter,
        runtime: assembled.options.threadLoop.runtime,
    }))));
    try {
        await admission;
        let joined = false;
        const interruption = Effect.runPromise(Fiber.interrupt(fiber)).then(() => {
            joined = true;
        });
        await Bun.sleep(30);
        expect(joined).toBe(false);
        expect(active).toBe(1);
        expect(calls).toBe(1);
        complete();
        await interruption;
        expect(joined).toBe(true);
        expect(active).toBe(0);
        expect(calls).toBe(1);
    }
    finally {
        if (active > 0)
            complete();
        await Effect.runPromise(Fiber.interrupt(fiber));
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
});

test("parsed final attempt default clips to a shorter shared phase without extending it", async () => {
    for (const invalid of ["0", "-1", "1.5", "Infinity", "2147483648"]) {
        await expect(assembly("127.0.0.1:1", {
            TETRAL_RUNTIME_SETTLEMENT_ATTEMPT_TIMEOUT_MS: invalid
        })).rejects.toThrow("invalid fixture config");
    }
    const server = new Server();
    let budget = 0;
    server.addService(AgentRuntimeBridgeServiceService, {
        finishIdle: (call: {
            getDeadline(): Date | number;
        }, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            budget = Number(call.getDeadline()) - Date.now();
            callback(null, {
                committed: {
                    idleEventId: "short_phase_idle"
                }
            });
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "20", TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS: "2000"
    }, 0, async (phase, options) => {
        // Invoke after the actual command's current-step phase transition.
        await Bun.sleep(Math.max(0, phase.currentStepDeadline - Date.now()) + 5);
        expect(await finishIdleWithRetry(options.threadLoop, {
            ...scope, durableTurnId: "short_phase", stopReason: {
                type: "end_turn"
            }
        })).toMatchObject({
            ok: true
        });
    });
    try {
        expect(assembled.config.lifecycle.settlementAttemptTimeoutMs).toBe(5000);
        expect(assembled.config.lifecycle.settlementTimeoutMs).toBe(2000);
        await assembled.dependencies.app.shutdown();
        expect(budget).toBeGreaterThan(1500);
        expect(budget).toBeLessThanOrEqual(2000);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
});

test("default final cap expires a long FinishIdle attempt and rejoins the same ready operation", async () => {
    const server = new Server();
    const requests: unknown[] = [], budgets: number[] = [];
    let active = 0, maximum = 0, cancelled = 0;
    let readyAt = Infinity;
    server.addService(AgentRuntimeBridgeServiceService, {
        finishIdle: (call: ServerUnaryCall<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleRequest, import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
            requests.push(structuredClone(call.request));
            budgets.push(Number(call.getDeadline()) - Date.now());
            active++;
            maximum = Math.max(maximum, active);
            let pending = true;
            const timer = setTimeout(() => {
                pending = false;
                active--;
                callback(null, {
                    committed: {
                        idleEventId: "idle_default_cap_original"
                    }
                });
            }, Math.max(0, readyAt - Date.now()));
            call.once("cancelled", () => {
                if (!pending)
                    return;
                pending = false;
                clearTimeout(timer);
                active--;
                cancelled++;
            });
        },
    });
    const assembled = await assembly(await bind(server), {
        TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "20"
    }, 0, async (phase, options) => {
        await Bun.sleep(Math.max(0, phase.currentStepDeadline - Date.now()) + 5);
        readyAt = Date.now() + 6000;
        expect(await finishIdleWithRetry(options.threadLoop, {
            ...scope, durableTurnId: "turn_default_cap_original", stopReason: {
                type: "end_turn"
            }
        })).toMatchObject({
            ok: true, idleEventId: "idle_default_cap_original"
        });
    });
    try {
        expect(assembled.config.lifecycle.settlementAttemptTimeoutMs).toBe(5000);
        expect(assembled.config.bridgeMethodPolicies.finishIdle).toEqual({
            kind: "fixed", timeoutMs: 35000
        });
        await assembled.dependencies.app.shutdown();
        expect(requests).toHaveLength(2);
        expect(requests[1]).toEqual(requests[0]);
        expect(budgets.every(budget => budget > 4500 && budget <= 5000)).toBe(true);
        expect(cancelled).toBe(1);
        expect(active).toBe(0);
        expect(maximum).toBe(1);
    }
    finally {
        await assembled.dependencies.app.shutdown();
        await assembled.dependencies.coreHosts.close();
        await stop(server);
    }
}, 15000);

test("FinishIdle deterministic rejection and stale result do not rejoin", async () => {
    for (const mode of ["stale", "authorization"] as const) {
        const server = new Server();
        let calls = 0;
        server.addService(AgentRuntimeBridgeServiceService, {
            finishIdle: (_call: unknown, callback: sendUnaryData<import("@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js").FinishIdleResponse>) => {
                calls++;
                if (mode === "stale")
                    callback(null, {
                        stale: {}
                    });
                else
                    callback({
                        code: status.PERMISSION_DENIED, details: "Denied."
                    }, null);
            },
        });
        const assembled = await assembly(await bind(server), {});
        try {
            const result = await finishIdleWithRetry(assembled.options.threadLoop, {
                ...scope, durableTurnId: "nonretryable_original", stopReason: {
                    type: "end_turn"
                }
            });
            expect(result).toMatchObject(mode === "stale" ? {
                ok: true, type: "stale"
            } : {
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
    }
});

async function assembly(address: string, extra: Record<string, string>, preparationMs = 0, duringQuiesce?: (phase: Parameters<Awaited<ReturnType<typeof buildRuntimeCoreHosts>>["quiesce"]>[0], options: RuntimeCoreHostsOptions) => Promise<void>) {
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
            ...(duringQuiesce === undefined ? {} : {
                runtimeProcessFactory: (runtimeProcessId: string) => ({
                    runtimeProcessId, register: async () => {
                    }, report: async () => {
                    }, release: async () => {
                        throw new Error("unexpected release");
                    }, close: async () => {
                    }
                }),
            }),
            outboundMetadataFactory: async () => {
                if (preparationMs)
                    await Bun.sleep(preparationMs);
                return new Metadata();
            }, coreHostsFactory: async (value) => {
                options = value;
                const hosts = await buildRuntimeCoreHosts(value);
                return duringQuiesce === undefined ? hosts : {
                    ...hosts,
                    quiesce: async (phase) => {
                        await duringQuiesce(phase, value);
                        await hosts.quiesce(phase);
                    },
                };
            }
        }
    });
    return {
        dependencies, options, config: parsed.config
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
