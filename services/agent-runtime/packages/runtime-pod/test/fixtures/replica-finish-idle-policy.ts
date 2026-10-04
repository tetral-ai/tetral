/** Actual Core and generated Bridge writer against the test's independently owned capture worker. */
import { readFile, writeFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { Effect } from "effect";
import { FinishIdleRequest } from "@tetral/agent-runtime-protocol/src/gen-bridge/tetral/bridge/v1/bridge.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { ThreadRuntime } from "@tetral/agent-runtime-core/src/thread-loop/thread-runtime.js";
import { RecordingContextLoader, runtimeThreadLoopLayer, testRunCustody } from "@tetral/agent-runtime-core/test/unit/thread-loop/thread-loop-test-support.js";
import { BridgeAPIEventWriter } from "../../src/bridge-client.js";
import { parseBridgeMethodPolicies } from "../../src/bridge-policy.js";
const input = JSON.parse(await readFile(process.argv[2]!, "utf8")) as {
    address: string;
    token: string;
    directory: string;
    request: unknown;
    env: Record<string, string>;
};
const request = FinishIdleRequest.fromJSON(input.request);
const policies = parseBridgeMethodPolicies(input.env);
if (policies === undefined || request.scope?.binding === undefined)
    throw new Error("Invalid fixture policy/scope.");
const writer = new BridgeAPIEventWriter({
    address: input.address,
    tokenPath: "/unused",
    methodPolicies: policies,
    metadataFactory: async () => {
        const metadata = new Metadata();
        metadata.set("authorization", `Bearer ${input.token}`);
        return metadata;
    },
});
const scope = request.scope;
const binding = scope.binding!;
const session = new ThreadRuntime({
    threadRole: "main",
    threadVisibility: "public",
    workspaceId: scope.workspaceId,
    sessionId: scope.sessionId,
    sessionThreadId: scope.sessionThreadId,
    bindingId: binding.bindingId,
    bindingGeneration: binding.bindingGeneration,
    targetPodUid: binding.targetPodUid,
    runtimeProcessId: binding.runtimeProcessId,
    runtimeBindingToken: "unused",
});
session.state.installThreadTurn({
    executionRunId: request.durableTurnId, pendingInputContextSequences: []
}, {
    routes: []
});
const backoffs: number[] = [];
try {
    const result = await Effect.runPromise(Effect.gen(function* () {
        return yield* (yield* ThreadLoop.Service).run(session, testRunCustody());
    }).pipe(Effect.provide(runtimeThreadLoopLayer(new RecordingContextLoader([], {
        type: "empty"
    }), {
        runtimeModel: () => undefined,
        writer,
        runtime: {
            now: () => new Date().toISOString(),
            monotonicMs: () => performance.now(),
            createId: prefix => `${prefix}_${crypto.randomUUID()}`,
            sleep: (ms, signal) => new Promise<boolean>(resolve => {
                backoffs.push(ms);
                if (signal.aborted) {
                    resolve(false);
                    return;
                }
                const cancel = (): void => {
                    clearTimeout(timer);
                    resolve(false);
                };
                const timer = setTimeout(() => {
                    signal.removeEventListener("abort", cancel);
                    resolve(true);
                }, ms);
                signal.addEventListener("abort", cancel, {
                    once: true
                });
            }),
        },
    }))));
    await writeFile(`${input.directory}/results.json`, JSON.stringify({
        result, backoffs
    }));
}
finally {
    await writer.close();
}
