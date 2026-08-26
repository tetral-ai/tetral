import { access, readFile, writeFile } from "node:fs/promises";
import * as SessionManager from "@tetral/agent-runtime-core/src/session/session-manager.js";
import * as ThreadLoop from "@tetral/agent-runtime-core/src/thread-loop/thread-loop.js";
import { Context, Effect, Exit, Layer, Scope } from "effect";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import { SessionRunHostCleanupController } from "../../src/cleanup-controller.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import type { RuntimeSessionRunHost } from "../../src/runtime-service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("cleanup composition input is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly targetPodUid: string;
  readonly sessionId: string;
  readonly mode: "delayed_busy" | "success" | "failure";
  readonly readyPath: string;
  readonly effectPath: string;
  readonly closePath: string;
};
let hostInvocations = 0;
let hostEffects = 0;

const threadLoopLayer = Layer.succeed(
  ThreadLoop.Service,
  ThreadLoop.Service.of({
    run: () => Effect.succeed({ type: "completed", modelMessageCount: 0 }),
    closeFailedRun: () => Effect.succeed({ type: "landed", disposition: "continuation" }),
		closeRecoveredOpenRequestForInterrupt: () => Effect.succeed({ type: "interrupted" }),
		settleIdleInterrupt: () => Effect.succeed({ type: "applied" }),
		settleToolConfirmation: () => Effect.succeed({ type: "duplicate" }),
		seedRuntimeModel: () => undefined,
    installLoadedPendingToolUses: () => Effect.succeed({ ok: true }),
    installLoadedSandboxExecutions: () => Effect.succeed({ ok: true }),
  }),
);
const managerLayer = SessionManager.layer({
  maxLocalSessions: 1,
  now: () => "2026-01-01T01:00:00.000Z",
}).pipe(Layer.provide(threadLoopLayer));
const managerScope = await Effect.runPromise(Scope.make());
const managerContext = await Effect.runPromise(
  Layer.buildWithScope(managerLayer, managerScope),
);
const manager = Context.get(managerContext, SessionManager.Service);
if (input.mode === "success") {
  const preloaded = await Effect.runPromise(manager.preloadThread({
    workspaceId: "default",
    sessionId: input.sessionId,
    sessionThreadId: `thrd_${input.sessionId}`,
    bindingId: `bind_${input.sessionId}`,
    bindingGeneration: 7,
    targetPodUid: input.targetPodUid,
    runtimeBindingToken: `rtbt_${input.sessionId}`,
    contextEntries: [],
    thread: { role: "main", visibility: "public", status: "idle" },
  }));
  if (!preloaded.ok || !preloaded.applied) {
    throw new Error("cleanup composition could not install real SessionManager state");
  }
}

const cleanupController = new SessionRunHostCleanupController({
  handleCleanupSession: async (scope) => {
    hostInvocations += 1;
    await new Promise<void>((resolve) => setTimeout(resolve, input.mode === "success" ? 200 : 5));
    if (input.mode === "failure") throw new Error("fixture cleanup failure");
    if (input.mode === "delayed_busy") {
      return { ok: false as const, sessionId: scope.sessionId, reason: "session_busy" as const };
    }
    const cleanup = await Effect.runPromise(manager.cleanupSession(scope.sessionId, scope));
    if (!cleanup.ok) return cleanup;
    if (cleanup.cleaned) hostEffects += 1;
    await writeFile(
      input.effectPath,
      JSON.stringify({ cleanupOperationId: scope.cleanupOperationId, hostInvocations, hostEffects, cleaned: cleanup.cleaned }),
      { mode: 0o600 },
    );
    return cleanup;
  },
});
const service = new RuntimeControlService({
  ownPod: {
    namespace: "tetral-agent-runtime",
    name: "runtime-cleanup-composition",
    uid: input.targetPodUid,
    ip: "127.0.0.1",
  },
  allowedBridge: { namespace: "tetral-system", name: "bridge" },
  authenticator: {
    authenticate: async () => ({
      ok: true as const,
      serviceAccount: { namespace: "tetral-system", name: "bridge" },
    }),
  },
  runHost: {} as RuntimeSessionRunHost,
  cleanupController,
  logger: { info: () => undefined, warn: () => undefined, error: () => undefined } as never,
  ready: () => true,
});
const server = createRuntimeGrpcServer(service);
const port = await server.bind("127.0.0.1:0");
await writeFile(input.readyPath, JSON.stringify({ port }), { mode: 0o600 });

try {
  for (;;) {
    try {
      await access(input.closePath);
      break;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
} finally {
  await server.shutdown();
  await Effect.runPromise(Scope.close(managerScope, Exit.void));
}
