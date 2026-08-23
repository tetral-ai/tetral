import { access, readFile, writeFile } from "node:fs/promises";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import { SessionRunHostCleanupController } from "../../src/cleanup-controller.js";
import {
  RuntimeControlService,
  type RuntimeSessionRunHost,
} from "../../src/runtime-service.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("cleanup composition input is required");
const input = JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly targetPodUid: string;
  readonly mode: "delayed_busy" | "success" | "failure";
  readonly readyPath: string;
  readonly effectPath: string;
  readonly closePath: string;
};
let hostInvocations = 0;
let hostEffects = 0;
const completedOperations = new Set<string>();

const cleanupController = new SessionRunHostCleanupController({
  handleCleanupSession: async (scope) => {
    hostInvocations += 1;
    await new Promise<void>((resolve) => setTimeout(resolve, input.mode === "success" ? 200 : 5));
    if (input.mode === "failure") throw new Error("fixture cleanup failure");
    if (input.mode === "delayed_busy") {
      return { ok: false as const, sessionId: scope.sessionId, reason: "session_busy" as const };
    }
    if (!completedOperations.has(scope.cleanupOperationId)) {
      completedOperations.add(scope.cleanupOperationId);
      hostEffects += 1;
    }
    await writeFile(
      input.effectPath,
      JSON.stringify({ cleanupOperationId: scope.cleanupOperationId, hostInvocations, hostEffects }),
      { mode: 0o600 },
    );
    return { ok: true as const, sessionId: scope.sessionId, cleaned: true };
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
}
