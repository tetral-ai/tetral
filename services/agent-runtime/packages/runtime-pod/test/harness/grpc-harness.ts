import { Metadata } from "@grpc/grpc-js";
import { createRuntimeGrpcServer } from "../../src/grpc-server.js";
import { createJsonLogger } from "../../src/logger.js";
import { RuntimeControlService } from "../../src/runtime-service.js";
import type { RuntimeAuthenticator, RuntimeCleanupController, RuntimeCommandScope, RuntimeSessionRunHost } from "../../src/runtime-service.js";

class HarnessAuthenticator implements RuntimeAuthenticator {
  async authenticate(input: { readonly metadata: Metadata }) {
    const values = input.metadata.get("authorization");
    if (values.length !== 1 || String(values[0]).toLowerCase() !== "bearer caller-token") {
      return { ok: false as const, code: "Unauthenticated" as const, message: "unauthenticated" };
    }
    return { ok: true as const, serviceAccount: { namespace: "engine", name: "bridge" } };
  }
}

class HarnessRunHost implements RuntimeSessionRunHost {
  async handleAcceptInput(command: Parameters<RuntimeSessionRunHost["handleAcceptInput"]>[0]) {
    return { ok: true as const, sessionId: command.sessionId, created: true, started: true, pendingWake: false };
  }

  async handleAgentMail(command: Parameters<RuntimeSessionRunHost["handleAgentMail"]>[0]) {
    return { ok: true as const, sessionId: command.sessionId, applied: true };
  }

  async handleInterruptControl(sessionId: string) {
    return { ok: true as const, sessionId, created: false, interrupted: true, idleInterrupt: false };
  }

  async handleToolConfirmation(sessionId: string) {
    return { ok: true as const, sessionId, created: false, applied: true };
  }

  async handleTaskNotification(sessionId: string) {
    return { ok: true as const, sessionId, created: false, applied: true };
  }

  async handleRuntimeConfigPatch(sessionId: string) {
    return { ok: true as const, sessionId, created: false, applied: true };
  }
}

class HarnessCleanupController implements RuntimeCleanupController {
  async startCleanup(scope: RuntimeCommandScope) {
    return {
      ok: true as const,
      sessionId: scope.sessionId,
      completion: Promise.resolve({ ok: true as const, sessionId: scope.sessionId, cleaned: true }),
    };
  }
}

const service = new RuntimeControlService({
  ownPod: {
    namespace: "engine",
    name: "runtime-pod-a",
    uid: "uid-a",
    ip: process.env.TETRAL_TEST_RUNTIME_POD_IP ?? "10.0.0.1",
  },
  allowedBridge: { namespace: "engine", name: "bridge" },
  authenticator: new HarnessAuthenticator(),
  runHost: new HarnessRunHost(),
  cleanupController: new HarnessCleanupController(),
  logger: createJsonLogger({ write: () => undefined }),
  ready: () => true,
});
const grpcServer = createRuntimeGrpcServer(service);
const port = await grpcServer.bind("127.0.0.1:0");
process.stdout.write(`${JSON.stringify({ address: `127.0.0.1:${port}` })}\n`);

process.on("SIGTERM", () => {
  void grpcServer.shutdown().then(() => process.exit(0));
});

await new Promise(() => undefined);
