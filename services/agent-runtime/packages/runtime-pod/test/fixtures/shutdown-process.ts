// Actual command/app ownership with a deliberately unjoinable producer and Core barrier.
import {
  runProcessEntry,
  registerProcessSignalHandlers,
} from "@tetral/ts-observability";
import { createRuntimePodApp } from "../../src/app.js";
import { runRuntimePodCommand } from "../../src/command.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";
import { commandEnv } from "./command-process.js";

const [sink = "normal", trigger = "SIGTERM", mode = "held"] =
  process.argv.slice(2);
Object.assign(process.env, commandEnv(), {
  TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "2000",
  TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS: "2000",
  TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS: "1000",
});
const emit = (event: string) =>
  process.stdout.write(JSON.stringify({ event, at: Date.now() }) + "\n");
const observe = (record: Record<string, unknown>) => {
  if (sink === "throw") throw new Error("synthetic diagnostic sink failure");
  if (sink === "normal") process.stderr.write(JSON.stringify(record) + "\n");
};
let release!: () => void;
const held = new Promise<void>((resolve) => {
  release = resolve;
});
let app: ReturnType<typeof createRuntimePodApp>;
await runProcessEntry((processBoundary) =>
  runRuntimePodCommand({
    processBoundary,
    logger: { info: observe, error: observe },
    dependencyBuilder: async ({ config, logger }) => {
      app = createRuntimePodApp({
        config: {
          ...config,
          grpcBindAddress: "127.0.0.1:0",
          httpBindAddress: "127.0.0.1:0",
        },
        logger,
        runtimeProcess: {
          runtimeProcessId: "shutdown-process",
          register: async () => undefined,
          report: async () => undefined,
          release: async () => {
            emit("handoff.unexpected");
            throw new Error("unexpected handoff");
          },
          close: async () => {
            emit("process-client.close");
          },
        },
        tokenReviewClient: {
          createTokenReview: async () => ({
            authenticated: false,
            audiences: [],
            username: "",
            podUid: "",
          }),
        },
        commandRunHost: {} as never,
        cleanupRunHost: {} as never,
        quiesce: async () => {
          emit("core.held");
          await held;
          emit("core.joined");
        },
        closeClients: async () => {
          emit("dependency.close");
        },
      });
      return {
        app,
        metrics: new RuntimePodMetricsRegistry(),
        tokenReviewClient: {} as never,
        coreHosts: {
          close: async () => {
            emit("core.close");
          },
        } as never,
      };
    },
    registerSignalHandlers: (shutdown) =>
      registerProcessSignalHandlers(() => {
        emit("shutdown.begin");
        if (mode === "cooperative") setTimeout(release, 100);
        return shutdown();
      }),
    waitForever: async () => {
      let entered!: () => void;
      const admission = new Promise<void>((resolve) => {
        entered = resolve;
      });
      void app.lifecycle
        .runCommand(async () => {
          emit("producer.held");
          entered();
          await held;
          emit("producer.joined");
        })
        .catch(() => undefined);
      await admission;
      if (trigger === "finally") {
        emit("shutdown.begin");
        return undefined as never;
      }
      process.kill(process.pid, trigger as NodeJS.Signals);
      return await new Promise<never>(() => undefined);
    },
  }),
);
