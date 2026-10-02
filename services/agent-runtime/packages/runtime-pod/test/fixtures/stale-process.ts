// Real command/app/client child; only the authenticated Bridge response is controlled by the parent.
import { Metadata } from "@grpc/grpc-js";
import { runProcessEntry } from "@tetral/ts-observability";
import { createRuntimePodApp } from "../../src/app.js";
import { runRuntimePodCommand } from "../../src/command.js";
import { BridgeRuntimeProcess } from "../../src/runtime-process.js";
import { RuntimePodMetricsRegistry } from "../../src/metrics.js";
import { commandEnv } from "./command-process.js";

const [address, sink = "normal"] = process.argv.slice(2);
if (!address) throw new Error("controlled Bridge address required");
Object.assign(process.env, commandEnv(), {
  TETRAL_RUNTIME_DRAIN_TIMEOUT_MS: "2000",
  TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS: "2000",
  TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS: "1000",
  TETRAL_RUNTIME_REPORT_TIMEOUT_MS: "30",
  TETRAL_RUNTIME_REPORT_INTERVAL_MS: "40",
  TETRAL_RUNTIME_PROCESS_FRESHNESS_MS: "120",
});
const emit = (event: string) =>
  process.stdout.write(JSON.stringify({ event, at: Date.now() }) + "\n");
const observe = (record: Record<string, unknown>) => {
  if (sink === "throw") throw new Error("synthetic diagnostic sink failure");
  if (sink === "normal") process.stderr.write(JSON.stringify(record) + "\n");
};
await runProcessEntry((processBoundary) =>
  runRuntimePodCommand({
    processBoundary,
    logger: { info: observe, error: observe },
    dependencyBuilder: async ({ config, logger }) => {
      const runtimeProcess = new BridgeRuntimeProcess(crypto.randomUUID(), {
        address,
        tokenPath: "/unused",
        policies: config.bridgeMethodPolicies,
        metadataFactory: async () => new Metadata(),
      });
      const app = createRuntimePodApp({
        config: {
          ...config,
          grpcBindAddress: "127.0.0.1:0",
          httpBindAddress: "127.0.0.1:0",
        },
        logger,
        runtimeProcess,
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
    waitForever: async () => {
      emit("process.ready");
      return await new Promise<never>(() => undefined);
    },
  }),
);
