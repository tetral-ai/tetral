// Actual Provider app worker remains held while command shutdown owns SQL and listeners.
import { Metadata } from "@grpc/grpc-js";
import {
  runProcessEntry,
  registerProcessSignalHandlers,
} from "@tetral/ts-observability";
import { createProviderGatewayApp } from "../../src/app.js";
import { runProviderGatewayCommand } from "../../src/command.js";
import { validProviderRequest } from "../unit/fixtures.js";
import { commandEnv, commandFixture } from "./command-process.js";
const [sink = "normal", trigger = "SIGTERM", mode = "held"] =
  process.argv.slice(2);
Object.assign(process.env, commandEnv(), {
  TETRAL_DRAIN_TIMEOUT_MS: "2000",
  TETRAL_SERVICE_CANCEL_JOIN_TIMEOUT_MS: "3000",
});
const emit = (event: string) =>
  process.stdout.write(JSON.stringify({ event, at: Date.now() }) + "\n");
const observe = (record: Record<string, unknown>) => {
  if (sink === "throw") throw new Error("synthetic diagnostic sink failure");
  if (sink === "normal") process.stderr.write(JSON.stringify(record) + "\n");
};
let release!: () => void, entered!: () => void;
const held = new Promise<void>((resolve) => {
  release = resolve;
});
const admission = new Promise<void>((resolve) => {
  entered = resolve;
});
let app: ReturnType<typeof createProviderGatewayApp>;
await runProcessEntry((processBoundary) =>
  runProviderGatewayCommand({
    processBoundary,
    logger: { info: observe, error: observe },
    dependencyBuilder: async ({ config, logger }) => {
      app = createProviderGatewayApp({
        config: {
          ...config,
          grpcBindAddress: "127.0.0.1:0",
          httpBindAddress: "127.0.0.1:0",
        },
        logger,
        bootstrap: async () => undefined,
        tokenReviewClient: {
          createTokenReview: async () => {
            emit("worker.held");
            entered();
            await held;
            emit("worker.joined");
            return {
              authenticated: false,
              audiences: [],
              username: "",
              podUid: "",
            };
          },
        },
      });
      const dependencies = await commandFixture("none").options
        .dependencyBuilder!({ config, logger });
      return {
        ...dependencies,
        app,
        close: async () => {
          emit("database.close");
        },
      };
    },
    registerSignalHandlers: (shutdown) =>
      registerProcessSignalHandlers(() => {
        emit("shutdown.begin");
        if (mode === "cooperative") setTimeout(release, 100);
        return shutdown();
      }),
    waitForever: async () => {
      const metadata = new Metadata();
      metadata.set("authorization", "bearer synthetic");
      const worker = (async () => {
        for await (const _event of app.service.streamProviderRequest(
          validProviderRequest(),
          metadata,
        )) {
        }
      })();
      void worker.catch(() => undefined);
      await Promise.race([
        admission,
        worker.then(() => {
          throw new Error("worker did not enter held boundary");
        }),
      ]);
      if (trigger === "finally") {
        emit("shutdown.begin");
        return undefined as never;
      }
      process.kill(process.pid, trigger as NodeJS.Signals);
      return await new Promise<never>(() => undefined);
    },
  }),
);
