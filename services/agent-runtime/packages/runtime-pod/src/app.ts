/**
 * Assembles the Runtime Pod process boundary from injected configuration and adapters. The command
 * entry point calls this module, which wires caller authentication, command handling, cleanup,
 * lifecycle management, and the gRPC and operational HTTP listeners. Startup reports success only
 * after both listeners exist and lifecycle readiness is true; shutdown closes command admission
 * before draining work and stopping the listeners.
 */
import type { RuntimeTokenReviewClient, ServiceAccountIdentity } from "./auth.js";
import { authenticateRuntimeCaller } from "./auth.js";
import type { RuntimeQuiesceOptions } from "@tetral/agent-runtime-core/src/session/session-manager.js";
import type { RuntimeProcessPort } from "./runtime-process.js";
import { RuntimePodLifecycle } from "./lifecycle.js";
import { createRuntimeGrpcServer } from "./grpc-server.js";
import { createRuntimeHttpServer } from "./http-server.js";
import { RuntimeControlService } from "./runtime-service.js";
import { SessionRunHostCleanupController } from "./cleanup-controller.js";
import type { RuntimePodBootstrap } from "./lifecycle.js";
import type { RuntimeGrpcServer } from "./grpc-server.js";
import type { RuntimeHttpServer } from "./http-server.js";
import type { RuntimeAuthenticator, RuntimeControlInputCommitter, RuntimeSessionRunHost } from "./runtime-service.js";
import type { RuntimeCoreCleanupHost } from "./cleanup-controller.js";
import type { RuntimePodConfig } from "./config.js";
import type { RuntimePodLogger } from "./logger.js";
import type { ContainerMemoryObservation } from "./metrics.js";
import type { RuntimePodMetricsSource } from "./metrics.js";

/**
 * Dependencies and optional lifecycle hooks used to assemble one Runtime Pod application.
 * Production composition supplies the boundary clients and run hosts, while tests can replace
 * bootstrap stages and commit adapters without introducing process-global state.
 */
export interface RuntimePodAppOptions {
  readonly readContainerMemory?: () => ContainerMemoryObservation | undefined;
  readonly runtimeProcessId?: string;
  readonly runtimeProcess?: RuntimeProcessPort;
  readonly quiesce?: (options: RuntimeQuiesceOptions) => Promise<void>;
  readonly closeClients?: () => Promise<void>;
  readonly config: RuntimePodConfig;
  readonly logger: RuntimePodLogger;
  readonly tokenReviewClient: RuntimeTokenReviewClient;
  readonly commandRunHost: RuntimeSessionRunHost;
  readonly controlInputCommitter?: RuntimeControlInputCommitter;
  readonly cleanupRunHost: RuntimeCoreCleanupHost;
  readonly shutdownActiveRuns?: () => Promise<void>;
  readonly drainTimeoutMs?: number;
  readonly metrics?: RuntimePodMetricsSource | undefined;
  readonly bootstrap?: Partial<RuntimePodBootstrap>;
}

/**
 * Owns the Runtime Pod control service and lifecycle together with listener startup and shutdown.
 * `start` resolves with bound endpoints only when the pod is ready, and `shutdown` stops new
 * commands before waiting for active work and listener termination.
 */
export interface RuntimePodApp {
  readonly service: RuntimeControlService;
  readonly lifecycle: RuntimePodLifecycle;
  readonly start: () => Promise<{ readonly grpcPort: number; readonly httpUrl: URL }>;
  readonly shutdown: () => Promise<void>;
}

/**
 * Creates a Runtime Pod application without binding listeners until `start` is called.
 *
 * The application authenticates Job Runner commands before service handling, runs accepted commands
 * through the lifecycle drain fence, and exposes only operational HTTP endpoints alongside the
 * internal command gRPC service.
 */
export function createRuntimePodApp(
  options: RuntimePodAppOptions,
): RuntimePodApp {
  const runtimeProcessId =
    options.runtimeProcess?.runtimeProcessId ??
    options.runtimeProcessId ??
    crypto.randomUUID();
  const authenticator = runtimeAuthenticator(options.tokenReviewClient, {
    namespace: options.config.jobRunner.namespace,
    name: options.config.jobRunner.serviceAccount,
  });
  const service = new RuntimeControlService({
    runtimeProcessId,
    ownPod: options.config.ownPod,
    allowedJobRunner: { namespace: options.config.jobRunner.namespace, name: options.config.jobRunner.serviceAccount },
    authenticator,
    runHost: options.commandRunHost,
    ...(options.controlInputCommitter !== undefined ? { controlInputCommitter: options.controlInputCommitter } : {}),
    cleanupController: new SessionRunHostCleanupController(options.cleanupRunHost),
    logger: options.logger,
    ready: () => lifecycle.ready().ready,
    ...(options.metrics !== undefined ? { metrics: options.metrics } : {}),
    commandRunner: {
      runCommand: async (command) => await lifecycle.runCommand(command),
    },
  });
  let grpcServer: RuntimeGrpcServer | undefined;
  let httpServer: RuntimeHttpServer | undefined;
  let boundGrpcPort: number | undefined;
  let stopping: Promise<void> | undefined;
  const lifecycle = new RuntimePodLifecycle({
    ...(options.runtimeProcess !== undefined
      ? { runtimeProcess: options.runtimeProcess }
      : {}),
    config: { ok: true, config: options.config },
    logger: options.logger,
    ...(options.drainTimeoutMs !== undefined ? { drainTimeoutMs: options.drainTimeoutMs } : {}),
    bootstrap: {
      runtime: options.bootstrap?.runtime ?? (async () => undefined),
      core: options.bootstrap?.core ?? (async () => undefined),
      authClient: options.bootstrap?.authClient ?? (async () => undefined),
      grpc: async () => {
        grpcServer = createRuntimeGrpcServer(service);
        boundGrpcPort = await grpcServer.bind(options.config.grpcBindAddress);
        httpServer = createRuntimeHttpServer(
          options.config.httpBindAddress,
          lifecycle,
          options.metrics,
          options.logger,
          options.readContainerMemory,
        );
        await options.bootstrap?.grpc?.();
      },
    },
    shutdownHooks: {
      ...(options.quiesce !== undefined ? { quiesce: options.quiesce } : {}),
      ...(options.shutdownActiveRuns !== undefined
        ? { shutdownActiveRuns: options.shutdownActiveRuns }
        : {}),
    },
  });

  return {
    service,
    lifecycle,
    start: async () => {
      await lifecycle.start();
      const httpUrl = httpServer?.url;
      if (grpcServer === undefined || httpUrl === undefined || !lifecycle.ready().ready) {
        throw new Error("runtime pod startup failed");
      }
      if (boundGrpcPort === undefined) {
        throw new Error("runtime pod startup failed");
      }
      return { grpcPort: boundGrpcPort, httpUrl };
    },
    shutdown: () => {
      if (stopping !== undefined) return stopping;
      stopping = (async () => {
        service.beginShutdown();
        // Retain concurrent drain/listener shutdown, attach rejection handlers
        // immediately, and join every started operation even if one fails.
        let failed = false,
          firstFailure: unknown;
        const observe = async (close: () => unknown): Promise<void> => {
          try {
            await close();
          } catch (error) {
            if (!failed) firstFailure = error;
            failed = true;
          }
        };
        await observe(() => lifecycle.shutdown());
        await observe(() => options.closeClients?.());
        await Promise.all([
          observe(() =>
            grpcServer?.shutdown(
              new Date(
                Date.now() + options.config.lifecycle.proxyJoinTimeoutMs,
              ),
            ),
          ),
          observe(() => httpServer?.stop()),
        ]);
        if (failed) throw firstFailure;
      })();
      return stopping;
    },
  };
}

function runtimeAuthenticator(
  tokenReviewClient: RuntimeTokenReviewClient,
  jobRunner: ServiceAccountIdentity,
): RuntimeAuthenticator {
  return {
    authenticate: async ({ metadata, method }) =>
      await authenticateRuntimeCaller({
        metadata,
        method,
        tokenReviewClient,
        allowedJobRunner: jobRunner,
      }),
  };
}
