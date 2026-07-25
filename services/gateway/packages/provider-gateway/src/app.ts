/**
 * @packageDocumentation
 *
 * Owns the provider-gateway application's in-process composition and listener
 * lifecycle. The process command supplies validated configuration and concrete
 * provider dependencies; this module wires caller authentication, Runtime
 * binding verification, credential and attachment resolution, provider
 * streaming, admission control, and operations state into the service shell.
 * It calls the gRPC and operations HTTP server adapters, keeps readiness false
 * through bootstrap and listener binding, and withdraws readiness before
 * shutdown while liveness remains independent of traffic readiness.
 */

import { authenticateGatewayCaller } from "./auth.js";
import { createRuntimeBindingTokenVerifier } from "@tetral/gateway-protocol/src/binding-token.js";
import { createGatewayGrpcServer } from "./grpc-server.js";
import { createGatewayHttpServer } from "./http-server.js";
import { startupFailureLogRecord } from "./logger.js";
import { ProviderGatewayServiceShell } from "./service.js";
import type { GatewayTokenReviewClient } from "./auth.js";
import type { GatewayGrpcServer } from "./grpc-server.js";
import type { GatewayHttpServer } from "./http-server.js";
import type { ProviderGatewayConfig } from "./config.js";
import type { GatewayLogger } from "./logger.js";
import type { ProviderCredentialResolver } from "./providers/credentials.js";
import type { ProviderAttachmentResolver, ProviderRequestStreamer } from "./service.js";

/** Supplies validated process configuration and collaborators to the provider-gateway application. */
export interface ProviderGatewayAppOptions {
  readonly config: ProviderGatewayConfig;
  readonly logger: GatewayLogger;
  readonly tokenReviewClient: GatewayTokenReviewClient;
  readonly credentialResolver?: ProviderCredentialResolver | undefined;
  readonly attachmentResolver?: ProviderAttachmentResolver | undefined;
  readonly providerStreamer?: ProviderRequestStreamer | undefined;
  readonly bootstrap?: () => Promise<void>;
}

/** Exposes the composed service shell together with process health, readiness, and listener lifecycle. */
export interface ProviderGatewayApp {
  readonly service: ProviderGatewayServiceShell;
  readonly health: () => { readonly ok: true };
  readonly ready: () => { readonly ready: boolean };
  readonly start: () => Promise<{ readonly grpcPort: number; readonly httpUrl: URL }>;
  readonly shutdown: () => Promise<void>;
}

/**
 * Creates a provider-gateway application without opening listeners.
 *
 * {@link ProviderGatewayApp.start} runs the optional bootstrap, binds the gRPC
 * data-plane listener, starts the operations listener, and marks the process
 * ready only after both listeners exist. Startup failures reset readiness,
 * emit a bounded log record, and throw a generic error. Shutdown resets
 * readiness before stopping both listeners.
 */
export function createProviderGatewayApp(options: ProviderGatewayAppOptions): ProviderGatewayApp {
  let readyFlag = false;
  let grpcServer: GatewayGrpcServer | undefined;
  let httpServer: GatewayHttpServer | undefined;
  let boundGrpcPort: number | undefined;
  const allowedRuntimePod = {
    namespace: options.config.allowedRuntimePod.namespace,
    name: options.config.allowedRuntimePod.serviceAccount,
  };
  const service = new ProviderGatewayServiceShell({
    logger: options.logger,
    ready: () => readyFlag,
    runtimeBindingTokenVerifier: createRuntimeBindingTokenVerifier({
      hmacKey: options.config.runtimeBindingTokenHMACKey,
    }),
    credentialResolver: options.credentialResolver,
    attachmentResolver: options.attachmentResolver,
    providerStreamer: options.providerStreamer,
    maxConcurrentTurns: options.config.maxConcurrentTurns,
    authenticator: {
      authenticate: async ({ metadata, method }) =>
        await authenticateGatewayCaller({
          metadata,
          method,
          tokenReviewClient: options.tokenReviewClient,
          allowedRuntimePod,
        }),
    },
  });
  const state = {
    health: () => ({ ok: true as const }),
    ready: () => ({ ready: readyFlag }),
    metricsText: () => service.metricsText(),
  };
  return {
    service,
    health: state.health,
    ready: state.ready,
    start: async () => {
      try {
        await options.bootstrap?.();
        grpcServer = createGatewayGrpcServer(service);
        boundGrpcPort = await grpcServer.bind(options.config.grpcBindAddress);
        httpServer = createGatewayHttpServer(options.config.httpBindAddress, state);
        readyFlag = true;
      } catch {
        readyFlag = false;
        options.logger.error(startupFailureLogRecord({ kind: "startup_error", message: "gateway service startup failed" }));
        throw new Error("gateway service startup failed");
      }
      if (boundGrpcPort === undefined || httpServer === undefined) {
        throw new Error("gateway service startup failed");
      }
      return { grpcPort: boundGrpcPort, httpUrl: httpServer.url };
    },
    shutdown: async () => {
      readyFlag = false;
      await httpServer?.stop();
      await grpcServer?.shutdown();
    },
  };
}
