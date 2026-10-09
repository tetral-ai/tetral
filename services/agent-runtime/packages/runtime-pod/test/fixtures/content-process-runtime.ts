/** Actual Runtime child with observation gates at the content owners. */
import { access, appendFile, readFile, writeFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import { z } from "zod/v4";
import { RequestContentProcessor } from "@tetral/agent-runtime-core/src/runtime/accumulator.js";
import { buildRuntimePodCommandDependencies } from "../../src/command.js";
import { loadRuntimePodConfig } from "../../src/config.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import { createJsonLogger } from "../../src/logger.js";
import { BridgeRuntimeProcess } from "../../src/runtime-process.js";

const input = z.strictObject({
  bridgeAddress: z.string().min(1),
  gatewayAddress: z.string().min(1),
  podUID: z.string().min(1),
  processID: z.string().min(1),
  token: z.string().min(1),
  directory: z.string().min(1),
  boundary: z.enum(["none", "frame-before-write", "commit-before-apply", "reasoning-staged"]),
}).parse(JSON.parse(await readFile(process.argv[2]!, "utf8")) as unknown);
const stop = new AbortController();
const metadataFactory = async () => {
  const metadata = new Metadata();
  metadata.set("authorization", `Bearer ${input.token}`);
  return metadata;
};
const exists = async (path: string) => {
  try { await access(path); return true; } catch { return false; }
};
const wait = async (path: string) => {
  const deadline = Date.now() + 180_000;
  while (!await exists(path)) {
    stop.signal.throwIfAborted();
    if (Date.now() >= deadline) throw new Error("fixture barrier deadline");
    await new Promise<void>(resolve => setTimeout(resolve, 10));
  }
};
const marker = async (name: string, value: unknown) => {
  await writeFile(`${input.directory}/${name}.json`, JSON.stringify(value));
};
const trace = async (boundary: string, fields: Readonly<Record<string, unknown>> = {}) => {
  await appendFile(`${input.directory}/trace.jsonl`, `${JSON.stringify({ boundary, ...fields })}\n`);
};
let boundaryReached = false;
const holdBoundary = async (boundary: string, fields: Readonly<Record<string, unknown>>) => {
  if (boundaryReached || input.boundary !== boundary) return;
  boundaryReached = true;
  await marker("crash-boundary", { boundary, pid: process.pid, processID: input.processID, ...fields });
  await wait(`${input.directory}/release-boundary`);
};
await writeFile(`${input.directory}/fixture-token`, input.token);
await writeFile(`${input.directory}/fixture-ca`, "fixture-readable-ca-material");

const parsed=loadRuntimePodConfig({
 TETRAL_RUNTIME_POD_NAMESPACE:"tetral-agent-runtime",TETRAL_RUNTIME_POD_NAME:"content-runtime",TETRAL_RUNTIME_POD_UID:input.podUID,TETRAL_RUNTIME_POD_IP:"127.0.0.1",TETRAL_RUNTIME_POD_GRPC_PORT:"19090",TETRAL_RUNTIME_POD_HTTP_ADDR:"127.0.0.1:0",
 TETRAL_DEPLOYMENT_ENVIRONMENT:"test",TETRAL_SERVICE_VERSION:"test",TETRAL_RUNTIME_POD_GRPC_AUDIENCE:"tetral-internal-grpc",TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS:"tetral-system/job-runner",
 KUBERNETES_API_SERVER_URL:"https://unused.test",KUBERNETES_API_CA_CERT_PATH:`${input.directory}/fixture-ca`,KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH:`${input.directory}/fixture-token`,TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH:`${input.directory}/fixture-token`,
 TETRAL_BRIDGE_API_GRPC_ADDR:input.bridgeAddress,TETRAL_GATEWAY_GRPC_ADDR:input.gatewayAddress,TETRAL_MCP_CONNECTOR_GRPC_ADDR:"127.0.0.1:1",TETRAL_WEB_CONNECTOR_GRPC_ADDR:"127.0.0.1:1",TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL:"anthropic/claude-opus-4-8",
 TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES:"32768",TETRAL_RUNTIME_DRAIN_TIMEOUT_MS:"2000",TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS:"2000",TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS:"1000",TETRAL_RUNTIME_PROXY_JOIN_TIMEOUT_MS:"1000",
});
if(!parsed.ok)throw new Error(parsed.error.message);


const logger = createJsonLogger({ write: line => { process.stderr.write(line); } });
const dependencies = await buildRuntimePodCommandDependencies({
  config: { ...parsed.config, grpcBindAddress: "127.0.0.1:0" },
  logger,
  builderOptions: {
    // This local child has no Kubernetes memory cgroup. Supply the external
    // observation while retaining real HTTP metrics and Runner admission.
    readContainerMemory: () => ({ usageBytes: 100, limitBytes: 1000 }),
    outboundMetadataFactory: metadataFactory,
    routingProxyReady: async () => {},
    tokenReviewClientFactory: () => ({ createTokenReview: async () => ({
      authenticated: true,
      username: "system:serviceaccount:tetral-system:job-runner",
      audiences: ["tetral-internal-grpc"],
    }) }),
    runtimeProcessFactory: (_id, config) => new BridgeRuntimeProcess(input.processID, {
      address: input.bridgeAddress,
      tokenPath: config.outboundInternalGrpcTokenPath,
      policies: config.bridgeMethodPolicies,
      metadataFactory,
    }),
    coreHostsFactory: async options => {
      const loader = options.contextLoader;
      const writer = options.threadLoop.sessionEventWriter;
      return await buildRuntimeCoreHosts({
        ...options,
        contextLoader: {
          loadThreadContext: async (...args) => {
            const result = await loader.loadThreadContext!(...args);
            // Fixture evidence excludes binding tokens, credentials and configuration.
            await marker("cold-context", {
              messages: result.messages,
              currentRequestMessage: result.currentRequestMessage,
              pendingToolUseCount: result.pendingToolUses?.length ?? 0,
              pendingSandboxExecutionCount: result.pendingSandboxExecutions?.length ?? 0,
            });
            await trace("cold-context-loaded", {
              messageCount: result.messages.length,
              messages: result.messages,
              turnFacts: result.turnFacts,
              currentRequestMessage: result.currentRequestMessage,
            });
            return result;
          },
          commitAcceptedInput: loader.commitAcceptedInput!.bind(loader),
          readAgentMail: loader.readAgentMail!.bind(loader),
          refreshRuntimeBindingToken: loader.refreshRuntimeBindingToken!.bind(loader),
        },
        threadLoop: {
          ...options.threadLoop,
          approvalMode: "full_access",
          runtimePolicy: session => ({ ...options.threadLoop.runtimePolicy?.(session), approvalMode: "full_access" }),
          createProcessor: processorOptions => {
            const processor = new RequestContentProcessor(processorOptions);
            const process = processor.process.bind(processor);
            processor.process = async (envelope, signal) => {
              const result = await process(envelope, signal);
              if (result.ok && envelope.event.type === "reasoning-complete") {
                await holdBoundary("reasoning-staged", {
                  modelRequestId: processorOptions.modelRequestId,
                  thinkingEventId: envelope.event.thinkingEventId,
                  providerPartId: envelope.event.providerPartId,
                });
              }
              return result;
            };
            return processor;
          },
          sessionEventWriter: {
            append: async envelope => {
              if (envelope.event.type === "agent.message") {
                await holdBoundary("frame-before-write", {
                  modelRequestId: envelope.modelRequestId,
                  eventId: envelope.preallocatedEventId,
                  writeId: envelope.writeId,
                });
              }
              const result = await writer.append(envelope);
              if (result.ok && result.type !== "stale") {
                await trace("event-ack", { eventType: envelope.event.type, eventId: result.eventId });
                if (envelope.event.type === "agent.message") {
                  await holdBoundary("commit-before-apply", {
                    modelRequestId: envelope.modelRequestId,
                    eventId: result.eventId,
                    writeId: envelope.writeId,
                  });
                }
                if (envelope.event.type === "agent.tool_use") {
                  await marker("tool-declared", { eventId: result.eventId, modelRequestId: envelope.modelRequestId });
                }
              }
              return result;
            },
            writeRequestEnd: async envelope => {
              const result = await writer.writeRequestEnd(envelope);
              if (result.ok && result.type !== "stale") {
                await marker("request-end-ack", { modelRequestId: envelope.modelRequestId, eventId: result.requestEndEventId });
              }
              return result;
            },
            settleToolResult: writer.settleToolResult.bind(writer),
            ...(writer.finishIdle === undefined ? {} : { finishIdle: writer.finishIdle.bind(writer) }),
            ...(writer.commitRuntimeTermination === undefined ? {} : { commitRuntimeTermination: writer.commitRuntimeTermination.bind(writer) }),
          },
        },
      });
    },
  },
});
try {
  const ready = await dependencies.app.start();
  await marker("ready", { port: ready.grpcPort, httpUrl: ready.httpUrl.href, pid: process.pid, processID: input.processID });
  await wait(`${input.directory}/stop`);
} finally {
  stop.abort();
  await dependencies.app.shutdown();
  await dependencies.coreHosts.close();
  await marker("closed", { joined: true });
}
