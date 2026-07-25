import { Metadata, Server, ServerCredentials } from "@grpc/grpc-js";
import {
  ProviderFinishReason,
  ProviderGatewayServiceService,
  ProviderRequest as ProviderRequestMessage,
  ProviderRequestKind,
  ProviderStreamEventType,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type {
  ProviderGatewayServiceServer,
  ProviderRequest,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { Effect, Stream } from "effect";
import { grpcServerOptions } from "../../../../../gateway/packages/provider-gateway/src/bounds.js";
import { gatewayGrpcChannelOptions } from "../../src/bounds.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";

const text = "x".repeat(5 * 1024 * 1024);
const request: ProviderRequest = {
  requestId: "req_transport_capacity",
  modelRequestId: "mreq_transport_capacity",
  requestKind: ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST,
  workspaceId: "wksp_transport",
  sessionId: "sesn_transport",
  sessionThreadId: "thr_transport",
  parentThreadId: undefined,
  bindingId: "bind_transport",
  bindingGeneration: 1,
  runtimeBindingToken: "binding-token",
  model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
  system: [{
    kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
    text: "System",
    cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE,
  }],
  messages: [{
    id: "msg_transport",
    role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
    status: "completed",
    origin: "user",
    parts: [{ id: "part_transport", text: { text } }],
  }],
  tools: [],
  attachments: [],
  limits: { maxOutputTokens: 128, timeoutMs: 30_000 },
};

let receivedBytes = 0;
let receivedTextBytes = 0;
const implementation: ProviderGatewayServiceServer = {
  streamProviderRequest(call) {
    receivedBytes = ProviderRequestMessage.encode(call.request).finish().byteLength;
    receivedTextBytes = Buffer.byteLength(call.request.messages[0]?.parts[0]?.text?.text ?? "", "utf8");
    call.write({
      requestId: call.request.requestId,
      modelRequestId: call.request.modelRequestId,
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
      finish: {
        reason: ProviderFinishReason.PROVIDER_FINISH_REASON_STOP,
        usage: {
          inputTotalTokens: 1,
          inputUncachedTokens: 1,
          outputTotalTokens: 1,
          totalTokens: 2,
          providerUsageJson: "{}",
        },
        metadataJson: "{}",
      },
    });
    call.end();
  },
  runWeb(_call, callback) {
    callback(new Error("not implemented"));
  },
};

const server = new Server(grpcServerOptions());
server.addService(ProviderGatewayServiceService, implementation);
const port = await new Promise<number>((resolve, reject) => {
  server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, boundPort) => {
    if (error !== null) {
      reject(error);
      return;
    }
    resolve(boundPort);
  });
});
const client = new RuntimePodGatewayClient({
  address: `127.0.0.1:${port}`,
  tokenPath: "/unused",
  channelOptions: gatewayGrpcChannelOptions(),
  metadataFactory: async () => new Metadata(),
});

try {
  const events = await Effect.runPromise(Stream.runCollect(client.streamProviderRequest(request)));
  process.stdout.write(`${JSON.stringify({
    requestBytes: ProviderRequestMessage.encode(request).finish().byteLength,
    receivedBytes,
    receivedTextBytes,
    eventCount: events.length,
  })}\n`);
} finally {
  await new Promise<void>((resolve) => server.tryShutdown(() => resolve()));
}
