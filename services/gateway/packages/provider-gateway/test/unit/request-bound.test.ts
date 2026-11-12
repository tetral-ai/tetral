// Layer 1: the Gateway's request bound in process.
import { describe, expect, test } from "bun:test";
import type { StatusObject } from "@grpc/grpc-js";
import { credentials, Metadata, status } from "@grpc/grpc-js";
import {
  MaxProviderContextTextJsonBytes,
  MaxProviderRequestMessageBytes,
  MaxProviderRequestToolOutputJsonBytes,
} from "@tetral/gateway-protocol/src/bounds.js";
import {
  ProviderContextRole,
  ProviderGatewayServiceClient,
  ProviderRequest as ProviderRequestMessage,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderContextEntry, ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createGatewayGrpcServer } from "../../src/grpc-server.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { ProviderCredentialResolver } from "../../src/providers/credentials.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import { validProviderRequest } from "./fixtures.js";

// The Runtime's client sends requests up to this same protocol bound; its
// tests prove its largest requests fit under it.
const routes = [
  { family: "anthropic", providerId: "anthropic", modelId: "claude-opus-4-8", supplyMode: "anthropic-api-key" },
  { family: "openai", providerId: "openai", modelId: "gpt-5.5", supplyMode: "openai-api-key" },
  { family: "openai-compatible", providerId: "deepseek", modelId: "deepseek-v4-pro", supplyMode: "deepseek-api-key" },
] as const;
type Route = (typeof routes)[number];

const systemMarkers = ["TETRAL_BOUND_BASE", "TETRAL_BOUND_AGENT", "TETRAL_BOUND_MEMORY", "TETRAL_BOUND_SKILL"] as const;
const contentMarkers = ["TETRAL_BOUND_FIRST", "TETRAL_BOUND_TOOL_HEAD", "TETRAL_BOUND_TOOL_TAIL", "TETRAL_BOUND_LAST"] as const;

describe("Gateway request bound", () => {
  for (const route of routes) {
    test(`receives a request at the protocol bound and lowers it for ${route.family}`, async () => {
      const request = boundedRequest(route, MaxProviderRequestMessageBytes);
      expect(ProviderRequestMessage.encode(request).finish().byteLength).toBe(MaxProviderRequestMessageBytes);
      const served = await serve(request);
      expect(served.code).toBe(status.OK);
      expect(served.receivedBytes).toEqual([MaxProviderRequestMessageBytes]);
      expect(served.bodies).toHaveLength(1);
      for (const marker of [...systemMarkers, ...contentMarkers]) expect(served.bodies[0]).toContain(marker);
    }, 60_000);
  }

  test("refuses a request a byte over the protocol bound before any provider call", async () => {
    const served = await serve(boundedRequest(routes[0], MaxProviderRequestMessageBytes + 1));
    expect(served.code).toBe(status.RESOURCE_EXHAUSTED);
    expect(served.receivedBytes).toEqual([]);
    expect(served.bodies).toEqual([]);
  }, 60_000);
});

// boundedRequest fills a valid request with text items at their own bound
// until it encodes to exactly encodedBytes.
function boundedRequest(route: Route, encodedBytes: number): ProviderRequest {
  const user = (text: string): ProviderContextEntry => ({ role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER, content: [{ text: { text } }] });
  const toolOutput = JSON.stringify({ text: "" });
  const toolText = marked(MaxProviderRequestToolOutputJsonBytes - toolOutput.length, contentMarkers[1], contentMarkers[2]);
  const fixed: ProviderContextEntry[] = [
    user(`${contentMarkers[0]} begin`),
    {
      role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_ASSISTANT,
      content: [
        { toolCall: { modelToolCallId: "call_bound", name: "Read", inputJson: "{}" } },
        { toolResult: { modelToolCallId: "call_bound", completed: { outputJson: JSON.stringify({ text: toolText }) } } },
      ],
    },
  ];
  const build = (filler: number): ProviderRequest => {
    const itemLength = MaxProviderContextTextJsonBytes - 2;
    const items: ProviderContextEntry[] = [];
    for (let left = filler; left > 0; left -= itemLength) items.push(user("x".repeat(Math.min(left, itemLength))));
    return validProviderRequest({
      requestId: `req_bound_${route.family}`,
      model: { providerId: route.providerId, modelId: route.modelId, variant: "" },
      system: [
        [SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE, systemMarkers[0]],
        [SystemSegmentKind.SYSTEM_SEGMENT_KIND_AGENT, systemMarkers[1]],
        [SystemSegmentKind.SYSTEM_SEGMENT_KIND_MEMORY, systemMarkers[2]],
        [SystemSegmentKind.SYSTEM_SEGMENT_KIND_SKILL, systemMarkers[3]],
      ].map(([kind, text]) => ({ kind: kind as SystemSegmentKind, cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_STABLE, text: text as string })),
      context: [...fixed, ...items, user(`${contentMarkers[3]} end`)],
    });
  };
  const encoded = (filler: number) => ProviderRequestMessage.encode(build(filler)).finish().byteLength;
  let filler = encodedBytes - encoded(0);
  for (let attempt = 0; attempt < 8 && encoded(filler) !== encodedBytes; attempt++) filler += encodedBytes - encoded(filler);
  return build(filler);
}

function marked(length: number, head: string, tail: string): string {
  return `${head}${"x".repeat(length - head.length - tail.length)}${tail}`;
}

// serve sends one request through the real Gateway server, whose provider is
// the production client with a recording fetch.
async function serve(request: ProviderRequest): Promise<{ code: number; receivedBytes: number[]; bodies: string[] }> {
  const receivedBytes: number[] = [];
  const bodies: string[] = [];
  const registry = new ProviderClientRegistry({
    fetch: Object.assign(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        bodies.push(await new Request(input, init).text());
        return new Response(JSON.stringify({ error: { type: "server_error", message: "recorded request" } }), {
          status: 500,
          headers: { "content-type": "application/json" },
        });
      },
      { preconnect: () => undefined },
    ),
  });
  const server = createGatewayGrpcServer(new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({ ok: true, serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_request_bound" } }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => true,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer: {
      stream: async function* (input) {
        receivedBytes.push(ProviderRequestMessage.encode(input.request).finish().byteLength);
        yield* registry.stream(input);
      },
    },
    credentialResolver: credentialResolver(),
  }));
  const port = await server.bind("127.0.0.1:0");
  const client = new ProviderGatewayServiceClient(`127.0.0.1:${port}`, credentials.createInsecure(), {
    "grpc.max_send_message_length": MaxProviderRequestMessageBytes * 2,
  });
  try {
    const metadata = new Metadata();
    metadata.set("authorization", "bearer request-token");
    const call = client.streamProviderRequest(request, metadata);
    call.on("data", () => undefined);
    call.on("error", () => { /* The final status is the oracle. */ });
    const { code } = await new Promise<StatusObject>((resolve) => call.once("status", resolve));
    return { code, receivedBytes, bodies };
  } finally {
    client.close();
    await server.shutdown();
  }
}

function credentialResolver(): ProviderCredentialResolver {
  return {
    resolve: async (request: Pick<ProviderRequest, "model">) => {
      const route = routes.find((candidate) => candidate.providerId === request.model?.providerId);
      if (route === undefined) throw new Error("request bound route is absent");
      return {
        ok: true,
        credential: {
          source: "session",
          authType: "provider_api_key",
          providerId: route.providerId,
          supplyMode: route.supplyMode,
          vaultId: "vault_request_bound",
          credentialId: "credential_request_bound",
          accessMode: "api_key",
          apiKey: "request-bound-provider-key",
        },
      };
    },
    recordPlatformFailure: () => undefined,
  } as unknown as ProviderCredentialResolver;
}
