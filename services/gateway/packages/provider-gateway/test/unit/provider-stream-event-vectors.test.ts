import { afterAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import type { StatusObject } from "@grpc/grpc-js";
import { credentials, Metadata } from "@grpc/grpc-js";
import { NormalizedProviderEventType } from "@tetral/gateway-lowering/src/normalized-stream.js";
import type { NormalizedProviderEvent } from "@tetral/gateway-lowering/src/normalized-stream.js";
import { MaxProviderResponseFrameBytes } from "@tetral/gateway-protocol/src/bounds.js";
import {
  ProviderFinishReason,
  ProviderGatewayServiceClient,
  ProviderRequestKind,
  ProviderStreamEvent,
  ProviderThreadRole,
  ProviderThreadVisibility,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createGatewayGrpcServer } from "../../src/grpc-server.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { ProviderCredentialResolver } from "../../src/providers/credentials.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import type { ProviderRequestStreamer } from "../../src/service.js";
import { createContentLifecycleGatewayFixture } from "../fixtures/content-lifecycle-gateway.js";
import type { ContentLifecycleGatewayFixtureOptions } from "../fixtures/content-lifecycle-gateway.js";
import { validProviderRequest } from "./fixtures.js";
import { pinnedVectors, pinVector } from "./vector-pin.js";

// The frames and final status a Runtime receives from the Gateway for one
// provider outcome, read by the Runtime's tests (see testdata/README.md at the
// repository root).
const vectors = new URL("../../../../../../testdata/provider-stream-events/", import.meta.url);

type Served = { readonly events: readonly ProviderStreamEvent[]; readonly code: number; readonly details: string };

const rows: Record<string, () => Promise<Served>> = {
  // The actual AI SDK path: reasoning, two texts and a Tool call.
  "sdk-interleaved": () => sdk({}),
  "sdk-empty-text": () => sdk({ scenario: "empty-text" }),
  "sdk-empty-then-valid": () => sdk({ scenario: "empty-then-valid" }),
  "sdk-truncated": () => sdk({ scenario: "truncated" }),
  "sdk-error-after-partial": () => sdk({ scenario: "error-after-partial" }),
  "large-tool-input": () => serve(catalogRequest(), { stream: largeToolInputStream }),
  "recorded-glm": recordedGLM,
  // The wire's zero retry delay.
  "zero-retry-delay": () => serve(catalogRequest(), { stream: zeroRetryDelayStream }),
  "pre-event-invalid-argument": () => serve(catalogRequest({ requestId: "" }), { stream: async function* () {} }),
  "pre-event-unavailable": () => serve(catalogRequest(), { stream: async function* () {} }, false),
};

const ran = new Set<string>();

describe("Provider stream event vectors", () => {
  test("every vector has a row and every row a vector", () => {
    expect(
      readdirSync(vectors)
        .filter((name) => name.endsWith(".json"))
        .map((name) => name.slice(0, -".json".length))
        .sort(),
    ).toEqual(Object.keys(rows).sort());
  });

  afterAll(() => {
    const unpinned = [...ran].filter((name) => !pinnedVectors.has(new URL(`${name}.json`, vectors).href));
    if (unpinned.length > 0) throw new Error(`Provider stream events not pinned: ${unpinned.join(", ")}`);
  });

  for (const [name, produce] of Object.entries(rows)) {
    test(`${name} is what the Gateway sends`, async () => {
      ran.add(name);
      pinVector(new URL(`${name}.json`, vectors), encoded(await produce()), "the Runtime's tests");
    }, 30_000);
  }

  // Every request kind gets the same frames; reviewers run on the platform
  // credential, the others on the Session's, and the finish says which.
  const kinds = [
    ["ordinary", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, false],
    ["reviewer", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER, true],
    ["compaction", ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY, false],
    ["reviewer compaction", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION, true],
  ] as const;
  const scenarios = [
    ["sdk-interleaved", "interleaved"],
    ["sdk-empty-text", "empty-text"],
    ["sdk-empty-then-valid", "empty-then-valid"],
  ] as const;
  for (const [kindName, kind, reviewer] of kinds) {
    for (const [vector, scenario] of scenarios) {
      test(`${kindName} requests get the ${vector} frames`, async () => {
        const served = await sdk({ scenario }, {
          requestKind: kind,
          threadRole: reviewer ? ProviderThreadRole.PROVIDER_THREAD_ROLE_APPROVAL_REVIEWER : ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,
          threadVisibility: reviewer ? ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_INTERNAL : ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC,
          ...(kind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER ? { outputSchemaJson: '{"type":"object"}' } : {}),
        }, (observations) => {
          expect(observations).toMatchObject({
            providerCalls: 1,
            activeProviderSources: 0,
            sdk: { active: false },
            platformSelections: reviewer ? 1 : 0,
            sessionCredentialReads: reviewer ? 0 : 1,
          });
        });
        const ordinary = readFileSync(new URL(`${vector}.json`, vectors), "utf8");
        expect(encoded(served)).toBe(
          reviewer ? ordinary.replace('\\"credential_source\\":\\"session\\"', '\\"credential_source\\":\\"platform\\"') : ordinary,
        );
      }, 30_000);
    }
  }

  // Python oracle: s = 'x' * n; len(json.dumps(s).encode()); sha256(s).
  for (const [textCodeUnits, canonicalBytes, digest] of [
    [8_388_608, 8_388_610, "0c77bc0a0795a93612d45256897456d0fcb24f151c44c150d07ecd03f4ef5168"],
    [16_777_214, 16_777_216, "5e01c49110c2e2b22e70986eff62ca694e6f3dd681bab492099ff95c4fc6e96b"],
  ] as const) {
    test(`the actual SDK path sends ${canonicalBytes} canonical text bytes in one frame`, async () => {
      const served = await sdk({ scenario: "text-large", textCodeUnits, fragmentCodeUnits: 65_536 }, {}, (observations) => {
        expect(observations).toMatchObject({ providerCalls: 1, activeProviderSources: 0, assembly: { retainedBytes: 0, segments: 0, openBlocks: 0, identities: 0 } });
      });
      expect(served.code).toBe(0);
      const text = served.events[0]?.textComplete?.text ?? "";
      expect(text.length).toBe(textCodeUnits);
      expect(Buffer.byteLength(JSON.stringify(text))).toBe(canonicalBytes);
      expect(createHash("sha256").update(text).digest("hex")).toBe(digest);
      expect(served.events.slice(1).map((event) => event.finish !== undefined)).toEqual([true]);
    }, 60_000);
  }
});

// encoded is the pinned form: protobuf JSON frames with Gateway-minted event
// ids renamed by first appearance, and the final status.
function encoded(served: Served): string {
  const names = new Map<string, string>();
  const json = JSON.stringify(
    { events: served.events.map((event) => ProviderStreamEvent.toJSON(event)), code: served.code, details: served.details },
    null,
    2,
  ).replace(/evt_[0-9a-f]{32}/g, (id) => {
    if (!names.has(id)) names.set(id, `evt_${(names.size + 1).toString(16).padStart(32, "0")}`);
    return names.get(id) as string;
  });
  return `${json}\n`;
}

// catalogRequest is a valid request for a model the Gateway catalog approves.
function catalogRequest(overrides: Partial<ProviderRequest> = {}): ProviderRequest {
  return validProviderRequest({ model: { providerId: "openai", modelId: "gpt-5.5", variant: "" }, ...overrides });
}

async function sdk(
  options: ContentLifecycleGatewayFixtureOptions,
  overrides: Partial<ProviderRequest> = {},
  observe: (observations: Record<string, unknown>) => void = () => undefined,
): Promise<Served> {
  const fixture = await createContentLifecycleGatewayFixture(options);
  try {
    const served = await collect(fixture.address, fixture.request(overrides), fixture.metadata());
    observe(fixture.observations() as unknown as Record<string, unknown>);
    return served;
  } finally {
    await fixture.shutdown();
  }
}

async function serve(request: ProviderRequest, providerStreamer: ProviderRequestStreamer, ready = true, credentialResolver?: ProviderCredentialResolver): Promise<Served> {
  const server = createGatewayGrpcServer(new ProviderGatewayServiceShell({
    authenticator: {
      authenticate: async () => ({ ok: true, serviceAccount: { namespace: "tetral", name: "agent-runtime", podUid: "pod_stream_vectors" } }),
    },
    runtimeBindingTokenVerifier: { verify: () => true },
    ready: () => ready,
    logger: { info: () => undefined, error: () => undefined },
    providerStreamer,
    ...(credentialResolver === undefined ? {} : { credentialResolver }),
  }));
  const port = await server.bind("127.0.0.1:0");
  try {
    const metadata = new Metadata();
    metadata.set("authorization", "bearer request-token");
    return await collect(`127.0.0.1:${port}`, request, metadata);
  } finally {
    server.server.forceShutdown();
  }
}

// collect reads every frame and the final status with the Runtime's receive
// limit, the protocol bound.
async function collect(address: string, request: ProviderRequest, metadata: Metadata): Promise<Served> {
  const client = new ProviderGatewayServiceClient(address, credentials.createInsecure(), {
    "grpc.max_receive_message_length": MaxProviderResponseFrameBytes,
  });
  try {
    const call = client.streamProviderRequest(request, metadata);
    const events: ProviderStreamEvent[] = [];
    call.on("data", (event: ProviderStreamEvent) => events.push(event));
    call.on("error", () => { /* The final status is recorded. */ });
    const status = await new Promise<StatusObject>((resolve) => call.once("status", resolve));
    return { events, code: status.code, details: status.details };
  } finally {
    client.close();
  }
}

async function* largeToolInputStream(): AsyncGenerator<NormalizedProviderEvent> {
  const inputs = [
    { action: "create", path: "notes/large.md", content: `CREATE_HEAD${"\u0001".repeat(9_000)}CREATE_TAIL` },
    {
      action: "replace",
      path: "notes/large.md",
      old_text: `OLD_HEAD${"<".repeat(5_000)}OLD_TAIL`,
      new_text: `NEW_HEAD${"\\".repeat(5_000)}NEW_TAIL`,
    },
  ];
  for (const [index, input] of inputs.entries()) {
    yield {
      type: NormalizedProviderEventType.ToolCall,
      toolCall: { id: `call_large_memory_${index}`, name: "memory", inputJson: JSON.stringify(input), metadataJson: "{}" },
    };
  }
  yield {
    type: NormalizedProviderEventType.Finish,
    finish: {
      reason: ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS,
      usage: { inputTotalTokens: 1, inputUncachedTokens: 1, outputTotalTokens: 1, totalTokens: 2, providerUsageJson: "{}" },
      metadataJson: "{}",
      contextWindowTokens: 1_050_000,
      outputTokenLimit: 128_000,
    },
  };
}

async function* zeroRetryDelayStream(): AsyncGenerator<NormalizedProviderEvent> {
  yield {
    type: NormalizedProviderEventType.ProviderError,
    providerError: {
      metadataJson: "{}",
      error: {
        code: "provider_unavailable",
        message: "Provider capacity is unavailable.",
        retryable: true,
        fatal: false,
        statusCode: 503,
        retryAfterMs: 0,
      },
    },
  };
}

// recordedGLM replays a recorded GLM stream through the production provider
// client; the normalized counts are this Gateway's own.
async function recordedGLM(): Promise<Served> {
  const recorded = readFileSync(new URL("../golden/fixtures/zai-glm-5.2-live-2026-07-04.sse", import.meta.url), "utf8");
  const registry = new ProviderClientRegistry({
    fetch: Object.assign(
      async () => new Response(recorded, { status: 200, headers: { "content-type": "text/event-stream" } }),
      { preconnect: () => undefined },
    ),
  });
  const counts: Record<string, number> = {};
  const served = await serve(
    validProviderRequest({
      requestId: "req_glm_recorded_stream",
      modelRequestId: "mreq_glm_recorded_stream",
      model: { providerId: "zai", modelId: "glm-5.2", variant: "" },
      tools: [
        {
          name: "Search",
          description: "Search.",
          function: {
            inputSchemaJson: JSON.stringify({
              type: "object",
              properties: { query: { type: "string" } },
              required: ["query"],
              additionalProperties: false,
            }),
          },
        },
      ],
    }),
    {
      stream: async function* (input) {
        for await (const event of registry.stream(input)) {
          counts[event.type] = (counts[event.type] ?? 0) + 1;
          yield event;
        }
      },
    },
    true,
    {
      resolve: async () => ({
        ok: true,
        credential: {
          source: "session",
          authType: "provider_api_key",
          providerId: "zai",
          supplyMode: "zai-api-key",
          vaultId: "vault_glm_recorded_stream",
          credentialId: "credential_glm_recorded_stream",
          accessMode: "api_key",
          apiKey: "fixture-zai-key",
        },
      }),
      recordPlatformFailure: () => undefined,
    } as unknown as ProviderCredentialResolver,
  );
  expect(counts).toMatchObject({
    "reasoning-start": 1,
    "reasoning-delta": 66,
    "reasoning-end": 1,
    "text-start": 1,
    "text-delta": 1,
    "text-end": 1,
    "tool-input-start": 1,
    "tool-input-delta": 1,
    "tool-input-end": 1,
  });
  return served;
}
