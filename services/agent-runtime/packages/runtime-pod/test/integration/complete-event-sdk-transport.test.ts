import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { Metadata } from "@grpc/grpc-js";
import { Effect, Stream } from "effect";
import {
  ProviderRequestKind,
  ProviderThreadRole,
  ProviderThreadVisibility,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createContentLifecycleGatewayFixture } from "../../../../../gateway/packages/provider-gateway/test/fixtures/content-lifecycle-gateway.js";
import type { ContentLifecycleGatewayFixtureOptions } from "../../../../../gateway/packages/provider-gateway/test/fixtures/content-lifecycle-gateway.js";
import { createLLMService } from "../../../core/src/llm/llm-service.js";
import { gatewayGrpcChannelOptions } from "../../src/bounds.js";
import { RuntimePodGatewayClient } from "../../src/gateway-client.js";

async function withSdk<T>(
  options: ContentLifecycleGatewayFixtureOptions,
  run: (fixture: Awaited<ReturnType<typeof createContentLifecycleGatewayFixture>>, client: RuntimePodGatewayClient) => Promise<T>,
): Promise<T> {
  const fixture = await createContentLifecycleGatewayFixture(options);
  const client = new RuntimePodGatewayClient({
    address: fixture.address, tokenPath: "/unused",
    channelOptions: gatewayGrpcChannelOptions(), metadataFactory: async () => {
      // Each workspace resolves its own grpc-js. Use the Runtime constructor
      // so its client recognizes this overload's Metadata argument.
      const metadata = new Metadata();
      for (const [key, value] of Object.entries(fixture.metadata().getMap())) metadata.set(key, value);
      return metadata;
    },
  });
  let result: T;
  try {
    result = await run(fixture, client);
  } finally {
    try { await client.close(); } finally { await fixture.shutdown(); }
  }
  expect(fixture.observations()).toMatchObject({ activeProviderSources: 0, sdk: { active: false } });
  return result;
}

for (const [name, kind, reviewer] of [
  ["ordinary", ProviderRequestKind.PROVIDER_REQUEST_KIND_AGENT_PROVIDER_REQUEST, false],
  ["reviewer", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER, true],
  ["compaction", ProviderRequestKind.PROVIDER_REQUEST_KIND_COMPACTION_SUMMARY, false],
  ["reviewer compaction", ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER_COMPACTION, true],
] as const) {
  for (const scenario of ["empty-text", "empty-then-valid"] as const) {
    test(`actual SDK ${scenario} suppresses empty blocks for ${name}`, async () => {
      await withSdk({ scenario }, async (fixture, client) => {
        const request = fixture.request({
          requestKind: kind,
          threadRole: reviewer ? ProviderThreadRole.PROVIDER_THREAD_ROLE_APPROVAL_REVIEWER : ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,
          threadVisibility: reviewer ? ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_INTERNAL : ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC,
          ...(kind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER ? { outputSchemaJson: '{"type":"object"}' } : {}),
        });
        const events = Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request))));
        expect(events.map(event => event.type)).toEqual(scenario === "empty-text" ? ["finish"] : ["text-complete", "finish"]);
        if (scenario === "empty-then-valid") expect(events[0]).toMatchObject({ text: "alpha" });
        expect(fixture.observations()).toMatchObject({ providerCalls: 1, activeProviderSources: 0 });
      });
    }, 30_000);
  }
  test(`actual SDK complete content reaches ${name} Runtime consumer`, async () => {
    await withSdk({}, async (fixture, client) => {
      const request = fixture.request({
        requestKind: kind,
        threadRole: reviewer ? ProviderThreadRole.PROVIDER_THREAD_ROLE_APPROVAL_REVIEWER : ProviderThreadRole.PROVIDER_THREAD_ROLE_MAIN,
        threadVisibility: reviewer ? ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_INTERNAL : ProviderThreadVisibility.PROVIDER_THREAD_VISIBILITY_PUBLIC,
        ...(kind === ProviderRequestKind.PROVIDER_REQUEST_KIND_APPROVAL_REVIEWER ? { outputSchemaJson: '{"type":"object"}' } : {}),
      });
      const events = Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(request))));
      expect(events.map(event => event.type)).toEqual(["thinking-started", "reasoning-complete", "text-complete", "text-complete", "tool-call-complete", "finish"]);
      expect(events[1]).toMatchObject({ text: "private R1", providerMetadata: { anthropic: { signature: "fixture_signature" } } });
      expect(events[2]).toMatchObject({ text: "alpha" });
      expect(events[3]).toMatchObject({ text: "beta" });
      expect(events[4]).toMatchObject({ id: "call_fixture_1", toolName: "Read", input: { file_path: "/workspace/note.txt" } });
      expect(fixture.observations().providerCalls).toBe(1);
      expect(fixture.observations().platformSelections).toBe(reviewer ? 1 : 0);
      expect(fixture.observations().sessionCredentialReads).toBe(reviewer ? 0 : 1);
    });
  }, 30_000);
}

// Independent Python oracle: s = 'x' * n; len(json.dumps(s).encode());
// hashlib.sha256(s.encode()).hexdigest(). These use the production limits at
// both ends and actual pinned SDK normalization; no fuse is raised here.
for (const [textCodeUnits, canonicalBytes, digest] of [
  [8_388_608, 8_388_610, "0c77bc0a0795a93612d45256897456d0fcb24f151c44c150d07ecd03f4ef5168"],
  [16_777_214, 16_777_216, "5e01c49110c2e2b22e70986eff62ca694e6f3dd681bab492099ff95c4fc6e96b"],
] as const) {
  test(`actual SDK preserves ${canonicalBytes} canonical text bytes through Runtime transport`, async () => {
    await withSdk({ scenario: "text-large", textCodeUnits, fragmentCodeUnits: 65_536 }, async (fixture, client) => {
      const events = Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(fixture.request()))));
      expect(events.map(event => event.type)).toEqual(["text-complete", "finish"]);
      const text = events[0];
      if (text?.type !== "text-complete") throw new Error("complete text missing");
      expect(text.text.length).toBe(textCodeUnits);
      expect(Buffer.byteLength(JSON.stringify(text.text))).toBe(canonicalBytes);
      expect(createHash("sha256").update(text.text).digest("hex")).toBe(digest);
      expect(fixture.observations()).toMatchObject({ providerCalls: 1, activeProviderSources: 0, assembly: { retainedBytes: 0, segments: 0, openBlocks: 0, identities: 0 } });
    });
  }, 60_000);
}

for (const scenario of ["truncated", "error-after-partial"] as const) {
  test(`actual SDK ${scenario} cannot become successful Runtime content`, async () => {
    await withSdk({ scenario }, async (fixture, client) => {
      const events = Array.from(await Effect.runPromise(Stream.runCollect(createLLMService(client).stream(fixture.request()))));
      expect(events.map(event => event.type)).toEqual(["provider-error"]);
      expect(fixture.observations().providerCalls).toBe(1);
    });
  }, 30_000);
}
