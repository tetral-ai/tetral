import { expect, test } from "bun:test";
import {
  ProviderFinishReason,
  ProviderStreamEventType,
  RuntimeMessageRole,
  SystemCacheHint,
  SystemSegmentKind,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { GatewaySupplyMode } from "../../src/providers/catalog.js";
import { validProviderRequest } from "../unit/fixtures.js";
import type {
  ProviderRequest,
  ProviderStreamEvent,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";

const LiveSmokeEnabled = process.env.TETRAL_GATEWAY_LIVE_PROVIDER_SMOKE === "1";

interface LiveProviderCase {
  readonly name: string;
  readonly providerId: "anthropic" | "openai" | "deepseek" | "moonshotai" | "zai";
  readonly modelId: string;
  readonly supplyMode: Exclude<GatewaySupplyMode, "openai-chatgpt-oauth">;
  readonly accessMode: "api_key" | "user_api_key";
  readonly envName: string;
}

const LiveProviderCases: readonly LiveProviderCase[] = [
  {
    name: "openai",
    providerId: "openai",
    modelId: "gpt-5.5",
    supplyMode: "openai-api-key",
    accessMode: "user_api_key",
    envName: "TETRAL_LIVE_OPENAI_API_KEY",
  },
  {
    name: "anthropic",
    providerId: "anthropic",
    modelId: "claude-opus-4-8",
    supplyMode: "anthropic-api-key",
    accessMode: "api_key",
    envName: "TETRAL_LIVE_ANTHROPIC_API_KEY",
  },
  {
    name: "deepseek",
    providerId: "deepseek",
    modelId: "deepseek-v4-pro",
    supplyMode: "deepseek-api-key",
    accessMode: "user_api_key",
    envName: "TETRAL_LIVE_DEEPSEEK_API_KEY",
  },
  {
    name: "kimi",
    providerId: "moonshotai",
    modelId: "kimi-k3",
    supplyMode: "moonshotai-code-api-key",
    accessMode: "api_key",
    envName: "TETRAL_LIVE_KIMI_API_KEY",
  },
  {
    name: "zai",
    providerId: "zai",
    modelId: "glm-5.2",
    supplyMode: "zai-coding-api-key",
    accessMode: "api_key",
    envName: "TETRAL_LIVE_ZAI_API_KEY",
  },
];

test("external-smoke exercises live provider Gateway streaming when explicitly enabled", async () => {
  if (!LiveSmokeEnabled) {
    return;
  }
  const enabledCases = LiveProviderCases
    .map((entry) => ({ entry, apiKey: process.env[entry.envName] ?? "" }))
    .filter((entry) => entry.apiKey !== "");
  if (enabledCases.length === 0) {
    throw new Error("TETRAL_GATEWAY_LIVE_PROVIDER_SMOKE=1 requires at least one TETRAL_LIVE_*_API_KEY");
  }

  const registry = new ProviderClientRegistry();
  for (const { entry, apiKey } of enabledCases) {
    const events = await collectEvents(registry.stream({
      request: liveProviderRequest(entry),
      credential: liveProviderCredential(entry, apiKey),
    }));
    const terminal = events.at(-1);
    expect(terminal?.type, entry.name).toBe(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH);
    expect(terminal?.finish?.reason, entry.name).not.toBe(ProviderFinishReason.PROVIDER_FINISH_REASON_ERROR);
    expect(events.some((event) => event.type === ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR), entry.name).toBe(false);
  }
});

async function collectEvents(events: AsyncIterable<ProviderStreamEvent>): Promise<readonly ProviderStreamEvent[]> {
  const output: ProviderStreamEvent[] = [];
  for await (const event of events) {
    output.push(event);
  }
  return output;
}

function liveProviderRequest(entry: LiveProviderCase): ProviderRequest {
  return validProviderRequest({
    requestId: `req_live_smoke_${entry.name}`,
    modelRequestId: `mreq_live_smoke_${entry.name}`,
    model: { providerId: entry.providerId, modelId: entry.modelId, variant: "" },
    sessionId: "sesn_live_provider_smoke",
    system: [
      {
        kind: SystemSegmentKind.SYSTEM_SEGMENT_KIND_BASE,
        text: "Reply tersely and do not call tools.",
        cacheHint: SystemCacheHint.SYSTEM_CACHE_HINT_NONE,
      },
    ],
    messages: [
      {
        id: `msg_live_smoke_${entry.name}`,
        role: RuntimeMessageRole.RUNTIME_MESSAGE_ROLE_USER,
        status: "completed",
        origin: "user",
        parts: [{
          id: `part_live_smoke_${entry.name}`,
          text: { text: "Reply with exactly: ok" },
        }],
      },
    ],
    tools: [],
    limits: {
      maxOutputTokens: 32,
      timeoutMs: 60_000,
    },
  });
}

function liveProviderCredential(entry: LiveProviderCase, apiKey: string): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: entry.providerId,
    supplyMode: entry.supplyMode,
    vaultId: "vlt_live_smoke",
    credentialId: `cred_live_smoke_${entry.name}`,
    accessMode: entry.accessMode,
    apiKey,
  };
}
