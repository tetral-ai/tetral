import { readFile } from "node:fs/promises";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";

const inputPath = process.argv[2];
if (inputPath === undefined) throw new Error("Runtime ProviderRequests are required");
const requests = (JSON.parse(await readFile(inputPath, "utf8")) as {
  readonly requests: readonly ProviderRequest[];
}).requests;

const scenarios: readonly {
  readonly family: "anthropic" | "openai" | "openai-compatible";
  readonly providerId: string;
  readonly credential: ResolvedProviderCredential;
  readonly fixture: URL;
}[] = [
  {
    family: "anthropic",
    providerId: "anthropic",
    credential: {
      source: "session", authType: "provider_api_key", providerId: "anthropic",
      supplyMode: "anthropic-api-key", vaultId: "vlt_prefix", credentialId: "cred_prefix",
      accessMode: "api_key", apiKey: "sk-prefix-anthropic",
    },
    fixture: new URL("../golden/fixtures/anthropic-claude-opus-4-8-live-2026-07-03.sse", import.meta.url),
  },
  {
    family: "openai",
    providerId: "openai",
    credential: {
      source: "session", authType: "provider_api_key", providerId: "openai",
      supplyMode: "openai-api-key", vaultId: "vlt_prefix", credentialId: "cred_prefix",
      accessMode: "user_api_key", apiKey: "sk-prefix-openai",
    },
    fixture: new URL("../golden/fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse", import.meta.url),
  },
  {
    family: "openai-compatible",
    providerId: "zai",
    credential: {
      source: "session", authType: "provider_api_key", providerId: "zai",
      supplyMode: "zai-coding-api-key", vaultId: "vlt_prefix", credentialId: "cred_prefix",
      accessMode: "api_key", apiKey: "sk-prefix-zai",
    },
    fixture: new URL("../golden/fixtures/zai-glm-5.2-live-2026-07-04.sse", import.meta.url),
  },
];

const summaries = [];
for (const scenario of scenarios) {
  const request = requests.find((candidate) => candidate.model?.providerId === scenario.providerId);
  if (request === undefined) throw new Error(`Runtime composition omitted ${scenario.providerId}`);
  const captured: Array<{ readonly pathname: string; readonly body: Record<string, unknown> }> = [];
  const fixture = await readFile(scenario.fixture, "utf8");
  const fetchImpl = Object.assign(async (
    input: Parameters<FetchFunction>[0],
    init?: Parameters<FetchFunction>[1],
  ): Promise<Response> => {
    const httpRequest = new Request(input, init);
    captured.push({
      pathname: new URL(httpRequest.url).pathname,
      body: await httpRequest.json() as Record<string, unknown>,
    });
    return new Response(fixture.endsWith("\n\n") ? fixture : `${fixture}\n`, {
      status: 200,
      headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
    });
  }, { preconnect: () => {} }) satisfies FetchFunction;
  for await (const _event of new ProviderClientRegistry({ fetch: fetchImpl }).stream({
    request,
    credential: scenario.credential,
  })) {
    // Consume the stream so the real provider adapter emits its HTTP request.
  }
  const wire = captured[0];
  if (captured.length !== 1 || wire === undefined) {
    throw new Error(`${scenario.family} emitted ${captured.length} HTTP requests`);
  }
  const encoded = JSON.stringify(wire.body);
  const safeTextCount = encoded.split("safe parent prefix text").length - 1;
  let toolCallCount = 0;
  if (scenario.family === "anthropic") {
    const messages = wire.body.messages as readonly { readonly content: readonly Record<string, unknown>[] }[];
    toolCallCount = messages.flatMap((message) => message.content).filter((part) => part.type === "tool_use").length;
  } else if (scenario.family === "openai") {
    toolCallCount = (wire.body.input as readonly Record<string, unknown>[]).filter((item) => item.type === "function_call").length;
  } else {
    const messages = wire.body.messages as readonly Record<string, unknown>[];
    toolCallCount = messages.flatMap((message) => Array.isArray(message.tool_calls) ? message.tool_calls : []).length;
  }
  summaries.push({ family: scenario.family, pathname: wire.pathname, safeTextCount, toolCallCount });
}

process.stdout.write(JSON.stringify(summaries));
