import { readFile } from "node:fs/promises";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import type { ProviderRequest } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";

const inputPath = process.argv[2];
if (inputPath === undefined) {
  throw new Error("a real Runtime ProviderRequest fixture is required");
}
const runtimeRequests = (JSON.parse(await readFile(inputPath, "utf8")) as { readonly requests: readonly ProviderRequest[] }).requests;
const firstRequest = runtimeRequests[0];
if (runtimeRequests.length < 3 || firstRequest === undefined) {
  throw new Error("the complete cold-composed provider request set is required");
}
const repairs = providerToolErrorPairs(firstRequest);
const repair = repairs[0];
if (
  repairs.length !== 1 || repair === undefined ||
  !firstRequest.context.some((entry) => entry.content.some((item) => item.text?.text === "continuing"))
) {
  throw new Error("Runtime continuation must contain completed text and exactly one Tool error");
}

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
      supplyMode: "anthropic-api-key", vaultId: "vlt_wire", credentialId: "cred_wire",
      accessMode: "api_key", apiKey: "sk-wire-anthropic",
    },
    fixture: new URL("../golden/fixtures/anthropic-claude-opus-4-8-live-2026-07-03.sse", import.meta.url),
  },
  {
    family: "openai",
    providerId: "openai",
    credential: {
      source: "session", authType: "provider_api_key", providerId: "openai",
      supplyMode: "openai-api-key", vaultId: "vlt_wire", credentialId: "cred_wire",
      accessMode: "user_api_key", apiKey: "sk-wire-openai",
    },
    fixture: new URL("../golden/fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse", import.meta.url),
  },
  {
    family: "openai-compatible",
    providerId: "zai",
    credential: {
      source: "session", authType: "provider_api_key", providerId: "zai",
      supplyMode: "zai-coding-api-key", vaultId: "vlt_wire", credentialId: "cred_wire",
      accessMode: "api_key", apiKey: "sk-wire-zai",
    },
    fixture: new URL("../golden/fixtures/zai-glm-5.2-live-2026-07-04.sse", import.meta.url),
  },
];

const summaries = [];
for (const scenario of scenarios) {
  const captured: Array<{ readonly pathname: string; readonly body: Record<string, unknown> }> = [];
  const fixture = await readFile(scenario.fixture, "utf8");
  const fetchImpl = Object.assign(async (
    input: Parameters<FetchFunction>[0],
    init?: Parameters<FetchFunction>[1],
  ): Promise<Response> => {
    const request = new Request(input, init);
    captured.push({
      pathname: new URL(request.url).pathname,
      body: await request.json() as Record<string, unknown>,
    });
    return new Response(fixture.endsWith("\n\n") ? fixture : `${fixture}\n`, {
      status: 200,
      headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
    });
  }, { preconnect: () => {} }) satisfies FetchFunction;
  const registry = new ProviderClientRegistry({ fetch: fetchImpl });
  const request = runtimeRequests.find((candidate) => candidate.model?.providerId === scenario.providerId);
  if (request === undefined) {
    throw new Error(`cold composition omitted ${scenario.providerId}`);
  }
  const requestRepairs = providerToolErrorPairs(request);
  if (requestRepairs.length !== 1 || JSON.stringify(requestRepairs[0]) !== JSON.stringify(repair)) {
    throw new Error(`${scenario.family} did not receive the same cold-composed repair`);
  }
  for await (const _event of registry.stream({ request, credential: scenario.credential })) {
    // Consuming the stream forces the real provider client to finish its HTTP conversion.
  }
  const wire = captured[0];
  if (captured.length !== 1 || wire === undefined) {
    throw new Error(`${scenario.family} emitted ${captured.length} HTTP requests; want exactly one`);
  }
  summaries.push(assertRepairWire(scenario.family, wire.pathname, wire.body, repair));
}

process.stdout.write(JSON.stringify(summaries));

function assertRepairWire(
  family: "anthropic" | "openai" | "openai-compatible",
  pathname: string,
  body: Record<string, unknown>,
  expected: ProviderToolErrorPair,
) {
  const input = JSON.parse(expected.inputJson) as unknown;
  const error = expected.errorJson;
  if (!JSON.stringify(body).includes("continuing")) {
    throw new Error(`${family} wire omitted the completed assistant text`);
  }
  if (family === "anthropic") {
    const messages = body.messages as Array<{ readonly role: string; readonly content: readonly Record<string, unknown>[] }>;
    const calls = messages.flatMap((message, messageIndex) => message.content.flatMap((part) =>
      part.type === "tool_use" ? [{ messageIndex, role: message.role, part }] : []
    ));
    const results = messages.flatMap((message, messageIndex) => message.content.flatMap((part) =>
      part.type === "tool_result" ? [{ messageIndex, role: message.role, part }] : []
    ));
    if (calls.length !== 1 || results.length !== 1) throw new Error("Anthropic wire must contain exactly one Tool Call/Error pair");
    const call = calls[0]!;
    const result = results[0]!;
    if (
      call.role !== "assistant" || result.role !== "user" || call.messageIndex >= result.messageIndex ||
      call.part.id !== expected.modelToolCallId || call.part.name !== expected.name || !jsonEqual(call.part.input, input) ||
      result.part.tool_use_id !== expected.modelToolCallId || result.part.content !== error || result.part.is_error !== true
    ) throw new Error("Anthropic Tool Call/Error wire pairing is invalid");
    return summary(family, pathname, expected, call.messageIndex, result.messageIndex);
  }
  if (family === "openai") {
    const items = body.input as readonly Record<string, unknown>[];
    const calls = items.flatMap((item, index) => item.type === "function_call" ? [{ index, item }] : []);
    const results = items.flatMap((item, index) => item.type === "function_call_output" ? [{ index, item }] : []);
    if (calls.length !== 1 || results.length !== 1) throw new Error("OpenAI wire must contain exactly one Tool Call/Error pair");
    const call = calls[0]!;
    const result = results[0]!;
    if (
      call.index >= result.index || call.item.call_id !== expected.modelToolCallId || call.item.name !== expected.name ||
      !jsonEqual(JSON.parse(String(call.item.arguments)), input) || result.item.call_id !== expected.modelToolCallId ||
      result.item.output !== error
    ) throw new Error("OpenAI Tool Call/Error wire pairing is invalid");
    return summary(family, pathname, expected, call.index, result.index);
  }
  const messages = body.messages as readonly Record<string, unknown>[];
  const calls = messages.flatMap((message, index) => Array.isArray(message.tool_calls)
    ? (message.tool_calls as readonly Record<string, unknown>[]).map((call) => ({ index, role: message.role, call }))
    : []
  );
  const results = messages.flatMap((message, index) => message.role === "tool" ? [{ index, message }] : []);
  if (calls.length !== 1 || results.length !== 1) throw new Error("OpenAI-compatible wire must contain exactly one Tool Call/Error pair");
  const call = calls[0]!;
  const result = results[0]!;
  const fn = call.call.function as Record<string, unknown>;
  if (
    call.role !== "assistant" || call.index >= result.index || call.call.id !== expected.modelToolCallId ||
    fn.name !== expected.name || !jsonEqual(JSON.parse(String(fn.arguments)), input) ||
    result.message.tool_call_id !== expected.modelToolCallId || result.message.content !== error
  ) throw new Error("OpenAI-compatible Tool Call/Error wire pairing is invalid");
  return summary(family, pathname, expected, call.index, result.index);
}

function summary(
  family: string,
  pathname: string,
  repair: ProviderToolErrorPair,
  callIndex: number,
  resultIndex: number,
) {
  return {
    family,
    pathname,
    callId: repair.modelToolCallId,
    toolName: repair.name,
    errorMessage: (JSON.parse(repair.errorJson) as { error: { message: string } }).error.message,
    callIndex,
    resultIndex,
  };
}

interface ProviderToolErrorPair {
  readonly modelToolCallId: string;
  readonly name: string;
  readonly inputJson: string;
  readonly errorJson: string;
}

function providerToolErrorPairs(request: ProviderRequest): ProviderToolErrorPair[] {
  const calls = new Map(request.context.flatMap((entry) => entry.content.flatMap((item) =>
    item.toolCall === undefined ? [] : [[item.toolCall.modelToolCallId, item.toolCall] as const]
  )));
  return request.context.flatMap((entry) => entry.content.flatMap((item) => {
    const result = item.toolResult;
    const call = result === undefined ? undefined : calls.get(result.modelToolCallId);
    return result?.error === undefined || call === undefined
      ? []
      : [{
          modelToolCallId: call.modelToolCallId,
          name: call.name,
          inputJson: call.inputJson,
          errorJson: result.error.errorJson,
        }];
  }));
}

function jsonEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}
