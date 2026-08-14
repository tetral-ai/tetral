import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { Metadata } from "@grpc/grpc-js";
import {
  ProviderContextRole,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { createJsonLogger, startupFailureLogRecord } from "../../src/logger.js";
import { ProviderClientRegistry } from "../../src/providers/clients.js";
import { ProviderGatewayServiceShell } from "../../src/service.js";
import { validProviderRequest } from "./fixtures.js";
import type { FetchFunction } from "@ai-sdk/provider-utils";
import type { GatewayAuthenticator } from "../../src/service.js";
import type { ProviderRequest, ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ResolvedProviderCredential } from "../../src/providers/credentials.js";

const RuntimePodUid = "pod_uid_leak_guard";
const OpenAIFixtureUrl = new URL("../golden/fixtures/openai-gpt-5.5-responses-live-2026-07-06.sse", import.meta.url);
const FixturesDir = new URL("../golden/fixtures/", import.meta.url);

const SentinelSecrets = [
  "sk-live-secret-contract",
  "oauth-access-secret-contract",
  "oauth-refresh-secret-contract",
  "fixture-provider-secret-contract",
  "provider-body-secret-contract",
] as const;

describe("Provider Gateway leak guards", () => {
  test("contract-level output channels do not expose sentinel credential material", async () => {
    const logLines: string[] = [];
    const logger = createJsonLogger({ write: (line) => logLines.push(line) });
    logger.error(startupFailureLogRecord({
      kind: "startup_error",
      message: `provider startup failed with ${SentinelSecrets[0]}`,
    }));

    const serviceLogs: unknown[] = [];
    const serviceRequest = validProviderRequest({
      model: { providerId: "openai", modelId: "gpt-5.5", variant: "" },
    });
    const service = new ProviderGatewayServiceShell({
      authenticator: okAuthenticator(),
      runtimeBindingTokenVerifier: { verify: () => true },
      ready: () => true,
      logger: { info: (record) => serviceLogs.push(record), error: (record) => serviceLogs.push(record) },
      providerStreamer: {
        stream: async function* () {
          throw new Error(`raw provider body carried ${SentinelSecrets[4]}`);
        },
      },
    });
    const errorEvents = await collectEvents(service.streamProviderRequest(serviceRequest, metadata()));

    const fixture = await readFile(OpenAIFixtureUrl, "utf8");
    const mock = createMockProviderServer(fixture);
    const registry = new ProviderClientRegistry({ fetch: mock.fetch });
    let providerEvents: readonly ProviderStreamEvent[];
    try {
      providerEvents = await collectEvents(registry.stream({
        request: openAIRequest(),
        credential: sentinelOpenAICredential(),
      }));
    } finally {
      await mock.close();
    }

    expectNoSentinel("startup logs", logLines);
    expectNoSentinel("service logs", serviceLogs);
    expectNoSentinel("provider error payload", errorEvents);
    expectNoSentinel("provider stream events", providerEvents);
    expectNoSentinel("golden fixtures", await readGoldenFixtures());
  });
});

function okAuthenticator(): GatewayAuthenticator {
  return {
    authenticate: async () => ({
      ok: true,
      serviceAccount: { namespace: "tetral-system", name: "bridge", podUid: RuntimePodUid },
    }),
  };
}

function metadata(): Metadata {
  const value = new Metadata();
  value.set("authorization", "bearer test-token");
  return value;
}

function openAIRequest(): ProviderRequest {
  return validProviderRequest({
    requestId: "req_leak_openai_1",
    modelRequestId: "mreq_leak_openai_1",
    model: { providerId: "openai", modelId: "gpt-5.5", variant: "xhigh" },
    context: [
      {
        role: ProviderContextRole.PROVIDER_CONTEXT_ROLE_USER,
        content: [{ text: { text: "Say ok and call Search once." } }],
      },
    ],
    tools: [{
      name: "Search",
      description: "Search.",
      function: { inputSchemaJson: JSON.stringify({
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false,
      }), outputSchemaJson: undefined },
    }],
    attachments: [],
  });
}

function sentinelOpenAICredential(): ResolvedProviderCredential {
  return {
    source: "session",
    authType: "provider_api_key",
    providerId: "openai",
    supplyMode: "openai-api-key",
    vaultId: "vlt_leak",
    credentialId: "cred_leak",
    accessMode: "user_api_key",
    apiKey: SentinelSecrets[0],
  };
}

interface CapturedProviderRequest {
  readonly headers: Record<string, string>;
}

function createMockProviderServer(fixture: string): {
  readonly requests: CapturedProviderRequest[];
  readonly fetch: FetchFunction;
  readonly close: () => Promise<void>;
} {
  const requests: CapturedProviderRequest[] = [];
  let server: ReturnType<typeof Bun.serve>;
  server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      requests.push({
        headers: Object.fromEntries(Array.from(request.headers.entries()).map(([key, value]) => [key.toLowerCase(), value])),
      });
      return new Response(fixture, {
        headers: {
          "content-type": "text/event-stream",
          "cache-control": "no-cache",
        },
      });
    },
  });
  return {
    requests,
    fetch: Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]) => {
      const original = new Request(input, init);
      const originalUrl = new URL(original.url);
      const body = original.method === "GET" || original.method === "HEAD" ? undefined : await original.text();
      const requestInit: RequestInit = {
        method: original.method,
        headers: original.headers,
        signal: original.signal,
      };
      if (body !== undefined) {
        requestInit.body = body;
      }
      return fetch(new URL(`${originalUrl.pathname}${originalUrl.search}`, server.url), requestInit);
    }, {
      preconnect: () => {},
    }) satisfies FetchFunction,
    close: async () => {
      await server.stop(true);
    },
  };
}

async function readGoldenFixtures(): Promise<readonly string[]> {
  const names = await readdir(FixturesDir);
  return await Promise.all(names.map(async (name) => await readFile(new URL(name, FixturesDir), "utf8")));
}

async function collectEvents(events: AsyncIterable<ProviderStreamEvent>): Promise<readonly ProviderStreamEvent[]> {
  const output: ProviderStreamEvent[] = [];
  for await (const event of events) {
    output.push(event);
  }
  return output;
}

function expectNoSentinel(label: string, value: unknown): void {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  for (const sentinel of SentinelSecrets) {
    expect(text, `${label} leaked ${sentinel}`).not.toContain(sentinel);
  }
  expect(text.toLowerCase(), `${label} leaked provider auth headers`).not.toContain("authorization");
  expect(text.toLowerCase(), `${label} leaked provider api-key headers`).not.toContain("x-api-key");
}
