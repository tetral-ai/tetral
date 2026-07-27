import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { relative, sep } from "node:path";

const providerRoot = new URL("../../", import.meta.url);
const packagesRoot = new URL("../", providerRoot);
const serviceRoot = new URL("../../", providerRoot);

const aiSdkDirectDependencyPins: Record<string, string> = {
  ai: "6.0.168",
  "@ai-sdk/provider": "3.0.8",
  "@ai-sdk/provider-utils": "4.0.23",
  "@ai-sdk/anthropic": "3.0.82",
  "@ai-sdk/openai": "3.0.53",
  "@ai-sdk/openai-compatible": "2.0.41",
};

describe("Gateway static boundaries", () => {
  test("Gateway AI SDK direct dependencies stay pinned to the contract versions", async () => {
    const packageJsonUrls = [
      new URL("package.json", serviceRoot),
      new URL("package.json", providerRoot),
    ];
    for (const packageJsonUrl of packageJsonUrls) {
      const packageJson = JSON.parse(await readFile(packageJsonUrl, "utf8")) as {
        dependencies?: Record<string, string>;
      };
      expect(packageJson.dependencies).toMatchObject(aiSdkDirectDependencyPins);
      const directAiSdkDependencyNames = Object.keys(packageJson.dependencies ?? {})
        .filter((name) => name === "ai" || name.startsWith("@ai-sdk/"))
        .sort();
      expect(directAiSdkDependencyNames).toEqual(Object.keys(aiSdkDirectDependencyPins).sort());
      expect(packageJson.dependencies?.hono).toBeUndefined();
    }

    const lockfile = await readFile(new URL("bun.lock", serviceRoot), "utf8");
    for (const [name, version] of Object.entries(aiSdkDirectDependencyPins)) {
      expect(lockfile).toContain(`"${name}": "${version}"`);
      expect(lockfile).toContain(`"${name}": ["${name}@${version}"`);
    }
  });

  test("Gateway protocol source and generated TypeScript artifacts are owned by protocol package", async () => {
    const proto = await readFile(new URL("proto/tetral/provider_gateway/v1/provider_gateway.proto", serviceRoot), "utf8");
    const generated = await readFile(new URL("protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.ts", packagesRoot), "utf8");
    const protocolBounds = await readFile(new URL("protocol/src/bounds.ts", packagesRoot), "utf8");
    const processBounds = await readFile(new URL("provider-gateway/src/bounds.ts", packagesRoot), "utf8");

    expect(proto).toContain("rpc StreamProviderRequest(ProviderRequest) returns (stream ProviderStreamEvent);");
    expect(proto).toContain("rpc RunWeb(RunWebRequest) returns (RunWebResponse);");
    expect(proto).toContain("WebToolInput input = 5;");
    expect(proto).toContain("message RequestUsage");
    expect(proto).toContain("optional RequestUsage usage = 2;");
    expect(proto).not.toContain("message ProviderUsage");
    expect(generated).toContain("export interface ProviderRequest");
    expect(generated).toContain("export interface ProviderStreamEvent");
    expect(generated).toContain("export interface WebToolInput");
    expect(generated).toContain("export interface RequestUsage");
    expect(generated).not.toContain("export interface ProviderUsage");
    expect(protocolBounds).toContain("validateProviderRequest");
    expect(protocolBounds).toContain("validateProviderStreamEvent");
    expect(protocolBounds).not.toContain("validateRunWebRequest");
    expect(protocolBounds).not.toContain("grpcServerOptions");
    expect(protocolBounds).not.toContain("unimplementedRunWebResponse");
    expect(processBounds).toContain("validateRunWebRequest");
    expect(processBounds).toContain("grpcServerOptions");
    expect(processBounds).not.toContain("unimplementedRunWebResponse");
  });

  test("lowering package stays pure and process package avoids durable writes", async () => {
    const loweringFiles = await collectTypeScriptFiles(new URL("lowering/src/", packagesRoot));
    const processFiles = await collectTypeScriptFiles(new URL("provider-gateway/src/", packagesRoot));
    const violations: string[] = [];

    for (const file of loweringFiles) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      for (const token of [
        "@grpc/grpc-js",
        "pg",
        "postgres",
        "fetch(",
        "Bun.",
        "process.env",
      ]) {
        if (text.includes(token)) {
          violations.push(`${path}: forbidden pure-lowering token ${token}`);
        }
      }
    }

    for (const file of processFiles) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      for (const token of [
        "session_events",
        "session_messages",
        "INSERT INTO",
        "UPDATE session_",
        "DELETE FROM session_",
      ]) {
        if (text.includes(token)) {
          violations.push(`${path}: forbidden Gateway durable-write token ${token}`);
        }
      }
      if (!path.endsWith("src/auth.ts") && !path.endsWith("src/providers/clients.ts") && !path.endsWith("src/providers/openai-oauth.ts") && !path.endsWith("src/providers/openai-oauth-refresh.ts") && /\bfetch\s*\(/.test(text)) {
        violations.push(`${path}: live fetch outside workload-auth TokenReview`);
      }
    }

    expect(violations).toEqual([]);
  });

  test("provider-gateway credential writes are confined to the OpenAI OAuth refresh adapter", async () => {
    const files = await collectTypeScriptFiles(new URL("provider-gateway/src/", packagesRoot));
    const violations: string[] = [];
    for (const file of files) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      const allowed = path.endsWith("src/providers/openai-oauth-refresh.ts");
      for (const token of [
        "FOR UPDATE",
        "UPDATE credentials",
        "INSERT INTO credentials",
        "DELETE FROM credentials",
      ]) {
        if (text.includes(token) && !allowed) {
          violations.push(`${path}: forbidden Vault credential write boundary token ${token}`);
        }
        if (allowed && (token === "INSERT INTO credentials" || token === "DELETE FROM credentials") && text.includes(token)) {
          violations.push(`${path}: OpenAI OAuth refresh adapter must not ${token}`);
        }
      }
    }
    expect(violations).toEqual([]);
  });

  test("platform provider key writes stay out of gateway runtime packages", async () => {
    const runtimeFiles = await collectTypeScriptFiles(new URL("provider-gateway/src/", packagesRoot));
    const scriptFiles = await collectTypeScriptFiles(new URL("scripts/", serviceRoot));
    const violations: string[] = [];

    for (const file of runtimeFiles) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      for (const token of [
        "INSERT INTO platform_provider_keys",
        "UPDATE platform_provider_keys",
        "DELETE FROM platform_provider_keys",
      ]) {
        if (text.includes(token)) {
          violations.push(`${path}: forbidden runtime platform key write token ${token}`);
        }
      }
    }

    const writerScripts = new Set<string>();
    for (const file of scriptFiles) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      if (
        text.includes("INSERT INTO platform_provider_keys") ||
        text.includes("UPDATE platform_provider_keys") ||
        text.includes("DELETE FROM platform_provider_keys")
      ) {
        writerScripts.add(path);
      }
    }

    expect(violations).toEqual([]);
    expect([...writerScripts]).toEqual(["scripts/platform-key.ts"]);
  });

  test("production command wires the OpenAI OAuth credential update-path adapter", async () => {
    const command = await readFile(new URL("provider-gateway/src/command.ts", packagesRoot), "utf8");

    expect(command).toContain("SQLOpenAIOAuthCredentialRefreshWriter");
    expect(command).toContain("openAIOAuthCredentialRefreshWriter");
  });

  test("provider-gateway logger uses the shared TypeScript observability wrapper", async () => {
    const logger = await readFile(new URL("provider-gateway/src/logger.ts", packagesRoot), "utf8");

    expect(logger).toContain("@tetral/ts-observability");
    expect(logger).toContain("createTetralJsonLogger");
    expect(logger).not.toContain("JSON.stringify({");
  });

  test("raw provider wire access is confined to clients and openai-oauth files", async () => {
    const files = await collectTypeScriptFiles(new URL("provider-gateway/src/", packagesRoot));
    const violations: string[] = [];
    for (const file of files) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      const allowed = path.endsWith("src/providers/clients.ts") || path.endsWith("src/providers/openai-oauth.ts");
      if (!allowed && (text.includes("rawBody") || text.includes("rawHeaders") || text.includes("authorization: Bearer"))) {
        violations.push(`${path}: raw provider wire touch outside approved files`);
      }
    }
    expect(violations).toEqual([]);
  });

  test("I7 clients raw-wire escape hatch is only the OpenAI Responses item id stripper", async () => {
    const clients = await readFile(new URL("provider-gateway/src/providers/clients.ts", packagesRoot), "utf8");

    expect(clients).toContain("function openAIResponsesWireFetch");
    expect(clients).toContain("function stripOpenAIStatelessItemIds");
    expect(clients).toContain("function stripOpenAIResponseInputItemIds");
    expect(clients).not.toContain("function stripObjectKey");
    expect(clients).not.toContain("openAICompatibleReasoningFetch");
    expect(clients).not.toContain("injectOpenAICompatibleReasoningContent");
    expect(clients).not.toContain("reasoning_content");
  });

  test("Gateway manifests expose headless service discovery and bounded egress intent", async () => {
    const service = await readFile(new URL("k8s/service.yaml", serviceRoot), "utf8");
    const networkPolicy = await readFile(new URL("k8s/networkpolicy.yaml", serviceRoot), "utf8");

    expect(service).toContain("clusterIP: None");
    expect(service).toContain("name: provider-grpc");
    expect(service).toContain("port: 9090");
    const egressIntent = networkPolicy.match(/tetral\.ai\/egress-intent:\s*"([^"]+)"/)?.[1]?.split(",").map((host) => host.trim()).sort();
    expect(egressIntent).toEqual(expect.arrayContaining([
      "api.anthropic.com",
      "api.deepseek.com",
      "api.kimi.com",
      "api.openai.com",
      "api.z.ai",
      "auth.openai.com",
      "chatgpt.com",
    ].sort()));
    expect(networkPolicy).toContain("- Egress");
    expect(networkPolicy).toContain("app.kubernetes.io/name: tetral-postgres");
    expect(networkPolicy).toContain("port: 5432");
    expect(networkPolicy).toContain("kubernetes.io/metadata.name: kube-system");
    expect(networkPolicy).toContain("port: 53");
    expect(networkPolicy).toContain("cidr: 0.0.0.0/0");
    expect(networkPolicy).toContain("port: 443");
  });
});

async function collectTypeScriptFiles(directoryUrl: URL): Promise<readonly string[]> {
  const entries = await readdir(directoryUrl, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const entryUrl = new URL(`${entry.name}${entry.isDirectory() ? "/" : ""}`, directoryUrl);
    if (entry.isDirectory()) {
      files.push(...(await collectTypeScriptFiles(entryUrl)));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(entryUrl.pathname);
    }
  }
  return files.sort((left, right) => left.localeCompare(right));
}

function workspaceRelative(filePath: string): string {
  return relative(serviceRoot.pathname, filePath).split(sep).join("/");
}

// The platform key writer targets a TIMESTAMPTZ column. Writing now()::text
// made PostgreSQL reject every insert, and the CLI's generic failure string
// hid the cause; both halves are pinned here.
test("platform key writer stores timestamps as timestamps and names unexpected failures", async () => {
  const script = await readFile(new URL("../../../../scripts/platform-key.ts", import.meta.url), "utf8");

  expect(script).not.toContain("now()::text");
  expect(script).toContain("now()");
  expect(script).toContain("error.constructor.name");
  expect(script).toContain("[REDACTED]");
});
