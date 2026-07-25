import { describe, expect, test } from "bun:test";
import { readdir, readFile, stat } from "node:fs/promises";
import { extname, relative, sep } from "node:path";

const runtimeRoot = new URL("../../", import.meta.url);
const sourceRoot = new URL("../../src/", import.meta.url);
const gatewayImport = "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

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
  return files;
}

function runtimeRelativePath(filePath: string): string {
  return relative(runtimeRoot.pathname, filePath).split(sep).join("/");
}

function packageBuildScript(text: string): string {
  const packageJson = JSON.parse(text) as { scripts?: Record<string, string> };
  return packageJson.scripts?.build ?? "";
}

describe("static Runtime Pod Gateway boundaries", () => {
  test("packages no longer expose provider SDK, provider smoke, or Runtime provider adapter build roots", async () => {
    for (const packageUrl of [new URL("../../../../package.json", import.meta.url), new URL("../../package.json", import.meta.url)]) {
      const packageText = await readFile(packageUrl, "utf8");
      const packageJson = JSON.parse(packageText) as {
        scripts?: Record<string, string>;
        dependencies?: Record<string, string>;
      };
      const buildScript = packageBuildScript(packageText);
      const serialized = JSON.stringify(packageJson);
      expect(serialized).not.toContain("@ai-sdk/openai");
      expect(serialized).not.toContain("@ai-sdk/anthropic");
      expect(serialized).not.toContain("\"ai\"");
      expect(serialized).not.toContain("smoke:providers");
      expect(serialized).not.toContain("src/providers/");
      expect(packageJson.dependencies?.["@tetral/gateway-protocol"]).toBeString();
      expect(buildScript.startsWith("rm -rf dist && bun build ")).toBe(true);
      expect(packageJson.scripts?.build).toContain("src/llm/llm-service.ts");
      expect(packageJson.scripts?.build).toContain("src/agent-loop/provider-call-assembly.ts");
    }
  });

  test("Runtime Pod source has no provider lowering directory or provider SDK imports", async () => {
    expect(await exists(new URL("../../src/providers/", import.meta.url))).toBe(false);
    expect(await exists(new URL("../../src/scripts/smoke-providers.ts", import.meta.url))).toBe(false);

    const files = await collectTypeScriptFiles(sourceRoot);
    const violations: string[] = [];
    for (const filePath of files) {
      const text = await readFile(filePath, "utf8");
      const relativePath = runtimeRelativePath(filePath);
      for (const forbidden of [
        "@ai-sdk/openai",
        "@ai-sdk/anthropic",
        "from \"ai\"",
        "from 'ai'",
        "createOpenAI",
        "createAnthropic",
        "ProviderModel",
        "ProviderPackage",
        "ProviderApiKind",
        "transformRuntimeProviderRequest",
        "prepareProviderAccess",
        "normalizeRawChunkToLLMEvent",
        "TETRAL_AGENT_PROVIDER_DUMMY_API_KEY",
        "TETRAL_AGENT_OPENAI_BASE_URL",
        "TETRAL_AGENT_ANTHROPIC_BASE_URL",
      ]) {
        if (text.includes(forbidden)) {
          violations.push(`${relativePath}: forbidden Runtime provider boundary token (${forbidden})`);
        }
      }
      if (text.includes("process.env")) {
        violations.push(`${relativePath}: process.env outside config`);
      }
      if (/\bas\s+any\b/.test(text)) {
        violations.push(`${relativePath}: as any`);
      }
      const importMatches = text.matchAll(/\b(?:import|export)\s+(?:[^"']*?\s+from\s+)?["']([^"']+)["']/g);
      for (const match of importMatches) {
        const importSpecifier = match[1];
        if (importSpecifier === undefined) {
          continue;
        }
        if ((importSpecifier === "zod" || importSpecifier.startsWith("zod/")) && importSpecifier !== "zod/v4") {
          violations.push(`${relativePath}: zod import must use zod/v4 (${importSpecifier})`);
        }
        if (importSpecifier.startsWith(".") && extname(importSpecifier) !== ".js") {
          violations.push(`${relativePath}: relative import without .js suffix (${importSpecifier})`);
        }
      }
    }

    expect(violations).toEqual([]);
  });

  test("ProviderRequest, ProviderStreamEvent, and RequestUsage are consumed only from the Gateway generated protocol", async () => {
    const files = await collectTypeScriptFiles(sourceRoot);
    const violations: string[] = [];
    for (const filePath of files) {
      const relativePath = runtimeRelativePath(filePath);
      const text = await readFile(filePath, "utf8");
      if (/\bProviderUsage\b/.test(text)) {
        violations.push(`${relativePath}: ProviderUsage proto type was replaced by RequestUsage`);
      }
      if (/\bProviderRequest\b|\bProviderStreamEvent\b|\bRequestUsage\b/.test(text) && !text.includes(gatewayImport)) {
        violations.push(`${relativePath}: provider protocol type not imported from Gateway generated protocol`);
      }
    }

    expect(violations).toEqual([]);
  });

  test("RequestTurn scope owns provider stream and tool fibers", async () => {
    const text = await readFile(new URL("../../src/agent-loop/agent-loop.ts", import.meta.url), "utf8");
    const requestTurnStart = text.indexOf("const requestTurnScope = yield* Scope.make()");
    const providerStart = text.indexOf("const providerStream =", requestTurnStart);
    const streamExit = text.indexOf("const streamExit =", providerStart);
    expect(requestTurnStart).toBeGreaterThanOrEqual(0);
    expect(providerStart).toBeGreaterThanOrEqual(0);
    expect(streamExit).toBeGreaterThan(providerStart);

    const providerFiberSection = text.slice(providerStart, streamExit);
    expect(providerFiberSection).toContain("Effect.forkIn(requestTurnScope)");
    expect(providerFiberSection).not.toContain("forkDetach");
    expect(text).toContain("requestTurnScope,");
    expect(text).toContain("Effect.forkIn(state.requestTurnScope)");
    expect(text).toContain("Scope.close(requestTurnScope, Exit.void)");
    expect(text).toContain("Effect.timeoutOption(\"25 millis\")");
  });

  test("compaction owns its provider stream without creating a RequestTurn", async () => {
    const text = await readFile(new URL("../../src/agent-loop/agent-loop.ts", import.meta.url), "utf8");
    const compactionStart = text.indexOf("const compactionScope = yield* Scope.make()");
    const providerStart = text.indexOf("const providerStream =", compactionStart);
    const requestTurnStart = text.indexOf("const requestTurnScope = yield* Scope.make()", compactionStart);
    expect(compactionStart).toBeGreaterThanOrEqual(0);
    expect(providerStart).toBeGreaterThan(compactionStart);
    expect(requestTurnStart).toBeGreaterThan(providerStart);

    const compactionSection = text.slice(compactionStart, requestTurnStart);
    expect(compactionSection).toContain("Effect.forkIn(compactionScope)");
    expect(compactionSection).toContain("Scope.close(compactionScope, Exit.void)");
    expect(compactionSection).not.toContain("requestTurnScope");
    expect(compactionSection).not.toContain("forkDetach");
  });

  test("Runtime package build roots include every production TS file outside deleted provider paths", async () => {
    const buildScript = packageBuildScript(await readFile(new URL("../../package.json", import.meta.url), "utf8"));
    const omittedFiles = (await collectTypeScriptFiles(sourceRoot))
      .map(runtimeRelativePath)
      .filter((relativePath) => !buildScript.includes(relativePath));

    expect(omittedFiles).toEqual([]);
  });
});

async function exists(url: URL): Promise<boolean> {
  try {
    await stat(url);
    return true;
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}
