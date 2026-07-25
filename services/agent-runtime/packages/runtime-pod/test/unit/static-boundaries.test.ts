import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { extname, relative, sep } from "node:path";
import * as ts from "typescript";

const workspaceRoot = new URL("../../../../", import.meta.url);
const coreRoot = new URL("packages/core/", workspaceRoot);
const podRoot = new URL("packages/runtime-pod/", workspaceRoot);
const protocolRoot = new URL("packages/protocol/", workspaceRoot);
const forbiddenGoInternalPath = ["engine", "internal"].join("/");

describe("Runtime Pod static boundaries", () => {
  test("core never imports the Runtime Pod and remains session-id-facing", async () => {
    const coreFiles = await collectTypeScriptFiles(new URL("src/", coreRoot));
    const violations: string[] = [];

    for (const file of coreFiles) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      if (importSpecifiers(text).some((specifier) => specifier.includes("runtime-pod"))) {
        violations.push(`${path}: imports runtime-pod`);
      }
      for (const token of ["workspace_id", "binding_id", "binding_generation", "AgentRuntimeService", "SessionBindingService"]) {
        if (text.includes(token)) {
          violations.push(`${path}: forbidden core token ${token}`);
        }
      }
      if (/from\s+["']@grpc\/grpc-js["']/.test(text)) {
        violations.push(`${path}: forbidden core gRPC metadata import`);
      }
    }

    expect(violations).toEqual([]);
  });

  test("import boundary parsing ignores comments and includes dynamic imports", () => {
    expect(importSpecifiers('// import "@tetral/agent-runtime";')).toEqual([]);
    expect(importSpecifiers('await import("@tetral/agent-runtime");')).toEqual([
      "@tetral/agent-runtime",
    ]);
  });

  test("Runtime Pod owns grpc shell without database or Go internal coupling", async () => {
    const packageJson = JSON.parse(await readFile(new URL("package.json", podRoot), "utf8")) as {
      readonly dependencies?: Record<string, string>;
    };
    const files = await collectTypeScriptFiles(new URL("src/", podRoot));
    const violations: string[] = [];

    for (const dependency of ["pg", "postgres", "drizzle", "prisma"]) {
      expect(packageJson.dependencies?.[dependency]).toBeUndefined();
    }

    for (const file of files) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      for (const token of ["DATABASE_URL", "session_events", "sessions.status", "session_runtime_bindings", "processed_at"]) {
        if (text.includes(token)) {
          violations.push(`${path}: forbidden DB token ${token}`);
        }
      }
      if (/from\s+["'](?:pg|postgres|drizzle|@prisma\/client|prisma)["']/.test(text)) {
        violations.push(`${path}: database import`);
      }
      if (/\b(?:SELECT|INSERT|UPDATE|DELETE)\s+/i.test(text)) {
        violations.push(`${path}: SQL construction`);
      }
      if (text.includes(forbiddenGoInternalPath)) {
        violations.push(`${path}: Go internal import`);
      }
    }

    expect(violations).toEqual([]);
  });

  test("protocol exposes Runtime Pod command envelope identity", async () => {
    const protocol = await readFile(
      new URL("src/gen/tetral/agent_runtime/v1/agent_runtime.ts", protocolRoot),
      "utf8",
    );

    expect(protocol).toContain("export interface RuntimeInputCommandRequest");
    expect(protocol).toContain("sessionThreadId: string;");
    expect(protocol).toContain("runtimeInputId: string;");
    expect(protocol).toContain("RuntimeCommandKind");
    expect(protocol).toContain("bindingGeneration: number;");
  });

  test("production build scripts clean stale Runtime Pod dist before bundling", async () => {
    for (const packageUrl of [new URL("package.json", workspaceRoot), new URL("package.json", podRoot)]) {
      const packageJson = JSON.parse(await readFile(packageUrl, "utf8")) as {
        readonly scripts?: Record<string, string>;
      };

      expect(packageJson.scripts?.build?.startsWith("rm -rf dist && bun build ")).toBe(true);
    }
  });

  test("Runtime Pod source follows Bun moved-package conventions", async () => {
    const files = await collectTypeScriptFiles(new URL("src/", podRoot));
    const violations: string[] = [];

    for (const file of files) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      if (!path.endsWith("config.ts") && text.includes("process.env")) {
        violations.push(`${path}: process.env outside config`);
      }
      if (/\bconsole\.log\s*\(/.test(text)) {
        violations.push(`${path}: production console.log`);
      }
      if (/\bas\s+any\b/.test(text)) {
        violations.push(`${path}: as any`);
      }
      if (/@ts-ignore|@ts-expect-error/.test(text)) {
        violations.push(`${path}: ts suppression`);
      }
      if ((text.includes("zod") || text.includes("from \"zod")) && !text.includes("zod/v4")) {
        violations.push(`${path}: zod import must use zod/v4`);
      }
      for (const specifier of importSpecifiers(text)) {
        if (specifier.startsWith(".") && extname(specifier) !== ".js") {
          violations.push(`${path}: relative import without .js suffix (${specifier})`);
        }
        if (specifier === "@grpc/grpc-js" && !text.includes("Server")) {
          continue;
        }
      }
    }

    expect(violations).toEqual([]);
  });

  test("workspace TypeScript conventions reject mixed imports and limit generated as-any exceptions", async () => {
    const files = await collectTypeScriptFiles(new URL("packages/", workspaceRoot));
    const violations: string[] = [];
    let generatedAsAnyCount = 0;

    expect(collectMixedTypeValueImportViolations("src/bad.ts", 'import { value, type TypeName } from "./dep.js";')).toEqual([
      "src/bad.ts: mixed type/value import",
    ]);
    expect(
      collectMixedTypeValueImportViolations(
        "src/good.ts",
        'import { value } from "./dep.js";\nimport type { TypeName } from "./dep.js";',
      ),
    ).toEqual([]);

    for (const file of files) {
      const path = workspaceRelative(file);
      const text = await readFile(file, "utf8");
      const generatedProtocol = path.startsWith("packages/protocol/src/gen/") || path.startsWith("packages/protocol/src/gen-bridge/");
      const staticConventionTest = path.endsWith("/static-boundaries.test.ts") || path.endsWith("/session-run-static-boundaries.test.ts");
      if (!generatedProtocol && !staticConventionTest) {
        violations.push(...collectMixedTypeValueImportViolations(path, text));
      }

      if (!/\bas\s+any\b/.test(text)) {
        continue;
      }
      if (generatedProtocol) {
        generatedAsAnyCount += 1;
        continue;
      }
      if (path.endsWith("/static-boundaries.test.ts") || path.endsWith("/session-run-static-boundaries.test.ts")) {
        continue;
      }
      violations.push(`${path}: as any outside generated protocol`);
    }

    // ts-proto emits `as any` in generated protocol stubs; hand-written source must not.
    expect(generatedAsAnyCount).toBeGreaterThan(0);
    expect(violations).toEqual([]);
  });

  test("shutdown code avoids future-obligation markers and owns bounded active-run settlement", async () => {
    const lifecycle = await readFile(new URL("src/lifecycle.ts", podRoot), "utf8");
    const futureObligationMarker = ["TO", "DO"].join("");

    expect(lifecycle).not.toContain(futureObligationMarker);
    expect(lifecycle).toContain("shutdownActiveRuns");
    expect(lifecycle).toContain("drainTimeoutMs");
    expect(lifecycle).toContain("runtime pod shutdown drain timed out");
  });

  test("production command entrypoint owns deployable Runtime Pod composition", async () => {
    const command = await readFile(new URL("src/command.ts", podRoot), "utf8");
    const coreHosts = await readFile(new URL("src/core-hosts.ts", podRoot), "utf8");
    const toolRunner = await readFile(new URL("src/tool-runner.ts", podRoot), "utf8");
    const workspacePackage = JSON.parse(await readFile(new URL("package.json", workspaceRoot), "utf8")) as {
      readonly scripts?: Record<string, string>;
    };
    const podPackage = JSON.parse(await readFile(new URL("package.json", podRoot), "utf8")) as {
      readonly scripts?: Record<string, string>;
    };

    expect(workspacePackage.scripts?.build).toContain("packages/runtime-pod/src/command.ts");
    expect(podPackage.scripts?.build).toContain("src/command.ts");
    expect(command).toContain("loadRuntimePodConfigFromProcessEnv");
    expect(command).toContain("createJsonLogger");
    expect(command).toContain("KubernetesTokenReviewClient");
    expect(command).not.toContain("GrpcBackendSessionBindingClient");
    expect(command).not.toContain("RuntimePodSessionBindingAdapter");
    expect(command).toContain("buildRuntimeCoreHosts");
    expect(command).toContain("BridgeAPIContextLoader");
    expect(command).toContain("BridgeAPIEventWriter");
    expect(command).toContain("RuntimePodGatewayClient");
    expect(command).toContain("channelOptions: gatewayGrpcChannelOptions()");
    expect(toolRunner).toContain("new ProviderGatewayServiceClient(options.webAddress, credentials.createInsecure(), grpcClientChannelOptions())");
    expect(toolRunner).toContain("new McpConnectorServiceClient(options.mcpConnectorAddress, credentials.createInsecure(), grpcClientChannelOptions())");
    expect(command).toContain("RuntimePodToolRunner");
    expect(command).toContain("createToolCatalog");
    expect(command).toContain("includeSubAgentTools: true");
    expect(command).toContain("providerCallRuntime");
    expect(command).toContain('approvalMode: "ask_for_approval"');
    expect(command).toContain("runtimePolicy");
    expect(command).toContain("runTool");
    expect(command).toContain("createLLMService");
    expect(command).not.toContain("RuntimePodBridgeReleaseBinding");
    expect(command).toContain("RuntimeHotMessageStore");
    expect(command).not.toContain("FailClosedRuntimeMessageStore");
    expect(command).not.toContain("Gateway provider stream client is not wired yet");
    expect(command).toContain('process.once("SIGTERM"');
    expect(command).toContain('process.once("SIGINT"');
    expect(coreHosts).toContain("SessionManager.layer");
    expect(coreHosts).toContain("SessionRunHost.layer");
    expect(coreHosts).not.toContain("sessionBinding");
    expect(coreHosts).not.toContain("RuntimePodLocalReleaseBinding");
    expect(coreHosts).not.toContain("GrpcBackendSessionBindingClient");
  });

  test("Runtime Pod uses headless Gateway DNS with grpc-js round-robin", async () => {
    const gatewayClient = await readFile(new URL("src/gateway-client.ts", podRoot), "utf8");
    const deployment = await readFile(new URL("../../k8s/deployment.yaml", podRoot), "utf8");

    expect(gatewayClient).toContain("\"grpc.service_config\"");
    expect(gatewayClient).toContain("round_robin");
    expect(deployment).toContain("TETRAL_GATEWAY_GRPC_ADDR");
    expect(deployment).toContain("dns:///gateway.tetral-system.svc.cluster.local:9090");
    expect(deployment).toContain("TETRAL_MCP_CONNECTOR_GRPC_ADDR");
    expect(deployment).toContain("dns:///gateway.tetral-system.svc.cluster.local:9091");
    expect(deployment).toContain("TETRAL_WEB_CONNECTOR_GRPC_ADDR");
    expect(deployment).toContain("dns:///gateway.tetral-system.svc.cluster.local:9092");
    // Presence is the invariant — the workload refuses to start without it.
    // Which model reviews approvals is an operator cost decision, so the value
    // is deliberately not pinned here.
    expect(deployment).toContain("TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL");
  });

  test("Runtime Pod log records are explicit safe diagnostics only", async () => {
    const files = await collectTypeScriptFiles(new URL("src/", podRoot));
    const violations: string[] = [];
    const forbiddenLogSources = [
      "metadata",
      "authorization",
      "token",
      "payload",
      "body",
      "content",
      "provider",
      "kubernetesApiServerUrl",
      "apiServerUrl",
      "TokenReview",
      "runtimePodName",
      "runtimePodUid",
      "runtimePodIp",
      "ownPod",
    ];

    for (const file of files) {
      const text = await readFile(file, "utf8");
      const path = workspaceRelative(file);
      for (const snippet of loggerRecordSnippets(text)) {
        for (const forbidden of forbiddenLogSources) {
          if (snippet.includes(forbidden)) {
            violations.push(`${path}: logger record includes forbidden source ${forbidden}`);
          }
        }
      }
    }

    expect(violations).toEqual([]);
  });

  test("Runtime Pod logger uses the shared TypeScript observability wrapper", async () => {
    const logger = await readFile(new URL("src/logger.ts", podRoot), "utf8");

    expect(logger).toContain("@tetral/ts-observability");
    expect(logger).toContain("createTetralJsonLogger");
    expect(logger).not.toContain("JSON.stringify({");
  });

  test("approval reviewer work is Effect-owned with no detached Promise launch", async () => {
    const reviewer = await readFile(new URL("src/approval-reviewer.ts", podRoot), "utf8");
    const agentLoop = await readFile(new URL("src/agent-loop/agent-loop.ts", coreRoot), "utf8");

    expect(reviewer).toContain("return (request) => Effect.gen(function* ()");
    expect(reviewer).toContain("yield* manager.fork(");
    expect(reviewer).not.toContain("return async (request)");
    expect(reviewer).not.toMatch(/\bvoid\s*\(\s*async\s*\(/);
    expect(agentLoop).toContain(") => Effect.Effect<ApprovalReviewerOutcome, never>;");
    expect(agentLoop).not.toContain("yield* Effect.promise(async () =>");
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

function importSpecifiers(text: string): readonly string[] {
  const source = ts.createSourceFile("boundary.ts", text, ts.ScriptTarget.Latest, false, ts.ScriptKind.TS);
  const specifiers: string[] = [];
  const visit = (node: ts.Node): void => {
    if (
      (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) &&
      node.moduleSpecifier !== undefined &&
      ts.isStringLiteralLike(node.moduleSpecifier)
    ) {
      specifiers.push(node.moduleSpecifier.text);
    } else if (
      ts.isImportEqualsDeclaration(node) &&
      ts.isExternalModuleReference(node.moduleReference) &&
      node.moduleReference.expression !== undefined &&
      ts.isStringLiteralLike(node.moduleReference.expression)
    ) {
      specifiers.push(node.moduleReference.expression.text);
    } else if (
      ts.isCallExpression(node) &&
      (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
        (ts.isIdentifier(node.expression) && node.expression.text === "require")) &&
      node.arguments.length === 1 &&
      ts.isStringLiteralLike(node.arguments[0]!)
    ) {
      specifiers.push(node.arguments[0]!.text);
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  return specifiers;
}

function workspaceRelative(filePath: string): string {
  return relative(workspaceRoot.pathname, filePath).split(sep).join("/");
}

function loggerRecordSnippets(text: string): readonly string[] {
  return [...text.matchAll(/\blogger\.(?:info|error)\s*\(\s*\{[\s\S]*?\}\s*\)/g)].map((match) => match[0] ?? "");
}

function collectMixedTypeValueImportViolations(path: string, text: string): readonly string[] {
  const violations: string[] = [];
  for (const match of text.matchAll(/\bimport\s*\{(?<imports>[^}]*)\}\s*from\s*["'][^"']+["']/gs)) {
    const imports = match.groups?.imports;
    if (imports === undefined) {
      continue;
    }
    if (/\btype\s+[A-Za-z_$][\w$]*/.test(imports) && /\b(?!type\b)[A-Za-z_$][\w$]*\b/.test(imports.replace(/\btype\s+[A-Za-z_$][\w$]*/g, ""))) {
      violations.push(`${path}: mixed type/value import`);
    }
  }
  return violations;
}
