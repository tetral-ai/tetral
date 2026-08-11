import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import * as ts from "typescript";
import { MCP_BLOB_MAX_BYTES } from "../../src/formatter.js";
import { MCP_CONNECTOR_GRPC_MAX_MESSAGE_BYTES } from "../../src/server.js";
import {
  MCP_CLAIM_LEASE_SECONDS,
  MCP_CLAIM_RPC_TIMEOUT_MS,
  MCP_CONNECT_TIMEOUT_MS,
  MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS,
  MCP_REFRESH_HTTP_TIMEOUT_MS,
} from "../../src/phase-budgets.js";
import { MCP_CALL_TIMEOUT_SECONDS } from "../../src/client.js";
import { MCP_COMMIT_RPC_TIMEOUT_MS } from "../../src/bridge-client.js";
import {
  McpGrpcKeepaliveTimeMs,
  McpGrpcKeepaliveTimeoutMs,
  mcpGrpcClientKeepaliveOptions,
  mcpGrpcServerKeepaliveOptions,
} from "../../src/transport.js";

describe("mcp-connector static boundaries", () => {
  test("does not import provider-gateway or lowering packages", () => {
    const files = sourceFiles(join(import.meta.dir, "../../src"));
    const forbidden: string[] = [];
    for (const file of files) {
      const text = readFileSync(file, "utf8");
      if (importSpecifiers(text).some((specifier) =>
        specifier.includes("@tetral/gateway-lowering") ||
        specifier.includes("@tetral/provider-gateway") ||
        specifier.includes("packages/provider-gateway")
      )) {
        forbidden.push(file);
      }
    }

    expect(forbidden).toEqual([]);
  });

  test("import boundary parsing ignores comments and includes dynamic imports", () => {
    expect(importSpecifiers('// import "@tetral/provider-gateway";')).toEqual([]);
    expect(importSpecifiers('await import("@tetral/provider-gateway");')).toEqual([
      "@tetral/provider-gateway",
    ]);
  });

  test("command uses the production SDK client instead of a placeholder client", () => {
    const command = readFileSync(join(import.meta.dir, "../../src/command.ts"), "utf8");
    const client = readFileSync(join(import.meta.dir, "../../src/client.ts"), "utf8");

    expect(command).toContain("new McpSDKClient");
    expect(command).toContain("new McpConnectorMetricsRegistry");
    expect(command).toContain("createMcpConnectorHttpServer");
    expect(command).not.toContain("unavailableClient");
    expect(client).toContain("StreamableHTTPClientTransport");
    expect(client).toContain("onToolsListChanged");
  });

  test("logger uses the shared TypeScript observability wrapper", () => {
    const logger = readFileSync(join(import.meta.dir, "../../src/logger.ts"), "utf8");
    const command = readFileSync(join(import.meta.dir, "../../src/command.ts"), "utf8");

    expect(logger).toContain("@tetral/ts-observability");
    expect(logger).toContain("createTetralJsonLogger");
    expect(command).not.toContain("function jsonLogger");
    expect(command).not.toContain("JSON.stringify({ level:");
  });

  test("credential resolver delegates OAuth write-back to the Vault update path", () => {
    const credential = readFileSync(join(import.meta.dir, "../../src/credential.ts"), "utf8");

    expect(credential).toContain("SQLVaultGitHubMcpCredentialUpdatePath");
    expect(credential).toContain("refreshWriter.refreshOAuthCredential");
    for (const forbidden of [
      "FOR UPDATE",
      "UPDATE credentials",
      "INSERT INTO credentials",
      "DELETE FROM credentials",
      "auth_public_json =",
      "materializeRefreshedAuth",
      "publicAuthFromSecret",
      "crypto.subtle.encrypt",
    ]) {
      expect(credential).not.toContain(forbidden);
    }
    expect(credential).toContain("this.sql.begin");
    expect(credential).toContain("set_config('tetral.workspace_id'");
    expect(credential).not.toMatch(/\bencrypted_auth\s*=\s*\$/);
  });

  test("grpc rail can carry the largest inline MCP attachment plus overhead", () => {
    expect(MCP_CONNECTOR_GRPC_MAX_MESSAGE_BYTES).toBeGreaterThan(MCP_BLOB_MAX_BYTES);
    expect(MCP_CONNECTOR_GRPC_MAX_MESSAGE_BYTES).toBeGreaterThanOrEqual(11 * 1024 * 1024);
    expect(McpGrpcKeepaliveTimeMs).toBe(30 * 1000);
    expect(McpGrpcKeepaliveTimeoutMs).toBe(10 * 1000);
    expect(mcpGrpcClientKeepaliveOptions()).toEqual({
      "grpc.keepalive_time_ms": McpGrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": McpGrpcKeepaliveTimeoutMs,
      "grpc.keepalive_permit_without_calls": 0,
    });
    expect(mcpGrpcServerKeepaliveOptions()).toEqual({
      "grpc.keepalive_time_ms": McpGrpcKeepaliveTimeMs,
      "grpc.keepalive_timeout_ms": McpGrpcKeepaliveTimeoutMs,
    });
    const server = readFileSync(join(import.meta.dir, "../../src/server.ts"), "utf8");
    const bridgeClient = readFileSync(join(import.meta.dir, "../../src/bridge-client.ts"), "utf8");
    expect(server).toContain("...mcpGrpcServerKeepaliveOptions()");
    expect(bridgeClient.match(/mcpGrpcClientKeepaliveOptions\(\)/g)).toHaveLength(2);
  });

  test("durable claim lease is pinned in the Bridge-owned reservation store", () => {
    const bridgeStore = readFileSync(join(import.meta.dir, "../../../../../bridge/bridge_api_store.go"), "utf8");
    const bridgeTests = readFileSync(join(import.meta.dir, "../../../../../bridge/bridge_api_mcp_test.go"), "utf8");

    expect(bridgeStore).toContain("mcpClaimLeaseTTL       = 180 * time.Second");
    expect(bridgeTests).toContain("func TestPostgreSQLBridgeAPIStoreMCPToolResultClaimLease");
    expect(bridgeTests).toContain("base.Add(181 * time.Second)");
    expect(bridgeTests).toContain("want committed renewed reservation");
  });

  test("pins every individual claim-to-commit phase budget", () => {
    expect(MCP_CLAIM_LEASE_SECONDS).toBe(180);
    expect(MCP_CREDENTIAL_RESOLUTION_TIMEOUT_MS).toBe(15_000);
    expect(MCP_REFRESH_HTTP_TIMEOUT_MS).toBe(10_000);
    expect(MCP_CONNECT_TIMEOUT_MS).toBe(10_000);
    expect(MCP_CALL_TIMEOUT_SECONDS).toBe(120);
    expect(MCP_CLAIM_RPC_TIMEOUT_MS).toBe(10_000);
    expect(MCP_COMMIT_RPC_TIMEOUT_MS).toBe(10_000);
  });

  test("kubernetes manifest exposes the MCP connector internal metrics port", () => {
    const deployment = readFileSync(join(import.meta.dir, "../../../../k8s/deployment.yaml"), "utf8");
    const service = readFileSync(join(import.meta.dir, "../../../../k8s/service.yaml"), "utf8");
    const networkPolicy = readFileSync(join(import.meta.dir, "../../../../k8s/networkpolicy.yaml"), "utf8");

    expect(deployment).toContain("TETRAL_MCP_CONNECTOR_HTTP_ADDR");
    expect(deployment).toContain("TETRAL_MCP_CONNECTOR_GRPC_ADDR");
    expect(deployment).toContain("name: mcp-http");
    expect(deployment).toContain("containerPort: 8081");
    expect(deployment).toContain("name: mcp-grpc");
    expect(deployment).toContain("containerPort: 9091");
    expect(deployment).toContain("path: /readyz");
    expect(service).toContain("name: mcp-http");
    expect(service).toContain("port: 8081");
    expect(service).toContain("name: mcp-grpc");
    expect(service).toContain("port: 9091");
    expect(networkPolicy).toContain("api.githubcopilot.com");
    expect(networkPolicy).toContain("port: 8081");
    expect(networkPolicy).toContain("port: 9091");
  });
});

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

function sourceFiles(root: string): string[] {
  const files: string[] = [];
  for (const entry of readdirSync(root)) {
    const path = join(root, entry);
    if (statSync(path).isDirectory()) {
      files.push(...sourceFiles(path));
    } else if (path.endsWith(".ts")) {
      files.push(path);
    }
  }
  return files;
}
