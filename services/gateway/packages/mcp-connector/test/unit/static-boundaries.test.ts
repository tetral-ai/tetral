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
import { MCP_COMMIT_RPC_TIMEOUT_MS, bridgeMcpCommitGrpcChannelOptions } from "../../src/bridge-client.js";
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
    expect(bridgeMcpCommitGrpcChannelOptions()).toMatchObject(mcpGrpcClientKeepaliveOptions());
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
