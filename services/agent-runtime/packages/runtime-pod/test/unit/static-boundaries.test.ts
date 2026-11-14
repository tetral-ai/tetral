import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { extname, join, posix, relative } from "node:path";
import * as ts from "typescript";

// Each check reads the workspace through the TypeScript parser or a parsed
// package manifest, so a comment, a string or a reformatting is part of none.

const workspace = join(import.meta.dir, "../../../..");
const generated = /^packages\/protocol\/src\/gen(?:-bridge)?\//;
const providerSdkOrDatabaseClient = /^(?:ai|@ai-sdk\/.+|pg|postgres|postgres-js|drizzle.*|prisma|@prisma\/.+|kysely|sqlite.*|better-sqlite3|bun:sqlite)$/;

function files(directory: string): readonly string[] {
  return readdirSync(join(workspace, directory), { recursive: true, encoding: "utf8" })
    .filter((file) => file.endsWith(".ts") && !file.split("/").includes("node_modules"))
    .map((file) => join(directory, file))
    .sort();
}

function read(file: string): string {
  return readFileSync(join(workspace, file), "utf8");
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

// What a file does against the conventions of the workspace. A source file
// of a package answers to all of them; a test only to the two on types.
function conventionBreaks(text: string, production: boolean, readsEnvironment = false): readonly string[] {
  const source = ts.createSourceFile("convention.ts", text, ts.ScriptTarget.Latest, false, ts.ScriptKind.TS);
  const breaks = new Set<string>();
  const access = (node: ts.Node, object: string, property: string): boolean =>
    ts.isPropertyAccessExpression(node) && ts.isIdentifier(node.expression) &&
    node.expression.text === object && node.name.text === property;
  const visit = (node: ts.Node): void => {
    if (ts.isAsExpression(node) && node.type.kind === ts.SyntaxKind.AnyKeyword) {
      breaks.add("as any");
    }
    if (ts.isImportDeclaration(node) && node.importClause?.namedBindings !== undefined && ts.isNamedImports(node.importClause.namedBindings)) {
      const names = node.importClause.namedBindings.elements;
      if (names.some((name) => name.isTypeOnly) && names.some((name) => !name.isTypeOnly)) {
        breaks.add("types and values in one import");
      }
    }
    if (production && access(node, "process", "env") && !readsEnvironment) {
      breaks.add("process.env outside the configuration");
    }
    // Records go through @tetral/ts-observability, which redacts and bounds them.
    if (
      production && ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression) &&
      ts.isIdentifier(node.expression.expression) && node.expression.expression.text === "console"
    ) {
      breaks.add("console call");
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  if (production) {
    for (const specifier of importSpecifiers(text)) {
      if (specifier.startsWith(".") && extname(specifier) !== ".js" && extname(specifier) !== ".json") {
        breaks.add(`relative import without its extension (${specifier})`);
      }
      if ((specifier === "zod" || specifier.startsWith("zod/")) && specifier !== "zod/v4") {
        breaks.add(`zod imported as ${specifier}`);
      }
    }
    const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, text);
    for (let token = scanner.scan(); token !== ts.SyntaxKind.EndOfFileToken; token = scanner.scan()) {
      const comment = token === ts.SyntaxKind.SingleLineCommentTrivia || token === ts.SyntaxKind.MultiLineCommentTrivia;
      if (comment && /^\/[/*]+\s*@ts-(?:ignore|expect-error)/.test(scanner.getTokenText())) {
        breaks.add("type check suppressed");
      }
    }
  }
  return [...breaks].sort();
}

describe("Runtime source boundaries", () => {
  test("core imports neither the Runtime Pod nor gRPC", () => {
    const imports = files("packages/core/src").flatMap((file) =>
      importSpecifiers(read(file))
        .filter((specifier) => specifier.includes("runtime-pod") || specifier === "@tetral/agent-runtime" || specifier === "@grpc/grpc-js")
        .map((specifier) => `${file}: ${specifier}`));

    expect(imports).toEqual([]);
  });

  test("no package declares or imports a provider SDK or a database client", () => {
    const found: string[] = [];
    for (const manifest of ["package.json", "packages/core/package.json", "packages/runtime-pod/package.json"]) {
      const { dependencies = {} } = JSON.parse(read(manifest)) as { dependencies?: Record<string, string> };
      found.push(...Object.keys(dependencies).filter((name) => providerSdkOrDatabaseClient.test(name)).map((name) => `${manifest}: ${name}`));
    }
    for (const file of [...files("packages/core/src"), ...files("packages/runtime-pod/src")]) {
      found.push(...importSpecifiers(read(file)).filter((specifier) => providerSdkOrDatabaseClient.test(specifier)).map((specifier) => `${file}: ${specifier}`));
    }

    expect(found).toEqual([]);
  });

  test("every file follows the conventions of the workspace", () => {
    const breaks: string[] = [];
    for (const file of files("packages")) {
      const path = relative("", file);
      if (generated.test(path)) {
        continue;
      }
      const production = /^packages\/(?:core|runtime-pod)\/src\//.test(path);
      breaks.push(...conventionBreaks(read(file), production, path === "packages/runtime-pod/src/config.ts").map((found) => `${path}: ${found}`));
    }

    expect(breaks).toEqual([]);
  });

  test("the checks read code, and neither comments nor strings", () => {
    expect(importSpecifiers('// import "@tetral/agent-runtime";')).toEqual([]);
    expect(importSpecifiers('await import("@tetral/agent-runtime");')).toEqual(["@tetral/agent-runtime"]);
    expect(conventionBreaks([
      "// process.env, console.log(1), value as any, @ts-ignore in prose",
      'const text = "process.env console.log( as any @ts-ignore";',
      'import { value } from "./dep.js";',
      'import type { TypeName } from "./dep.js";',
    ].join("\n"), true)).toEqual([]);
    expect(conventionBreaks([
      'import { value, type TypeName } from "./dep";',
      'import { z } from "zod";',
      "// @ts-expect-error",
      "console.log(process.env.HOME as any);",
    ].join("\n"), true)).toEqual([
      "as any",
      "console call",
      "process.env outside the configuration",
      "relative import without its extension (./dep)",
      "type check suppressed",
      "types and values in one import",
      "zod imported as zod",
    ]);
  });

  test("the core build names every source file", () => {
    const { scripts = {} } = JSON.parse(read("packages/core/package.json")) as { scripts?: Record<string, string> };
    const roots = new Set((scripts.build ?? "").split(/\s+/).map((token) => posix.normalize(token)));

    expect(roots.size).toBeGreaterThan(1);
    expect(files("packages/core/src").map((file) => relative("packages/core", file)).filter((file) => !roots.has(file))).toEqual([]);
  });
});
