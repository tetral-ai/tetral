import { expect, test } from "bun:test";
import { readdirSync } from "node:fs";
import { join, relative } from "node:path";
import * as ts from "typescript";

const workspace = join(import.meta.dir, "../../../..");
const source = join(workspace, "packages/lowering/src");

// Lowering is pure: it turns one value into another. Every name it uses is
// declared by itself, by the protocol package, by the protobuf codec of the
// generated messages or by the language. Of what a runtime declares it uses
// the text codecs and their error, never the network, the process or a file.
const pureRuntimeNames = new Set(["DOMException", "TextDecoder", "TextEncoder"]);

test("lowering uses no name that a runtime declares, the text codecs apart", () => {
  const configuration = ts.getParsedCommandLineOfConfigFile(join(workspace, "tsconfig.json"), {}, {
    ...ts.sys,
    onUnRecoverableConfigFileDiagnostic: (diagnostic) => {
      throw new Error(ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n"));
    },
  })!;
  const files = readdirSync(source, { recursive: true, encoding: "utf8" })
    .filter((file) => file.endsWith(".ts"))
    .map((file) => join(source, file));
  const program = ts.createProgram(files, { ...configuration.options, lib: ["lib.es2024.d.ts"], types: [] });
  const checker = program.getTypeChecker();

  const owner = (declaration: ts.Declaration): string | undefined => {
    const file = declaration.getSourceFile();
    if (program.isSourceFileDefaultLibrary(file)) {
      return undefined;
    }
    const dependency = /.*\/node_modules\/((?:@[^/]+\/)?[^/]+)\//.exec(file.fileName)?.[1];
    if (dependency !== undefined) {
      return dependency === "@bufbuild/protobuf" ? undefined : dependency;
    }
    const path = relative(workspace, file.fileName);
    return path.startsWith("packages/lowering/src/") || path.startsWith("packages/protocol/src/") ? undefined : path;
  };
  const foreign = new Set<string>();
  for (const path of files) {
    const file = program.getSourceFile(path)!;
    const name = relative(workspace, path);
    // A name nothing declares is a runtime's too: its types are not loaded here.
    for (const diagnostic of program.getSemanticDiagnostics(file)) {
      foreign.add(`${name}: ${ts.flattenDiagnosticMessageText(diagnostic.messageText, " ")}`);
    }
    const visit = (node: ts.Node): void => {
      const member = (): boolean => ts.isPropertyAccessExpression(node.parent) && node.parent.name === node;
      if (ts.isIdentifier(node) && !member() && !pureRuntimeNames.has(node.text)) {
        const named = checker.getSymbolAtLocation(node);
        const symbol = named !== undefined && (named.flags & ts.SymbolFlags.Alias) !== 0 ? checker.getAliasedSymbol(named) : named;
        for (const declaration of symbol?.declarations ?? []) {
          const from = owner(declaration);
          if (from !== undefined) {
            foreign.add(`${name}: ${node.text} is declared by ${from}`);
          }
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(file);
  }

  expect([...foreign].sort()).toEqual([]);
});
