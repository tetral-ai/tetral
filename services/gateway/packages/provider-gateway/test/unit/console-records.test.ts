import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import * as ts from "typescript";

const workspace = join(import.meta.dir, "../../../..");

// Every record of the Gateway workspace goes through @tetral/ts-observability,
// which redacts and bounds it. A console call writes around that.
test("no production source of the workspace calls the console", () => {
  const calls: string[] = [];
  const sources = readdirSync(workspace, { recursive: true, encoding: "utf8" }).filter((file) =>
    /^(?:packages\/[^/]+\/src|scripts)\/.*\.ts$/.test(file) && !file.includes("/gen/") && !file.includes("node_modules/"));
  for (const file of sources) {
    const source = ts.createSourceFile(file, readFileSync(join(workspace, file), "utf8"), ts.ScriptTarget.Latest, false, ts.ScriptKind.TS);
    const visit = (node: ts.Node): void => {
      if (
        ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression) &&
        ts.isIdentifier(node.expression.expression) && node.expression.expression.text === "console"
      ) {
        calls.push(`${file}:${source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1}`);
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }

  expect(sources.length).toBeGreaterThan(50);
  expect(calls).toEqual([]);
});
