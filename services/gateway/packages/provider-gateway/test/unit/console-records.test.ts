// Layer 1: the workspace's production sources, parsed in process.
import { expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import * as ts from "typescript";

const workspace = join(import.meta.dir, "../../../..");
const dbconnect = join(workspace, "../../internal/ts-dbconnect");

// Every record of the Gateway workspace and of the database library it runs
// goes through @tetral/ts-observability, which redacts and bounds it. A
// console call writes around that.
test("no production source of the workspace or its database library calls the console", () => {
  const calls: string[] = [];
  const sources = readdirSync(workspace, { recursive: true, encoding: "utf8" }).filter((file) =>
    /^(?:packages\/[^/]+\/src|scripts)\/.*\.ts$/.test(file) && !file.includes("/gen/") && !file.includes("node_modules/")).map((file) => join(workspace, file));
  const library = readdirSync(join(dbconnect, "src"), { recursive: true, encoding: "utf8" }).filter((file) => file.endsWith(".ts")).map((file) => join(dbconnect, "src", file));
  for (const file of [...sources, ...library]) {
    const source = ts.createSourceFile(file, readFileSync(file, "utf8"), ts.ScriptTarget.Latest, false, ts.ScriptKind.TS);
    const visit = (node: ts.Node): void => {
      if (
        ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression) &&
        ts.isIdentifier(node.expression.expression) && node.expression.expression.text === "console"
      ) {
        calls.push(`${relative(join(workspace, "../.."), file)}:${source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1}`);
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }

  expect(sources.length).toBeGreaterThan(50);
  expect(library.length).toBeGreaterThan(0);
  expect(calls).toEqual([]);
});
