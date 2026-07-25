import { describe, expect, test } from "bun:test";
import { readdir, readFile } from "node:fs/promises";
import { GatewayAuxiliaryRuleIds, GatewayProviderRules } from "../src/rules/index.js";

const unitTestRoot = new URL("unit/", import.meta.url);

describe("Gateway lowering rules coverage", () => {
  test("every implemented provider rule ID appears in a named unit test", async () => {
    const testSource = (await Promise.all((await collectTestFiles(unitTestRoot)).map((file) => readFile(file, "utf8")))).join("\n");
    const ruleIds = [...GatewayProviderRules.flatMap((rules) => rules.ruleIds), ...GatewayAuxiliaryRuleIds];
    const missing = ruleIds.filter((ruleId) => !new RegExp(`\\btest\\(\\s*["'\`][^"'\`]*${escapeRegExp(ruleId)}\\b`).test(testSource));

    expect(missing).toEqual([]);
  });
});

async function collectTestFiles(directoryUrl: URL): Promise<readonly URL[]> {
  const entries = await readdir(directoryUrl, { withFileTypes: true });
  const files: URL[] = [];
  for (const entry of entries) {
    const entryUrl = new URL(`${entry.name}${entry.isDirectory() ? "/" : ""}`, directoryUrl);
    if (entry.isDirectory()) {
      files.push(...(await collectTestFiles(entryUrl)));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".test.ts")) {
      files.push(entryUrl);
    }
  }
  return files.sort((left, right) => left.pathname.localeCompare(right.pathname));
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
