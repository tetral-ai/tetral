// Layer 1: the AI SDK pins of the package manifests, read in process.
import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";

// The provider wire a release speaks is the AI SDK's at one exact version.
// The workspace and the gateway package each declare it, and a range in
// either would let an install pick another.
test("the workspace and the provider gateway pin the same exact AI SDK versions", async () => {
  const [workspace, gateway] = await Promise.all(["../../../../package.json", "../../package.json"].map(async (path) => {
    const manifest = JSON.parse(await readFile(new URL(path, import.meta.url), "utf8")) as {
      dependencies?: Record<string, string>;
    };
    return Object.fromEntries(Object.entries(manifest.dependencies ?? {})
      .filter(([name]) => name === "ai" || name.startsWith("@ai-sdk/")));
  }));

  expect(Object.keys(gateway!)).toContain("ai");
  expect(workspace).toEqual(gateway!);
  for (const [name, version] of Object.entries(gateway!)) {
    expect(`${name}@${version}`).toMatch(/@\d+\.\d+\.\d+$/);
  }
});
