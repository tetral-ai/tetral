import { describe, expect, test } from "bun:test";
import { SessionConfiguration } from "../../src/session/session-configuration.js";
import type { RuntimeConfigurationPatch } from "../../src/session/session-configuration.js";

function config(generation: number, contentJson = `{"generation":${generation}}`): RuntimeConfigurationPatch {
  return {
    generation,
    contentJson,
    installedBuiltinFamily: "gpt",
  };
}

function manifest(
  mcpServerName: string,
  generation: number,
  manifestETag: string,
): RuntimeConfigurationPatch {
  return {
    generation,
    mcpServerName,
    manifestETag,
    manifestReadiness: "ready",
    contentJson: JSON.stringify({ mcpServerName, generation }),
  };
}

describe("SessionConfiguration", () => {
  test("advances config and each MCP server through independent generation gates", () => {
    const configuration = new SessionConfiguration();

    expect(configuration.apply(config(2))).toBe("applied");
    expect(configuration.apply(config(1))).toBe("stale");
    expect(configuration.apply(manifest("linear", 4, "etag-4"))).toBe("applied");
    expect(configuration.apply(manifest("github", 1, "etag-1"))).toBe("applied");
    expect(configuration.apply(manifest("linear", 3, "etag-3"))).toBe("stale");

    expect(configuration.configGeneration()).toBe(2);
    expect(configuration.manifestGeneration("linear")).toBe(4);
    expect(configuration.manifestGeneration("github")).toBe(1);
  });

  test("captures a detached immutable execution snapshot", () => {
    const configuration = new SessionConfiguration();
    configuration.apply(config(3));
    configuration.apply(manifest("linear", 2, "etag-2"));

    const snapshot = configuration.snapshot({
      currentModel: { providerId: "openai", modelId: "gpt-5.5" },
      approvalMode: "ask_for_approval",
      toolPolicyJson: "{\"allow\":[]}",
      toolCatalogJson: "{\"tools\":[]}",
    });
    configuration.apply(config(4));
    configuration.apply(manifest("linear", 3, "etag-3"));

    expect(snapshot.configGeneration).toBe(3);
    expect(snapshot.installedBuiltinFamily).toBe("gpt");
    expect(snapshot.mcpManifests.linear?.generation).toBe(2);
    expect(snapshot.currentModel).toEqual({ providerId: "openai", modelId: "gpt-5.5" });
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.mcpManifests)).toBe(true);
  });
});
