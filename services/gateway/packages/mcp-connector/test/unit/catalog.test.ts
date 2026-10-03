import { describe, expect, test } from "bun:test";
import { requireAdapter, adapterById, MCP_ADAPTERS } from "../../src/adapters/registry.js";

describe("MCP adapter registry", () => {
  test("pins registered GitHub and Slack endpoints", () => {
    expect(MCP_ADAPTERS).toEqual([
      { id: "github", endpoint: "https://api.githubcopilot.com/mcp/", requestHeaders: { "X-MCP-Toolsets": "default,actions" } },
      { id: "slack", endpoint: "https://mcp.slack.com/mcp", requestHeaders: {} },
    ]);
    expect(adapterById("github")).toEqual(MCP_ADAPTERS[0]!);
  });

  test("keeps the default alias rather than enumerating its constituent toolsets", () => {
    expect(MCP_ADAPTERS[0]?.requestHeaders["X-MCP-Toolsets"]).toBe("default,actions");
    expect(MCP_ADAPTERS[0]?.requestHeaders["X-MCP-Toolsets"]?.split(",")).toEqual(["default", "actions"]);
  });

  test("accepts only registered endpoint variants and rejects unregistered URLs", () => {
    expect(requireAdapter("https://api.githubcopilot.com/mcp")).toEqual(MCP_ADAPTERS[0]!);
    expect(() => requireAdapter("https://API.GITHUBCOPILOT.COM/mcp")).toThrow("Unsupported MCP server endpoint");
    expect(() => requireAdapter("https://api.githubcopilot.com:443/mcp")).toThrow("Unsupported MCP server endpoint");
    expect(() => requireAdapter("https://not-github.example.com/mcp")).toThrow("Unsupported MCP server endpoint");
    expect(() => requireAdapter("https://api.githubcopilot.com/mcp//")).toThrow("Unsupported MCP server endpoint");
    expect(() => requireAdapter("https://api.githubcopilot.com/mcp/?token=secret")).toThrow("Unsupported MCP server endpoint");
    expect(() => requireAdapter("https://api.githubcopilot.com/mcp/#fragment")).toThrow("Unsupported MCP server endpoint");
    expect(() => requireAdapter("https://user:pass@api.githubcopilot.com/mcp/")).toThrow("Unsupported MCP server endpoint");
  });
});
