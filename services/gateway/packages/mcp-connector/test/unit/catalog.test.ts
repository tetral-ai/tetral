import { describe, expect, test } from "bun:test";
import { assertCatalogURL, catalogEntryByName, MCP_CATALOG } from "../../src/catalog.js";

describe("MCP catalog", () => {
  test("pins the closed GitHub catalog entry", () => {
    expect(MCP_CATALOG).toEqual([
      { name: "github", url: "https://api.githubcopilot.com/mcp/" },
    ]);
    expect(catalogEntryByName("github")).toEqual(MCP_CATALOG[0]);
  });

  test("accepts only catalog URL variants and rejects off-catalog URLs", () => {
    expect(assertCatalogURL("https://api.githubcopilot.com/mcp")).toEqual(MCP_CATALOG[0]);
    expect(() => assertCatalogURL("https://API.GITHUBCOPILOT.COM/mcp")).toThrow("curated catalog");
    expect(() => assertCatalogURL("https://api.githubcopilot.com:443/mcp")).toThrow("curated catalog");
    expect(() => assertCatalogURL("https://not-github.example.com/mcp")).toThrow("curated catalog");
    expect(() => assertCatalogURL("https://api.githubcopilot.com/mcp//")).toThrow("curated catalog");
    expect(() => assertCatalogURL("https://api.githubcopilot.com/mcp/?token=secret")).toThrow("curated catalog");
    expect(() => assertCatalogURL("https://api.githubcopilot.com/mcp/#fragment")).toThrow("curated catalog");
    expect(() => assertCatalogURL("https://user:pass@api.githubcopilot.com/mcp/")).toThrow("curated catalog");
  });
});
