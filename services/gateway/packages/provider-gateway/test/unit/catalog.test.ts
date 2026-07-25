import { describe, expect, test } from "bun:test";
import {
  GatewayModelCatalog,
  lookupGatewayModel,
  routeEffectiveGatewayModelLimits,
} from "../../src/providers/catalog.js";

describe("Gateway model catalog", () => {
  test("enumerates exactly the approved seven provider/model pairs", () => {
    const supported = GatewayModelCatalog
      .map((entry) => `${entry.providerId}/${entry.modelId}`)
      .sort((left, right) => left.localeCompare(right));

    expect(supported).toEqual([
      "anthropic/claude-opus-4-8",
      "anthropic/claude-fable-5",
      "deepseek/deepseek-v4-pro",
      "moonshotai/kimi-k3",
      "openai/gpt-5.5",
      "openai/gpt-5.6-sol",
      "zai/glm-5.2",
    ].sort((left, right) => left.localeCompare(right)));
    expect(GatewayModelCatalog.map((entry) => entry.supplyProviderId).sort()).toEqual([
      "anthropic",
      "anthropic",
      "deepseek",
      "moonshotai",
      "openai",
      "openai",
      "zai",
    ]);
    expect(GatewayModelCatalog.map((entry) => [entry.providerId, entry.platformHosted])).toEqual([
      ["openai", true],
      ["openai", true],
      ["anthropic", true],
      ["anthropic", true],
      ["deepseek", true],
      ["moonshotai", false],
      ["zai", false],
    ]);
    expect(lookupGatewayModel("openai", "gpt-5.6-sol")).toMatchObject({
      apiModelId: "gpt-5.6-sol",
      supplyModes: ["openai-api-key", "openai-chatgpt-oauth"],
      baseURLs: ["https://api.openai.com", "https://chatgpt.com/backend-api/codex/responses"],
      platformHosted: true,
    });
    expect(lookupGatewayModel("anthropic", "claude-fable-5")).toMatchObject({
      apiModelId: "claude-fable-5",
      supplyModes: ["anthropic-api-key"],
      baseURLs: ["https://api.anthropic.com"],
      platformHosted: true,
    });
    expect(lookupGatewayModel("moonshotai", "kimi-k3")).toMatchObject({
      apiModelId: "k3",
      supplyModes: ["moonshotai-code-api-key"],
      baseURLs: ["https://api.kimi.com/coding/v1"],
      platformHosted: false,
    });
    expect(lookupGatewayModel("zai", "glm-5.2")).toMatchObject({
      apiModelId: "glm-5.2",
      baseURLs: ["https://api.z.ai/api/coding/paas/v4"],
      platformHosted: false,
    });
    expect(lookupGatewayModel("anthropic", "claude-opus-4-8")?.apiModelId).toBe("claude-opus-4-8");
    expect(Object.fromEntries(GatewayModelCatalog.map((entry) => [
      `${entry.providerId}/${entry.modelId}`,
      entry.modelOutputTokenLimit,
    ]))).toEqual({
      "openai/gpt-5.5": 128_000,
      "openai/gpt-5.6-sol": 128_000,
      "anthropic/claude-opus-4-8": 128_000,
      "anthropic/claude-fable-5": 128_000,
      "deepseek/deepseek-v4-pro": 384_000,
      "moonshotai/kimi-k3": 131_072,
      "zai/glm-5.2": 131_072,
    });
    expect(Object.fromEntries(GatewayModelCatalog.map((entry) => [
      `${entry.providerId}/${entry.modelId}`,
      entry.modelContextWindowTokens,
    ]))).toEqual({
      "openai/gpt-5.5": 1_050_000,
      "openai/gpt-5.6-sol": 1_050_000,
      "anthropic/claude-opus-4-8": 1_000_000,
      "anthropic/claude-fable-5": 1_000_000,
      "deepseek/deepseek-v4-pro": 1_000_000,
      "moonshotai/kimi-k3": 1_048_576,
      "zai/glm-5.2": 1_048_576,
    });
    for (const model of supported) {
      const [providerId, modelId] = model.split("/");
      expect(lookupGatewayModel(providerId ?? "", modelId ?? "")).toBeDefined();
    }
  });

  test("reports route-effective OpenAI limits for API-key and ChatGPT OAuth supplies", () => {
    const gpt55 = lookupGatewayModel("openai", "gpt-5.5");
    const gpt56 = lookupGatewayModel("openai", "gpt-5.6-sol");
    expect(gpt55).toBeDefined();
    expect(gpt56).toBeDefined();

    expect(routeEffectiveGatewayModelLimits(gpt55!, "openai-api-key")).toEqual({
      contextWindowTokens: 1_050_000,
      inputLimitTokens: undefined,
      outputTokenLimit: 128_000,
    });
    expect(routeEffectiveGatewayModelLimits(gpt55!, "openai-chatgpt-oauth")).toEqual({
      contextWindowTokens: 400_000,
      inputLimitTokens: 272_000,
      outputTokenLimit: 128_000,
    });
    expect(routeEffectiveGatewayModelLimits(gpt56!, "openai-api-key")).toEqual({
      contextWindowTokens: 1_050_000,
      inputLimitTokens: undefined,
      outputTokenLimit: 128_000,
    });
    expect(routeEffectiveGatewayModelLimits(gpt56!, "openai-chatgpt-oauth")).toEqual({
      contextWindowTokens: 500_000,
      inputLimitTokens: 372_000,
      outputTokenLimit: 128_000,
    });
  });

  test("fails closed for providerless and same-provider non-catalog models", () => {
    expect(lookupGatewayModel("", "gpt-5.5")).toBeUndefined();
    expect(lookupGatewayModel("openai", "gpt-4.1")).toBeUndefined();
    expect(lookupGatewayModel("anthropic", "claude-opus-4-9")).toBeUndefined();
    expect(lookupGatewayModel("zai", "glm-5.1")).toBeUndefined();
  });
});
