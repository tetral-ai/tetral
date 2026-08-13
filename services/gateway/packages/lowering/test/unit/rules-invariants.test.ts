import { describe, expect, test } from "bun:test";
import {
  AnthropicFable5DefaultEffort,
  AnthropicFable5Rules,
  AnthropicOpus48DefaultEffort,
  AnthropicOpus48Rules,
} from "../../src/rules/anthropic.js";
import { DeepSeekV4ProDefaultEffort, DeepSeekV4ProRules } from "../../src/rules/deepseek.js";
import { MoonshotKimiK3DefaultEffort, MoonshotKimiK3Rules } from "../../src/rules/moonshotai.js";
import {
  OpenAIGPT55DefaultEffort,
  OpenAIGPT55Rules,
  OpenAIGPT56SolDefaultEffort,
  OpenAIGPT56SolRules,
} from "../../src/rules/openai.js";
import { ZaiGLM52DefaultEffort, ZaiGLM52Rules } from "../../src/rules/zai.js";
import { GatewayAuxiliaryRuleIds } from "../../src/rules/index.js";

const rules = [
  AnthropicOpus48Rules,
  AnthropicFable5Rules,
  OpenAIGPT55Rules,
  OpenAIGPT56SolRules,
  DeepSeekV4ProRules,
  MoonshotKimiK3Rules,
  ZaiGLM52Rules,
] as const;

describe("provider lowering rule invariants", () => {
  test("rule id sets stay pinned to the implemented provider behavior matrix", () => {
    expect(AnthropicOpus48Rules.ruleIds).toEqual([
      "anthropic-content-normalization",
      "anthropic-tool-id-scrubbing",
      "anthropic-media-lowering",
      "anthropic-cache-placement",
      "anthropic-adaptive-thinking",
      "anthropic-reasoning-metadata",
      "anthropic-beta-header",
      "anthropic-sampling-defaults",
      "anthropic-schema-passthrough",
      "anthropic-error-mapping",
      "anthropic-output-cap",
    ]);
    expect(AnthropicFable5Rules.ruleIds).toEqual(AnthropicOpus48Rules.ruleIds);
    expect(OpenAIGPT55Rules.ruleIds).toEqual([
      "openai-reasoning-metadata",
      "openai-effort-lowering",
      "openai-request-options",
      "openai-tool-strictness",
      "openai-base-reasoning",
      "openai-output-cap-omission",
      "openai-media-lowering",
      "openai-header-absence",
      "openai-sampling-defaults",
      "openai-schema-lowering",
      "openai-error-mapping",
      "openai-output-cap",
    ]);
    expect(OpenAIGPT56SolRules.ruleIds).toEqual(OpenAIGPT55Rules.ruleIds);
    expect(DeepSeekV4ProRules.ruleIds).toEqual([
      "deepseek-reasoning-default",
      "deepseek-reasoning-remap",
      "deepseek-unsupported-media",
      "deepseek-provider-options",
      "deepseek-effort-lowering",
      "deepseek-sampling-defaults",
      "deepseek-cache-absence",
      "deepseek-schema-passthrough",
      "deepseek-error-mapping",
      "deepseek-output-cap",
    ]);
    expect(MoonshotKimiK3Rules.ruleIds).toEqual([
      "kimi-content-media-lowering",
      "kimi-cache-placement",
      "kimi-tool-streaming",
      "kimi-beta-effort-absence",
      "kimi-sampling-defaults",
      "kimi-schema-lowering",
      "kimi-error-mapping",
      "kimi-terminal-cache-usage",
      "kimi-output-cap",
    ]);
    expect(ZaiGLM52Rules.ruleIds).toEqual([
      "zai-unsupported-media",
      "zai-reasoning-remap",
      "zai-thinking-mode",
      "zai-effort-lowering",
      "zai-cache-absence",
      "zai-sampling-defaults",
      "zai-schema-passthrough",
      "zai-error-mapping",
      "zai-output-cap",
    ]);
    expect(GatewayAuxiliaryRuleIds).toEqual(["OAI-OAUTH"]);
  });

  test("anthropic-output-cap pins Anthropic output-cap provenance", () => expect(AnthropicOpus48Rules.ruleIds).toContain("anthropic-output-cap"));
  test("claude-fable-5 freezes the Claude Opus rule family for its exact model id", () => {
    const { modelId: _opusModelId, providerOptions: _opusProviderOptions, ...opusFamily } = AnthropicOpus48Rules;
    const { modelId: _fableModelId, providerOptions: _fableProviderOptions, ...fableFamily } = AnthropicFable5Rules;
    expect(fableFamily).toEqual(opusFamily);
  });
  test("claude-opus-4-8 freezes xhigh as its platform default effort", () => {
    expect(AnthropicOpus48DefaultEffort).toBe("xhigh");
    expect(AnthropicOpus48Rules.providerOptions.anthropic).toMatchObject({ effort: "xhigh" });
  });
  test("claude-fable-5 freezes high as its platform default effort", () => {
    expect(AnthropicFable5DefaultEffort).toBe("high");
    expect(AnthropicFable5Rules.providerOptions.anthropic).toMatchObject({ effort: "high" });
  });
  test("openai-output-cap pins inert OpenAI output-cap provenance", () => expect(OpenAIGPT55Rules.ruleIds).toContain("openai-output-cap"));
  test("gpt-5.5 freezes medium as its platform default effort", () => {
    expect(OpenAIGPT55DefaultEffort).toBe("medium");
    expect(OpenAIGPT55Rules.providerOptions.openai).toMatchObject({ reasoningEffort: "medium" });
  });
  test("gpt-5.6-sol freezes medium as its platform default effort", () => {
    expect(OpenAIGPT56SolDefaultEffort).toBe("medium");
    expect(OpenAIGPT56SolRules.providerOptions.openai).toMatchObject({ reasoningEffort: "medium" });
    expect(OpenAIGPT56SolRules.effort).toEqual(OpenAIGPT55Rules.effort);
    expect(OpenAIGPT56SolRules.modelId).toBe("gpt-5.6-sol");
  });
  test("OAI-OAUTH pins the subscription transport delta", () => expect(GatewayAuxiliaryRuleIds).toContain("OAI-OAUTH"));
  test("deepseek-output-cap pins DeepSeek output-cap provenance", () => expect(DeepSeekV4ProRules.ruleIds).toContain("deepseek-output-cap"));
  test("deepseek-v4-pro freezes high as its platform default effort", () => {
    expect(DeepSeekV4ProDefaultEffort).toBe("high");
    expect(DeepSeekV4ProRules.providerOptions.deepseek).toMatchObject({ reasoningEffort: "high" });
  });
  test("kimi-terminal-cache-usage pins terminal-frame cache accounting", () => expect(MoonshotKimiK3Rules.ruleIds).toContain("kimi-terminal-cache-usage"));
  test("kimi-output-cap pins Kimi output-cap provenance", () => expect(MoonshotKimiK3Rules.ruleIds).toContain("kimi-output-cap"));
  test("kimi-k3 freezes native thinking and exact-id sampling/schema constants", () => {
    expect(MoonshotKimiK3Rules.dynamicProviderOptions).toBeUndefined();
    expect(MoonshotKimiK3Rules.sampling).toEqual({
      temperature: undefined,
      topP: undefined,
      topK: undefined,
    });
    expect(MoonshotKimiK3Rules.schemaStrategy).toBe("moonshot");
  });
  test("kimi-k3 freezes its max reasoning posture as transport-native field absence", () => {
    expect(MoonshotKimiK3DefaultEffort).toBeUndefined();
    expect(MoonshotKimiK3Rules.effort).toBeUndefined();
    expect(MoonshotKimiK3Rules.providerOptions.anthropic).not.toHaveProperty("effort");
    expect(MoonshotKimiK3Rules.providerOptions.anthropic).not.toHaveProperty("thinking");
  });
  test("zai-output-cap pins Z.ai output-cap provenance", () => expect(ZaiGLM52Rules.ruleIds).toContain("zai-output-cap"));
  test("zai-glm-5.2 freezes high as its platform default effort", () => {
    expect(ZaiGLM52DefaultEffort).toBe("high");
    expect(ZaiGLM52Rules.providerOptions.zai).toMatchObject({ reasoningEffort: "high" });
  });

  test("only the empty string is treated as no-effort ModelRef.variant", () => {
    for (const rule of rules) {
      if (rule.effort === undefined) {
        continue;
      }
      expect(rule.effort.noEffortVariants, `${rule.providerId}/${rule.modelId}`).toEqual([""]);
    }
  });

  test("model rules declare provider-wire structured-output capability", () => {
    expect(AnthropicOpus48Rules.structuredOutputStrategy).toBe("native_json_schema");
    expect(AnthropicFable5Rules.structuredOutputStrategy).toBe("native_json_schema");
    expect(DeepSeekV4ProRules.structuredOutputStrategy).toBe("json_object");
    for (const rule of rules.filter((rule) =>
      rule !== AnthropicOpus48Rules && rule !== AnthropicFable5Rules && rule !== DeepSeekV4ProRules
    )) {
      expect(rule.structuredOutputStrategy, `${rule.providerId}/${rule.modelId}`).toBe("unsupported");
    }
  });
});
