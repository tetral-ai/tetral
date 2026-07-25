/**
 * @packageDocumentation
 * Collects the fixed provider/model lowering records into the package's exact-match registry. It
 * guards the approved model set by returning a policy only when both identifiers match a listed
 * record; it performs no family fallback or model substitution. The provider-gateway client calls
 * the lookup before lowering a request, while coverage tests inspect the registry metadata. This
 * module imports the provider policy records and its lookup only scans the in-memory array.
 */
import { AnthropicFable5Rules, AnthropicOpus48Rules } from "./anthropic.js";
import { DeepSeekV4ProRules } from "./deepseek.js";
import { MoonshotKimiK3Rules } from "./moonshotai.js";
import { OpenAIGPT55Rules, OpenAIGPT56SolRules } from "./openai.js";
import { ZaiGLM52Rules } from "./zai.js";

/** Fixed lowering-policy registry for every approved provider/model pair. */
export const GatewayProviderRules = [
  AnthropicOpus48Rules,
  AnthropicFable5Rules,
  DeepSeekV4ProRules,
  MoonshotKimiK3Rules,
  OpenAIGPT55Rules,
  OpenAIGPT56SolRules,
  ZaiGLM52Rules,
] as const;
/** Coverage metadata for provider behavior implemented outside the model policy records. */
export const GatewayAuxiliaryRuleIds = ["OAI-OAUTH"] as const;

/**
 * Finds the lowering policy for an exact provider/model pair.
 *
 * Returns `undefined` for every unregistered pair so the provider client can fail closed.
 */
export function lookupGatewayProviderRules(providerId: string, modelId: string) {
  return GatewayProviderRules.find((rules) => rules.providerId === providerId && rules.modelId === modelId);
}
