import { describe, expect, test } from "bun:test";
import { lookupGatewayProviderRules } from "@tetral/gateway-lowering/src/rules/index.js";
import { lookupGatewayModel } from "../../src/providers/catalog.js";

// The chart's default rendering and the Runtime's own manifest are held equal
// to the first file by the deployment tests.
const manifests = [
  new URL("../../../../../../deploy/kubernetes/agent-runtime.yaml", import.meta.url),
  new URL("../../../../../../deploy/kubernetes/profiles/hardened/workloads.yaml", import.meta.url),
] as const;

const reviewerModelVariable = "TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL";

type Manifest = {
  readonly kind?: string;
  readonly spec?: {
    readonly template?: {
      readonly spec?: { readonly containers?: readonly { readonly env?: readonly { readonly name: string; readonly value?: unknown }[] }[] };
    };
  };
};

async function reviewerModels(manifest: URL): Promise<unknown[]> {
  const parsed: unknown = Bun.YAML.parse(await Bun.file(manifest).text());
  const documents = (Array.isArray(parsed) ? parsed : [parsed]) as readonly (Manifest | null)[];
  return documents
    .filter((document) => document?.kind === "Deployment")
    .flatMap((deployment) => deployment?.spec?.template?.spec?.containers ?? [])
    .flatMap((container) => container.env ?? [])
    .filter((variable) => variable.name === reviewerModelVariable)
    .map((variable) => variable.value);
}

describe("approval reviewer deployment capability", () => {
  test("every Runtime deployment names a reviewer model the Gateway routes with structured output", async () => {
    for (const manifest of manifests) {
      const models = await reviewerModels(manifest);
      expect(models, manifest.pathname).toHaveLength(1);
      const [providerId, modelId, extra] = String(models[0]).split("/");
      expect(extra, String(models[0])).toBeUndefined();
      expect(lookupGatewayModel(providerId ?? "", modelId ?? ""), String(models[0])).toBeDefined();
      const rules = lookupGatewayProviderRules(providerId ?? "", modelId ?? "");
      expect(rules, String(models[0])).toBeDefined();
      expect(rules?.structuredOutputStrategy, String(models[0])).not.toBe("unsupported");
    }
  });
});
