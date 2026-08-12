import { describe, expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { lookupGatewayProviderRules } from "@tetral/gateway-lowering/src/rules/index.js";
import { lookupGatewayModel } from "../../src/providers/catalog.js";

const deploymentURLs = [
  new URL("../../../../../../deploy/kubernetes/agent-runtime.yaml", import.meta.url),
  new URL("../../../../../../deploy/helm/tetral/templates/agent-runtime.yaml", import.meta.url),
  new URL("../../../../../agent-runtime/k8s/deployment.yaml", import.meta.url),
] as const;

describe("approval reviewer deployment capability", () => {
  test("every checked-in Runtime deployment selects a structured-output-capable Gateway route", async () => {
    for (const deploymentURL of deploymentURLs) {
      const source = await readFile(deploymentURL, "utf8");
      const modelRef = source.match(
        /- name: TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL\s+value: ([a-z0-9-]+\/[a-zA-Z0-9._-]+)/,
      )?.[1];
      expect(modelRef, deploymentURL.pathname).toBeDefined();
      const [providerId, modelId, extra] = modelRef?.split("/") ?? [];
      expect(extra, modelRef).toBeUndefined();
      const catalogEntry = lookupGatewayModel(providerId ?? "", modelId ?? "");
      expect(catalogEntry, modelRef).toBeDefined();
      const rules = lookupGatewayProviderRules(providerId ?? "", modelId ?? "");
      expect(rules, modelRef).toBeDefined();
      expect(rules?.structuredOutputStrategy, modelRef).not.toBe("unsupported");
    }
  });
});
