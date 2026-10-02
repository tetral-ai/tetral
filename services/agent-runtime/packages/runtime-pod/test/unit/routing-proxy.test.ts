import { describe, expect, test } from "bun:test";
import { hardenedRoutingSecretsReady } from "../../src/routing-proxy.js";

const leaf = { name: "sds.tetral.runtime.direct_leaf.update_success", value: 1 };
const validation = { name: "sds.tetral.runtime.direct_validation.update_success", value: 1 };
describe("hardened routing readiness", () => {
  test("requires independent successful initial leaf and trust SDS updates", () => {
    expect(hardenedRoutingSecretsReady({ stats: [leaf, validation] })).toBe(true);
    expect(hardenedRoutingSecretsReady({ stats: [leaf] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [validation] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [{ ...leaf, value: 0 }, validation] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [leaf, { ...validation, value: 0 }] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [leaf, leaf, validation] })).toBe(false);
    expect(hardenedRoutingSecretsReady({ stats: [{ ...leaf, value: Infinity }, validation] })).toBe(
      false,
    );
    expect(hardenedRoutingSecretsReady({ stats: [{ ...leaf, value: "1" }, validation] })).toBe(
      false,
    );
    expect(hardenedRoutingSecretsReady({ stats: [] })).toBe(false);
  });
});
