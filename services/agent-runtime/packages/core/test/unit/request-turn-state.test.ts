import { describe, expect, test } from "bun:test";
import { RequestTurnState } from "../../src/agent-loop/request-turn-state.js";
import type { RequestExecutionSnapshot } from "../../src/agent-loop/request-turn-state.js";

const snapshot: RequestExecutionSnapshot = Object.freeze({
  configGeneration: 4,
  currentModel: Object.freeze({ providerId: "openai", modelId: "gpt-5.5" }),
  approvalMode: "ask_for_approval",
  toolPolicyJson: "{\"allow\":[]}",
  installedBuiltinFamily: "gpt",
  mcpManifests: Object.freeze({}),
  toolCatalogJson: "{\"tools\":[]}",
});

describe("RequestTurnState", () => {
  test("tracks the closed request and effects phases separately", () => {
    const state = new RequestTurnState(snapshot);

    expect(state.phase()).toBe("assembling");
    state.openProvider();
    expect(state.phase()).toBe("provider_open");
    state.closeProvider();
    expect(state.phase()).toBe("provider_closed");
    state.beginEffectsDrain();
    expect(state.phase()).toBe("effects_draining");
    state.closeTurn();
    expect(state.phase()).toBe("turn_closed");
  });

  test("allows retry wait only after request-derived effects drain", () => {
    const state = new RequestTurnState(snapshot);
    state.openProvider();
    state.closeProvider();
    state.beginEffectsDrain();
    state.beginRetryWait();
    expect(state.phase()).toBe("retry_wait");

    state.beginRetry(snapshot);
    expect(state.phase()).toBe("assembling");
    expect(state.snapshot()).toBe(snapshot);
  });

  test("rejects phase skips", () => {
    const state = new RequestTurnState(snapshot);
    expect(() => state.closeProvider()).toThrow("invalid request turn phase transition");
  });
});
