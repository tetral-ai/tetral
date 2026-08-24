import { describe, expect, test } from "bun:test";
import { SessionRunHostCleanupController } from "../../src/cleanup-controller.js";

const scope = {
  workspaceId: "wksp_cleanup",
  sessionId: "sesn_cleanup",
  bindingId: "bind_cleanup",
  bindingGeneration: 1,
  targetPodUid: "pod_cleanup",
  cleanupOperationId: "cleanup_operation",
};

describe("SessionRunHostCleanupController", () => {
  test("preserves a delayed busy result as the typed cleanup outcome", async () => {
    const controller = new SessionRunHostCleanupController({
      handleCleanupSession: async (command) => {
        await new Promise<void>((resolve) => setTimeout(resolve, 1));
        return { ok: false as const, sessionId: command.sessionId, reason: "session_busy" as const };
      },
    });

    expect(await controller.startCleanup(scope)).toEqual({
      ok: false,
      sessionId: scope.sessionId,
      reason: "session_busy",
    });
  });
});
