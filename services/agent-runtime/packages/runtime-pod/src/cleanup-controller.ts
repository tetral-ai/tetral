/**
 * Adapts Runtime Core session cleanup to the control service's admission-plus-completion shape.
 *
 * The adapter guards the distinction between an immediately busy session and cleanup that has
 * started but has not finished: admitted cleanup remains incomplete until Runtime Core reports
 * success, and a later busy result rejects that completion. `RuntimeControlService` calls this
 * adapter, which delegates to the cleanup host assembled by the Runtime Core host layer.
 */
import type { RuntimeCleanupController, RuntimeCommandScope } from "./runtime-service.js";

/**
 * In-process Runtime Core port that removes a session's hot runtime state when it is not busy.
 */
export interface RuntimeCoreCleanupHost {
  readonly handleCleanupSession: (scope: RuntimeCommandScope) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly cleaned: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "session_busy" }
  >;
}

/**
 * Presents Runtime Core cleanup as the controller consumed by `RuntimeControlService`.
 */
export class SessionRunHostCleanupController implements RuntimeCleanupController {
  constructor(private readonly host: RuntimeCoreCleanupHost) {}

  /**
   * Starts cleanup, returns an immediately observed busy refusal, and exposes admitted work through
   * a completion promise that succeeds only after the host confirms cleanup.
   */
  async startCleanup(scope: RuntimeCommandScope) {
    const completion = this.host.handleCleanupSession(scope);
    let immediateResult:
      | Awaited<ReturnType<RuntimeCoreCleanupHost["handleCleanupSession"]>>
      | undefined;
    completion.then(
      (result) => {
        immediateResult = result;
      },
      () => undefined,
    );
    await Promise.resolve();
    if (immediateResult !== undefined && !immediateResult.ok) {
      return immediateResult;
    }
    return {
      ok: true as const,
      sessionId: scope.sessionId,
      completion: completion.then((result) => {
        if (!result.ok) {
          throw new Error("runtime cleanup failed");
        }
        return result;
      }),
    };
  }
}
