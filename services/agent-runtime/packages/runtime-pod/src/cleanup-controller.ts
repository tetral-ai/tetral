/**
 * Adapts Runtime Core session cleanup to the control service's final typed result.
 * `RuntimeControlService` calls this adapter, which delegates to the cleanup host assembled by
 * the Runtime Core host layer. The host result, not Promise timing, owns busy versus completed.
 */
import type { RuntimeCleanupCommand, RuntimeCleanupController } from "./runtime-service.js";

/**
 * In-process Runtime Core port that removes a session's hot runtime state when it is not busy.
 */
export interface RuntimeCoreCleanupHost {
  readonly handleCleanupSession: (scope: RuntimeCleanupCommand) => Promise<
    | { readonly ok: true; readonly sessionId: string; readonly cleaned: boolean }
    | { readonly ok: false; readonly sessionId: string; readonly reason: "session_busy" }
  >;
}

/**
 * Presents Runtime Core cleanup as the controller consumed by `RuntimeControlService`.
 */
export class SessionRunHostCleanupController implements RuntimeCleanupController {
  constructor(private readonly host: RuntimeCoreCleanupHost) {}

  async startCleanup(scope: RuntimeCleanupCommand) {
    return await this.host.handleCleanupSession(scope);
  }
}
