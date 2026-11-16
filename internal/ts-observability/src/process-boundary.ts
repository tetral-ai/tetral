import { causeOf, semanticErrorFields } from "./failure.js";

/** The fixed phases of a process's life in which a command or its cleanup fails. */
export type ProcessFailurePhase =
  | "configuration"
  | "dependency"
  | "listener"
  | "wait"
  | "app"
  | "runtime_core"
  | "http"
  | "grpc"
  | "mcp_clients"
  | "database";

/** The failure record of a command, or of its cleanup, that failed in phase with error. */
export function processFailureLogRecord(
  phase: ProcessFailurePhase,
  error: unknown,
  cleanup = false,
) {
  return {
    event: cleanup ? "workload.cleanup_failed" : "workload.command_failed",
    "event.kind": cleanup
      ? "workload.cleanup_failed"
      : "workload.command_failed",
    operation: "workload.lifecycle",
    component: "workload",
    phase,
    ...semanticErrorFields({ errorCode: phase, cause: causeOf(error) }),
  } as const;
}

/** An executable owns one absolute application shutdown deadline, never a library worker. */
export interface ExecutableProcessBoundary {
  beginShutdown(deadline: number, reportTimeout: () => void): () => void;
}

/** The failure record of an application shutdown that passed its deadline. */
export function processShutdownFailureLogRecord() {
  return {
    event: "workload.shutdown_deadline_exceeded",
    "event.kind": "workload.shutdown_deadline_exceeded",
    operation: "workload.lifecycle",
    component: "workload",
    phase: "app",
    "timeout.kind": "application_shutdown",
    ...semanticErrorFields({
      errorCode: "shutdown_deadline_exceeded",
      cause: causeOf("the application shutdown passed its deadline; the process exits"),
    }),
  } as const;
}

/** Executable failure fuse; reusable commands receive no process-exit policy by default. */
export async function runProcessEntry(
  run: (boundary: ExecutableProcessBoundary) => Promise<unknown>,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let armed = false;
  const disarm = (): void => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const boundary: ExecutableProcessBoundary = {
    beginShutdown: (deadline, reportTimeout) => {
      if (armed) return disarm;
      armed = true;
      if (!Number.isFinite(deadline))
        throw new Error("invalid executable shutdown deadline");
      const schedule = (): void => {
        timer = setTimeout(
          () => {
            if (Date.now() < deadline) {
              schedule();
              return;
            }
            try {
              reportTimeout();
            } catch {
              /* diagnostics cannot delay executable failure */
            }
            process.exit(1);
          },
          Math.max(0, Math.min(2_147_483_647, deadline - Date.now())),
        );
      };
      schedule();
      return disarm;
    },
  };
  try {
    await run(boundary);
  } catch {
    process.exit(1);
  } finally {
    disarm();
  }
}

/** Commands own cleanup and diagnostics; this adapter owns only signal exit status. */
export function registerProcessSignalHandlers(
  shutdown: () => Promise<void>,
): () => void {
  const stop = (): void => {
    try {
      void Promise.resolve(shutdown()).then(
        () => process.exit(0),
        () => process.exit(1),
      );
    } catch {
      process.exit(1);
    }
  };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  return () => {
    process.off("SIGTERM", stop);
    process.off("SIGINT", stop);
  };
}
