import { semanticErrorFields } from "./index.js";

/** Fixed process phases; no exception messages, stacks or dependency data. */
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

/** Builds a diagnostic from a closed phase vocabulary, never from an exception. */
export function processFailureLogRecord(phase: ProcessFailurePhase, cleanup = false) {
  return {
    event: cleanup ? "workload.cleanup_failed" : "workload.command_failed",
    "event.kind": cleanup ? "workload.cleanup_failed" : "workload.command_failed",
    operation: "workload.lifecycle",
    component: "workload",
    phase,
    ...semanticErrorFields({
      errorClass: cleanup ? "cleanup_error" : "process_error",
      errorCode: phase,
      messageSafe: cleanup ? "workload resource cleanup failed" : "workload command failed",
    }),
  } as const;
}

/** Executable boundary only; programmatic command rejection remains unchanged. */
export async function runProcessEntry(run: () => Promise<unknown>): Promise<void> {
  try {
    await run();
  } catch {
    process.exit(1);
  }
}

/** Commands own cleanup and diagnostics; this adapter owns only signal exit status. */
export function registerProcessSignalHandlers(shutdown: () => Promise<void>): () => void {
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
