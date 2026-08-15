/**
 * @packageDocumentation
 * Defines the single cold thread-load boundary and the durable declaration
 * boundary used by Runtime orchestration. It guards the separation between a
 * database-stamped cold baseline and typed commands applied during one hot
 * residency. SessionManager calls the cold loader once per ThreadEntry;
 * ThreadLoop calls the declaration writer and binding-token adapter.
 */
import { Context, Layer } from "effect";
import type { RuntimeProviderAttachment } from "../contracts/runtime.js";
import type {
  DurableRuntimeMessage,
  RuntimeJsonValue,
  RuntimeMessage,
  RuntimeMessageCreate,
} from "../contracts/runtime.js";
import type { RuntimeDeclarationReceipt } from "../runtime/runtime-declaration.js";
import type {
  RuntimeAcceptedInputState,
  RuntimeAcceptedThreadMetadataState,
  RuntimeColdCoverage,
  RuntimeConfigPatchState,
  RuntimePreloadedBackgroundToolState,
  RuntimePreloadedSandboxExecutionState,
  RuntimeThreadAddressState,
} from "../thread-loop/thread-state.js";
import type { RuntimeThreadIdentity } from "../thread-loop/thread-runtime.js";
import type { ThreadContextPrefix } from "../session/context-manager.js";
import type { ThreadTurnLoadFacts } from "../thread-loop/thread-turn-checkpoint.js";

/** Operations through which session orchestration cold-loads and commits thread state. */
export interface ContextLoader {
  /**
   * Reads one complete database-stamped cold baseline for a ThreadEntry.
   * A residency never calls this operation again after installation.
   */
  readonly loadThreadContext?: (
    command: RuntimeThreadAddressState,
  ) => Promise<{
    readonly messages: readonly DurableRuntimeMessage[];
    readonly turnFacts: ThreadTurnLoadFacts;
    readonly threadContextPrefix?: ThreadContextPrefix | undefined;
    readonly durableTurnId?: string | undefined;
    readonly runtimeBindingToken: string;
    readonly thread?: RuntimeAcceptedThreadMetadataState | undefined;
    readonly runtimeConfigPatch?: RuntimeConfigPatchState | undefined;
    readonly mcpManifests?: readonly RuntimeConfigPatchState[] | undefined;
    readonly pendingToolUses?: readonly RuntimeLoadedPendingToolUse[] | undefined;
    readonly pendingSandboxExecutions?: readonly RuntimePreloadedSandboxExecutionState[] | undefined;
    readonly backgroundTools?: readonly RuntimePreloadedBackgroundToolState[] | undefined;
    readonly pendingAttachments?: readonly RuntimeProviderAttachment[] | undefined;
    readonly pendingAgentMail?: readonly RuntimeLoadedAgentMail[] | undefined;
    readonly coldCoverage: RuntimeColdCoverage;
  }>;
  /** Refreshes the binding credential without changing durable declaration state; stale custody rejects as superseded. */
  readonly refreshRuntimeBindingToken?: (
    identity: RuntimeThreadIdentity,
    options?: { readonly force?: boolean | undefined },
  ) => Promise<string>;
  /**
   * Commits one frozen declaration and returns the database receipt that may
   * advance HotState only under current custody.
   */
  readonly commitAcceptedInput?: (
    input: RuntimeAcceptedInputState,
    options?: {
      readonly messageCreates?: readonly RuntimeMessageCreate[] | undefined;
    },
  ) => Promise<AcceptedInputCommitResult>;
  /** Reads the latest durable envelope owned by the addressed target thread. */
  readonly readAgentMail?: (
    command: RuntimeThreadAddressState,
    sourceThreadId: string,
  ) => Promise<RuntimeLoadedAgentMail | undefined>;
}

/** Result of committing one accepted command before mutating its hot thread state. */
export type AcceptedInputCommitResult =
  | {
      readonly type: "receipt";
      readonly inputDisposition: "committed" | "duplicate";
      readonly applicationDisposition: "current_custody" | "stale_custody";
      readonly receipt: RuntimeDeclarationReceipt;
    }
  | { readonly type: "task_notification_deferred" }
  | {
      readonly type: "task_notification_rejected";
      readonly errorCode:
        | "task_notification_result_invalid"
        | "task_notification_message_invalid"
        | "task_notification_payload_mismatch";
    }
  | { readonly type: "stale_custody" };

/** Durable pending tool-use state reconstructed during a cold thread load. */
export interface RuntimeLoadedPendingToolUse {
  readonly toolUseEventId: string;
  readonly modelRequestId: string;
  readonly modelToolCallId: string;
  readonly toolName: string;
  readonly input: RuntimeJsonValue;
  readonly decision?: "allow" | "deny" | undefined;
  readonly denyMessage?: string | undefined;
  readonly status: "pending" | "resolving";
}

/** Unconsumed agent mail reconstructed from the target thread's durable inbox. */
export interface RuntimeLoadedAgentMail {
  readonly deliveryId: string;
  readonly message: RuntimeMessage;
  readonly content: string;
}

/** Effect service tag through which orchestration obtains the active context loader. */
export class ContextLoaderService extends Context.Service<ContextLoaderService, ContextLoader>()("tetral-agent/ContextLoader") {}

/** Provides a concrete context loader as an Effect layer. */
export function layer(loader: ContextLoader): Layer.Layer<ContextLoaderService> {
  return Layer.succeed(ContextLoaderService, ContextLoaderService.of(loader));
}
