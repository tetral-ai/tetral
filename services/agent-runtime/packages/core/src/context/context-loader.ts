/**
 * @packageDocumentation
 * Defines the single cold thread-load boundary and the durable declaration
 * boundary used by Runtime orchestration. It guards the separation between a
 * database-stamped cold baseline and typed commands applied during one hot
 * residency. SessionManager calls the cold loader once per ThreadEntry;
 * ThreadLoop calls the declaration writer and binding-token adapter.
 */
import { Context, Layer } from "effect";
import type { ProviderRequestAttachment } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
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
  RuntimeThreadControlState,
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
    command: RuntimeThreadControlState,
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
    readonly pendingAttachments?: readonly ProviderRequestAttachment[] | undefined;
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
  /** Resolves one durable sent envelope into its database-stamped input source. */
  readonly resolveAgentMail?: (
    command: RuntimeThreadControlState,
    childThreadId: string,
    deliveryId?: string | undefined,
  ) => Promise<RuntimeResolvedAgentMail>;
}

/** Result of committing one accepted command before mutating its hot thread state. */
export type AcceptedInputCommitResult = {
  readonly type: "receipt";
  readonly inputDisposition: "committed" | "duplicate";
  readonly applicationDisposition: "current_custody" | "stale_custody";
  readonly receipt: RuntimeDeclarationReceipt;
};

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

/** Unreceipted child completion reconstructed from the durable sent-event ledger. */
export interface RuntimeLoadedAgentMail {
  readonly deliveryId: string;
  readonly sourceThreadId: string;
  readonly sourceToolUseEventId: string;
}

/** Database-stamped agent-mail input returned by the durable resolver. */
export interface RuntimeResolvedAgentMail extends RuntimeLoadedAgentMail {
  readonly targetThreadId: string;
  readonly receivedEventId: string;
  readonly receivedSequence: number;
  readonly message: RuntimeMessage;
  readonly publicMessageJson: string;
}

/** Effect service tag through which orchestration obtains the active context loader. */
export class ContextLoaderService extends Context.Service<ContextLoaderService, ContextLoader>()("tetral-agent/ContextLoader") {}

/** Provides a concrete context loader as an Effect layer. */
export function layer(loader: ContextLoader): Layer.Layer<ContextLoaderService> {
  return Layer.succeed(ContextLoaderService, ContextLoaderService.of(loader));
}
