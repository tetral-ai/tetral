/**
 * @packageDocumentation
 *
 * Raises SDK stream parts into Gateway-private normalized fragments.
 * It guards stable synthesized IDs for id-less
 * fragments, streamed tool names, metadata redaction, finish-reason mapping,
 * finish-only usage, and a single successful terminal event. Provider client
 * adapters call the raiser after converting SDK parts to `GatewayStreamPart`;
 * it calls usage normalization and bounded redaction, while outer
 * provider-gateway orchestration converts thrown SDK errors and aborts into
 * terminal provider-error events.
 */
import {
  ProviderFinishReason,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { NormalizedProviderEventType } from "./normalized-stream.js";
import type { NormalizedProviderEvent, NormalizedTextEventType, NormalizedReasoningEventType, NormalizedToolInputEventType } from "./normalized-stream.js";
import { redactedProviderMetadataJson } from "./redaction.js";
import { normalizeProviderUsage } from "./usage.js";
import type { ProviderUsageInput, ProviderUsageWireFamily } from "./usage.js";

import { MaxMetadataBytes } from "@tetral/gateway-protocol/src/bounds.js";

/**
 * Closed set of adapted SDK parts consumed by `ProviderStreamRaiser`.
 * Non-forwarded SDK parts arrive as `raw`, `finish-step` supplies the preferred
 * terminal provider-usage frame, and SDK error or abort parts are thrown by the
 * provider client adapter before reaching this union.
 */
export type GatewayStreamPart =
  | { readonly type: "text-start"; readonly id?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "text-delta"; readonly id?: string | undefined; readonly textDelta?: string | undefined; readonly delta?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "text-end"; readonly id?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "reasoning-start"; readonly id?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "reasoning-delta"; readonly id?: string | undefined; readonly textDelta?: string | undefined; readonly delta?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "reasoning-end"; readonly id?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "tool-input-start"; readonly id?: string | undefined; readonly name?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "tool-input-delta"; readonly id?: string | undefined; readonly name?: string | undefined; readonly textDelta?: string | undefined; readonly delta?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "tool-input-end"; readonly id?: string | undefined; readonly name?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "tool-call"; readonly id?: string | undefined; readonly name?: string | undefined; readonly input?: unknown; readonly inputJson?: string | undefined; readonly metadata?: unknown }
  | { readonly type: "finish-step"; readonly usage?: ProviderUsageInput | undefined }
  | { readonly type: "finish"; readonly finishReason?: string | undefined; readonly usage?: ProviderUsageInput | undefined; readonly metadata?: unknown }
  | { readonly type: "raw"; readonly value?: unknown };

/** Supplies usage-family accounting and route-effective limits for finish events. */
export interface ProviderStreamRaiserOptions {
  readonly usageWireFamily: ProviderUsageWireFamily;
  readonly modelLimits: {
    readonly contextWindowTokens: number;
    readonly inputLimitTokens?: number | undefined;
    readonly outputTokenLimit: number;
  };
}

// ProviderStreamRaiser maps provider SDK stream parts to Gateway-private
// normalized fragments while holding a single terminal latch and synthesizing
// stable fragment ids. Gateway's ProviderBlockAssembler consumes these
// fragments; none of them cross the Runtime RPC, and Runtime validates only the
// complete-frame protocol it receives.
//
// Terminal latch state table:
//   state    | meaning                      | writers               | readers      | legal transitions
//   ---------+------------------------------+-----------------------+--------------+-------------------------------------
//   open     | stream accepting parts       | constructor init      | map() guard  | open -> terminal on a "finish" part
//   terminal | a "finish" event was emitted | map() "finish" branch | map() guard  | none; any further part throws
//
// "raw" and "finish-step" parts are handled ahead of the terminal guard: "raw"
// yields no event and "finish-step" only records finalStepUsage, so both are
// accepted in either state and neither drives a transition.
//
// Additional producer guards (each throws on violation):
//   - fragment ids: an id-less part gets a synthesized id (kind_N) that stays
//     stable for the active run of a kind and is cleared at its end; ids the
//     provider supplies pass through unchanged.
//   - a "tool-call" whose name contradicts the name streamed on its tool-input
//     parts is rejected.
//   - a missing tool name is rejected.
// These are producer-side safeguards, not complete stream-wire validation.
// Gateway block assembly owns full fragment lifecycle, duplicate and completeness
// checks, attachment-rejection placement, and terminal validation.
/**
 * Stateful producer that maps one provider stream to ordered Gateway events.
 * A `finish` part latches the raiser terminal; subsequent forwardable parts
 * throw, while `raw` remains dropped and `finish-step` remains usage-only.
 */
export class ProviderStreamRaiser {
  private readonly activeImplicitIds = new Map<string, string>();
  private readonly toolInputNames = new Map<string, string>();
  private nextImplicitId = 1;
  private terminal = false;
  private finalStepUsage: ProviderUsageInput | undefined;

  constructor(private readonly options: ProviderStreamRaiserOptions) {}

  /**
   * Raises one adapted SDK part into zero or one Gateway events.
   * The method throws on post-finish output, contradictory tool names, or a
   * missing required tool name, leaving outer orchestration to raise the error.
   */
  map(part: GatewayStreamPart): readonly NormalizedProviderEvent[] {
    if (part.type === "raw") {
      return [];
    }
    if (part.type === "finish-step") {
      this.finalStepUsage = part.usage;
      return [];
    }
    if (this.terminal) {
      throw new Error("provider stream emitted after terminal");
    }
    switch (part.type) {
      case "text-start":
        return [this.textEvent(NormalizedProviderEventType.TextStart, this.startIdFor("text", part.id), part.metadata, "")];
      case "text-delta":
        return [this.textEvent(NormalizedProviderEventType.TextDelta, this.currentIdFor("text", part.id), part.metadata, deltaText(part))];
      case "text-end":
        return [this.textEvent(NormalizedProviderEventType.TextEnd, this.endIdFor("text", part.id), part.metadata, "")];
      case "reasoning-start":
        return [this.reasoningEvent(NormalizedProviderEventType.ReasoningStart, this.startIdFor("reasoning", part.id), part.metadata, "")];
      case "reasoning-delta":
        return [this.reasoningEvent(NormalizedProviderEventType.ReasoningDelta, this.currentIdFor("reasoning", part.id), part.metadata, deltaText(part))];
      case "reasoning-end":
        return [this.reasoningEvent(NormalizedProviderEventType.ReasoningEnd, this.endIdFor("reasoning", part.id), part.metadata, "")];
      case "tool-input-start": {
        const id = this.startIdFor("tool", part.id);
        const name = requiredName(part.name);
        this.toolInputNames.set(id, name);
        return [this.toolInputEvent(NormalizedProviderEventType.ToolInputStart, id, name, "", part.metadata)];
      }
      case "tool-input-delta": {
        const id = this.currentIdFor("tool", part.id);
        const name = part.name ?? this.toolInputNames.get(id) ?? "";
        return [this.toolInputEvent(NormalizedProviderEventType.ToolInputDelta, id, name, deltaText(part), part.metadata)];
      }
      case "tool-input-end": {
        const id = this.currentIdFor("tool", part.id);
        const name = part.name ?? this.toolInputNames.get(id) ?? "";
        return [this.toolInputEvent(NormalizedProviderEventType.ToolInputEnd, id, name, "", part.metadata)];
      }
      case "tool-call":
        return [this.toolCallEvent(part)];
      case "finish":
        this.terminal = true;
        return [{
          type: NormalizedProviderEventType.Finish,
          finish: {
            reason: finishReason(part.finishReason),
            usage: normalizeProviderUsage(part.usage, {
              wireFamily: this.options.usageWireFamily,
              providerUsage: this.finalStepUsage ?? part.usage,
            }),
            metadataJson: metadataJson(part.metadata),
            contextWindowTokens: this.options.modelLimits.contextWindowTokens,
            inputLimitTokens: this.options.modelLimits.inputLimitTokens,
            outputTokenLimit: this.options.modelLimits.outputTokenLimit,
          },
        }];
    }
  }

  private textEvent(type: NormalizedTextEventType, id: string, metadata: unknown, text: string): NormalizedProviderEvent {
    return {
      type,
      text: {
        id,
        text,
        metadataJson: metadataJson(metadata),
      },
    };
  }

  private reasoningEvent(type: NormalizedReasoningEventType, id: string, metadata: unknown, text: string): NormalizedProviderEvent {
    return {
      type,
      reasoning: {
        id,
        text,
        metadataJson: metadataJson(metadata),
      },
    };
  }

  private toolInputEvent(type: NormalizedToolInputEventType, id: string, name: string, text: string, metadata: unknown): NormalizedProviderEvent {
    return {
      type,
      toolInput: {
        id,
        name,
        text,
        metadataJson: metadataJson(metadata),
      },
    };
  }

  private toolCallEvent(part: Extract<GatewayStreamPart, { readonly type: "tool-call" }>): NormalizedProviderEvent {
    const id = this.currentIdFor("tool", part.id);
    const name = requiredName(part.name ?? this.toolInputNames.get(id));
    const streamedName = this.toolInputNames.get(id);
    if (streamedName !== undefined && streamedName !== name) {
      throw new Error("tool-call name contradicts streamed tool input");
    }
    if (part.id === undefined || part.id.length === 0) {
      this.activeImplicitIds.delete("tool");
    }
    return {
      type: NormalizedProviderEventType.ToolCall,
      toolCall: {
        id,
        name,
        inputJson: part.inputJson ?? JSON.stringify(part.input ?? {}),
        metadataJson: metadataJson(part.metadata),
      },
    };
  }

  private startIdFor(kind: string, id: string | undefined): string {
    if (id !== undefined && id.length > 0) {
      return id;
    }
    const synthesized = this.nextId(kind);
    this.activeImplicitIds.set(kind, synthesized);
    return synthesized;
  }

  private currentIdFor(kind: string, id: string | undefined): string {
    if (id !== undefined && id.length > 0) {
      return id;
    }
    const existing = this.activeImplicitIds.get(kind);
    if (existing !== undefined) {
      return existing;
    }
    const synthesized = this.nextId(kind);
    this.activeImplicitIds.set(kind, synthesized);
    return synthesized;
  }

  private endIdFor(kind: string, id: string | undefined): string {
    const resolved = this.currentIdFor(kind, id);
    if (id === undefined || id.length === 0) {
      this.activeImplicitIds.delete(kind);
    }
    return resolved;
  }

  private nextId(kind: string): string {
    const synthesized = `${kind}_${this.nextImplicitId}`;
    this.nextImplicitId += 1;
    return synthesized;
  }
}

function deltaText(part: { readonly textDelta?: string | undefined; readonly delta?: string | undefined }): string {
  return part.textDelta ?? part.delta ?? "";
}

function requiredName(name: string | undefined): string {
  if (name === undefined || name.length === 0) {
    throw new Error("provider stream tool name is missing");
  }
  return name;
}

function finishReason(reason: string | undefined): ProviderFinishReason {
  switch (reason) {
    case "stop":
      return ProviderFinishReason.PROVIDER_FINISH_REASON_STOP;
    case "length":
      return ProviderFinishReason.PROVIDER_FINISH_REASON_LENGTH;
    case "tool-calls":
      return ProviderFinishReason.PROVIDER_FINISH_REASON_TOOL_CALLS;
    case "content-filter":
      return ProviderFinishReason.PROVIDER_FINISH_REASON_CONTENT_FILTER;
    case "error":
      return ProviderFinishReason.PROVIDER_FINISH_REASON_ERROR;
    case "unknown":
      return ProviderFinishReason.PROVIDER_FINISH_REASON_UNKNOWN;
    default:
      return ProviderFinishReason.PROVIDER_FINISH_REASON_OTHER;
  }
}

function metadataJson(value: unknown): string {
  return redactedProviderMetadataJson(value, MaxMetadataBytes);
}
