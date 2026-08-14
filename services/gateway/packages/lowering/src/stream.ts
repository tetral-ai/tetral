/**
 * @packageDocumentation
 *
 * Raises provider SDK stream parts into generated `ProviderStreamEvent` values.
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
  ProviderStreamEventType,
} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import type { ProviderStreamEvent } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import { boundedRedactedJson } from "./redaction.js";
import { normalizeProviderUsage } from "./usage.js";
import type { ProviderUsageInput, ProviderUsageWireFamily } from "./usage.js";

const ProviderMetadataJsonMaxBytes = 16 * 1024;

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

// ProviderStreamRaiser is the producer-side enforcer of the stream wire
// contract. It maps provider SDK stream parts to ProviderStreamEvents while
// holding a single terminal latch and synthesizing stable fragment ids. The
// mirror consumer,
// services/agent-runtime/packages/core/src/llm/llm-service.ts
// (ProviderStreamValidator), re-checks the same ordering/terminal rules on
// receipt.
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
// Runtime owns fragment start/delta/end lifecycle, duplicate and completeness
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
  map(part: GatewayStreamPart): readonly ProviderStreamEvent[] {
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
        return [this.textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START, this.startIdFor("text", part.id), part.metadata, "")];
      case "text-delta":
        return [this.textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA, this.currentIdFor("text", part.id), part.metadata, deltaText(part))];
      case "text-end":
        return [this.textEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END, this.endIdFor("text", part.id), part.metadata, "")];
      case "reasoning-start":
        return [this.reasoningEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START, this.startIdFor("reasoning", part.id), part.metadata, "")];
      case "reasoning-delta":
        return [this.reasoningEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA, this.currentIdFor("reasoning", part.id), part.metadata, deltaText(part))];
      case "reasoning-end":
        return [this.reasoningEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END, this.endIdFor("reasoning", part.id), part.metadata, "")];
      case "tool-input-start": {
        const id = this.startIdFor("tool", part.id);
        const name = requiredName(part.name);
        this.toolInputNames.set(id, name);
        return [this.toolInputEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START, id, name, "", part.metadata)];
      }
      case "tool-input-delta": {
        const id = this.currentIdFor("tool", part.id);
        const name = part.name ?? this.toolInputNames.get(id) ?? "";
        return [this.toolInputEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA, id, name, deltaText(part), part.metadata)];
      }
      case "tool-input-end": {
        const id = this.currentIdFor("tool", part.id);
        const name = part.name ?? this.toolInputNames.get(id) ?? "";
        return [this.toolInputEvent(ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END, id, name, "", part.metadata)];
      }
      case "tool-call":
        return [this.toolCallEvent(part)];
      case "finish":
        this.terminal = true;
        return [{
          type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,
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

  private textEvent(type: ProviderStreamEventType, id: string, metadata: unknown, text: string): ProviderStreamEvent {
    return {
      type,
      text: {
        id,
        text,
        metadataJson: metadataJson(metadata),
      },
    };
  }

  private reasoningEvent(type: ProviderStreamEventType, id: string, metadata: unknown, text: string): ProviderStreamEvent {
    return {
      type,
      reasoning: {
        id,
        text,
        metadataJson: metadataJson(metadata),
      },
    };
  }

  private toolInputEvent(type: ProviderStreamEventType, id: string, name: string, text: string, metadata: unknown): ProviderStreamEvent {
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

  private toolCallEvent(part: Extract<GatewayStreamPart, { readonly type: "tool-call" }>): ProviderStreamEvent {
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
      type: ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL,
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
  return boundedRedactedJson(value, ProviderMetadataJsonMaxBytes);
}
