/** Gateway-private normalized SDK fragments. None of these fragments cross the RPC. */
import type { ProviderFinishPayload, ProviderErrorPayload, ProviderAttachmentRejectionsPayload } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
export enum NormalizedProviderEventType {
  TextStart = "text-start",
  TextDelta = "text-delta",
  TextEnd = "text-end",
  ReasoningStart = "reasoning-start",
  ReasoningDelta = "reasoning-delta",
  ReasoningEnd = "reasoning-end",
  ToolInputStart = "tool-input-start",
  ToolInputDelta = "tool-input-delta",
  ToolInputEnd = "tool-input-end",
  ToolCall = "tool-call",
  Finish = "finish",
  ProviderError = "provider-error",
  AttachmentRejections = "attachment-rejections",
}
export type NormalizedTextEventType = NormalizedProviderEventType.TextStart | NormalizedProviderEventType.TextDelta | NormalizedProviderEventType.TextEnd;
export type NormalizedReasoningEventType = NormalizedProviderEventType.ReasoningStart | NormalizedProviderEventType.ReasoningDelta | NormalizedProviderEventType.ReasoningEnd;
export type NormalizedToolInputEventType = NormalizedProviderEventType.ToolInputStart | NormalizedProviderEventType.ToolInputDelta | NormalizedProviderEventType.ToolInputEnd;
interface Fragment { readonly id: string; readonly text: string; readonly metadataJson: string }
// Optional-never members allow bounded diagnostic callers to inspect a payload
// without destroying the discriminated normalized union.
interface AbsentPayloads { readonly text?: never; readonly reasoning?: never; readonly toolInput?: never; readonly toolCall?: never; readonly finish?: never; readonly providerError?: never; readonly attachmentRejections?: never }
type Event<T, K extends keyof AbsentPayloads, P> = Omit<AbsentPayloads, K> & { readonly type: T } & { readonly [M in K]: P };
export type NormalizedProviderEvent =
  | Event<NormalizedTextEventType, "text", Fragment>
  | Event<NormalizedReasoningEventType, "reasoning", Fragment>
  | Event<NormalizedToolInputEventType, "toolInput", Fragment & { readonly name: string }>
  | Event<NormalizedProviderEventType.ToolCall, "toolCall", { readonly id: string; readonly name: string; readonly inputJson: string; readonly metadataJson: string }>
  | Event<NormalizedProviderEventType.Finish, "finish", ProviderFinishPayload>
  | Event<NormalizedProviderEventType.ProviderError, "providerError", ProviderErrorPayload>
  | Event<NormalizedProviderEventType.AttachmentRejections, "attachmentRejections", ProviderAttachmentRejectionsPayload>;

import { MaxIdBytes, MaxProviderContextTextJsonBytes, validateProviderStreamEvent, validProviderMetadataJson, validProviderToolCallInputJson } from "@tetral/gateway-protocol/src/bounds.js";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
/** Private fragments may split a surrogate pair, but controls never carry bodies. */
export function validateNormalizedProviderEvent(event: NormalizedProviderEvent): boolean {
  const count = [event.text,event.reasoning,event.toolInput,event.toolCall,event.finish,event.providerError,event.attachmentRejections].filter(value=>value!==undefined).length;
  if (count !== 1) return false;
  const encoder = new TextEncoder();
  const id = (value: string): boolean => value.length > 0 && encoder.encode(value).byteLength <= MaxIdBytes;
  switch(event.type) {
    case NormalizedProviderEventType.TextStart:
    case NormalizedProviderEventType.TextDelta:
    case NormalizedProviderEventType.TextEnd:
      return id(event.text.id) && validProviderMetadataJson(event.text.metadataJson) &&
        (event.type === NormalizedProviderEventType.TextDelta ? encoder.encode(event.text.text).byteLength <= MaxProviderContextTextJsonBytes : event.text.text === "");
    case NormalizedProviderEventType.ReasoningStart:
    case NormalizedProviderEventType.ReasoningDelta:
    case NormalizedProviderEventType.ReasoningEnd:
      return id(event.reasoning.id) && validProviderMetadataJson(event.reasoning.metadataJson) &&
        (event.type === NormalizedProviderEventType.ReasoningDelta ? encoder.encode(event.reasoning.text).byteLength <= MaxProviderContextTextJsonBytes : event.reasoning.text === "");
    case NormalizedProviderEventType.ToolInputStart:
    case NormalizedProviderEventType.ToolInputDelta:
    case NormalizedProviderEventType.ToolInputEnd:
      return id(event.toolInput.id) && id(event.toolInput.name) && validProviderMetadataJson(event.toolInput.metadataJson) &&
        (event.type === NormalizedProviderEventType.ToolInputDelta || event.toolInput.text === "");
    case NormalizedProviderEventType.ToolCall:
      return id(event.toolCall.id) && id(event.toolCall.name) && validProviderToolCallInputJson(event.toolCall.inputJson) && validProviderMetadataJson(event.toolCall.metadataJson);
    case NormalizedProviderEventType.Finish:
      return validateProviderStreamEvent({ frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,finish:event.finish }).ok;
    case NormalizedProviderEventType.ProviderError:
      return validateProviderStreamEvent({ frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,providerError:event.providerError }).ok;
    case NormalizedProviderEventType.AttachmentRejections:
      return validateProviderStreamEvent({ frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS,attachmentRejections:event.attachmentRejections }).ok;
    default: return false;
  }
}
