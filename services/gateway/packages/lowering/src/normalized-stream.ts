/** Gateway-private normalized SDK fragments. None of these fragments cross the RPC. */
import type { ProviderFinishPayload, ProviderErrorPayload, ProviderAttachmentRejectionsPayload } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
export enum NormalizedProviderEventType {
  PROVIDER_STREAM_EVENT_TYPE_TEXT_START = "text-start",
  PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA = "text-delta",
  PROVIDER_STREAM_EVENT_TYPE_TEXT_END = "text-end",
  PROVIDER_STREAM_EVENT_TYPE_REASONING_START = "reasoning-start",
  PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA = "reasoning-delta",
  PROVIDER_STREAM_EVENT_TYPE_REASONING_END = "reasoning-end",
  PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START = "tool-input-start",
  PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA = "tool-input-delta",
  PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END = "tool-input-end",
  PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL = "tool-call",
  PROVIDER_STREAM_EVENT_TYPE_FINISH = "finish",
  PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR = "provider-error",
  PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS = "attachment-rejections",
}
export type NormalizedTextEventType = NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START | NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA | NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END;
export type NormalizedReasoningEventType = NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START | NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA | NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END;
export type NormalizedToolInputEventType = NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START | NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA | NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END;
interface Fragment { readonly id: string; readonly text: string; readonly metadataJson: string }
// Optional-never members allow bounded diagnostic callers to inspect a payload
// without destroying the discriminated normalized union.
interface AbsentPayloads { readonly text?: never; readonly reasoning?: never; readonly toolInput?: never; readonly toolCall?: never; readonly finish?: never; readonly providerError?: never; readonly attachmentRejections?: never }
type Event<T, K extends keyof AbsentPayloads, P> = Omit<AbsentPayloads, K> & { readonly type: T } & { readonly [M in K]: P };
export type NormalizedProviderEvent =
  | Event<NormalizedTextEventType, "text", Fragment>
  | Event<NormalizedReasoningEventType, "reasoning", Fragment>
  | Event<NormalizedToolInputEventType, "toolInput", Fragment & { readonly name: string }>
  | Event<NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL, "toolCall", { readonly id: string; readonly name: string; readonly inputJson: string; readonly metadataJson: string }>
  | Event<NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH, "finish", ProviderFinishPayload>
  | Event<NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR, "providerError", ProviderErrorPayload>
  | Event<NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS, "attachmentRejections", ProviderAttachmentRejectionsPayload>;

import { MaxIdBytes, MaxProviderContextTextJsonBytes, validateProviderStreamEvent, validProviderMetadataJson, validProviderToolCallInputJson } from "@tetral/gateway-protocol/src/bounds.js";
import { ProviderStreamEventType } from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
/** Private fragments may split a surrogate pair, but controls never carry bodies. */
export function validateNormalizedProviderEvent(event: NormalizedProviderEvent): boolean {
  const count = [event.text,event.reasoning,event.toolInput,event.toolCall,event.finish,event.providerError,event.attachmentRejections].filter(value=>value!==undefined).length;
  if (count !== 1) return false;
  const encoder = new TextEncoder();
  const id = (value: string): boolean => value.length > 0 && encoder.encode(value).byteLength <= MaxIdBytes;
  switch(event.type) {
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_START:
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA:
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_END:
      return id(event.text.id) && validProviderMetadataJson(event.text.metadataJson) &&
        (event.type === NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TEXT_DELTA ? encoder.encode(event.text.text).byteLength <= MaxProviderContextTextJsonBytes : event.text.text === "");
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_START:
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA:
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_END:
      return id(event.reasoning.id) && validProviderMetadataJson(event.reasoning.metadataJson) &&
        (event.type === NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_REASONING_DELTA ? encoder.encode(event.reasoning.text).byteLength <= MaxProviderContextTextJsonBytes : event.reasoning.text === "");
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_START:
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA:
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_END:
      return id(event.toolInput.id) && id(event.toolInput.name) && validProviderMetadataJson(event.toolInput.metadataJson) &&
        (event.type === NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_INPUT_DELTA || event.toolInput.text === "");
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_TOOL_CALL:
      return id(event.toolCall.id) && id(event.toolCall.name) && validProviderToolCallInputJson(event.toolCall.inputJson) && validProviderMetadataJson(event.toolCall.metadataJson);
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH:
      return validateProviderStreamEvent({ frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_FINISH,finish:event.finish }).ok;
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR:
      return validateProviderStreamEvent({ frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_PROVIDER_ERROR,providerError:event.providerError }).ok;
    case NormalizedProviderEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS:
      return validateProviderStreamEvent({ frameSequence:1,type:ProviderStreamEventType.PROVIDER_STREAM_EVENT_TYPE_ATTACHMENT_REJECTIONS,attachmentRejections:event.attachmentRejections }).ok;
    default: return false;
  }
}
