/** Gateway's one-step V3 boundary. Official adapters own wire parsing; no output history or tee is retained here. */
import type { LanguageModelV3, LanguageModelV3CallOptions, LanguageModelV3Message, LanguageModelV3StreamPart, LanguageModelV3TextPart, LanguageModelV3FilePart, LanguageModelV3ReasoningPart, LanguageModelV3ToolCallPart, SharedV3Warning } from "@ai-sdk/provider";
import { asSchema, safeParseJSON, safeValidateTypes } from "@ai-sdk/provider-utils";
import { MissingToolResultsError } from "ai";
import type { ModelMessage, TextStreamPart, ToolSet } from "ai";
import type { GatewayModelStreamInput, GatewayModelStreamResult } from "./clients.js";

/** Count-only lifecycle telemetry; these counters never reject content or retain records. */
export interface ProviderModelStreamResources {
  readonly operationId: number;
  readonly sourceRecords: number;
  readonly active: boolean;
}
/** Convert only the resolved, lowered message surface owned by Gateway (no remote downloads). */
export function languageModelPrompt(messages: readonly ModelMessage[]): LanguageModelV3Message[] {
  const prompt: LanguageModelV3Message[] = [];
  const pending = new Set<string>();
  for (const message of messages) {
    if ((message.role === "user" || message.role === "system") && pending.size > 0) {
      throw new MissingToolResultsError({toolCallIds:[...pending]});
    }
    const options = message.providerOptions === undefined ? {} : {providerOptions:message.providerOptions};
    if (message.role === "system") { prompt.push({role:"system",content:message.content,...options}); continue; }
    if (message.role === "user") {
      const content = typeof message.content === "string" ? [{type:"text" as const,text:message.content}] : message.content.flatMap<LanguageModelV3TextPart | LanguageModelV3FilePart>(part => {
        switch (part.type) {
          case "text": return part.text === "" ? [] : [part];
          case "image": {
            if (!(part.image instanceof Uint8Array)) throw new Error("Gateway image must be resolved bytes");
            return [{type:"file" as const,data:part.image,mediaType:imageMediaType(part.image) ?? part.mediaType ?? "image/*",...(part.providerOptions === undefined ? {} : {providerOptions:part.providerOptions})}];
          }
          case "file": {
            if (!(part.data instanceof Uint8Array)) throw new Error("Gateway file must be resolved bytes");
            return [{type:"file" as const,data:part.data,mediaType:part.mediaType,...(part.filename === undefined ? {} : {filename:part.filename}),...(part.providerOptions === undefined ? {} : {providerOptions:part.providerOptions})}];
          }
        }
      });
      prompt.push({role:"user",content,...options}); continue;
    }
    if (message.role === "assistant") {
      const content = typeof message.content === "string" ? [{type:"text" as const,text:message.content}] : message.content.flatMap<LanguageModelV3TextPart | LanguageModelV3ReasoningPart | LanguageModelV3ToolCallPart>(part => {
        switch (part.type) {
          case "text": return part.text === "" && part.providerOptions == null ? [] : [part];
          case "reasoning": return [part];
          case "tool-call": if (!part.providerExecuted) pending.add(part.toolCallId); return [part];
          // Gateway lowering does not generate assistant files, results, or approval requests.
          default: throw new Error("unsupported Gateway assistant content");
        }
      });
      prompt.push({role:"assistant",content,...options}); continue;
    }
    const content = message.content.map(part => {
      if (part.type !== "tool-result") throw new Error("unsupported Gateway tool content");
      pending.delete(part.toolCallId);
      if (part.output.type !== "json" && part.output.type !== "error-json") throw new Error("unsupported Gateway tool result");
      return {...part,output:part.output};
    });
    const previous = prompt.at(-1);
    if (previous?.role === "tool") previous.content.push(...content);
    else if (content.length > 0) prompt.push({role:"tool",content,...options});
  }
  if (pending.size > 0) throw new MissingToolResultsError({toolCallIds:[...pending]});
  return prompt;
}

async function languageModelTools(tools: ToolSet | undefined): Promise<LanguageModelV3CallOptions["tools"]> {
  if (tools === undefined || Object.keys(tools).length === 0) return undefined;
  return Promise.all(Object.entries(tools).map(async ([name,tool]) => tool.type === "provider"
    ? {type:"provider" as const,name,id:tool.id,args:tool.args}
    : {type:"function" as const,name,...(tool.description === undefined ? {} : {description:tool.description}),inputSchema:await asSchema(tool.inputSchema).jsonSchema,
      ...(tool.providerOptions === undefined ? {} : {providerOptions:tool.providerOptions}),...(tool.strict === undefined ? {} : {strict:tool.strict}),
      ...(tool.inputExamples === undefined ? {} : {inputExamples:tool.inputExamples})}));
}

/** Preserve SDK's safe parsing fallback: malformed/schema-invalid calls still carry parsed JSON or their raw input. */
export async function parsedToolInput(part: Extract<LanguageModelV3StreamPart,{type:"tool-call"}>, tools: ToolSet | undefined): Promise<unknown> {
  const tool = tools?.[part.toolName];
  if (tool !== undefined) {
    const schema = asSchema(tool.inputSchema);
    const parsed = part.input.trim() === "" ? await safeValidateTypes({value:{},schema}) : await safeParseJSON({text:part.input,schema});
    if (parsed.success) return parsed.value;
  } else if (part.providerExecuted && part.dynamic && part.input.trim() === "") return {};
  const fallback = await safeParseJSON({text:part.input});
  return fallback.success ? fallback.value : part.input;
}

export function streamLanguageModel(input: GatewayModelStreamInput): GatewayModelStreamResult {
  return {fullStream:(async function* () {
    let reader: ReadableStreamDefaultReader<LanguageModelV3StreamPart> | undefined;
    let complete = false;
    let finish: Extract<LanguageModelV3StreamPart,{type:"finish"}> | undefined;
    let warnings: SharedV3Warning[] = [];
    let response = {id:"",timestamp:new Date(),modelId:""};
    try {
      const model = input.model as LanguageModelV3;
      if (model.specificationVersion !== "v3") throw new Error("Gateway requires an official LanguageModelV3 adapter");
      response.modelId=model.modelId;
      const tools = await languageModelTools(input.tools);
      const responseFormat = await input.output?.responseFormat;
      const call: LanguageModelV3CallOptions = {
        prompt:languageModelPrompt(input.messages),
        ...(tools === undefined ? {} : {tools,toolChoice:{type:"auto"}}),
        ...(responseFormat === undefined ? {} : {responseFormat}),
        ...(input.providerOptions === undefined ? {} : {providerOptions:input.providerOptions}),
        ...(input.maxOutputTokens === undefined ? {} : {maxOutputTokens:input.maxOutputTokens}),
        ...(input.temperature === undefined ? {} : {temperature:input.temperature}),
        ...(input.topP === undefined ? {} : {topP:input.topP}),
        ...(input.topK === undefined ? {} : {topK:input.topK}),
        ...(input.headers === undefined ? {} : {headers:input.headers}),
        ...(input.abortSignal === undefined ? {} : {abortSignal:input.abortSignal}),
      };
      const {stream} = await model.doStream(call);
      reader = stream.getReader();
      while (true) {
        const next = await reader.read();
        if (next.done) {
          complete=true;
          // The high-level SDK exposes terminal usage only after the adapter has joined EOF.
          const usage=finish === undefined ? {
            inputTokens:undefined,inputTokenDetails:{noCacheTokens:undefined,cacheReadTokens:undefined,cacheWriteTokens:undefined},
            outputTokens:undefined,outputTokenDetails:{textTokens:undefined,reasoningTokens:undefined},totalTokens:undefined,
          } : {
            inputTokens:finish.usage.inputTokens.total,
            inputTokenDetails:{noCacheTokens:finish.usage.inputTokens.noCache,cacheReadTokens:finish.usage.inputTokens.cacheRead,cacheWriteTokens:finish.usage.inputTokens.cacheWrite},
            outputTokens:finish.usage.outputTokens.total,
            outputTokenDetails:{textTokens:finish.usage.outputTokens.text,reasoningTokens:finish.usage.outputTokens.reasoning},
            totalTokens:finish.usage.inputTokens.total == null && finish.usage.outputTokens.total == null ? undefined : (finish.usage.inputTokens.total ?? 0)+(finish.usage.outputTokens.total ?? 0),
            ...(finish.usage.raw === undefined ? {} : {raw:finish.usage.raw}),
            reasoningTokens:finish.usage.outputTokens.reasoning,cachedInputTokens:finish.usage.inputTokens.cacheRead,
          };
          const finishReason=finish?.finishReason.unified ?? "other",rawFinishReason=finish?.finishReason.raw;
          reportModelWarnings(warnings,input.onWarnings);
          yield {type:"finish-step",finishReason,rawFinishReason,usage,providerMetadata:finish?.providerMetadata,response};
          yield {type:"finish",finishReason,rawFinishReason,totalUsage:usage};
          return;
        }
        const part = next.value;
        switch (part.type) {
          case "text-delta": if (part.delta !== "") yield {...part,text:part.delta}; break;
          case "reasoning-delta": yield {...part,text:part.delta}; break;
          case "text-start": case "text-end": case "reasoning-start": case "reasoning-end":
          case "tool-input-start": case "tool-input-delta": case "tool-input-end": case "error": yield part; break;
          case "tool-call": yield {...part,input:await parsedToolInput(part,input.tools)} as TextStreamPart<ToolSet>; break;
          case "finish": finish=part; break;
          case "stream-start": warnings=part.warnings; break;
          case "response-metadata":
            response={id:part.id ?? response.id,timestamp:part.timestamp ?? response.timestamp,modelId:part.modelId ?? response.modelId};
            break;
          // These were raw/ignored by the Gateway raiser before this conversion.
          case "raw": case "source": case "file": case "tool-result": case "tool-approval-request": break;
        }
      }
    } catch (error) {
      // The former high-level stream emitted initialization failures as error parts,
      // but rejected its reader for body/pipeline failures. Preserve that distinction.
      if (reader !== undefined) throw error;
      yield {type:"error",error};
    }
    finally {
      if (reader !== undefined) {
        if (!complete) try {await reader.cancel(input.abortSignal?.reason);} catch { /* Keep the primary provider failure. */ }
        reader.releaseLock();
      }
    }
  })()};
}

function imageMediaType(bytes: Uint8Array): string | undefined {
  const signatures: readonly [string,readonly (number|null)[]][] = [
    ["image/gif",[0x47,0x49,0x46]],["image/png",[0x89,0x50,0x4e,0x47]],["image/jpeg",[0xff,0xd8]],
    ["image/webp",[0x52,0x49,0x46,0x46,null,null,null,null,0x57,0x45,0x42,0x50]],
    ["image/bmp",[0x42,0x4d]],["image/tiff",[0x49,0x49,0x2a,0]],["image/tiff",[0x4d,0x4d,0,0x2a]],
    ["image/avif",[0,0,0,0x20,0x66,0x74,0x79,0x70,0x61,0x76,0x69,0x66]],
    ["image/heic",[0,0,0,0x20,0x66,0x74,0x79,0x70,0x68,0x65,0x69,0x63]],
  ];
  return signatures.find(([,prefix]) => bytes.length >= prefix.length && prefix.every((byte,index)=>byte===null || bytes[index]===byte))?.[0];
}

/**
 * Reports adapter stream-start warnings as type and feature only. Messages and
 * details are adapter text and never leave this boundary; an observer failure
 * cannot change the stream.
 */
function reportModelWarnings(warnings: readonly SharedV3Warning[], onWarnings: GatewayModelStreamInput["onWarnings"]): void {
  if (warnings.length === 0 || onWarnings === undefined) return;
  try {
    onWarnings(warnings.map(warning => warning.type === "other" ? {type:warning.type} : {type:warning.type,feature:warning.feature}));
  } catch { /* Diagnostics fail open. */ }
}
