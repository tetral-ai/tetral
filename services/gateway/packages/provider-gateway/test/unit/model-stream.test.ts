import {expect,test} from "bun:test";
import {credentials} from "@grpc/grpc-js";
import {jsonSchema} from "ai";
import {ProviderGatewayServiceClient} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";
import {createContentLifecycleGatewayFixture} from "../fixtures/content-lifecycle-gateway.js";
import {languageModelPrompt,parsedToolInput} from "../../src/providers/model-stream.js";

test("resolved prompt preserves metadata, signed reasoning and grouped Tool results",()=>{
 const image=new Uint8Array([0x89,0x50,0x4e,0x47]),options={anthropic:{cacheControl:{type:"ephemeral"}}};
 const prompt=languageModelPrompt([
  {role:"user",content:[{type:"text",text:""},{type:"image",image,mediaType:"image/jpeg",providerOptions:options}]},
  {role:"assistant",content:[{type:"reasoning",text:"private",providerOptions:{anthropic:{signature:"signed"}}},{type:"tool-call",toolCallId:"a",toolName:"Read",input:{}},{type:"tool-call",toolCallId:"b",toolName:"Read",input:{}}]},
  {role:"tool",content:[{type:"tool-result",toolCallId:"a",toolName:"Read",output:{type:"json",value:{ok:true}}}]},
  {role:"tool",content:[{type:"tool-result",toolCallId:"b",toolName:"Read",output:{type:"error-json",value:{error:"failed"}}}]},
 ]);
 expect(prompt).toHaveLength(3);expect(prompt[0]).toEqual({role:"user",content:[{type:"file",data:image,mediaType:"image/png",providerOptions:options}]});
 expect(prompt[1]).toMatchObject({role:"assistant",content:[{type:"reasoning",text:"private",providerOptions:{anthropic:{signature:"signed"}}},{type:"tool-call",toolCallId:"a"},{type:"tool-call",toolCallId:"b"}]});
 expect(prompt[2]?.content).toHaveLength(2);
 expect(()=>languageModelPrompt([{role:"assistant",content:[{type:"tool-call",toolCallId:"missing",toolName:"Read",input:{}}]}])).toThrow("Tool result is missing");
});

test("Tool parsing keeps SDK safe fallback and prototype protection",async()=>{
 const call={type:"tool-call" as const,toolCallId:"call",toolName:"Read",input:""};
 const tools={Read:{inputSchema:jsonSchema({type:"object"})}};
 expect(await parsedToolInput(call,tools)).toEqual({});
 expect(await parsedToolInput({...call,input:'{"value":1}'},tools)).toEqual({value:1});
 for(const input of ['{"__proto__":{"polluted":true}}','{"constructor":{"prototype":{"polluted":true}}}','broken'])expect(await parsedToolInput({...call,input},tools)).toBe(input);
 expect(await parsedToolInput({...call,toolName:"Unknown",input:'{"value":1}'},tools)).toEqual({value:1});
 expect(({} as Record<string,unknown>).polluted).toBeUndefined();
});

for(const options of [
 {scenario:"empty-blocks-storm" as const,emptyRecords:20},
 {scenario:"tool-whitespace" as const,textCodeUnits:33,fragmentCodeUnits:4},
])test(`actual official adapter completes without history rejection: ${options.scenario}`,async()=>{
 const fixture=await createContentLifecycleGatewayFixture(options),client=new ProviderGatewayServiceClient(fixture.address,credentials.createInsecure());
 try {
  const events=[];for await(const event of client.streamProviderRequest(fixture.request(),fixture.metadata()))events.push(event);
  expect(events.at(-1)?.finish).toBeDefined();expect(events.some(event=>event.providerError!==undefined)).toBe(false);
  if(options.scenario==="tool-whitespace")expect(events[0]?.toolCallComplete?.inputJson).toBe("{}");
  await fixture.shutdown();const state=fixture.observations();expect(state.nativeIterators).toMatchObject({active:0,started:1,joined:1});expect(state.sdkTotals.active).toBe(0);expect(state.activeProviderSources).toBe(0);expect(state.assembly?.retainedBytes).toBe(0);
 } finally {client.close();await fixture.shutdown();}
});

// These are bridge ownership controls, not substitutes for the real-adapter parity tests.
const terminalPart:Extract<import("@ai-sdk/provider").LanguageModelV3StreamPart,{type:"finish"}>={
 type:"finish",finishReason:{unified:"stop",raw:"end_turn"},
 usage:{inputTokens:{total:7,noCache:5,cacheRead:2,cacheWrite:undefined},outputTokens:{total:3,text:2,reasoning:1}},
};
function bridgeInput(stream:ReadableStream<import("@ai-sdk/provider").LanguageModelV3StreamPart>):import("../../src/providers/clients.js").GatewayModelStreamInput {
 const model:import("@ai-sdk/provider").LanguageModelV3={specificationVersion:"v3",provider:"unit",modelId:"unit",supportedUrls:{},async doStream(){return {stream};},async doGenerate(){throw new Error("unused");}};
 return {model,messages:[{role:"user",content:"test"}],maxRetries:0};
}

test("V3 Finish waits for clean EOF before ordered usage and public Finish",async()=>{
 let source!:ReadableStreamDefaultController<import("@ai-sdk/provider").LanguageModelV3StreamPart>;
 const stream=new ReadableStream<import("@ai-sdk/provider").LanguageModelV3StreamPart>({start(c){source=c;c.enqueue(terminalPart);}});
 const {streamLanguageModel}=await import("../../src/providers/model-stream.js");
 const iterator=streamLanguageModel(bridgeInput(stream)).fullStream[Symbol.asyncIterator]();
 let delivered=false;const first=iterator.next().then(value=>{delivered=true;return value;});
 await Bun.sleep(0);expect(delivered).toBe(false);
 source.close();const step=await first;expect(step.value).toMatchObject({type:"finish-step",finishReason:"stop",rawFinishReason:"end_turn",usage:{inputTokens:7,outputTokens:3,totalTokens:10,cachedInputTokens:2,reasoningTokens:1}});
 expect((await iterator.next()).value).toMatchObject({type:"finish",totalUsage:{inputTokens:7,outputTokens:3,totalTokens:10}});expect((await iterator.next()).done).toBe(true);expect(stream.locked).toBe(false);
});

test("body rejection after V3 Finish preserves failure and joins cancellation without successful Finish",async()=>{
 let source!:ReadableStreamDefaultController<import("@ai-sdk/provider").LanguageModelV3StreamPart>;
 const stream=new ReadableStream<import("@ai-sdk/provider").LanguageModelV3StreamPart>({start(c){source=c;c.enqueue(terminalPart);}});
 let cancelEntered=false,cancelJoined=false,released=false,releaseJoin!:()=>void;
 const joinGate=new Promise<void>(resolve=>{releaseJoin=resolve;});
 const getReader=stream.getReader.bind(stream);
 Object.defineProperty(stream,"getReader",{value:()=>{
  const reader=getReader();
  return new Proxy(reader,{get(target,key){
   if(key==="cancel")return async(reason:unknown)=>{cancelEntered=true;try{await target.cancel(reason);}finally{await joinGate;cancelJoined=true;}};
   if(key==="releaseLock")return ()=>{released=true;target.releaseLock();};
   const value=Reflect.get(target,key,target);return typeof value==="function"?value.bind(target):value;
  }});
 }});
 const {streamLanguageModel}=await import("../../src/providers/model-stream.js"),iterator=streamLanguageModel(bridgeInput(stream)).fullStream[Symbol.asyncIterator]();
 let settled=false;const first=iterator.next();void first.then(()=>{settled=true;},()=>{settled=true;});
 await Bun.sleep(0);expect(settled).toBe(false);
 const failure=new TypeError("fixture body read rejected");source.error(failure);await Bun.sleep(0);
 expect(cancelEntered).toBe(true);expect(settled).toBe(false);expect(cancelJoined).toBe(false);
 releaseJoin();await expect(first).rejects.toBe(failure);expect(cancelJoined).toBe(true);expect(released).toBe(true);expect((await iterator.next()).done).toBe(true);
});
