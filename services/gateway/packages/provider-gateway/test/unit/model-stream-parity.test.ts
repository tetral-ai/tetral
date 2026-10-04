/** Compare complete Gateway semantics and native requests against the pinned high-level baseline. */
import {expect,test} from "bun:test";
import {streamText} from "ai";
import type {FetchFunction} from "@ai-sdk/provider-utils";
import {ProviderClientRegistry} from "../../src/providers/clients.js";
import type {GatewayModelStreamResult} from "../../src/providers/clients.js";
import {ProviderBlockAssembler} from "../../src/providers/block-assembler.js";
import {ProviderAssemblyCalibrationCandidate} from "../../src/providers/resource-policy.js";
import {validProviderRequest} from "./fixtures.js";
import type {ResolvedProviderCredential} from "../../src/providers/credentials.js";
import type {ProviderStreamEvent} from "@tetral/gateway-protocol/src/gen/tetral/provider_gateway/v1/provider_gateway.js";

const event=(part:unknown,anthropic=false)=>`${anthropic?`event: ${(part as {type:string}).type}\n`:""}data: ${JSON.stringify(part)}\n\n`;
function wire(family:"anthropic"|"openai"|"deepseek"):string {
 const e=(part:unknown)=>event(part,family==="anthropic");
 if(family==="anthropic")return [
  {type:"message_start",message:{id:"msg_test",type:"message",role:"assistant",model:"claude-opus-4-8",content:[],stop_reason:null,stop_sequence:null,usage:{input_tokens:10,output_tokens:1,cache_read_input_tokens:3,cache_creation_input_tokens:2}}},
  {type:"content_block_start",index:0,content_block:{type:"thinking",thinking:"",signature:""}},
  {type:"content_block_delta",index:0,delta:{type:"thinking_delta",thinking:"reason"}},
  {type:"content_block_delta",index:0,delta:{type:"signature_delta",signature:"signed"}},
  {type:"content_block_stop",index:0},
  {type:"content_block_start",index:1,content_block:{type:"text",text:""}},
  {type:"content_block_delta",index:1,delta:{type:"text_delta",text:"hello"}},
  {type:"content_block_stop",index:1},
  {type:"content_block_start",index:2,content_block:{type:"tool_use",id:"call_test",name:"Read",input:{}}},
  {type:"content_block_delta",index:2,delta:{type:"input_json_delta",partial_json:'{"path":"test"}'}},
  {type:"content_block_stop",index:2},
  {type:"message_delta",delta:{stop_reason:"tool_use",stop_sequence:null},usage:{output_tokens:7}},
  {type:"message_stop"},
 ].map(e).join("");
 if(family==="openai")return [
  {type:"response.created",response:{id:"resp_test",created_at:1,model:"gpt-5.5"}},
  {type:"response.output_item.added",output_index:0,item:{type:"message",id:"msg_test"}},
  {type:"response.output_text.delta",item_id:"msg_test",delta:"hello"},
  {type:"response.output_item.done",output_index:0,item:{type:"message",id:"msg_test"}},
  {type:"response.output_item.added",output_index:1,item:{type:"function_call",id:"fc_test",call_id:"call_test",name:"Read",arguments:""}},
  {type:"response.function_call_arguments.delta",item_id:"fc_test",output_index:1,delta:'{"path":"test"}'},
  {type:"response.output_item.done",output_index:1,item:{type:"function_call",id:"fc_test",call_id:"call_test",name:"Read",arguments:'{"path":"test"}',status:"completed"}},
  {type:"response.completed",response:{usage:{input_tokens:15,output_tokens:7,input_tokens_details:{cached_tokens:3},output_tokens_details:{reasoning_tokens:2}}}},
 ].map(e).join("");
 return [
  {id:"chat_test",choices:[{index:0,delta:{reasoning_content:"reason",content:"hello"},finish_reason:null}]},
  {id:"chat_test",choices:[{index:0,delta:{tool_calls:[{index:0,id:"call_test",type:"function",function:{name:"Read",arguments:'{"path":"test"}'}}]},finish_reason:null}]},
  {id:"chat_test",choices:[{index:0,delta:{},finish_reason:"tool_calls"}],usage:{prompt_tokens:15,completion_tokens:7,total_tokens:22,prompt_tokens_details:{cached_tokens:3}}},
 ].map(e).join("")+"data: [DONE]\n\n";
}
for(const family of ["anthropic","openai","deepseek"] as const)test(`official ${family} native request and complete output parity`,async()=>{
 const requests:unknown[]=[];
 const fetchImpl=Object.assign(async(input:Parameters<FetchFunction>[0],init?:Parameters<FetchFunction>[1])=>{
  const request=new Request(input,init);requests.push(JSON.parse(await request.text()));
  return new Response(wire(family),{headers:{"content-type":"text/event-stream"}});
 },{preconnect:()=>{}}) as FetchFunction;
 const request=validProviderRequest({model:{providerId:family,modelId:family==="anthropic"?"claude-opus-4-8":family==="openai"?"gpt-5.5":"deepseek-v4-pro",variant:""}});
 const credential:ResolvedProviderCredential={source:"session",authType:"provider_api_key",providerId:family,supplyMode:family==="anthropic"?"anthropic-api-key":family==="openai"?"openai-api-key":"deepseek-api-key",vaultId:"test",credentialId:"test",accessMode:"api_key",apiKey:"fixture"};
 const outputs:ProviderStreamEvent[][]=[];
 for(const baseline of [true,false]){
  const registry=new ProviderClientRegistry({fetch:fetchImpl,...(baseline?{streamModel:input=>streamText(input as unknown as Parameters<typeof streamText>[0]) as GatewayModelStreamResult}:{})});
  let id=0;const assembler=new ProviderBlockAssembler({bounds:ProviderAssemblyCalibrationCandidate,request,allocateEventId:()=>`evt_${String(++id).padStart(32,"0")}`});
  const complete:ProviderStreamEvent[]=[];
  try {for await(const part of registry.stream({request,credential}))complete.push(...assembler.accept(part));assembler.assertComplete();outputs.push(complete);}
  finally {assembler.release();await registry.close();}
 }
 expect(requests).toHaveLength(2);expect(requests[1]).toEqual(requests[0]);expect(outputs[1]).toEqual(outputs[0]);
 expect(outputs[1]?.some(part=>part.toolCallComplete?.inputJson==='{"path":"test"}')).toBe(true);expect(outputs[1]?.at(-1)?.finish).toBeDefined();
});
