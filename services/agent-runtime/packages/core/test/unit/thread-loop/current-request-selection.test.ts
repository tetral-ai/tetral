import {expect,test} from "bun:test";
import fixture from "../../../../protocol/testdata/current-request-selection.json";
import {extractThreadTurnCheckpoint,selectCurrentRequestStart,ThreadTurnLoadFactsSchema,validateCurrentRequestMessage} from "../../../src/thread-loop/turn/load.js";
import type {RuntimeContextEntry} from "../../../src/contracts/runtime.js";
for(const item of fixture){
 // Duplicate durable Message→Request links are rejected by the Bridge projection,
 // before this model-free Runtime envelope exists; its owning Go test uses this row.
 if(item.name==="duplicate-selected-assistant-fails")continue;
 test(`shared cold selection: ${item.name}`,()=>{
  const facts=ThreadTurnLoadFactsSchema.parse({events:item.events,internalRepairs:[]});
  const messages:RuntimeContextEntry[]=item.messages.map(message=>({messageSequence:message.messageSequence,contextKind:"assistant",parts:[{type:"text",text:"fixture"}]}));
  if(item.error){expect(()=>extractThreadTurnCheckpoint({messages,facts})).toThrow("duplicate Request Start");return;}
  expect(selectCurrentRequestStart(facts)?.modelRequestId??null).toBe(item.expectedRequestId);
  // The checkpoint includes validated member facts; rows with historical closed
  // retention census intentionally only exercise the shared selection above.
  if(item.name==="ordinary-idle-closes-request"||item.name==="terminal-closes-request"||item.name==="ordinary-new-run-excludes-old-request"||item.name==="latest-no-content-does-not-fall-back")return;
  const checkpoint=extractThreadTurnCheckpoint({messages,facts});
  validateCurrentRequestMessage(item.expected,messages,checkpoint);
  if(item.expected!==null){expect(()=>validateCurrentRequestMessage({...item.expected,modelRequestId:"unrelated"},messages,checkpoint)).toThrow("differs from checkpoint");expect(()=>validateCurrentRequestMessage({...item.expected,assistantMessageSequence:99},messages,checkpoint)).toThrow("no unique");}
 });
}
