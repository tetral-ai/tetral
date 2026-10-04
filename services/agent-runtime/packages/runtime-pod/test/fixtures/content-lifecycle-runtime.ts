/** Real Runtime factory child for Go-owned durable content/Tool compositions. */
import { access, appendFile, readFile, rename, writeFile } from "node:fs/promises";
import { appendFileSync } from "node:fs";
import { Metadata } from "@grpc/grpc-js";
import { z } from "zod/v4";
import { buildRuntimePodCommandDependencies } from "../../src/command.js";
import { loadRuntimePodConfig } from "../../src/config.js";
import { buildRuntimeCoreHosts } from "../../src/core-hosts.js";
import type { RuntimeThreadAddressState } from "@tetral/agent-runtime-core/src/thread-loop/input/accepted-input.js";
import type { RuntimeCoreHosts } from "../../src/core-hosts.js";
import { createJsonLogger } from "../../src/logger.js";
import { BridgeRuntimeProcess } from "../../src/runtime-process.js";

const input = z.strictObject({
 bridgeAddress:z.string().min(1),gatewayAddress:z.string().min(1),podUID:z.string().min(1),processID:z.string().min(1),token:z.string().min(1),directory:z.string().min(1),
 holdRefreshAfterDeclaration:z.boolean().optional(),observeQueuedTools:z.boolean().optional(),evictIdle:z.boolean().optional(),approvalMode:z.enum(["full_access","ask_for_approval","approve_for_me"]).optional(),
 evictIdleStopReason:z.enum(["requires_action","end_turn"]).optional(),observeContainerMemory:z.boolean().optional(),
 observeStages:z.boolean().optional(),controlCommands:z.boolean().optional(),observeColdLoad:z.boolean().optional(),observeContextEntries:z.boolean().optional(),maxConcurrentTools:z.number().int().positive().optional(),
}).parse(JSON.parse(await readFile(process.argv[2]!,"utf8")) as unknown);
const stop = new AbortController();
const metadataFactory=async()=>{const metadata=new Metadata();metadata.set("authorization",`Bearer ${input.token}`);return metadata;};
const exists=async(path:string)=>{try{await access(path);return true;}catch{return false;}};
const wait=async(path:string)=>{const deadline=Date.now()+180_000;while(!await exists(path)){stop.signal.throwIfAborted();if(Date.now()>=deadline)throw new Error("fixture barrier deadline");await new Promise<void>(resolve=>setTimeout(resolve,10));}};
const trace=async(boundary:string,fields:Readonly<Record<string,string|number|boolean>>={})=>{await appendFile(`${input.directory}/trace.jsonl`,`${JSON.stringify({boundary,...fields})}\n`);};
const marker=async(name:string,value:unknown)=>{await writeFile(`${input.directory}/${name}.json`,JSON.stringify(value));};
await writeFile(`${input.directory}/fixture-token`,input.token);
// No Kubernetes request is sent: the real bootstrap checks readable material;
// TokenReview itself is the explicit authenticated test port supplied below.
await writeFile(`${input.directory}/fixture-ca`,"fixture-readable-ca-material");
const parsed=loadRuntimePodConfig({
 ...(input.maxConcurrentTools===undefined?{}:{TETRAL_RUNTIME_MAX_CONCURRENT_TOOLS:String(input.maxConcurrentTools)}),
 ...(input.observeStages?{TETRAL_LOG_LEVEL:"debug",TETRAL_LOG_BURST:"1000"}:{}),
 TETRAL_RUNTIME_POD_NAMESPACE:"tetral-agent-runtime",TETRAL_RUNTIME_POD_NAME:"content-runtime",TETRAL_RUNTIME_POD_UID:input.podUID,TETRAL_RUNTIME_POD_IP:"127.0.0.1",TETRAL_RUNTIME_POD_GRPC_PORT:"19090",TETRAL_RUNTIME_POD_HTTP_ADDR:"127.0.0.1:0",
 TETRAL_DEPLOYMENT_ENVIRONMENT:"test",TETRAL_SERVICE_VERSION:"test",TETRAL_RUNTIME_POD_GRPC_AUDIENCE:"tetral-internal-grpc",TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS:"tetral-system/job-runner",
 KUBERNETES_API_SERVER_URL:"https://unused.test",KUBERNETES_API_CA_CERT_PATH:`${input.directory}/fixture-ca`,KUBERNETES_TOKEN_REVIEW_REVIEWER_TOKEN_PATH:`${input.directory}/fixture-token`,TETRAL_RUNTIME_POD_OUTBOUND_GRPC_TOKEN_PATH:`${input.directory}/fixture-token`,
 TETRAL_BRIDGE_API_GRPC_ADDR:input.bridgeAddress,TETRAL_GATEWAY_GRPC_ADDR:input.gatewayAddress,TETRAL_MCP_CONNECTOR_GRPC_ADDR:"127.0.0.1:1",TETRAL_WEB_CONNECTOR_GRPC_ADDR:"127.0.0.1:1",TETRAL_RUNTIME_APPROVAL_REVIEWER_MODEL:"anthropic/claude-opus-4-8",
 TETRAL_RUNTIME_SKILL_GUIDANCE_DESCRIPTION_BUDGET_BYTES:"32768",TETRAL_RUNTIME_DRAIN_TIMEOUT_MS:"2000",TETRAL_RUNTIME_SETTLEMENT_TIMEOUT_MS:"2000",TETRAL_RUNTIME_LOCAL_JOIN_TIMEOUT_MS:"1000",TETRAL_RUNTIME_PROXY_JOIN_TIMEOUT_MS:"1000",
});
if(!parsed.ok)throw new Error(parsed.error.message);
let hosts:RuntimeCoreHosts|undefined;
let declared=false,refreshHeld=false;
let latestMainScope:RuntimeThreadAddressState|undefined;
let coldLoadOrdinal=0;
let lastIdleStopReason:string|undefined;
const observations=new Set<Promise<void>>();
const trackObservation=(operation:Promise<void>)=>{
 observations.add(operation);
 void operation.then(()=>observations.delete(operation),()=>observations.delete(operation));
};
const controlSchema=z.strictObject({
 id:z.number().int().positive(),operation:z.enum(["inspect","cleanup"]),
 scope:z.strictObject({workspaceId:z.string().min(1),sessionId:z.string().min(1),sessionThreadId:z.string().min(1),
  bindingId:z.string().min(1),bindingGeneration:z.number().int().nonnegative(),targetPodUid:z.string().min(1),runtimeProcessId:z.string().min(1)}),
});
const logger=createJsonLogger({diagnostics:parsed.config.diagnostics,write:line=>{
 process.stderr.write(line);
 if(input.observeStages)appendFileSync(`${input.directory}/runtime-observations.jsonl`,`${line}\n`);
}});
const dependencies=await buildRuntimePodCommandDependencies({config:{...parsed.config,grpcBindAddress:"127.0.0.1:0"},logger,builderOptions:{
 // Explicit external observation on test hosts whose cgroup does not expose a finite limit.
 ...(input.observeContainerMemory?{readContainerMemory:()=>({usageBytes:100,limitBytes:1000})}:{}),
 outboundMetadataFactory:metadataFactory,
 routingProxyReady:async()=>{},
 tokenReviewClientFactory:()=>({createTokenReview:async()=>({authenticated:true,username:"system:serviceaccount:tetral-system:job-runner",audiences:["tetral-internal-grpc"]})}),
 runtimeProcessFactory:(_id,config)=>new BridgeRuntimeProcess(input.processID,{address:input.bridgeAddress,tokenPath:config.outboundInternalGrpcTokenPath,policies:config.bridgeMethodPolicies,metadataFactory}),
 coreHostsFactory:async options=>{
  const loader=options.contextLoader,writer=options.threadLoop.sessionEventWriter;
  hosts=await buildRuntimeCoreHosts({...options,contextLoader:{
   loadThreadContext:async(command,loadOptions)=>{
    const result=await loader.loadThreadContext!(command,loadOptions);
    if(result.thread?.role==="main")latestMainScope=command;
    if(input.evictIdle||input.observeColdLoad){
     const observation={ordinal:++coldLoadOrdinal,sessionId:command.sessionId,sessionThreadId:command.sessionThreadId,
      currentRequestMessage:result.currentRequestMessage,pendingToolUseEventIds:(result.pendingToolUses??[]).map(tool=>tool.toolUseEventId),
      ...(input.observeColdLoad?{messages:result.messages}:{})};
     await marker("cold-context-loaded",observation);
     await marker(`cold-context-loaded-${observation.ordinal}`,observation);
     await trace("cold-context-loaded",{ordinal:observation.ordinal,sessionThreadId:command.sessionThreadId,pendingToolCount:observation.pendingToolUseEventIds.length,threadRole:result.thread?.role??"unspecified"});
    }
    return result;
   },commitAcceptedInput:loader.commitAcceptedInput!.bind(loader),readAgentMail:loader.readAgentMail!.bind(loader),
   refreshRuntimeBindingToken:async(identity,refreshOptions)=>{
    if(input.holdRefreshAfterDeclaration&&declared&&!refreshHeld){refreshHeld=true;await marker("refresh-held",{sessionId:identity.sessionId});await trace("refresh-held");await wait(`${input.directory}/release-refresh`);await trace("refresh-released");}
    return loader.refreshRuntimeBindingToken!(identity,refreshOptions);
   },
  },threadLoop:{...options.threadLoop,
   ...(input.approvalMode===undefined?{}:{approvalMode:input.approvalMode,runtimePolicy:session=>({...options.threadLoop.runtimePolicy?.(session),approvalMode:input.approvalMode!})}),
   sessionEventWriter:{
    append:async envelope=>{const result=await writer.append(envelope);if(result.ok&&result.type!=="stale"){
     await trace("event-ack",{eventType:envelope.event.type,eventId:result.eventId,writeId:envelope.writeId});
     if(envelope.event.type==="agent.tool_use"||envelope.event.type==="agent.mcp_tool_use"){declared=true;await marker("tool-declared",{eventId:result.eventId,modelRequestId:envelope.modelRequestId});}
    }return result;},
    writeRequestEnd:async envelope=>{const result=await writer.writeRequestEnd(envelope);if(result.ok&&result.type!=="stale"){
     await marker("request-end-ack",{modelRequestId:envelope.modelRequestId,eventId:result.requestEndEventId});await trace("request-end-ack",{modelRequestId:envelope.modelRequestId,eventId:result.requestEndEventId});
     const observation = (async () => {
      if (!declared) return;
      const deadline = Date.now() + 15_000;
      while (!stop.signal.aborted && Date.now() < deadline) {
       const snapshot = await hosts!.subAgentRunHost.inspectThread(envelope);
       if (snapshot.ok && snapshot.observed && snapshot.requestEndEventId === result.requestEndEventId && snapshot.entries.some(message => message.contextKind === "assistant" && message.parts.some(part => part.type === "tool_call"))) {
        if (input.observeQueuedTools) {
         if (snapshot.activeToolReferences?.length !== 2 || snapshot.sessionToolPermits?.running !== 1 || snapshot.sessionToolPermits.waiting !== 1) {
          await new Promise<void>(resolve => setTimeout(resolve, 10));
          continue;
         }
         await marker("queued-tool-ownership", {
          modelRequestId: snapshot.modelRequestId,
          requestEndEventId: snapshot.requestEndEventId,
          currentRequestMessage: snapshot.currentRequestMessage,
          activeToolReferences: snapshot.activeToolReferences,
          sessionToolPermits: snapshot.sessionToolPermits,
         });
        }
        await marker("request-end-projected", { modelRequestId: envelope.modelRequestId });
        return;
       }
       await new Promise<void>(resolve => setTimeout(resolve, 10));
      }
      if (!stop.signal.aborted) throw new Error("End ACK never became provider-visible committed Tool content");
     })().catch(async () => { await marker("projection-observation-failed", { failed: true }); });
     trackObservation(observation);
    }return result;},
    settleToolResult:async envelope=>{const result=await writer.settleToolResult(envelope);await trace("tool-settlement",{toolUseEventId:envelope.settlement.toolUseEventId,ack:result.ok});return result;},
    ...(writer.finishIdle===undefined?{}:{finishIdle:async (envelope,controls)=>{
     const result=await writer.finishIdle!(envelope,controls);
     if(result.ok&&result.type!=="stale"){
      lastIdleStopReason=envelope.stopReason.type;
      await trace("finish-idle-ack",{sessionThreadId:envelope.sessionThreadId,stopReason:envelope.stopReason.type});
     }
     return result;
    }}),...(writer.commitRuntimeTermination===undefined?{}:{commitRuntimeTermination:writer.commitRuntimeTermination.bind(writer)}),
   },
   acceptSandboxExecution:async(request)=>{await trace("sandbox-accept-attempt",{toolUseEventId:request.toolUseEventId});return options.threadLoop.acceptSandboxExecution!(request);},
   awaitSandboxExecution:async(request)=>{await trace("sandbox-await",{toolUseEventId:request.toolUseEventId});return options.threadLoop.awaitSandboxExecution!(request);},
  }});
  return hosts;
 },
}});
try{
 const ready=await dependencies.app.start();await marker("ready",{port:ready.grpcPort,httpUrl:ready.httpUrl.href});
 // Test-only commands call the existing owners. Gauges are process-wide;
 // selected-thread fields are derived from inspection, never from a shadow ledger.
 if(input.controlCommands){
  const controls=(async()=>{
   for(let ordinal=1;!stop.signal.aborted;ordinal++){
    const path=`${input.directory}/control-${ordinal}.json`;
    while(!stop.signal.aborted&&!await exists(path))await new Promise<void>(resolve=>setTimeout(resolve,10));
    if(stop.signal.aborted)return;
    let reply:unknown;
    try{
     const command=controlSchema.parse(JSON.parse(await readFile(path,"utf8")) as unknown);
     if(command.id!==ordinal)throw new Error("control ordinal mismatch");
     const cleanup=command.operation==="cleanup"?await hosts!.cleanupRunHost.handleCleanupSession({
      ...command.scope,cleanupOperationId:`fixture_cleanup_${input.processID}_${ordinal}`,
     }):undefined;
     const inspected=await hosts!.subAgentRunHost.inspectThread(command.scope);
     const metrics=dependencies.metrics.snapshot();
     reply={id:ordinal,ok:true,...(cleanup===undefined?{}:{cleanup}),
      thread:inspected.ok?{observed:inspected.observed,status:inspected.status??null,
       ...(input.observeContextEntries?{contextEntries:inspected.entries??null}:{}),
       currentRequestMessage:inspected.currentRequestMessage??null,modelRequestId:inspected.modelRequestId??null,
       requestEndEventId:inspected.requestEndEventId??null,activeToolReferences:inspected.activeToolReferences??null,
       activeToolCount:inspected.activeToolReferences===undefined?null:inspected.activeToolReferences.length,
       sessionToolPermits:inspected.sessionToolPermits??null,
       hasPendingApprovalToolJobs:inspected.hasPendingApprovalToolJobs??null,
       hasUnsettledToolOwner:inspected.hasUnsettledToolOwner??null}: {rejected:inspected.reason},
      processOwners:{activeSessions:metrics.activeSessions,activeThreads:metrics.activeThreads,activeFibers:metrics.activeFibers,
       activeToolFibers:metrics.activeToolFibers,pendingApprovals:metrics.pendingApprovals,
       pendingContentEntries:metrics.pendingContentEntries??null,pendingContentBytes:metrics.pendingContentBytes??null,
       approvalWaitOutstanding:metrics.approvalWaitOutstanding===undefined?null:[...metrics.approvalWaitOutstanding.entries()]},
     };
    }catch{reply={id:ordinal,ok:false,error:"fixture_control_failed"};}
    const temporary=`${input.directory}/control-${ordinal}-reply.tmp`;
    await writeFile(temporary,JSON.stringify(reply),{mode:0o600});
    await rename(temporary,`${input.directory}/control-${ordinal}-reply.json`);
   }
  })().catch(async()=>{if(!stop.signal.aborted)await marker("control-loop-failed",{failed:true});});
  trackObservation(controls);
 }

 if(input.evictIdle){
  const eviction=(async()=>{
   await wait(`${input.directory}/evict-idle`);
   const deadline=Date.now()+15_000;
   while(!stop.signal.aborted&&Date.now()<deadline){
    const scope=latestMainScope;
    if(scope!==undefined){
     const before=await hosts!.subAgentRunHost.inspectThread(scope);
     const expected=input.evictIdleStopReason??"requires_action";
     const matchingOwner=expected==="requires_action"?before.ok&&before.hasPendingApprovalToolJobs:before.ok&&!before.hasPendingApprovalToolJobs&&!before.hasUnsettledToolOwner;
     if(before.ok&&before.observed&&matchingOwner&&lastIdleStopReason===expected&&(before.status==="idle"||before.status==="requires_action")){
      const result=await hosts!.cleanupRunHost.handleCleanupSession({...scope,cleanupOperationId:`fixture_cleanup_${input.processID}`});
      await trace("idle-eviction-attempt",{accepted:result.ok});
      if(result.ok){
       const after=await hosts!.subAgentRunHost.inspectThread(scope);
       await marker("idle-evicted",{accepted:true,cleaned:result.cleaned,observed:after.ok?after.observed:true,stopReason:expected,sessionId:scope.sessionId,sessionThreadId:scope.sessionThreadId});
       return;
      }
     }
    }
    await new Promise<void>(resolve=>setTimeout(resolve,10));
   }
   if(!stop.signal.aborted){
    const snapshot=latestMainScope===undefined?undefined:await hosts!.subAgentRunHost.inspectThread(latestMainScope);
    await marker("idle-eviction-failed",{failed:true,mainScopeObserved:latestMainScope!==undefined,lastIdleStopReason:lastIdleStopReason??null,
     snapshot:snapshot===undefined?null:snapshot.ok?{observed:snapshot.observed,status:snapshot.status??null,hasPendingApprovalToolJobs:snapshot.hasPendingApprovalToolJobs??false,hasUnsettledToolOwner:snapshot.hasUnsettledToolOwner??false}: {reason:snapshot.reason}});
    return;
   }
  })().catch(async()=>{if(!stop.signal.aborted)await marker("idle-eviction-failed",{failed:true});});
  trackObservation(eviction);
 }
 await wait(`${input.directory}/stop`);
}finally{
 stop.abort();
 await dependencies.app.shutdown();
 await dependencies.coreHosts.close();
 await Promise.all(observations);
 logger.close();
 await marker("closed",{joined:true,diagnostics:logger.stats()});
}
