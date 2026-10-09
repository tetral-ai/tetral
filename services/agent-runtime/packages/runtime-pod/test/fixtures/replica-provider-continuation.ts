import assert from "node:assert/strict";
import {access,readFile,writeFile} from "node:fs/promises";
import {Metadata} from "@grpc/grpc-js";
import {createLLMService} from "@tetral/agent-runtime-core/src/llm/llm-service.js";
import {createToolCatalog} from "@tetral/agent-runtime-core/src/tools/tool-catalog.js";
import {DefaultProviderCallRuntimeConfig} from "@tetral/agent-runtime-core/src/thread-loop/provider-request.js";
import {createProviderGatewayApp} from "../../../../../gateway/packages/provider-gateway/src/app.js";
import {ProviderClientRegistry} from "../../../../../gateway/packages/provider-gateway/src/providers/clients.js";
import type {ProviderGatewayConfig} from "../../../../../gateway/packages/provider-gateway/src/config.js";
import {BridgeAPIContextLoader,BridgeAPIEventWriter,BridgeAPIControlInputCommitter} from "../../src/bridge-client.js";
import {buildRuntimeCoreHosts} from "../../src/core-hosts.js";
import {RuntimePodGatewayClient} from "../../src/gateway-client.js";
import {createRuntimeGrpcServer} from "../../src/grpc-server.js";
import {RuntimeControlService} from "../../src/runtime-service.js";
const input=JSON.parse(await readFile(process.argv[2]!,"utf8"));
const logger={info:()=>{},warn:()=>{},error:()=>{}};
const metadataFactory=async()=>{const m=new Metadata();m.set("authorization","Bearer replica-runtime");return m;};
const bridgeOptions={address:input.bridgeAddress,tokenPath:"unused",metadataFactory};
const loader=new BridgeAPIContextLoader(bridgeOptions),writer=new BridgeAPIEventWriter(bridgeOptions),committer=new BridgeAPIControlInputCommitter(bridgeOptions);
const config:ProviderGatewayConfig={deploymentEnvironment:"test",diagnostics:{level:"info",maxRecordBytes:16384,summaryIntervalMs:30000,burst:1},serviceVersion:"replica-test",grpcBindAddress:"127.0.0.1:0",httpBindAddress:"127.0.0.1:0",allowedRuntimePod:{namespace:"tetral-agent-runtime",serviceAccount:"agent-runtime"},runtimeBindingTokenHMACKey:input.signingKey,databaseUrl:"postgres://not-opened/fixture",databasePool:{max:2,idleTimeout:30,maxLifetime:1800,connectionTimeout:30,statementTimeoutMs:30000},drainTimeoutMs:1000,cancelJoinTimeoutMs:1000,vaultKeyHex:"00".repeat(32),kubernetesApiServerUrl:"https://kubernetes.default.svc",kubernetesApiCaCertPath:"unused",tokenReviewReviewerTokenPath:"unused",bridgeApiGrpcAddress:input.bridgeAddress,bridgeTokenPath:"unused",maxConcurrentTurns:2};
let providerInvocations=0,finishIdleInvocations=0,finishIdleResult="none",nextID=0;
const modelRequests:string[]=[];
const save=async()=>writeFile(input.statePath,JSON.stringify({providerInvocations,finishIdleInvocations,finishIdleResult,modelRequests}));
const app=createProviderGatewayApp({config,logger,tokenReviewClient:{createTokenReview:async()=>({authenticated:true,audiences:["tetral-internal-grpc"],username:"system:serviceaccount:tetral-agent-runtime:agent-runtime",podUid:input.targetPodUid})},providerStreamer:{stream:async function*(request){
 const number=++providerInvocations,marker=`replica_core_${number}`;modelRequests.push(request.request.modelRequestId);await save();
 const fetchProvider=Object.assign(async(resource:RequestInfo|URL,init?:RequestInit)=>{const original=new Request(resource,init);const headers=new Headers(original.headers);headers.set("x-replica-marker",marker);return fetch(new Request(`${input.backendURL}/provider`,{...init,body:original.body,method:original.method,headers,signal:original.signal}));},{preconnect:()=>{}});
 const registry=new ProviderClientRegistry({fetch:fetchProvider});let controlled=false;
 for await(const event of registry.stream({...request,credential:{source:"session",authType:"provider_api_key",providerId:"anthropic",supplyMode:"anthropic-api-key",vaultId:"fixture",credentialId:"fixture",accessMode:"api_key",apiKey:"fixture-provider-key"}})){
  yield event;
  if(!controlled&&event.text?.text){controlled=true;const action=number===1?"drop":"finish";assert.equal((await fetch(`${input.backendURL}/${action}?marker=${marker}`)).status,200);}
 }
}}});
const gateway=await app.start();const gatewayClient=new RuntimePodGatewayClient({address:`127.0.0.1:${gateway.grpcPort}`,tokenPath:"unused",metadataFactory});
const hosts=await buildRuntimeCoreHosts({maxLocalSessions:1,now:()=>new Date().toISOString(),logger,
 contextLoader:loader,
 threadLoop:{internalToolRepairStore:{} as never,sessionEventWriter:{append:writer.append.bind(writer),settleToolResult:writer.settleToolResult.bind(writer),writeRequestEnd:writer.writeRequestEnd.bind(writer),commitRuntimeTermination:writer.commitRuntimeTermination.bind(writer),finishIdle:async envelope=>{finishIdleInvocations++;const result=await writer.finishIdle(envelope);finishIdleResult=result.ok?result.type:result.error.code;await save();return result;}},
 runtime:{now:()=>new Date().toISOString(),monotonicMs:()=>performance.now(),createId:prefix=>`${prefix}_replica_core_${++nextID}`,sleep:async(duration,signal)=>await new Promise<boolean>(resolve=>{let settled=false;const done=(value:boolean)=>{if(settled)return;settled=true;clearTimeout(timer);signal.removeEventListener("abort",abort);resolve(value);};const abort=()=>done(false);const timer=setTimeout(()=>done(true),duration);signal.addEventListener("abort",abort,{once:true});if(signal.aborted)abort();})},
 llmService:createLLMService(gatewayClient),storeOperationTimeoutMs:5000,approvalMode:"full_access",providerCallRuntime:{...DefaultProviderCallRuntimeConfig,systemInstructions:"Replica lifecycle composition",timeoutMs:15000},runtimeModel:()=>({providerId:"anthropic",modelId:"claude-opus-4-8"}),runtimePolicy:()=>({toolCatalog:createToolCatalog({family:"claude"}),providerRescheduleBudget:0})}});
const service=new RuntimeControlService({runtimeProcessId:input.runtimeProcessId,ownPod:{namespace:"tetral-agent-runtime",name:"runtime-pod-0",uid:input.targetPodUid,ip:"127.0.0.1"},allowedJobRunner:{namespace:"tetral-system",name:"job-runner"},authenticator:{authenticate:async()=>({ok:true,serviceAccount:{namespace:"tetral-system",name:"job-runner"}})},runHost:hosts.commandRunHost,controlInputCommitter:committer,cleanupController:{startCleanup:async()=>{throw new Error("unexpected cleanup");}},logger,ready:()=>true});
const runtimeServer=createRuntimeGrpcServer(service);const port=await runtimeServer.bind("127.0.0.1:0");await writeFile(input.readyPath,JSON.stringify({port}));
try{for(;;){try{await access(input.closePath);break;}catch{await new Promise(resolve=>setTimeout(resolve,10));}}}
finally{await hosts.shutdownActiveRuns();await runtimeServer.shutdown();await hosts.close();await gatewayClient.close();await app.shutdown();await Promise.all([loader.close(),writer.close(),committer.close()]);}
assert.equal(new Set(modelRequests).size,2);assert.equal(providerInvocations,2);console.log(JSON.stringify({providerInvocations,finishIdleInvocations,finishIdleResult,modelRequests}));
