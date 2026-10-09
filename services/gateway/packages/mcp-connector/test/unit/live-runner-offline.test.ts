import { mkdtemp, rm, writeFile, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";
import { expect, test } from 'bun:test';
import { DRIVER_CONTRACT, runInteroperability, validateFixture, commandAdapter } from '../live/github-slack-interoperability.js';
import type { LiveFixture, DriverObservation, DriverRequest, Counters } from '../live/github-slack-interoperability.js';
function fixture():LiveFixture{return {contract:DRIVER_CONTRACT,engineRevision:'a'.repeat(40),sdkRevision:'b'.repeat(40),environmentId:'offline-environment',workspaceId:'offline-workspace',sessionId:'offline-session',vaultRefs:['vault-ref'],github:{serverName:'work-github',name:'get_me',arguments:{},definitionHash:'e3557e4b6b549858e8a605879b3cfcafb864f3be0cc6c2228290c62bb76e3f61',oracles:[{providerPath:['login'],runtimePath:['login'],committedPath:['login'],expected:'independent-login'},{providerPath:['id'],runtimePath:['id'],committedPath:['id'],expected:42}]},slack:{serverName:'work-slack',name:'users_profile',arguments:{user:'U-independent'},definitionHash:'882f47bec53ed2b1c6d44712e67789a18e8826d6847c7f653382f6706efb7921',oracles:[{providerPath:['user'],runtimePath:['user'],committedPath:['user'],expected:'U-independent'},{providerPath:['team'],runtimePath:['team'],committedPath:['team'],expected:'T-independent'}]},slackCredentialRef:'dedicated-credential-ref',slackUserId:'U-independent',slackTeamId:'T-independent',slackScope:'users:read',slackRefreshEndpoint:'https://slack.com/api/oauth.v2.user.access',slackConfidentialClient:true,providerContracts:{github:{revision:'github-contract-offline',sha256:'d'.repeat(64)},slack:{revision:'slack-contract-offline',sha256:'e'.repeat(64)}},driver:{sourceSha256:'f'.repeat(64),entrypoint:'offline-driver',argv:[],sha256:'c'.repeat(64),revision:'offline-v1',counterProvenance:'independent-issuer-and-mcp-ingress-cumulative'}};}
function synthetic(f:LiveFixture,mutate?:(request:DriverRequest,observation:DriverObservation)=>void){
 const requests:DriverRequest[]=[];let expired=false;const counters:Counters={externalCalls:0,successfulToolResponses:0,tokenRequests:0,githubDiscoveryPasses:0,slackDiscoveryPasses:0,githubInitializations:0,slackInitializations:0};
 return {requests,async invoke(request:DriverRequest){requests.push(request);if(request.operation==='read'){counters.externalCalls++;counters.successfulToolResponses++;if(request.phase!=='warm'){if(request.adapter==='github'){counters.githubDiscoveryPasses++;counters.githubInitializations++;}else{counters.slackDiscoveryPasses++;counters.slackInitializations++;}}if(expired)counters.tokenRequests++;}
 const observation:DriverObservation={contract:DRIVER_CONTRACT,operation:request.operation,runId:request.runId,environmentId:f.environmentId,engineRevision:f.engineRevision,sdkRevision:f.sdkRevision,counterProvenance:f.driver.counterProvenance,counters:{...counters}};
 if(request.operation==='preflight')Object.assign(observation,{providerContracts:f.providerContracts,protocolFacts:{github:{transportMode:'json',protocolVersion:'2025-11-25',serverCapabilities:{tools:{}}},slack:{transportMode:'sse',protocolVersion:'2025-11-25',serverCapabilities:{tools:{listChanged:true}}}},coldResetConfirmed:true,workspaceId:f.workspaceId,sessionId:f.sessionId,vaultRefs:f.vaultRefs,installedServers:[{name:'work-github',endpoint:'https://api.githubcopilot.com/mcp/'},{name:'work-slack',endpoint:'https://mcp.slack.com/mcp'}],catalogs:{github:[{name:'get_me',definition:{name:'get_me'}}],slack:[{name:'users_profile',definition:{name:'users_profile'}}]}});
 if(request.operation==='read')Object.assign(observation,{protocol:{transportMode:request.adapter==='github'?'json':'sse',protocolVersion:'2025-11-25',serverCapabilities:{tools:{listChanged:request.adapter==='slack'}}},toolUseEventId:request.toolUseEventId,claimId:'claim-'+request.toolUseEventId,serverName:request.call!.serverName,toolName:request.call!.toolName,arguments:request.call!.arguments,providerResult:request.adapter==='github'?{login:'independent-login',id:42}:{user:'U-independent',team:'T-independent'},phase:request.phase,clientIdentity:request.adapter+'-client',runtimeResult:request.adapter==='github'?{login:'independent-login',id:42}:{user:'U-independent',team:'T-independent'},committedResult:request.adapter==='github'?{login:'independent-login',id:42}:{user:'U-independent',team:'T-independent'},durableStatus:'completed',backendPods:['synthetic-runtime','synthetic-connector','synthetic-bridge'],...(expired?{tokenEndpointSucceeded:true,encryptedWriteCommitted:true,replacementCredentialUsed:true}:{})});
 if(request.operation==='expire-slack'){expired=true;Object.assign(observation,{expiryDue:true,refreshMaterialPreserved:true,dedicatedCredential:true,credentialRef:f.slackCredentialRef});}
 if(request.operation==='cleanup')observation.cleanupConfirmed=true;mutate?.(request,observation);return observation;
 }};
}
test('synthetic offline runner orchestrates complete calls, refresh proof and cleanup without live evidence',async()=>{
 const f=fixture(),driver=synthetic(f);const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});
 expect(evidence).toMatchObject({kind:'synthetic-offline',status:'PASS',cleanup:'confirmed'});
 expect(driver.requests.map(request=>request.operation)).toEqual(['preflight','read','read','read','read','expire-slack','read','cleanup']);
 expect(driver.requests.filter(request=>request.operation==='read').map(request=>request.call)).toEqual([{serverName:'work-github',toolName:'get_me',arguments:{}},{serverName:'work-github',toolName:'get_me',arguments:{}},{serverName:'work-slack',toolName:'users_profile',arguments:{user:'U-independent'}},{serverName:'work-slack',toolName:'users_profile',arguments:{user:'U-independent'}},{serverName:'work-slack',toolName:'users_profile',arguments:{user:'U-independent'}}]);
 expect(new Set(driver.requests.filter(request=>request.operation==='read').map(request=>request.toolUseEventId)).size).toBe(5);
});
for(const failure of ['changed-definition','duplicate-definition','wrong-user','wrong-runtime','wrong-scope','duplicate-invocation','missing-claim','too-many-calls','too-many-refresh','too-many-list','wrong-revision','missing-refresh']as const)test(`synthetic runner rejects ${failure} and always verifies cleanup`,async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{
  if(request.operation==='cleanup')return;
  if(request.operation==='preflight'){
   if(failure==='changed-definition')observation.catalogs!.slack[0]!.definition={name:'changed'};
   if(failure==='duplicate-definition')observation.catalogs!.github.push(observation.catalogs!.github[0]!);
   if(failure==='wrong-scope')observation.workspaceId='different';
   if(failure==='wrong-revision')observation.sdkRevision='different';
   if(failure==='too-many-list')observation.counters.slackDiscoveryPasses=7;
  }
  if(request.operation==='read'){
   if(failure==='wrong-user')observation.providerResult={user:'different',team:'T-independent'};
   if(failure==='wrong-runtime')observation.runtimeResult={text:'different'};
   if(failure==='duplicate-invocation')observation.toolUseEventId='different';
   if(failure==='missing-claim')delete observation.claimId;
   if(failure==='too-many-calls')observation.counters.externalCalls=7;
   if(failure==='too-many-refresh')observation.counters.tokenRequests=3;
   if(failure==='missing-refresh')delete observation.encryptedWriteCommitted;
  }
 });
 const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).not.toBe('PASS');expect(driver.requests.at(-1)?.operation).toBe('cleanup');expect(evidence.firstFailure).toBeDefined();
 expect(JSON.stringify(evidence)).not.toContain('synthetic-model-visible');expect(JSON.stringify(evidence)).not.toContain('independent-login');
});
test('missing fixture or environment adapter is rejected before any calls',async()=>{
 expect(()=>validateFixture(undefined)).toThrow('contract');const f=fixture();f.driver.entrypoint='';const requests:DriverRequest[]=[];await expect(runInteroperability(f,async request=>{requests.push(request);throw new Error('not reached');},{synthetic:true})).rejects.toThrow('adapter');expect(requests).toEqual([]);
});
test('arbitrary child errors are projected safely and first failure survives cleanup failure',async()=>{
 const f=fixture();let calls=0;const evidence=await runInteroperability(f,async()=>{calls++;throw new Error('access_token=NEVER_PERSIST_SENTINEL');},{synthetic:true});expect(calls).toBe(2);expect(evidence.firstFailure).toBe('Environment adapter observation failed.');expect(JSON.stringify(evidence)).not.toContain('NEVER_PERSIST_SENTINEL');expect(evidence.cleanup).toBe('unconfirmed');
});
test('duration exhaustion still attempts cleanup and cannot produce PASS',async()=>{
 const f=fixture(),driver=synthetic(f);let clock=0;const evidence=await runInteroperability(f,async request=>{const observation=await driver.invoke(request);clock=870001;return observation;},{synthetic:true,now:()=>clock});expect(evidence.status).not.toBe('PASS');expect(driver.requests.map(request=>request.operation)).toEqual(['preflight','cleanup']);
});

test('account oracle is checked at Runtime and committed boundaries, not just provider JSON',async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(request.operation==='read'){observation.runtimeResult={unrelated:'garbage'};observation.committedResult={unrelated:'garbage'};}});
 const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).toBe('FAIL');expect(evidence.firstFailure).toBe('Independent account identity oracle mismatch.');expect(evidence.cleanup).toBe('confirmed');
});
for(const violation of ['warm-client-changed','warm-listed-again','cold-no-reset']as const)test(`synthetic runner rejects ${violation}`,async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(violation==='cold-no-reset'&&request.operation==='preflight')observation.coldResetConfirmed=false;if(request.phase==='warm'){if(violation==='warm-client-changed')observation.clientIdentity='another-client';if(violation==='warm-listed-again')observation.counters.githubDiscoveryPasses++;}});
 const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).not.toBe('PASS');expect(driver.requests.at(-1)?.operation).toBe('cleanup');
});
test('first established contract failure survives failed cleanup; unknown child fields never persist',async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{Object.assign(observation.counters,{extra:{access_token:'NEVER_PERSIST_SENTINEL'}});Object.assign(observation,{stdout:'NEVER_PERSIST_SENTINEL',stderr:'NEVER_PERSIST_SENTINEL'});if(request.operation==='read'){observation.runtimeResult={garbage:true};observation.committedResult={garbage:true};}if(request.operation==='cleanup')observation.cleanupConfirmed=false;});
 const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).toBe('FAIL');expect(evidence.cleanup).toBe('unconfirmed');expect(evidence.firstFailure).toBe('Independent account identity oracle mismatch.');expect(JSON.stringify(evidence)).not.toContain('NEVER_PERSIST_SENTINEL');expect(Object.keys(evidence.steps[0]!.counters)).toHaveLength(7);
});
test('remaining independent counter budget is passed before every dispatch',async()=>{
 const f=fixture(),driver=synthetic(f);await runInteroperability(f,driver.invoke,{synthetic:true});expect(driver.requests.filter(request=>request.operation==='read').map(request=>request.remainingBudget.externalCalls)).toEqual([6,5,4,3,2]);expect(driver.requests.at(-1)!.remainingBudget.tokenRequests).toBe(1);
});

for(const mode of ['ignore-termination','stdout-overflow','stderr-overflow']as const)test(`offline command adapter bounds and joins ${mode}`,async()=>{
 const f=fixture();f.driver.entrypoint=process.execPath;f.driver.argv=['-e',mode==='ignore-termination'?"process.on('SIGTERM',()=>{});setInterval(()=>{},1000);":mode==='stdout-overflow'?"process.stdout.write('NEVER_PERSIST_SENTINEL'.repeat(100000));setInterval(()=>{},1000);":"process.stderr.write('NEVER_PERSIST_SENTINEL'.repeat(100000));setInterval(()=>{},1000);"];
 const request:DriverRequest={contract:DRIVER_CONTRACT,operation:'preflight',runId:'synthetic-command',fixture:f,remainingMs:800,remainingBudget:{externalCalls:6,tokenRequests:2,githubDiscoveryPasses:6,slackDiscoveryPasses:6}};
 const started=performance.now();let failure:unknown;try{await commandAdapter(f)(request);}catch(error){failure=error;}
 expect(failure).toBeInstanceOf(Error);expect(String(failure)).not.toContain('NEVER_PERSIST_SENTINEL');expect(performance.now()-started).toBeLessThan(1500);
},3000);

test('explicit offline preflight validates a bound local fixture and source without executing its adapter',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'mcp-live-offline-'));
 try{
  const f=fixture();f.driver.entrypoint=process.execPath;f.driver.argv=['-e',"throw new Error('ADAPTER_MUST_NOT_EXECUTE');"];f.driver.sha256=createHash('sha256').update(await readFile(process.execPath)).digest('hex');
  const file=join(directory,'fixture.json');await writeFile(file,JSON.stringify(f));
  const child=Bun.spawn({cmd:[process.execPath,resolve(import.meta.dir,'../live/github-slack-interoperability.ts'),'--fixture',file,'--output',directory,'--offline-preflight'],stdout:'pipe',stderr:'pipe'});
  const [code,stdout,stderr]=await Promise.all([child.exited,new Response(child.stdout).text(),new Response(child.stderr).text()]);expect(code).toBe(0);expect(JSON.parse(stdout)).toMatchObject({kind:'offline-preflight',contract:DRIVER_CONTRACT});expect(stderr).not.toContain('ADAPTER_MUST_NOT_EXECUTE');
 }finally{await rm(directory,{recursive:true,force:true});}
},5000);


for(const missing of ['transport','capabilities','protocol-version','retrieved-contract','contract-hash','read-transport','read-capabilities']as const)test(`synthetic runner rejects missing or contradictory ${missing} fact`,async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(request.operation==='preflight'){if(missing==='transport')Reflect.deleteProperty(observation.protocolFacts!.github,'transportMode');if(missing==='capabilities')Reflect.deleteProperty(observation.protocolFacts!.slack,'serverCapabilities');if(missing==='protocol-version')observation.protocolFacts!.github.protocolVersion='';if(missing==='retrieved-contract')delete observation.providerContracts;if(missing==='contract-hash')observation.providerContracts={...f.providerContracts,slack:{...f.providerContracts.slack,sha256:'0'.repeat(64)}};}if(request.operation==='read'){if(missing==='read-transport')delete observation.protocol;if(missing==='read-capabilities')observation.protocol!.serverCapabilities={};}});
 const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).not.toBe('PASS');expect(driver.requests.at(-1)!.operation).toBe('cleanup');
});
test('safe transport capabilities and hashed chain identities persist while unknown sentinels do not',async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(request.operation==='read'){observation.claimId='NEVER_PERSIST_SENTINEL'+request.toolUseEventId;observation.backendPods=['NEVER_PERSIST_SENTINEL'];Object.assign(observation.protocol!.serverCapabilities,{experimental:{token:'NEVER_PERSIST_SENTINEL'}});Object.assign(observation.protocol!,{unused:'NEVER_PERSIST_SENTINEL'});}});
 const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).toBe('PASS');expect(evidence.protocolFacts!.github).toMatchObject({transportMode:'json',tools:true,toolsListChanged:false});expect(evidence.protocolFacts!.slack).toMatchObject({transportMode:'sse',tools:true,toolsListChanged:true});const reads=evidence.steps.filter(step=>step.operation==='read');expect(reads.every(step=>/^[a-f0-9]{64}$/.test(step.claimSha256!)&&/^[a-f0-9]{64}$/.test(step.clientSha256!)&&step.backendPodSha256!.every(hash=>/^[a-f0-9]{64}$/.test(hash)))).toBe(true);expect(reads[0]!.clientSha256).toBe(reads[1]!.clientSha256);expect(evidence.driverSourceSha256).toBe(f.driver.sourceSha256);expect(JSON.stringify(evidence)).not.toContain('NEVER_PERSIST_SENTINEL');expect(JSON.stringify(evidence)).not.toContain('synthetic-runtime');expect(JSON.stringify(evidence)).not.toContain('independent-login');
});


test('reused claim for different fresh Tool Uses cannot establish independent execution',async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(request.operation==='read')observation.claimId='reused-claim';});const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).not.toBe('PASS');expect(driver.requests.at(-1)!.operation).toBe('cleanup');
});
test('malformed advertised capability flags cannot establish protocol evidence',async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(request.operation==='preflight')observation.protocolFacts!.slack.serverCapabilities={tools:{listChanged:'unknown'}};});const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).not.toBe('PASS');expect(driver.requests.at(-1)!.operation).toBe('cleanup');
});
test('missing provider contract pin or reviewed adapter source hash fails before dispatch',async()=>{
 for(const missing of ['provider','source']){const f=fixture();if(missing==='provider')Reflect.deleteProperty(f,'providerContracts');else f.driver.sourceSha256='';const calls:DriverRequest[]=[];await expect(runInteroperability(f,async request=>{calls.push(request);throw new Error('must not dispatch');},{synthetic:true})).rejects.toBeInstanceOf(Error);expect(calls).toEqual([]);}
});


for(const status of [401,403]as const)test(`synthetic live bound permits one genuine HTTP${status} retry and one successful read result`,async()=>{
 const f=fixture();let extra=0;const driver=synthetic(f,(request,observation)=>{if(request.operation==='read'&&extra===0){extra=1;observation.rejectedToolCalls=[{provenance:'http-status',status}];}observation.counters.externalCalls+=extra;});const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).toBe('PASS');expect(evidence.steps.at(-1)!.counters).toMatchObject({externalCalls:6,successfulToolResponses:5});expect(evidence.steps.find(step=>step.rejectedAuthStatuses)?.rejectedAuthStatuses).toEqual([status]);
});
for(const invalid of ['duplicate-success','missing-success','jsonrpc-retry','generic-replay']as const)test(`synthetic runner rejects ${invalid} attempt evidence`,async()=>{
 const f=fixture(),driver=synthetic(f,(request,observation)=>{if(request.operation!=='read')return;if(invalid==='duplicate-success')observation.counters.successfulToolResponses++;if(invalid==='missing-success')Reflect.deleteProperty(observation.counters,'successfulToolResponses');if(invalid==='jsonrpc-retry'){observation.counters.externalCalls++;observation.rejectedToolCalls=[{provenance:'jsonrpc-code',status:401}]as never;}if(invalid==='generic-replay')observation.counters.externalCalls++;});const evidence=await runInteroperability(f,driver.invoke,{synthetic:true});expect(evidence.status).not.toBe('PASS');expect(driver.requests.at(-1)!.operation).toBe('cleanup');
});
