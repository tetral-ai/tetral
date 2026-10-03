import { openPostgresSQLOwner } from "../../../internal/ts-dbconnect/src/owner.ts";
import { SQLGatewayCredentialStore } from "../../../services/gateway/packages/provider-gateway/src/providers/credentials.ts";
import { SQLMcpCredentialResolver } from "../../../services/gateway/packages/mcp-connector/src/credential.ts";
import type { GatewayCredentialSQL } from "../../../services/gateway/packages/provider-gateway/src/providers/credentials.ts";
import type { McpCredentialSQL } from "../../../services/gateway/packages/mcp-connector/src/credential.ts";
import { SQLOpenAIOAuthCredentialRefreshWriter } from "../../../services/gateway/packages/provider-gateway/src/providers/openai-oauth-refresh.ts";
import { SQLVaultMcpCredentialUpdatePath } from "../../../services/gateway/packages/mcp-connector/src/credential-update-path.ts";

import { adapterByEndpoint } from "../../../services/gateway/packages/mcp-connector/src/adapters/registry.ts";
import type { ResolvedMcpServer } from "../../../services/gateway/packages/mcp-connector/src/server-resolver.ts";
import type { McpCredentialUpdateRow } from "../../../services/gateway/packages/mcp-connector/src/credential-update-path.ts";

// Fixed registered identity for the protected SQL-owner composition.
const adapter = adapterByEndpoint("https://api.githubcopilot.com/mcp/");
if (adapter === undefined) throw new Error("fixture MCP adapter is unavailable");
const resolvedServer: ResolvedMcpServer = Object.freeze({configuredName:"github",endpoint:adapter.endpoint,adapter});

const config = await Bun.file(process.argv[2]!).json() as { url: string; caPath: string; serverName: string };
const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
let maxPools = 0, pools = 0, opened = 0, activated = 0, failed = 0, recovered = 0, closed = false, reason = "";
let diagnosticMode = "normal";
const diagnostics: {kind:string;failedCount?:number;reason?:string}[] = [];
const owner = await openPostgresSQLOwner({ url: config.url, tls: config, pool: { max: 2, idleTimeout: 30, maxLifetime: 1800, connectionTimeout: 2, statementTimeoutMs: 5000 }, drainTimeoutSeconds: 2,
  verify: async (sql) => { const rows = await sql`SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()`; if (rows[0]?.ssl !== true) throw new Error("verified TLS connection is required"); },
  observe: (event) => { pools = event.pools; maxPools = Math.max(maxPools, pools); if (event.kind === "opened") opened++; if (event.kind === "activated") activated++; if (event.kind === "reload_failed") {failed++; reason = event.reason ?? "";} if(event.kind === "reload_recovered") recovered++;
    if(event.kind === "reload_failed" || event.kind === "reload_recovered") {
      if(diagnosticMode === "error") throw new Error("fixture sink failure");
      if(diagnosticMode !== "silent") diagnostics.push({kind:event.kind,failedCount:event.failedCount,reason:event.reason});
    }
  },
});
const provider = new SQLGatewayCredentialStore(owner as unknown as {withSQL<T>(operation:(sql:GatewayCredentialSQL)=>Promise<T>):Promise<T>});
const mcp = new SQLMcpCredentialResolver(owner as unknown as {withSQL<T>(operation:(sql:McpCredentialSQL)=>Promise<T>):Promise<T>},key);
let issuerCalls = 0, refreshDone = false, releaseIssuer: (() => void) | undefined;
const issuerGate = new Promise<void>((resolve) => { releaseIssuer = resolve; });
const issuer = async () => {
  issuerCalls++;
  await issuerGate;
  return Response.json({access_token:"rotated-access",refresh_token:"rotated-refresh",expires_in:3600});
};
const providerWriter = new SQLOpenAIOAuthCredentialRefreshWriter({sql:owner as unknown as {withSQL<T>(operation:(sql:GatewayCredentialSQL)=>Promise<T>):Promise<T>},masterKeyHex:key,fetch:issuer});
const mcpWriter = new SQLVaultMcpCredentialUpdatePath(owner as unknown as {withSQL<T>(operation:(sql:McpCredentialSQL)=>Promise<T>):Promise<T>},key,()=>new Date(),issuer);
let providerRefresh: ReturnType<typeof providerWriter.refreshOpenAIOAuthCredential> | undefined;
let mcpRefresh: ReturnType<typeof mcpWriter.refreshOAuthCredential> | undefined;
let release: (() => void) | undefined, held: Promise<unknown> | undefined;
let heldPID = 0, releasedPID = 0;
const server = Bun.serve({hostname:"0.0.0.0",port:8888,async fetch(request){
  const path = new URL(request.url).pathname;
  try {
    if (path === "/state") return Response.json({pools,maxPools,opened,activated,failed,recovered,reason,diagnostics,heldPID,releasedPID,closed,issuerCalls,refreshDone});
    if (path === "/diagnostics") {diagnosticMode=(await request.json() as {mode:string}).mode;return Response.json({ok:true});}
    if (path === "/refresh-start") {
      await owner.withSQL(async(sql)=> await sql`UPDATE credentials SET archived_at=NULL WHERE id='cred_mcp_oauth'`);
      providerRefresh = providerWriter.refreshOpenAIOAuthCredential({workspaceId:"wksp_fixture",abortSignal:AbortSignal.timeout(10000),credential:{source:"session",authType:"provider_oauth",providerId:"openai",supplyMode:"openai-chatgpt-oauth",vaultId:"vlt_fixture",credentialId:"cred_oauth",accessMode:"oauth",accessToken:"old-access",refreshToken:"old-refresh",expiresAt:"2000-01-01T00:00:00.000Z",accountId:"acct_fixture"}});
      const [row] = await owner.withSQL(async(sql)=> await sql<readonly McpCredentialUpdateRow[]>`SELECT id,vault_id,encrypted_auth,auth_public_json,auth_public_json::jsonb->>'mcp_server_url' AS mcp_server_url FROM credentials WHERE id='cred_mcp_oauth'`);
      if (row === undefined) throw new Error("fixture selected MCP credential is unavailable");
      mcpRefresh = mcpWriter.refreshOAuthCredential({workspaceId:"wksp_fixture",sessionId:"sesn_fixture",mcpServerName:"github",row,vaultId:"vlt_fixture",credentialId:"cred_mcp_oauth",force:true});
      return Response.json({started:true});
    }
    if (path === "/refresh-release") {
      releaseIssuer?.();
      const [providerResult,mcpResult] = await Promise.all([providerRefresh,mcpRefresh]);
      if(!providerResult?.ok || providerResult.credential.accessToken!=="rotated-access" || !mcpResult?.ok || mcpResult.mode!=="bearer" || mcpResult.token!=="rotated-access") throw new Error("actual refresh transaction failed");
      await owner.withSQL(async(sql)=> await sql`UPDATE credentials SET archived_at='fixture-closed' WHERE id='cred_mcp_oauth'`);
      refreshDone = true;
      return Response.json({ok:true});
    }
    if (path === "/read") {
      const rows = await provider.loadActiveSessionProviderAuth({workspaceId:"wksp_fixture",sessionId:"sesn_fixture"});
      const absent = await provider.loadActiveSessionProviderAuth({workspaceId:"wksp_wrong",sessionId:"sesn_fixture"});
      const token = await mcp.resolve({workspaceId:"wksp_fixture",sessionId:"sesn_fixture",mcpServerName:resolvedServer.configuredName,resolvedServer});
      if(rows.length!==1||absent.length!==0||!token.ok||token.mode!=="bearer"||token.token!=="fixture-token") throw new Error("actual credential store result differs");
      const [{pid}] = await owner.withSQL(async(sql)=> await sql`SELECT pg_backend_pid() AS pid`);
      return Response.json({ok:true,pid});
    }
    if (path === "/hold") {
      if(held!==undefined) throw new Error("transaction is already held");
      heldPID = 0; releasedPID = 0;
      const gate = new Promise<void>((resolve)=>{release=resolve;});
      held = owner.withSQL(async(sql)=> await sql.begin(async(tx)=>{
        [{pid:heldPID}] = await tx`SELECT pg_backend_pid() AS pid`;
        await gate;
        [{pid:releasedPID}] = await tx`SELECT pg_backend_pid() AS pid`;
        await tx`INSERT INTO transport_effects(value) VALUES ('committed')`;
      }));
      void held.catch(()=>{});
      return Response.json({ok:true});
    }
    if (path === "/release") {release?.();try {await held;} finally {held=undefined;release=undefined;}return Response.json({ok:true});}
    if (path === "/negative") {
      const input = await request.json() as {url?:string;caPath?:string;serverName?:string};
      let candidate;
      try {candidate=await openPostgresSQLOwner({url:input.url??config.url,tls:{caPath:input.caPath??config.caPath,serverName:input.serverName??config.serverName},pool:{max:1,idleTimeout:1,maxLifetime:1,connectionTimeout:1,statementTimeoutMs:1000},verify:async(sql)=>{await sql`SELECT 1`;}});return Response.json({accepted:true});}
      catch {return Response.json({accepted:false});}
      finally {await candidate?.close();}
    }
    if(path === "/close"){await owner.close({deadline:new Date(Date.now()+2000)});closed=true;return Response.json({ok:true,pools});}
    if(path === "/stop"){release?.();await held;await owner.close();server.stop();setTimeout(()=>process.exit(0),10);return Response.json({ok:true});}
    return new Response("missing",{status:404});
  }catch {return Response.json({ok:false},{status:500});}
}});
