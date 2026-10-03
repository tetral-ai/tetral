import { expect, test } from 'bun:test';
import { SQLMcpServerResolver } from '../../src/server-resolver.js';
import type { McpCredentialSQL } from '../../src/credential.js';
function resolver(installed: unknown) {
 const queries:{sql:string;values:readonly unknown[]}[]=[];
 const tx=(async(strings:TemplateStringsArray,...values:readonly unknown[])=>{const sql=strings.join('?');queries.push({sql,values});return sql.includes('FROM sessions')?[{installed_tools_json:installed}]:[];}) as McpCredentialSQL;
 const sql=(async()=>[]) as McpCredentialSQL;sql.begin=async action=>action(tx);
 return {resolver:new SQLMcpServerResolver(sql),queries};
}
for(const endpoint of ['https://api.githubcopilot.com/mcp/','https://mcp.slack.com/mcp'])test('installed name resolves immutable registered snapshot '+endpoint,async()=>{
 const f=resolver({mcp_servers:[{type:'url',name:'work-selected',url:endpoint}]});
 const result=await f.resolver.resolve({workspaceId:'workspace-selected',sessionId:'session-selected',mcpServerName:'work-selected'});
 expect(result).toMatchObject({configuredName:'work-selected',endpoint});expect(Object.isFrozen(result)).toBe(true);
 expect(f.queries[0]!.sql).toContain("set_config('tetral.workspace_id'");expect(f.queries[0]!.values).toEqual(['workspace-selected']);expect(f.queries[1]!.values).toEqual(['workspace-selected','session-selected']);
});
for(const installed of [undefined,'{invalid-secret',[{}],{mcp_servers:[]},{mcp_servers:[{type:'url',name:'different',url:'https://mcp.slack.com/mcp'}]},{mcp_servers:[{type:'command',name:'work-selected',url:'https://mcp.slack.com/mcp'}]},{mcp_servers:[{type:'url',name:'work-selected',url:'https://mcp.slack.com/mcp?token=secret'}]},{mcp_servers:[{type:'url',name:'work-selected',url:'https://mcp.slack.com/mcp'},{type:'url',name:'work-selected',url:'https://api.githubcopilot.com/mcp/'}]}])test('invalid or ambiguous installed server fails closed '+JSON.stringify(installed),async()=>{
 const f=resolver(installed);const error=await f.resolver.resolve({workspaceId:'w',sessionId:'s',mcpServerName:'work-selected'}).catch(error=>error);expect(error).toMatchObject({code:'mcp_invalid_input',message:'Configured MCP server is unavailable or unsupported.'});expect(error.message).not.toContain('secret');
});
