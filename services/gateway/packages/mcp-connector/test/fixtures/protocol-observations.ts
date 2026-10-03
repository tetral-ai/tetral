import type { McpHTTPProtocolFixture } from './mcp-http-protocol.js';

/** Safe component evidence: fixed fixture facts, never headers, schemas or results. */
export function observeProtocol(peer: McpHTTPProtocolFixture, caseName: string): void {
  const close = peer.close.bind(peer);
  peer.close = async () => {
    console.info(JSON.stringify({kind:'mcp-component-observation',case:caseName,adapter:peer.adapter,
      counts:{initialize:peer.counts.initialize,list:peer.counts.list,call:peer.counts.call,effects:peer.counts.effects,cancelledCalls:peer.counts.cancelledCalls,notifications:peer.counts.notifications},
      requests:peer.requests.map(request=>({method:request.method,session:request.session,origin:request.origin,
        hasCursor:request.cursor!==undefined,credentialLabel:/^(selected-vault|generation-[0-3])$/.test(request.credentialLabel)?request.credentialLabel:'fixture-token',
        githubToolset:request.toolset==='default,actions',protocolVersion:request.protocolVersion??'initialize',
        acceptsJson:request.accept.includes('application/json'),acceptsSse:request.accept.includes('text/event-stream'),jsonContentType:request.contentType.includes('application/json')}))}));
    await close();
  };
}
