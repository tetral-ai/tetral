import {expect,test} from "bun:test";
import {readFile} from "node:fs/promises";
import {gzipSync} from "node:zlib";
import {createProviderTransport} from "../../src/providers/transport.js";

const cert=await readFile(new URL("../fixtures/tls/cert.pem",import.meta.url),"utf8");
const key=await readFile(new URL("../fixtures/tls/key.pem",import.meta.url),"utf8");

test("official dispatcher preserves TLS upload and decodes success/error bodies",async()=>{
 const uploads:string[]=[];
 const server=Bun.serve({port:0,hostname:"127.0.0.1",tls:{key,cert},async fetch(request){
  uploads.push(await request.text());
  return new Response(gzipSync(Buffer.from("decoded:☃")),{status:new URL(request.url).pathname==="/error"?429:200,headers:{"content-encoding":"gzip","content-type":"text/plain","x-fixture":"kept"}});
 }});
 const transport=createProviderTransport({ca:cert});
 try {
  for(const path of ["/ok","/error"]){
   const response=await transport.fetch(new Request(`https://127.0.0.1:${server.port}${path}`,{method:"POST",body:new ReadableStream({start(c){c.enqueue(new TextEncoder().encode("request:☃"));c.close();}}),redirect:"manual"}));
   expect(response.status).toBe(path==="/error"?429:200);expect(response.headers.get("x-fixture")).toBe("kept");expect(response.headers.has("content-encoding")).toBe(false);expect(await response.text()).toBe("decoded:☃");
  }
  expect(uploads).toEqual(["request:☃","request:☃"]);
 } finally {await transport.close(new Date(Date.now()+2000));await server.stop(true);}
},5000);

for(const method of ["GET","HEAD"] as const)test(`null-body responses and manual redirects ${method}`,async()=>{
 const server=Bun.serve({port:0,fetch(request){const path=new URL(request.url).pathname;return path==="/redirect"?new Response("discarded",{status:302,headers:{location:"/final"}}):new Response(null,{status:204});}});
 const transport=createProviderTransport();
 try {
  const response=await transport.fetch(new Request(`http://127.0.0.1:${server.port}/null`,{method,redirect:"manual"}));expect(response.status).toBe(204);expect(response.body).toBeNull();
  const redirect=await transport.fetch(new Request(`http://127.0.0.1:${server.port}/redirect`,{method,redirect:"manual"}));expect(redirect.status).toBe(302);await redirect.body?.cancel();
  await expect(transport.fetch(`http://127.0.0.1:${server.port}/null`)).rejects.toThrow("manual redirects");
 } finally {await transport.close(new Date(Date.now()+2000));await server.stop(true);}
},5000);

test("held consumer cancellation joins decoded TLS source and transport",async()=>{
 let cancelled=false;let sent=0;
 const server=Bun.serve({port:0,hostname:"127.0.0.1",tls:{key,cert},fetch(){return new Response(new ReadableStream<Uint8Array>({async pull(c){await new Promise(resolve=>setTimeout(resolve,2));c.enqueue(new Uint8Array(16384).fill(120));sent++;},cancel(){cancelled=true;}}),{headers:{"content-type":"text/event-stream"}});}});
 const transport=createProviderTransport({ca:cert}),abort=new AbortController();
 try {
  const response=await transport.fetch(new Request(`https://127.0.0.1:${server.port}/stream`,{signal:abort.signal,redirect:"manual"}));const reader=response.body!.getReader();
  expect((await reader.read()).value?.byteLength).toBeGreaterThan(0);await new Promise(resolve=>setTimeout(resolve,30));expect(sent).toBeGreaterThan(0);
  await reader.cancel();reader.releaseLock();await transport.close(new Date(Date.now()+2000));
  for(let i=0;i<100&&!cancelled;i++)await new Promise(resolve=>setTimeout(resolve,5));expect(cancelled).toBe(true);
 } finally {abort.abort();await transport.close(new Date(Date.now()+2000));await server.stop(true);}
},5000);

test("early HTTP rejection releases an unfinished upload",async()=>{
 let cancelled=false;let pulls=0;let release!:()=>void;
 const held=new Promise<void>(resolve=>{release=resolve;});
 const upload=new ReadableStream<Uint8Array>({async pull(c){if(pulls++===0)c.enqueue(new Uint8Array(16384));else await held;},cancel(){cancelled=true;release();}});
 const server=Bun.serve({port:0,fetch(){return new Response("rejected",{status:413});}}),transport=createProviderTransport(),abort=new AbortController();
 try {
  const response=await transport.fetch(new Request(`http://127.0.0.1:${server.port}/upload`,{method:"POST",body:upload,signal:abort.signal,redirect:"manual"}));
  expect(response.status).toBe(413);expect(await response.text()).toBe("rejected");
  for(let i=0;i<100&&!cancelled;i++)await new Promise(resolve=>setTimeout(resolve,5));expect(cancelled).toBe(true);
 } finally {abort.abort();release();await transport.close(new Date(Date.now()+2000));await server.stop(true);}
},5000);

test("close deadline cancels a held response even after Agent.close begins",async()=>{
 let cancelled=false;
 const server=Bun.serve({port:0,fetch(){return new Response(new ReadableStream<Uint8Array>({async pull(c){await new Promise(resolve=>setTimeout(resolve,5));c.enqueue(new Uint8Array(16384));},cancel(){cancelled=true;}}));}}),transport=createProviderTransport();
 try {
  const response=await transport.fetch(new Request(`http://127.0.0.1:${server.port}/held`,{redirect:"manual"}));expect(response.status).toBe(200);
  const start=performance.now();await transport.close(new Date(Date.now()+30));expect(performance.now()-start).toBeLessThan(1000);
  for(let i=0;i<100&&!cancelled;i++)await new Promise(resolve=>setTimeout(resolve,5));expect(cancelled).toBe(true);
 } finally {await transport.close(new Date(Date.now()+500));await server.stop(true);}
},5000);
