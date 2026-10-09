/** Maintained HTTP dispatcher with demand-driven decoded response reads. */
import { Readable } from "node:stream";
import { finished } from "node:stream/promises";
// The explicit public package entry avoids Bun's built-in `undici` compatibility shim.
import { Agent, interceptors } from "undici/index.js";
import type { FetchFunction } from "@ai-sdk/provider-utils";

export interface ProviderTransport {
  readonly fetch: FetchFunction;
  readonly close: (deadline?: Date) => Promise<void>;
}

export function createProviderTransport(options: {readonly ca?: string} = {}): ProviderTransport {
  const agent = new Agent(options.ca === undefined ? {} : {connect:{ca:options.ca}});
  // Pinned official experimental interceptor owns decoding and upstream pause/resume.
  const dispatcher = agent.compose(interceptors.decompress({skipErrorResponses:false,maxSize:0}));
  const active = new Set<(reason?: unknown) => Promise<void>>();
  let closing: Promise<void> | undefined;
  const fetchImpl: FetchFunction = Object.assign(async (input: Parameters<FetchFunction>[0], init?: Parameters<FetchFunction>[1]): Promise<Response> => {
    if (closing !== undefined) throw new TypeError("Provider transport is closing");
    const request = input instanceof Request && init === undefined ? input : new Request(input,init);
    if (request.redirect !== "manual") throw new TypeError("Provider transport requires allowlisted manual redirects");
    const url = new URL(request.url), controller = new AbortController();
    const signal = AbortSignal.any([request.signal,controller.signal]);
    const uploadReader = request.body?.getReader();
    let reading = false;
    const upload = uploadReader === undefined ? undefined : new Readable({
      highWaterMark:16*1024,
      read() {
        if (reading) return;
        reading=true;
        void uploadReader.read().then(part=>{
          reading=false;
          if (this.destroyed) return;
          this.push(part.done ? null : part.value);
        },error=>{reading=false;this.destroy(error instanceof Error ? error : new Error("Provider upload failed"));});
      },
      destroy(error,callback) {
        void uploadReader.cancel(error).catch(()=>{}).then(()=>{
          try {uploadReader.releaseLock();} catch { /* Pending cancellation owns the reader. */ }
          callback(error);
        });
      },
    });
    upload?.on("error",()=>{});
    let responseBody: Awaited<ReturnType<typeof dispatcher.request>>["body"] | undefined;
    let iterator: AsyncIterator<Uint8Array> | undefined;
    let webController: ReadableStreamDefaultController<Uint8Array> | undefined;
    let terminal = false, joining: Promise<void> | undefined;
    const cancelAndJoin = (reason?: unknown): Promise<void> => {
      if (joining !== undefined) return joining;
      terminal=true;
      if (reason !== undefined) try {webController?.error(reason);} catch { /* The Web reader may already be cancelled. */ }
      // Schedule after assignment so abort listeners cannot recursively create a second join.
      joining=Promise.resolve().then(async()=>{
        controller.abort(reason);
        responseBody?.destroy(reason instanceof Error ? reason : undefined);
        upload?.destroy();
        try {await iterator?.return?.();} catch { /* Keep the initiating failure. */ }
        if (responseBody !== undefined) try {await finished(responseBody);} catch { /* Keep first cause. */ }
        if (upload !== undefined) try {await finished(upload);} catch { /* Keep first cause. */ }
      }).finally(()=>{signal.removeEventListener("abort",onAbort);active.delete(cancelAndJoin);});
      return joining;
    };
    const onAbort = (): void => {void cancelAndJoin(signal.reason);};
    active.add(cancelAndJoin);
    signal.addEventListener("abort",onAbort,{once:true});
    try {
      signal.throwIfAborted();
      const result = await dispatcher.request({origin:url.origin,path:url.pathname+url.search,
        method:request.method as Parameters<typeof dispatcher.request>[0]["method"],
        headers:Object.fromEntries(request.headers),...(upload === undefined ? {} : {body:upload}),signal});
      responseBody=result.body;
      signal.throwIfAborted();
      const headers=new Headers();
      for (const [name,value] of Object.entries(result.headers)) {
        if (value === undefined) continue;
        for (const item of Array.isArray(value) ? value : [value]) headers.append(name,item);
      }
      iterator=result.body[Symbol.asyncIterator]();
      if (request.method === "HEAD" || [204,205,304].includes(result.statusCode)) {
        await cancelAndJoin();
        return new Response(null,{status:result.statusCode,headers});
      }
      const stream=new ReadableStream<Uint8Array>({
        start(c) {webController=c;if(signal.aborted)c.error(signal.reason);},
        async pull(c) {
          if (terminal) return;
          try {
            const part=await iterator!.next();
            if (terminal) return;
            if (part.done) {await cancelAndJoin();c.close();}
            else c.enqueue(part.value);
          } catch (error) {await cancelAndJoin(error);c.error(error);}
        },
        cancel:cancelAndJoin,
      },{highWaterMark:0});
      return new Response(stream,{status:result.statusCode,headers});
    } catch (error) {
      // A header completion can race signal cancellation before response ownership is assigned.
      responseBody?.destroy(error instanceof Error ? error : undefined);
      await cancelAndJoin(error);
      if (responseBody !== undefined) try {await finished(responseBody);} catch { /* Preserve first cause. */ }
      throw error;
    }
  },{
    // Optional Bun connection hint; it has no wire semantics.
    preconnect:(()=>{}) satisfies FetchFunction["preconnect"],
  });
  return {fetch:fetchImpl,close:(deadline)=>{
    if (closing !== undefined) return closing;
    closing=(async()=>{
      let timer:ReturnType<typeof setTimeout> | undefined;
      let deadlineJoin:Promise<unknown> | undefined;
      try {
        // Agent.close clears its client map. Abort our live request owners, rather than
        // relying on a later Agent.destroy to find already-closing connections.
        if (deadline !== undefined) timer=setTimeout(()=>{
          const reason=new Error("Provider transport shutdown deadline exceeded");
          deadlineJoin=Promise.allSettled([...active].map(cancel=>cancel(reason)));
        },Math.max(0,deadline.getTime()-Date.now()));
        await agent.close();
        await deadlineJoin;
      } finally {if(timer!==undefined)clearTimeout(timer);}
    })();
    return closing;
  }};
}
