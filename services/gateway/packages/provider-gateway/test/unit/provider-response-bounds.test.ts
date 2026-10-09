import { describe, expect, test } from "bun:test";
import {
  createJsonErrorResponseHandler,
  createJsonResponseHandler,
  createStatusCodeErrorResponseHandler,
  DownloadError,
} from "@ai-sdk/provider-utils";
import { z } from "zod";

// GHSA-866g-f22w-33x8 affects all three buffered response handlers.
// A claimed length above the upstream 2 GiB cap exercises rejection without
// allocating a large body; cancellation must release the underlying stream.
const oversizedContentLength = String(2 * 1024 * 1024 * 1024 + 1);
const handlers = [
  { name: "JSON success", status: 200, handler: createJsonResponseHandler(z.object({ message: z.string() })) },
  {
    name: "JSON error",
    status: 400,
    handler: createJsonErrorResponseHandler({
      errorSchema: z.object({ message: z.string() }),
      errorToMessage: (error) => error.message,
    }),
  },
  { name: "status-code error", status: 500, handler: createStatusCodeErrorResponseHandler() },
];

describe("provider response body bounds", () => {
  for (const { name, status, handler } of handlers) {
    test(`${name} rejects an oversized body before reading and cancels it`, async () => {
      let reads = 0;
      let cancellations = 0;
      const response = new Response(new ReadableStream<Uint8Array>({
        pull(controller) {
          reads += 1;
          controller.enqueue(new TextEncoder().encode('{"message":"small"}'));
          controller.close();
        },
        cancel() { cancellations += 1; },
      }, { highWaterMark: 0 }), {
        status,
        headers: { "content-type": "application/json", "content-length": oversizedContentLength },
      });

      await expect(handler({
        url: "https://api.anthropic.com/v1/messages",
        requestBodyValues: {},
        response,
      })).rejects.toBeInstanceOf(DownloadError);
      expect(reads).toBe(0);
      expect(cancellations).toBe(1);
    });
  }
});
