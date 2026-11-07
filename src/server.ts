import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { Queue } from "./queue.ts";
import type { EnqueueOptions, JobState } from "./queue.ts";

const STATES: JobState[] = ["pending", "leased", "done", "dead"];

let cachedPage: string | null = null;

function indexHtml(): string {
  if (cachedPage === null) {
    cachedPage = readFileSync(new URL("../public/index.html", import.meta.url), "utf8");
  }
  return cachedPage;
}

function send(res: ServerResponse, status: number, body: unknown, type = "application/json; charset=utf-8"): void {
  const payload = typeof body === "string" ? body : JSON.stringify(body);
  res.writeHead(status, {
    "content-type": type,
    "content-length": Buffer.byteLength(payload),
    "cache-control": "no-store",
  });
  res.end(payload);
}

async function readJson(req: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (raw === "") return {};
  try {
    const parsed = JSON.parse(raw);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("request body must be a JSON object");
    }
    return parsed as Record<string, unknown>;
  } catch (err) {
    throw new Error(err instanceof Error && err.message.startsWith("request body") ? err.message : "request body is not valid JSON");
  }
}

export function createApp(): Server {
  const queue = new Queue(":memory:", 30_000);

  return createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    try {
      if (req.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html")) {
        return send(res, 200, indexHtml(), "text/html; charset=utf-8");
      }

      if (req.method === "GET" && url.pathname === "/health") {
        return send(res, 200, { status: "ok" });
      }

      if (req.method === "GET" && url.pathname === "/jobs") {
        const jobs = STATES.flatMap((state) => queue.byState(state));
        return send(res, 200, { stats: queue.stats(), jobs });
      }

      if (req.method === "POST" && url.pathname === "/jobs") {
        const body = await readJson(req);
        const opts: EnqueueOptions = {};
        if (Number.isInteger(body.maxAttempts)) opts.maxAttempts = Number(body.maxAttempts);
        if (typeof body.idempotencyKey === "string") opts.idempotencyKey = body.idempotencyKey;
        if (Number.isInteger(body.runAt)) opts.runAt = Number(body.runAt);
        const result = queue.enqueue(String(body.kind ?? ""), body.payload ?? null, opts, Date.now());
        return send(res, 201, result);
      }

      if (req.method === "POST" && url.pathname === "/claim") {
        const body = await readJson(req);
        const job = queue.claim({ workerId: String(body.workerId ?? "worker-1"), now: Date.now() });
        return send(res, 200, { job });
      }

      if (req.method === "POST" && url.pathname === "/run") {
        const body = await readJson(req);
        const workerId = String(body.workerId ?? "worker-1");
        const advanceMs = Number.isInteger(body.advanceMs) ? Number(body.advanceMs) : 0;
        const at = Date.now() + Math.max(0, advanceMs);

        const job = queue.claim({ workerId, now: at });
        if (!job) return send(res, 200, { job: null, message: "nothing is due to run" });

        if (body.outcome === "fail") {
          queue.fail(job.id, workerId, String(body.error ?? "simulated failure"), at);
        } else {
          queue.ack(job.id, workerId, at);
        }
        return send(res, 200, { job: queue.get(job.id) });
      }

      if (req.method === "POST" && url.pathname === "/recover") {
        const body = await readJson(req);
        const advanceMs = Number.isInteger(body.advanceMs) ? Number(body.advanceMs) : 0;
        const recovered = queue.recoverExpired(Date.now() + Math.max(0, advanceMs));
        return send(res, 200, { recovered });
      }

      return send(res, 404, { error: "not_found", path: url.pathname });
    } catch (err) {
      if (err instanceof RangeError) return send(res, 400, { error: "invalid", message: err.message });
      if (err instanceof Error && err.message.startsWith("request body")) {
        return send(res, 400, { error: "invalid", message: err.message });
      }
      return send(res, 500, { error: "internal", message: "unexpected failure" });
    }
  });
}

// pathToFileURL matters on Windows: argv[1] is a drive path there, which never
// equals the file:/// URL form.
const isMainModule =
  process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMainModule) {
  const port = Number(process.env.PORT ?? 4108);
  createApp().listen(port, () => {
    console.log(`ratchet listening on http://localhost:${port}`);
  });
}
