import test from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { createApp } from "../src/server.ts";

async function withServer(fn: (url: string) => Promise<void>): Promise<void> {
  const server = createApp();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  try {
    await fn(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

const post = (url: string, path: string, body: unknown) =>
  fetch(`${url}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });

test("the page is served with an empty queue", async () => {
  await withServer(async (url) => {
    const page = await fetch(`${url}/`);
    assert.equal(page.status, 200);
    assert.match(await page.text(), /Ratchet/);

    const data = await (await fetch(`${url}/jobs`)).json();
    assert.deepEqual(data.stats, { pending: 0, leased: 0, done: 0, dead: 0 });
    assert.deepEqual(data.jobs, []);
  });
});

test("a job runs once and is marked done", async () => {
  await withServer(async (url) => {
    const queued = await (await post(url, "/jobs", { kind: "send-email", payload: { to: "a@b.c" }, maxAttempts: 3 })).json();
    assert.equal(queued.job.state, "pending");
    assert.equal(queued.job.attempts, 0);

    const run = await (await post(url, "/run", { workerId: "w1", outcome: "succeed" })).json();
    assert.equal(run.job.state, "done");
    assert.equal(run.job.attempts, 1);

    const stats = await (await fetch(`${url}/jobs`)).json();
    assert.equal(stats.stats.done, 1);
    assert.equal(stats.stats.pending, 0);

    const again = await (await post(url, "/run", { workerId: "w1", outcome: "succeed" })).json();
    assert.equal(again.job, null);
  });
});

test("a failure backs off before the job is claimable again", async () => {
  await withServer(async (url) => {
    await post(url, "/jobs", { kind: "webhook", payload: {}, maxAttempts: 3 });

    const failed = await (await post(url, "/run", { workerId: "w1", outcome: "fail" })).json();
    assert.equal(failed.job.state, "pending");
    assert.equal(failed.job.attempts, 1);
    assert.equal(failed.job.lastError, "simulated failure");

    const tooEarly = await (await post(url, "/run", { workerId: "w1", outcome: "succeed" })).json();
    assert.equal(tooEarly.job, null);

    const later = await (await post(url, "/run", { workerId: "w1", outcome: "succeed", advanceMs: 60_000 })).json();
    assert.equal(later.job.state, "done");
    assert.equal(later.job.attempts, 2);
  });
});

test("a job that runs out of attempts is buried, not retried forever", async () => {
  await withServer(async (url) => {
    await post(url, "/jobs", { kind: "flaky", payload: {}, maxAttempts: 2 });

    await post(url, "/run", { workerId: "w1", outcome: "fail" });
    const buried = await (await post(url, "/run", { workerId: "w1", outcome: "fail", advanceMs: 60_000 })).json();
    assert.equal(buried.job.state, "dead");
    assert.equal(buried.job.attempts, 2);

    const stats = await (await fetch(`${url}/jobs`)).json();
    assert.equal(stats.stats.dead, 1);
  });
});

test("a lease is exclusive: a second worker gets nothing", async () => {
  await withServer(async (url) => {
    await post(url, "/jobs", { kind: "slow", payload: {} });

    const first = await (await post(url, "/claim", { workerId: "w1" })).json();
    assert.equal(first.job.state, "leased");

    const second = await (await post(url, "/claim", { workerId: "w2" })).json();
    assert.equal(second.job, null);
  });
});

test("an idempotency key stops a duplicate job", async () => {
  await withServer(async (url) => {
    const first = await (await post(url, "/jobs", { kind: "invoice", payload: { id: 1 }, idempotencyKey: "inv-1" })).json();
    const second = await (await post(url, "/jobs", { kind: "invoice", payload: { id: 1 }, idempotencyKey: "inv-1" })).json();
    assert.equal(second.deduped, true);
    assert.equal(second.job.id, first.job.id);

    const stats = await (await fetch(`${url}/jobs`)).json();
    assert.equal(stats.stats.pending, 1);
    assert.equal((await post(url, "/jobs", { kind: "" })).status, 400);
  });
});
