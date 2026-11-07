import test from "node:test";
import assert from "node:assert/strict";
import { Queue, backoffDelay, DEFAULT_BACKOFF } from "../src/queue.ts";

const T0 = 1_700_000_000_000;

function fresh(): Queue {
  return new Queue(":memory:", 1_000);
}

test("enqueue then claim leases the job and consumes one attempt", () => {
  const q = fresh();
  const { job } = q.enqueue("email", { to: "a@b.c" }, {}, T0);
  assert.equal(job.state, "pending");
  assert.equal(job.attempts, 0);

  const claimed = q.claim({ workerId: "w1", now: T0 });
  assert.ok(claimed);
  assert.equal(claimed.id, job.id);
  assert.equal(claimed.state, "leased");
  assert.equal(claimed.attempts, 1);
  assert.equal(claimed.leaseOwner, "w1");
  assert.equal(claimed.leaseUntil, T0 + 1_000);
  q.close();
});

test("payload survives the JSON round trip", () => {
  const q = fresh();
  const payload = { nested: { n: [1, 2, 3] }, ok: true };
  const { job } = q.enqueue("work", payload, {}, T0);
  assert.deepEqual(JSON.parse(job.payload), payload);
  q.close();
});

test("a leased job cannot be claimed twice", () => {
  const q = fresh();
  q.enqueue("work", {}, {}, T0);
  assert.ok(q.claim({ workerId: "w1", now: T0 }));
  assert.equal(q.claim({ workerId: "w2", now: T0 }), null);
  q.close();
});

test("ack moves the job to done and it is never claimed again", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, {}, T0);
  const claimed = q.claim({ workerId: "w1", now: T0 })!;
  assert.equal(q.ack(claimed.id, "w1", T0), true);
  assert.equal(q.get(job.id)!.state, "done");
  assert.equal(q.claim({ workerId: "w2", now: T0 + 5_000 }), null);
  q.close();
});

test("a finalised job is only settled by the worker holding its lease", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, {}, T0);
  q.claim({ workerId: "w1", now: T0 });
  assert.equal(q.ack(job.id, "w2", T0), false);
  assert.equal(q.fail(job.id, "w2", "nope", T0), false);
  assert.equal(q.get(job.id)!.state, "leased");
  q.close();
});

test("fail schedules a retry with exponential backoff", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, {}, T0);
  q.claim({ workerId: "w1", now: T0 });
  assert.equal(q.fail(job.id, "w1", "boom", T0), true);

  const after = q.get(job.id)!;
  assert.equal(after.state, "pending");
  assert.equal(after.lastError, "boom");
  assert.equal(after.attempts, 1);
  assert.equal(after.runAt, T0 + backoffDelay(1));
  q.close();
});

test("backoff grows geometrically and respects the ceiling", () => {
  const policy = { baseMs: 100, factor: 3, maxMs: 1_000 };
  assert.equal(backoffDelay(0, policy), 0);
  assert.equal(backoffDelay(1, policy), 100);
  assert.equal(backoffDelay(2, policy), 300);
  assert.equal(backoffDelay(3, policy), 900);
  assert.equal(backoffDelay(4, policy), 1_000);
  assert.equal(backoffDelay(50, policy), policy.maxMs);
  assert.equal(backoffDelay(1), DEFAULT_BACKOFF.baseMs);
});

test("a job is invisible until its runAt time", () => {
  const q = fresh();
  q.enqueue("work", {}, { runAt: T0 + 10_000 }, T0);
  assert.equal(q.claim({ workerId: "w1", now: T0 }), null);
  assert.equal(q.claim({ workerId: "w1", now: T0 + 9_999 }), null);
  assert.ok(q.claim({ workerId: "w1", now: T0 + 10_000 }));
  q.close();
});

test("due jobs are claimed oldest runAt first", () => {
  const q = fresh();
  const late = q.enqueue("work", { n: 3 }, { runAt: T0 + 300 }, T0).job;
  const early = q.enqueue("work", { n: 1 }, { runAt: T0 + 100 }, T0).job;
  const mid = q.enqueue("work", { n: 2 }, { runAt: T0 + 200 }, T0).job;

  const order = [q.claim({ workerId: "w", now: T0 + 400 }), q.claim({ workerId: "w", now: T0 + 400 })];
  assert.deepEqual(order.map((j) => j!.id), [early.id, mid.id]);
  assert.equal(q.claim({ workerId: "w", now: T0 + 400 })!.id, late.id);
  q.close();
});

test("attempts are capped and the job ends up in the dead-letter state", () => {
  const q = fresh();
  const { job } = q.enqueue("flaky", {}, { maxAttempts: 2 }, T0);
  let now = T0;

  for (let i = 0; i < 2; i++) {
    const claimed = q.claim({ workerId: "w1", now });
    assert.ok(claimed, `attempt ${i + 1} should be claimable`);
    q.fail(job.id, "w1", `failure ${i + 1}`, now);
    now += 100_000;
  }

  assert.equal(q.get(job.id)!.state, "dead");
  assert.equal(q.get(job.id)!.attempts, 2);
  assert.equal(q.claim({ workerId: "w1", now: now + 1_000_000 }), null);
  assert.deepEqual(q.stats(), { pending: 0, leased: 0, done: 0, dead: 1 });
  q.close();
});

test("an expired lease is reclaimed by another worker", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, {}, T0);
  q.claim({ workerId: "w1", now: T0, leaseMs: 500 });

  assert.equal(q.claim({ workerId: "w2", now: T0 + 100 }), null, "still leased");
  assert.equal(q.recoverExpired(T0 + 499), 0);
  assert.equal(q.recoverExpired(T0 + 500), 1);

  const reclaimed = q.claim({ workerId: "w2", now: T0 + 500 });
  assert.ok(reclaimed);
  assert.equal(reclaimed.id, job.id);
  assert.equal(reclaimed.attempts, 2, "the crashed worker's attempt is still counted");
  q.close();
});

test("a crashed worker cannot ack after its lease was reclaimed", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, {}, T0);
  q.claim({ workerId: "w1", now: T0, leaseMs: 500 });
  q.recoverExpired(T0 + 500);
  const taken = q.claim({ workerId: "w2", now: T0 + 500 })!;
  assert.equal(taken.id, job.id);
  assert.equal(q.ack(job.id, "w1", T0 + 600), false, "stale worker must not finalise");
  assert.equal(q.ack(job.id, "w2", T0 + 600), true);
  q.close();
});

test("a lease that expires with no attempts left is buried, not retried", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, { maxAttempts: 1 }, T0);
  q.claim({ workerId: "w1", now: T0, leaseMs: 100 });
  q.recoverExpired(T0 + 200);
  assert.equal(q.get(job.id)!.state, "dead");
  q.close();
});

test("renew extends a live lease but not a lost one", () => {
  const q = fresh();
  const { job } = q.enqueue("work", {}, {}, T0);
  q.claim({ workerId: "w1", now: T0, leaseMs: 1_000 });
  assert.equal(q.renew(job.id, "w1", T0 + 500, 1_000), true);
  assert.equal(q.get(job.id)!.leaseUntil, T0 + 1_500);
  assert.equal(q.renew(job.id, "w1", T0 + 2_000, 1_000), false);
  assert.equal(q.renew(job.id, "w2", T0 + 600, 1_000), false);
  q.close();
});

test("idempotency keys dedupe enqueues and keep one row", () => {
  const q = fresh();
  const first = q.enqueue("invoice", { id: 7 }, { idempotencyKey: "inv-7" }, T0);
  const second = q.enqueue("invoice", { id: 7 }, { idempotencyKey: "inv-7" }, T0 + 50);

  assert.equal(first.deduped, false);
  assert.equal(second.deduped, true);
  assert.equal(second.job.id, first.job.id);
  assert.equal(q.byState("pending").length, 1);

  const otherKind = q.enqueue("receipt", { id: 7 }, { idempotencyKey: "inv-7" }, T0);
  assert.equal(otherKind.deduped, false, "the key is scoped to the job kind");
  q.close();
});

test("rejects invalid input instead of storing a job that can never run", () => {
  const q = fresh();
  assert.throws(() => q.enqueue("", {}), RangeError);
  assert.throws(() => q.enqueue("   ", {}), RangeError);
  assert.throws(() => q.enqueue("work", {}, { maxAttempts: 0 }), RangeError);
  assert.throws(() => q.enqueue("work", {}, { maxAttempts: 1.5 }), RangeError);
  assert.deepEqual(q.stats(), { pending: 0, leased: 0, done: 0, dead: 0 });
  q.close();
});
