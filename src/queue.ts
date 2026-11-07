import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";

export type JobState = "pending" | "leased" | "done" | "dead";

export interface Job {
  id: string;
  kind: string;
  payload: string;
  state: JobState;
  attempts: number;
  maxAttempts: number;
  runAt: number;
  leaseUntil: number | null;
  leaseOwner: string | null;
  lastError: string | null;
  idempotencyKey: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface EnqueueOptions {
  /** Deduplicates enqueues of the same kind. A repeat returns the original job. */
  idempotencyKey?: string;
  /** Earliest time the job may be claimed, as epoch milliseconds. */
  runAt?: number;
  maxAttempts?: number;
}

export interface BackoffPolicy {
  baseMs: number;
  factor: number;
  maxMs: number;
}

export const DEFAULT_BACKOFF: BackoffPolicy = { baseMs: 1_000, factor: 2, maxMs: 300_000 };

/** Delay before the next attempt. `attempts` is the number already consumed. */
export function backoffDelay(attempts: number, policy: BackoffPolicy = DEFAULT_BACKOFF): number {
  if (attempts <= 0) return 0;
  const raw = policy.baseMs * Math.pow(policy.factor, attempts - 1);
  return Math.min(Math.round(raw), policy.maxMs);
}

export interface ClaimOptions {
  workerId: string;
  now: number;
  /** How long the caller may hold the job before another worker may reclaim it. */
  leaseMs?: number;
}

export interface QueueStats {
  pending: number;
  leased: number;
  done: number;
  dead: number;
}

interface JobRow {
  id: string;
  kind: string;
  payload: string;
  state: JobState;
  attempts: number;
  max_attempts: number;
  run_at: number;
  lease_until: number | null;
  lease_owner: string | null;
  last_error: string | null;
  idempotency_key: string | null;
  created_at: number;
  updated_at: number;
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS jobs (
  id              TEXT PRIMARY KEY,
  kind            TEXT NOT NULL,
  payload         TEXT NOT NULL,
  state           TEXT NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL,
  run_at          INTEGER NOT NULL,
  lease_until     INTEGER,
  lease_owner     TEXT,
  last_error      TEXT,
  idempotency_key TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency
  ON jobs (kind, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_due ON jobs (state, run_at);
`;

function toJob(r: JobRow): Job {
  return {
    id: r.id,
    kind: r.kind,
    payload: r.payload,
    state: r.state,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    runAt: r.run_at,
    leaseUntil: r.lease_until,
    leaseOwner: r.lease_owner,
    lastError: r.last_error,
    idempotencyKey: r.idempotency_key,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

export class Queue {
  private db: DatabaseSync;
  private leaseMs: number;

  constructor(path = ":memory:", leaseMs = 30_000) {
    this.db = new DatabaseSync(path);
    this.db.exec(SCHEMA);
    this.leaseMs = leaseMs;
  }

  /** Runs `fn` inside a write transaction so a claim cannot interleave with another claim. */
  private tx<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const out = fn();
      this.db.exec("COMMIT");
      return out;
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch {
        /* the transaction may already be gone; the original error matters more */
      }
      throw err;
    }
  }

  enqueue(
    kind: string,
    payload: unknown,
    opts: EnqueueOptions = {},
    now = Date.now(),
  ): { job: Job; deduped: boolean } {
    if (!kind || kind.trim() === "") throw new RangeError("kind must be a non-empty string");
    const maxAttempts = opts.maxAttempts ?? 5;
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new RangeError("maxAttempts must be a positive integer");
    }
    const key = opts.idempotencyKey ?? null;

    if (key !== null) {
      const existing = this.findByIdempotencyKey(kind, key);
      if (existing) return { job: existing, deduped: true };
    }

    const id = randomUUID();
    const runAt = opts.runAt ?? now;
    try {
      this.db
        .prepare(
          `INSERT INTO jobs
             (id, kind, payload, state, attempts, max_attempts, run_at, created_at, updated_at, idempotency_key)
           VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?)`,
        )
        .run(id, kind, JSON.stringify(payload ?? null), maxAttempts, runAt, now, now, key);
    } catch (err) {
      // Lost a race against a concurrent enqueue with the same key.
      if (key !== null && String(err).includes("UNIQUE")) {
        const existing = this.findByIdempotencyKey(kind, key);
        if (existing) return { job: existing, deduped: true };
      }
      throw err;
    }
    return { job: this.get(id)!, deduped: false };
  }

  /**
   * Atomically leases the next due job to `workerId`. Claiming increments `attempts`,
   * so a worker that dies mid-job still consumes one of the job's attempts.
   */
  claim(opts: ClaimOptions): Job | null {
    const leaseMs = opts.leaseMs ?? this.leaseMs;
    return this.tx(() => {
      const due = this.db
        .prepare(
          `SELECT id FROM jobs
            WHERE state = 'pending' AND run_at <= ? AND attempts < max_attempts
            ORDER BY run_at ASC, created_at ASC
            LIMIT 1`,
        )
        .get(opts.now) as { id: string } | undefined;
      if (!due) return null;

      this.db
        .prepare(
          `UPDATE jobs
              SET state = 'leased', lease_owner = ?, lease_until = ?,
                  attempts = attempts + 1, updated_at = ?
            WHERE id = ? AND state = 'pending'`,
        )
        .run(opts.workerId, opts.now + leaseMs, opts.now, due.id);

      return this.get(due.id);
    });
  }

  ack(id: string, workerId: string, now = Date.now()): boolean {
    const res = this.db
      .prepare(
        `UPDATE jobs SET state = 'done', lease_owner = NULL, lease_until = NULL, updated_at = ?
          WHERE id = ? AND state = 'leased' AND lease_owner = ?`,
      )
      .run(now, id, workerId);
    return Number(res.changes) === 1;
  }

  /** Records a failure and schedules the retry, or buries the job when attempts run out. */
  fail(id: string, workerId: string, error: string, now = Date.now()): boolean {
    return this.tx(() => {
      const row = this.db
        .prepare(`SELECT * FROM jobs WHERE id = ? AND state = 'leased' AND lease_owner = ?`)
        .get(id, workerId) as unknown as JobRow | undefined;
      if (!row) return false;

      const exhausted = row.attempts >= row.max_attempts;
      const nextRun = exhausted ? row.run_at : now + backoffDelay(row.attempts);
      this.db
        .prepare(
          `UPDATE jobs
              SET state = ?, run_at = ?, last_error = ?, lease_owner = NULL,
                  lease_until = NULL, updated_at = ?
            WHERE id = ?`,
        )
        .run(exhausted ? "dead" : "pending", nextRun, error, now, id);
      return true;
    });
  }

  /** Extends a lease that is still in progress. Returns false if the lease was already lost. */
  renew(id: string, workerId: string, now = Date.now(), leaseMs = this.leaseMs): boolean {
    const res = this.db
      .prepare(
        `UPDATE jobs SET lease_until = ?, updated_at = ?
          WHERE id = ? AND state = 'leased' AND lease_owner = ? AND lease_until > ?`,
      )
      .run(now + leaseMs, now, id, workerId, now);
    return Number(res.changes) === 1;
  }

  /**
   * Returns expired leases to the pool. A job that has burned every attempt while
   * leased goes straight to the dead-letter state instead of being retried forever.
   */
  recoverExpired(now = Date.now()): number {
    return this.tx(() => {
      const rows = this.db
        .prepare(`SELECT * FROM jobs WHERE state = 'leased' AND lease_until <= ?`)
        .all(now) as unknown as JobRow[];
      for (const row of rows) {
        const exhausted = row.attempts >= row.max_attempts;
        this.db
          .prepare(
            `UPDATE jobs
                SET state = ?, lease_owner = NULL, lease_until = NULL,
                    last_error = COALESCE(last_error, 'lease expired'), updated_at = ?
              WHERE id = ?`,
          )
          .run(exhausted ? "dead" : "pending", now, row.id);
      }
      return rows.length;
    });
  }

  get(id: string): Job | null {
    const row = this.db.prepare(`SELECT * FROM jobs WHERE id = ?`).get(id) as unknown as JobRow | undefined;
    return row ? toJob(row) : null;
  }

  findByIdempotencyKey(kind: string, key: string): Job | null {
    const row = this.db
      .prepare(`SELECT * FROM jobs WHERE kind = ? AND idempotency_key = ?`)
      .get(kind, key) as unknown as JobRow | undefined;
    return row ? toJob(row) : null;
  }

  byState(state: JobState): Job[] {
    const rows = this.db
      .prepare(`SELECT * FROM jobs WHERE state = ? ORDER BY run_at ASC, created_at ASC`)
      .all(state) as unknown as JobRow[];
    return rows.map(toJob);
  }

  stats(): QueueStats {
    const rows = this.db
      .prepare(`SELECT state, COUNT(*) AS n FROM jobs GROUP BY state`)
      .all() as unknown as { state: JobState; n: number }[];
    const out: QueueStats = { pending: 0, leased: 0, done: 0, dead: 0 };
    for (const r of rows) out[r.state] = r.n;
    return out;
  }

  close(): void {
    this.db.close();
  }
}
