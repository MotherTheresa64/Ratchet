# Ratchet

A durable job queue over SQLite. Jobs survive process restarts, are claimed by exactly one
worker at a time, retry with exponential backoff, and end up in a dead-letter state instead
of looping forever.

Zero dependencies — `node:sqlite` and the standard library only.

## Why this exists

The interesting part of a queue is not "push" and "pop". It is what happens when a worker
dies halfway through a job, when two workers race for the same row, and when a job has
already failed four times. Ratchet models those cases explicitly.

## Run it

Requires Node 24+ (developed on Node 26). Nothing to install.

```bash
node --test          # 16 tests
```

## Model

```
pending ──claim──▶ leased ──ack──▶ done
   ▲                  │
   └──── fail / ──────┘
     lease expired ──▶ pending  (or dead, when attempts are spent)
```

A job carries `attempts`, `run_at`, `lease_until`, `lease_owner`, `max_attempts` and
`last_error`. Every transition is a single SQL statement inside `BEGIN IMMEDIATE`, so a
claim either wins the row or sees it as unavailable — there is no read-then-write window.

## API

| Method | Behaviour |
| --- | --- |
| `enqueue(kind, payload, opts?, now?)` | Inserts a job. `opts.idempotencyKey` dedupes per kind; a repeat returns the original job with `deduped: true`. |
| `claim({ workerId, now, leaseMs? })` | Leases the oldest due job. **Claiming increments `attempts`**, so a worker that crashes mid-job still consumes a retry. |
| `ack(id, workerId, now?)` | Marks done. Only the worker holding the lease can settle it. |
| `fail(id, workerId, error, now?)` | Schedules a retry at `now + backoffDelay(attempts)`, or buries the job when attempts are exhausted. |
| `renew(id, workerId, now?, leaseMs?)` | Extends a live lease. Returns `false` if the lease was already lost. |
| `recoverExpired(now?)` | Returns expired leases to the pool, or buries them if no attempts remain. |
| `stats()` | Counts per state. |

`now` is always injected rather than read from the clock, which is what makes the
time-dependent behaviour testable without sleeping.

## Design decisions

**Attempts are consumed on claim, not on failure.** A worker that is SIGKILLed never gets
to record its failure. Counting at claim time means a poison job dies after `max_attempts`
claims regardless of whether the failure was ever reported.

**Idempotency is a unique index, not a lookup.** Two concurrent enqueues with the same key
both pass a `SELECT`; only one survives the `INSERT`. The loser catches the `UNIQUE`
violation and returns the winner's row.

**Backoff is capped.** Attempt 20 with `factor: 2` and no ceiling would schedule a retry
years out, which is indistinguishable from a dead job. `backoffDelay` clamps to `maxMs`.

**Lease expiry is not the same as failure.** Recovery puts the job back with its attempt
count intact, so a flapping worker cannot retry forever by dying at the right moment.

## Tests

16 tests covering: claim exclusivity, payload round-trip, ack/fail ownership, backoff math
and its ceiling, `run_at` visibility, FIFO ordering by due time, attempt exhaustion into
the dead-letter state, lease reclamation by a second worker, a stale worker being refused
after its lease was taken, burial on expiry with no attempts left, lease renewal, idempotent
enqueue across kinds, and input validation.
