# TaskRelay

[![CI](https://github.com/alandoure005/taskrelay-python/actions/workflows/ci.yml/badge.svg)](https://github.com/alandoure005/taskrelay-python/actions/workflows/ci.yml)

A persistent Python job queue with SQLite transactions, expiring worker leases, retry backoff, and dead-letter states. Multiple worker processes can coordinate through one local database.

## Quick start

Requires Python 3.10+. Uses only the standard library.

```sh
python3 -m unittest discover -s tests -v
python3 taskrelay.py enqueue --key report-001 --text 'monthly report'
python3 taskrelay.py worker --once
python3 taskrelay.py stats
```

The final command reports one completed job. Repeat the enqueue command with the same key and payload: it returns the existing job ID. A different payload using the same key is rejected.

```sh
python3 taskrelay.py --db queue.db worker
python3 taskrelay.py --db queue.db show JOB_ID
```

Run the worker command in two terminals to process jobs concurrently. Ctrl-C stops an idle worker; a process interrupted during work leaves a lease that can later be reclaimed. The included handler computes a SHA-256 digest without executing user-supplied commands.

## State machine

```text
pending --claim--> running --complete--> done
   ^                  |
   +----retry delay---+--failure
                      +--attempt limit--> dead

expired running lease --> pending or dead (on the next claim)
```

`BEGIN IMMEDIATE` serializes claims. A unique token identifies each lease. Completion, failure, and heartbeat operations require both the token and an unexpired lease. A stalled worker cannot acknowledge a newer worker's claim. Claiming increments the attempt count, including attempts that later crash.

Failures retry after `min(300, 2**attempts)` seconds; the default attempt limit is three. Expired leases are reclaimed immediately on the next claim. Jobs that exhaust their attempts stay inspectable as `dead`.

## Python API

```python
from taskrelay import Queue

queue = Queue('queue.db')
job_id = queue.enqueue('example', {'type': 'sha256', 'text': 'hello'})
job = queue.claim(lease_seconds=30)
if job:
    accepted = queue.complete(job['id'], job['token'], {'answer': 42})
```

`heartbeat` extends a live lease; handlers running longer than the lease must arrange periodic heartbeats. The sample worker's short hashing operation does not start a heartbeat thread. Tests use an injectable clock to check expiration boundaries without sleeps and real concurrent connections to check exclusive claims.

## Operational notes

- Delivery is **at least once**. Lease fencing protects queue state, not external side effects. Make handlers idempotent.
- Use a local filesystem. SQLite WAL is not a distributed broker and should not be placed on a network share.
- The default lease is 30 seconds. Keep host time stable because leases use wall-clock time.
- Job payloads are limited to 64 KiB; use references for large inputs. Completed-job retention and result-size limits are not implemented.
- Expiration cleanup runs on claim, so `stats` alone does not reclaim abandoned jobs.
- A SQLite-aware backup is required while workers are active; copying only the `.db` file can omit WAL changes.

## Further development

Add worker heartbeat scheduling, dead-letter replay, retention policies, and measurements comparing one versus multiple workers. Queue state and results remain local; there is no HTTP interface or authentication layer in this version.
