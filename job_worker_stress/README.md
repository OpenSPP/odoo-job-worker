# job_worker_stress

Test-only Odoo addon. Adds two models that exist solely to provide
predictable job bodies the `job_worker` stress test suite can enqueue.

**Do not install this in production.** It is intentionally
`auto_install = False` so that nothing pulls it in by accident, but if
you do install it the new tables will be created on a real database
even though their only consumer is the test suite. There is no
production code path that calls any of these helpers.

## When to install

When you want to run the `stress` or `tier2` test tags. Those scenarios
need job bodies that sleep, fail, or fail-then-succeed on demand — the
stress addon provides them via stable model methods.

Install alongside `job_worker`:

```bash
docker compose -f docker/docker-compose.yml run --rm odoo odoo \
  -d test_stress \
  --workers 0 \
  --test-enable \
  --test-tags=stress,tier2 \
  --addons-path=/opt/odoo/odoo/addons,/opt/odoo/odoo/odoo/addons,/mnt/extra-addons,/mnt/extra-addons/odoo-job-worker \
  --stop-after-init \
  -i job_worker,job_worker_stress
```

## The stress scenarios

Each scenario lives in its own test file under
`job_worker/tests/test_stress_*.py`. Scenarios are tagged into two
tiers:

- **Tier 1 (`stress`)** — `TransactionCase` tests, no subprocesses,
  fast. Run with `--test-tags=stress`.
- **Tier 2 (`tier2`)** — spawn real `job_worker_runner.py` subprocesses
  via `subprocess.Popen` to get genuine multi-process behaviour. Slow
  (workers boot Odoo). Run with `--test-tags=tier2`.

| ID | Tier | Needs this addon? | What it tests |
|---|---|---|---|
| **S1** | 1 | no | Sustained throughput: 5,000 trivial jobs drained via batched `run_now()`. Reports chunk-latency percentiles. |
| **S2** | 1 | no | Burst enqueue under depth: 10k pre-fill across 10 channels + 1k burst, plus `EXPLAIN ANALYZE` of the worker's `acquire_job_lock` SQL at 11k depth. Catches missing-index regressions. |
| **S3** | 2 | yes (`sleep_for`) | Multi-worker contention: 4 real worker subprocesses (advisory lock disabled) × concurrency=4 against 500 jobs. Asserts > 1 distinct `worker_id` live to prove inter-process `SKIP LOCKED` is exercised. |
| **S4** | 2 | yes (`sleep_for`) | Channel concurrency limit: `queue.limit.limit = 2` against 16 effective slots. Live-samples `count(state='started')` every 50 ms and asserts the observed peak vs. the limit (with overshoot tolerance — the acquire path has a TOCTOU window). |
| **S5** | 2 | yes (`sleep_for`) | Channel rate limit: `rate_limit = 10` against 100 jobs. Asserts drain time is bounded below by `(jobs / rate_limit) × 0.8`. Reports observed peak RPS. |
| **S6** | 1 | yes (`retry_once_then_succeed`) | Retry storm: 500 jobs that raise `RetryableJobError(seconds=0)` on the first attempt and succeed on the second. Asserts sum of `attempts` field == 500 (i.e. exactly one retry per job — `attempts` only increments on retry, not on success). |
| **S7** | 2 | yes (`sleep_for`) | Timeout enforcement under load: jobs sleep longer than the worker's heartbeat interval (15 s default), so the heartbeat tick detects `timeout=1s` and the job ends in `failed` with `TimeoutJobError` in `exc_info`. |
| **S8a** | 2 | yes (`sleep_for`) | SIGTERM graceful shutdown: verifies the documented contract (`docs/deployment.md`) that the runner finishes in-flight jobs before exiting. In-flight rows reach `done`; queued-but-not-started rows stay `pending`. |
| **S8b** | 2 | yes (`sleep_for`) | SIGKILL hard kill: worker has no chance to clean up. In-flight rows stay `started` with frozen heartbeat. A fresh worker reclaims via the stale-heartbeat path (default 60 s) and they reach `done`. |
| **S9** | 2 | yes (`sleep_for`) | NOTIFY/LISTEN pickup latency under flood: worker parks on `select()`, then 200 jobs are enqueued in a tight loop (each commit fires `NOTIFY queue_job_wake_up`). Reports p50/p95/p99 of `started_at - create_date`. |
| **S10** | 1 | no | PG18 serialization storm: 200 jobs pre-set to `state='started'`, then 10 threads drive simultaneous heartbeat + completion `UPDATE`s through `_retry_db_operation` to verify the retry wrapper absorbs PG's serialization failures at volume. |

## What's inside

### `job.worker.stress.helper` (AbstractModel)

Methods callable as queue.job bodies via `model_name="job.worker.stress.helper"`.

| Method | Behaviour | Used by |
|---|---|---|
| `sleep_for(seconds)` | `time.sleep(seconds)`; returns the same number. | S3, S4, S5, S7, S8a, S8b, S9 |
| `fail_always(reason="stress fail")` | Raises `RuntimeError(reason)`. | (available, not yet used) |
| `retry_once_then_succeed(key)` | First call raises `RetryableJobError(seconds=0)`; second call returns 2. State persists between attempts via the counter table. | S6 |

### `job.worker.stress.counter` (Model)

A single-purpose table that backs `retry_once_then_succeed`. One row
per `key`, with an integer `count`. The unique constraint on `key`
makes the counter safe to look up between attempts.

The table is also a useful audit point for tests: after S6 runs you
can `SELECT count(*), bool_and(count = 2) FROM job_worker_stress_counter`
to confirm every counter advanced exactly to 2.

Tests are expected to clear this table in `setUp` (see
`stress_common.clear_queue` and the per-test cleanup blocks).

## Why a separate addon instead of helpers inside `job_worker`?

Keeps the stress helpers out of the production code path. If they
lived in `job_worker`, the model registry on every production database
would contain test-only methods (sleep, fail, retry_once_then_succeed)
plus a counter table — usable from any RPC call. Installing them in a
clearly-labelled separate addon makes the test-only intent explicit and
opt-in.

## Why does `retry_once_then_succeed` work?

`queue.job.run_now()`'s `RetryableJobError` branch does **not** roll
back the executing cursor — it just sets `state = pending` and
`scheduled_at`. The counter row created inside `retry_once_then_succeed`
on the first attempt is committed alongside that state change, so on
the second attempt the row is visible and the counter is incremented
to 2 instead of raising again.

This relies on the run_now contract for retryable errors; if that
contract changes (e.g. a future version starts rolling back on
RetryableJobError), this helper breaks and S6 will fail with attempts
diverging from the expected value.
