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

Tier 1 scenarios S1, S2, S10 do **not** need this addon (they call
`res.users.search`); S6 does. All Tier 2 scenarios (S3, S4, S5, S7,
S8a, S8b, S9) need it.

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
