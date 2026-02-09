# Migrating from OCA queue_job to job_worker

This guide covers migrating Odoo modules from OCA's `queue_job` to `job_worker`.

`job_worker` is a ground-up reimplementation with a SQL PULL architecture,
flat channel model, consolidated payload JSON, and heartbeat-based stale
detection. The delay API (`with_delay`, `delayable`, `group`, `chain`,
`on_done`) is compatible. Most migration work is mechanical: changing import
paths, replacing channel/function XML data, and adjusting state names.

## Table of contents

1. [Manifest dependency](#1-manifest-dependency)
2. [Import paths](#2-import-paths)
3. [Channel configuration](#3-channel-configuration)
4. [Function registry](#4-function-registry-removed)
5. [State names](#5-state-names)
6. [with_delay and delayable API](#6-with_delay-and-delayable-api)
7. [group, chain, on_done](#7-group-chain-on_done)
8. [identity_key](#8-identity_key)
9. [RetryableJobError](#9-retryablejobrerror)
10. [Job UUID](#10-job-uuid)
11. [Testing with trap_jobs](#11-testing-with-trap_jobs)
12. [Field name changes](#12-field-name-changes)
13. [Removed features](#13-removed-features)
14. [Synchronous execution bypass](#14-synchronous-execution-bypass)
15. [_job_store_values hook](#15-_job_store_values-hook)
16. [Related actions](#16-related-actions)
17. [Step-by-step checklist](#17-step-by-step-checklist)

---

## 1. Manifest dependency

In every module's `__manifest__.py`, replace the dependency:

```python
# Before
"depends": ["queue_job", "base"],

# After
"depends": ["job_worker", "base"],
```

## 2. Import paths

All Python imports from `odoo.addons.queue_job` must change to
`odoo.addons.job_worker`. The module-internal structure differs, so
here is the full mapping:

### Delay API

```python
# Before
from odoo.addons.queue_job.delay import Delayable
from odoo.addons.queue_job.delay import DelayableRecordset
from odoo.addons.queue_job.delay import group
from odoo.addons.queue_job.delay import chain

# After
from odoo.addons.job_worker.delay import Delayable
from odoo.addons.job_worker.delay import DelayableRecordset
from odoo.addons.job_worker.delay import group
from odoo.addons.job_worker.delay import chain
```

### Exceptions

```python
# Before
from odoo.addons.queue_job.exception import RetryableJobError
from odoo.addons.queue_job.exception import JobError
from odoo.addons.queue_job.exception import FailedJobError

# After
from odoo.addons.job_worker.exception import RetryableJobError
from odoo.addons.job_worker.exception import JobError
from odoo.addons.job_worker.exception import FailedJobError
```

### Identity functions

```python
# Before
from odoo.addons.queue_job.job import identity_exact

# After
from odoo.addons.job_worker.job import identity_exact
```

### State constants

```python
# Before
from odoo.addons.queue_job.job import PENDING, ENQUEUED, DONE, FAILED, STARTED

# After
from odoo.addons.job_worker.job import PENDING, DONE, FAILED, STARTED, WAITING, CANCELLED
```

See [State names](#5-state-names) for the semantic mapping.

### Test helpers

```python
# Before
from odoo.addons.queue_job.tests.common import trap_jobs, JobMixin

# After
from odoo.addons.job_worker.tests.common import trap_jobs
```

See [Testing with trap_jobs](#11-testing-with-trap_jobs) for API differences.

## 3. Channel configuration

OCA `queue_job` uses a hierarchical channel model (`queue.job.channel`) where
channels have parent/child relationships (e.g., `root.export.heavy`) and
concurrency limits cascade down the tree.

`job_worker` uses **flat string channel names** on the job record itself, plus
an optional `queue.limit` model for concurrency and rate control.

### Remove channel XML data

Delete all `queue.job.channel` records from your module's XML data files:

```xml
<!-- DELETE this -->
<record id="channel_root_export" model="queue.job.channel">
    <field name="name">export</field>
    <field name="parent_id" ref="queue_job.channel_root"/>
</record>
```

### Add queue.limit records (if needed)

If a channel needs concurrency or rate limiting, create a `queue.limit`
record. If no `queue.limit` record exists for a channel, the default
concurrency is **1** (only one job runs at a time per channel).

```xml
<record id="limit_export" model="queue.limit">
    <field name="name">export</field>
    <field name="limit">3</field>        <!-- max concurrent jobs in this channel -->
    <field name="rate_limit">0</field>   <!-- max jobs per second; 0 = unlimited -->
</record>
```

Setting `limit` to `0` blocks the channel entirely (no jobs will be acquired).

### Flatten hierarchical channel names

If your code was using hierarchical channel names like `root.export.heavy`,
you should decide on a flat name like `export_heavy` or just `export`.
Update all `with_delay(channel=...)` and `delayable(channel=...)` calls
to use the flat name, and create a matching `queue.limit` record.

The string you pass as `channel` is just a tag -- there is no hierarchy.

## 4. Function registry (removed)

OCA `queue_job` has a `queue.job.function` model that registers which
model/method combinations are allowed to be delayed. `job_worker` has **no
function registry** -- any method on any model can be delayed.

### Remove function XML data

Delete all `queue.job.function` records from your module's XML data:

```xml
<!-- DELETE this -->
<record id="job_function_do_import" model="queue.job.function">
    <field name="model_id" ref="model_my_model"/>
    <field name="method">do_import</field>
    <field name="channel_id" ref="channel_root_export"/>
</record>
```

### Remove function-related code

If your code reads from or writes to `queue.job.function`, remove those
references. Common patterns to look for:

- `self.env["queue.job.function"].search(...)`
- XML records with `model="queue.job.function"`
- References to `retry_pattern` on the function model (see
  [RetryableJobError](#9-retryablejobrerror) for the replacement)

## 5. State names

The state values differ between OCA `queue_job` and `job_worker`:

| OCA queue_job       | job_worker   | Meaning                              |
|---------------------|--------------|--------------------------------------|
| `wait_dependencies` | `waiting`    | Blocked on parent job (graph dep)    |
| `pending`           | `waiting`    | Same as above in some OCA contexts   |
| `enqueued`          | `pending`    | Ready to be picked up by a worker    |
| `started`           | `started`    | Currently executing                  |
| `done`              | `done`       | Completed successfully               |
| `failed`            | `failed`     | Failed after exhausting retries      |
| `cancelled`         | `cancelled`  | Manually cancelled                   |

The key changes:

- **`enqueued` does not exist.** Replace with `pending`.
- **`wait_dependencies` does not exist.** Replace with `waiting`.
- **`cancelled` is available.** OCA had it too, but it is worth noting
  `job_worker` supports it natively with a `cancelled_at` timestamp.

Update all domain filters, state checks, and search calls:

```python
# Before
jobs = self.env["queue.job"].search([("state", "=", "enqueued")])

# After
jobs = self.env["queue.job"].search([("state", "=", "pending")])
```

## 6. with_delay and delayable API

The core delay API is **compatible**. These patterns work identically:

```python
# Simple delay (unchanged)
record.with_delay(channel="export", priority=15).do_export()

# Explicit delayable (unchanged)
record.delayable(channel="export", eta=3600).do_export().delay()
```

### Supported options

All common `with_delay()` / `delayable()` keyword arguments are supported:

| Option         | Supported | Notes                                  |
|----------------|-----------|----------------------------------------|
| `channel`      | Yes       | Flat string, not hierarchical path     |
| `priority`     | Yes       | Lower = higher priority. Default: 10   |
| `eta`          | Yes       | datetime, timedelta, or seconds (int)  |
| `max_retries`  | Yes       | 0 = infinite. Default: 5               |
| `description`  | Yes       | Stored in the `name` field on the job  |
| `identity_key` | Yes       | String or callable                     |

### Additional option: scheduled_at

`job_worker` accepts `scheduled_at` as an alias for `eta`. You can use either.
Do not pass conflicting values for both.

### split() method

`job_worker` supports `Delayable.split(size, chain=False)` to split a
recordset into chunks:

```python
delayable = records.delayable(channel="export")
delayable.do_export()
group_of_chunks = delayable.split(100)  # 100 records per chunk
group_of_chunks.delay()
```

Pass `chain=True` to get a `DelayableChain` instead of a `DelayableGroup`.

## 7. group, chain, on_done

Core graph composition is compatible:

```python
from odoo.addons.job_worker.delay import group, chain

# Fan-out (parallel execution)
g = group(
    record1.delayable().do_work(),
    record2.delayable().do_work(),
)
g.delay()

# Sequential execution
c = chain(
    record1.delayable().step_one(),
    record2.delayable().step_two(),
)
c.delay()

# Callback after completion
main = record.delayable().do_heavy_work()
callback = record.delayable().notify_done()
main.on_done(callback)
main.delay()
```

**Implementation note:** In `job_worker`, chain dependencies are tracked via
`parent_id` and `graph_uuid` fields on the job record. Child jobs start in
`waiting` state and move to `pending` when their parent completes.

**Current limitation:** `chain()` sequencing is enforced for `Delayable`
steps. `group().on_done(...)` and `chain(..., group(...), ...)` do not create
a wait-for-all dependency barrier in the current implementation.

## 8. identity_key

`identity_key` works the same way. It can be a string or a callable that
receives the `Delayable` object and returns a string:

```python
# String key
record.with_delay(identity_key="export_partner_42").do_export()

# Callable (identity_exact is provided)
from odoo.addons.job_worker.job import identity_exact
record.with_delay(identity_key=identity_exact).do_export()
```

The `identity_exact` function hashes `(model_name, method_name, record_ids,
args, kwargs)` into a SHA-1 hex digest. The deduplication is enforced by a
partial unique index on `identity_key` for active jobs (states: `waiting`,
`pending`, `started`).

**Callables receive a `Delayable` object** (not a `Job` object as in OCA).
The `Delayable` exposes the same attributes used by typical identity
functions: `model_name`, `method_name`, `args`, `kwargs`, and
`recordset` (with `.ids`).

## 9. RetryableJobError

`RetryableJobError` has the same constructor signature:

```python
from odoo.addons.job_worker.exception import RetryableJobError

def my_job_method(self):
    if not_ready:
        raise RetryableJobError(
            "Service not ready",
            seconds=120,         # retry in 120s (overrides exponential backoff)
            ignore_retry=True,   # don't count this toward max_retries
        )
```

### Differences from OCA

- **No `retry_pattern` on functions.** OCA's `queue.job.function` model
  supported per-function `retry_pattern` dicts mapping attempt numbers to
  delays. `job_worker` does not have this. Use `RetryableJobError(seconds=N)`
  to control retry delay, or rely on the default exponential backoff
  (`10 * 2^(attempt-1)` seconds, capped at 3600).

- **`FailedJobError` is not special-cased.** It follows the same retry flow as
  other non-`RetryableJobError` exceptions, based on `max_retries`.

- **Manual re-enqueue with `eta` still works.** If your code catches errors
  and re-enqueues with a new `eta` instead of raising `RetryableJobError`,
  that pattern continues to work.

## 10. Job UUID

Jobs have a `uuid` field (auto-generated UUIDv4 string). If your code stores
the job UUID for later reference (e.g., for tracking or correlation), this
works the same way:

```python
job = record.with_delay(channel="export").do_export()
transaction.job_uuid = job.uuid  # store for later lookup
```

To look up a job by UUID:

```python
job = self.env["queue.job"].search([("uuid", "=", stored_uuid)], limit=1)
```

## 11. Testing with trap_jobs

`job_worker` provides a `trap_jobs()` context manager for testing, but the
API differs from OCA's version.

### OCA trap_jobs (before)

```python
from odoo.addons.queue_job.tests.common import trap_jobs

with trap_jobs() as trap:
    record.with_delay().do_export()
    trap.assert_jobs_count(1)
    trap.assert_enqueued_job(
        self.env["my.model"].do_export,  # bound method object
        args=(...),
        kwargs={...},
        properties=dict(channel="export", priority=15),
    )
    trap.perform_enqueued_jobs()
```

### job_worker trap_jobs (after)

```python
from odoo.addons.job_worker.tests.common import trap_jobs

with trap_jobs(self.env) as trap:
    record.with_delay().do_export()
    trap.assert_jobs_count(1)
    trap.assert_enqueued_job(
        "my.model",       # model name as string
        "do_export",      # method name as string
        args=[...],
        kwargs={...},
    )
    trap.perform_enqueued_jobs()
```

Key differences:

| Aspect                | OCA                                     | job_worker                          |
|-----------------------|-----------------------------------------|-------------------------------------|
| Context manager arg   | `trap_jobs()` (no args)                 | `trap_jobs(env)` (pass environment) |
| `assert_enqueued_job` | Takes bound method object               | Takes `model` and `method` strings  |
| `properties` param    | Dict of job properties to match         | Use `**extra_filters` kwargs        |
| Job storage           | Jobs are NOT written to DB              | Jobs ARE written to DB              |
| `JobMixin`            | Mixin class for test cases              | Not provided                        |
| `mock_with_delay()`   | Deprecated helper available             | Not provided                        |

### Additional test helpers

`job_worker` also provides helpers in `job_worker.tests.common`:

```python
from odoo.addons.job_worker.tests.common import external_env, now_with_slack

# Open an isolated cursor/environment for multi-transaction tests
with external_env(self.env) as (cr, env):
    env["queue.job"].search([...])
    cr.commit()

# Get (lower, upper) datetime bounds for assertions
lower, upper = now_with_slack(seconds=5)
```

## 12. Field name changes

`job_worker` uses different field names internally but provides **computed
alias fields** for backward compatibility. Both the old and new names work
in searches and reads:

| OCA field name   | job_worker field name | Alias provided |
|------------------|-----------------------|----------------|
| `date_created`   | `create_date`         | Yes            |
| `date_enqueued`  | `create_date`         | No             |
| `date_started`   | `started_at`          | Yes            |
| `date_done`      | `completed_at`        | Yes            |
| `exec_time`      | `duration`            | Yes            |
| `eta`            | `scheduled_at`        | Yes            |
| `retry`          | `attempts`            | Yes            |

The aliases support `compute`, `inverse`, and `search`, so existing code
using the old names in domain filters will work:

```python
# Both work
jobs = self.env["queue.job"].search([("date_done", ">", cutoff)])
jobs = self.env["queue.job"].search([("completed_at", ">", cutoff)])
```

### New fields

`job_worker` adds stored computed fields extracted from the payload JSON:

- `model_name` -- the target model (e.g., `"res.partner"`)
- `method_name` -- the target method (e.g., `"write"`)
- `func_string` -- formatted as `model.method(ids)` for display
- `record_ids` -- JSON list of target record IDs
- `name` -- description string (populated from the `description` parameter)

These are indexed and searchable.

## 13. Removed features

The following OCA `queue_job` features do not exist in `job_worker`:

### _patch_job_auto_delay

OCA's mechanism for monkey-patching a method to automatically delay itself
when called with a context key. This pattern is fragile and not supported.
Instead, call `with_delay()` explicitly at the call site.

### queue.job.channel (hierarchical channels)

The hierarchical channel tree with cascading limits does not exist. Channels
are flat strings. Configure limits per channel with `queue.limit` records.

### queue.job.function (function registry)

There is no whitelist of allowed job functions. Any model method can be
delayed.

### retry_pattern on functions

Per-function retry patterns configured via `queue.job.function` do not exist.
Use `RetryableJobError(seconds=N)` inside the job method, or rely on the
default exponential backoff.

### @job decorator

The `@job` decorator for marking methods as delayable does not exist and is
not needed. Any method can be delayed.

### JobSerialized field

The custom `JobSerialized` field type from OCA is not used. `job_worker` uses
standard `fields.Json` for payload and result storage.

### _job_prepare_context_before_enqueue

The hook for filtering context keys before storing jobs does not exist.
`job_worker` does not store the full Odoo context in the job payload.

## 14. Synchronous execution bypass

For testing or emergency scenarios, jobs can be forced to execute
synchronously (immediately in the current transaction, without going through
the worker).

### Environment variable

```bash
export QUEUE_JOB__NO_DELAY=1
```

### Context key

```python
record.with_context(queue_job__no_delay=True).with_delay().do_export()
```

When either is set, `delay()` still creates the job record in the database
but immediately calls `run_now()` on it.

**Note:** This is compatible with OCA's `TEST_QUEUE_JOB_NO_DELAY` pattern.
If your test harness sets `QUEUE_JOB__NO_DELAY`, it will work. If it sets a
different env var, update it.

## 15. _job_store_values hook

Both OCA and `job_worker` provide a `_job_store_values()` hook on the base
model. Override it to inject extra field values into the job record at
creation time:

```python
class MyModel(models.Model):
    _inherit = "my.model"

    def _job_store_values(self, job_vals):
        """Add custom values to queue.job records targeting this model."""
        values = super()._job_store_values(job_vals)
        values["company_id"] = self.env.company.id
        return values
```

### Difference from OCA

In OCA, `_job_store_values` receives a `Job` object. In `job_worker`, it
receives the `vals` dictionary that will be passed to `queue.job.create()`.
Adjust your override accordingly:

```python
# OCA signature
def _job_store_values(self, job):
    return {"company_id": job.company.id}

# job_worker signature
def _job_store_values(self, job_vals):
    return {"company_id": self.env.company.id}
```

## 16. Related actions

OCA's "Related Action" system uses `queue.job.function` configuration to
define which action opens when clicking the related record button on a job.

`job_worker` provides `open_related_action()` as a built-in method on
`queue.job`. It reads `model_name` and `record_ids` from the payload and
opens the appropriate form or list view. No configuration is needed.

If your module defined custom `related_action` on `queue.job.function`,
delete that configuration. The built-in action covers the standard case
(open the target records). For custom behavior, override
`open_related_action()` on `queue.job`.

## 17. Step-by-step checklist

For each module being migrated:

- [ ] **Manifest:** Change `"queue_job"` to `"job_worker"` in `depends`
- [ ] **Python imports:** Find and replace all `odoo.addons.queue_job` with
      `odoo.addons.job_worker`, adjusting sub-module paths as listed in
      [Import paths](#2-import-paths)
- [ ] **Channel XML:** Delete all `queue.job.channel` records. Create
      `queue.limit` records for channels that need concurrency > 1
- [ ] **Function XML:** Delete all `queue.job.function` records
- [ ] **State references:** Replace `"enqueued"` with `"pending"` and
      `"wait_dependencies"` with `"waiting"` in all Python and XML
- [ ] **View references:** Update any XML `ref="queue_job...."` external IDs
      that point to queue_job views or actions
- [ ] **retry_pattern:** If using per-function retry_pattern, migrate to
      `RetryableJobError(seconds=N)` inside the job method
- [ ] **Test imports:** Update test imports from `queue_job.tests.common` to
      `job_worker.tests.common` and adjust `trap_jobs()` usage
- [ ] **identity_key callables:** Verify custom identity functions work with
      `Delayable` objects (they expose `model_name`, `method_name`, `args`,
      `kwargs`, `recordset`)
- [ ] **Context keys:** Replace `TEST_QUEUE_JOB_NO_DELAY` with
      `QUEUE_JOB__NO_DELAY` if used in CI/test configuration
- [ ] **Run tests:** Install `job_worker` alongside your module and run the
      full test suite to catch any remaining references
