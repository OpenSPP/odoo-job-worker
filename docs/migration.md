# Migration Guide

This guide covers migrating Odoo modules from OCA's `queue_job` to `job_worker`.

`job_worker` is a ground-up reimplementation with a SQL Pull architecture,
flat channel model, consolidated payload JSON, and heartbeat-based stale
detection. The delay API (`with_delay`, `delayable`, `group`, `chain`,
`on_done`) is compatible. Most migration work is mechanical: changing import
paths, replacing channel/function XML data, and adjusting state names.

## Manifest Dependency

In every module's `__manifest__.py`, replace the dependency:

```python
# Before
"depends": ["queue_job", "base"],

# After
"depends": ["job_worker", "base"],
```

## Import Paths

All Python imports from `odoo.addons.queue_job` must change to
`odoo.addons.job_worker`. The module-internal structure differs:

### Delay API

```python
# Before
from odoo.addons.queue_job.delay import Delayable, DelayableRecordset, group, chain

# After
from odoo.addons.job_worker.delay import Delayable, DelayableRecordset, group, chain
```

### Exceptions

```python
# Before
from odoo.addons.queue_job.exception import RetryableJobError, JobError, FailedJobError

# After
from odoo.addons.job_worker.exception import RetryableJobError, JobError, FailedJobError
```

### Identity Functions

```python
# Before
from odoo.addons.queue_job.job import identity_exact

# After
from odoo.addons.job_worker.job import identity_exact
```

### State Constants

```python
# Before
from odoo.addons.queue_job.job import PENDING, ENQUEUED, DONE, FAILED, STARTED

# After
from odoo.addons.job_worker.job import PENDING, DONE, FAILED, STARTED, WAITING, CANCELLED
```

### Test Helpers

```python
# Before
from odoo.addons.queue_job.tests.common import trap_jobs, JobMixin

# After
from odoo.addons.job_worker.tests.common import trap_jobs
```

## Channel Configuration

OCA `queue_job` uses a hierarchical channel model (`queue.job.channel`) where
channels have parent/child relationships (e.g., `root.export.heavy`) and
concurrency limits cascade down the tree.

`job_worker` uses **flat string channel names** plus an optional `queue.limit`
model for concurrency and rate control.

### Remove Channel XML Data

Delete all `queue.job.channel` records from your module's XML data files:

```xml
<!-- DELETE this -->
<record id="channel_root_export" model="queue.job.channel">
    <field name="name">export</field>
    <field name="parent_id" ref="queue_job.channel_root"/>
</record>
```

### Add `queue.limit` Records

If a channel needs concurrency or rate limiting, create a `queue.limit`
record. If no `queue.limit` record exists for a channel, the default
concurrency is **1** (only one job runs at a time per channel).

```xml
<record id="limit_export" model="queue.limit">
    <field name="name">export</field>
    <field name="limit">3</field>
    <field name="rate_limit">0</field>
</record>
```

Setting `limit` to `0` blocks the channel entirely (no jobs will be acquired).

### Flatten Hierarchical Channel Names

If your code was using hierarchical channel names like `root.export.heavy`,
decide on a flat name like `export_heavy` or just `export`. Update all
`with_delay(channel=...)` and `delayable(channel=...)` calls to use the flat
name, and create a matching `queue.limit` record.

## Function Registry (Removed)

OCA `queue_job` has a `queue.job.function` model that registers which
model/method combinations are allowed to be delayed. `job_worker` has **no
function-registry behavior** — any method on any model can be delayed.

!!! note "The model still exists"
    `queue.job.function` (and `queue.job.channel`) still exist in `job_worker`, but
    only as passive name-lookup tables that are auto-populated from each job's
    `channel`/method. They have no whitelist, no `retry_pattern`, and no hierarchy,
    and they need no configuration. You never create records in them.

### Remove Function XML Data

Delete all `queue.job.function` records from your module's XML data:

```xml
<!-- DELETE this -->
<record id="job_function_do_import" model="queue.job.function">
    <field name="model_id" ref="model_my_model"/>
    <field name="method">do_import</field>
    <field name="channel_id" ref="channel_root_export"/>
</record>
```

### Remove Function-Related Code

If your code reads from or writes to `queue.job.function`, remove those
references. Common patterns to look for:

- `self.env["queue.job.function"].search(...)`
- XML records with `model="queue.job.function"`
- References to `retry_pattern` on the function model

## State Names

The state values differ between OCA `queue_job` and `job_worker`:

| OCA queue_job | job_worker | Meaning |
|---|---|---|
| `wait_dependencies` | `waiting` | Blocked on parent job (graph dependency) |
| `pending` | `pending` | Created and ready (not blocked on dependencies) |
| `enqueued` | `pending` | Ready to be picked up by a worker |
| `started` | `started` | Currently executing |
| `done` | `done` | Completed successfully |
| `failed` | `failed` | Failed after exhausting retries |
| `cancelled` | `cancelled` | Manually cancelled |

Key changes:

- **`enqueued` does not exist** — replace with `pending`
- **`wait_dependencies` does not exist** — replace with `waiting`
- **`cancelled` has a `cancelled_at` timestamp**

Update all domain filters, state checks, and search calls:

```python
# Before
jobs = self.env["queue.job"].search([("state", "=", "enqueued")])

# After
jobs = self.env["queue.job"].search([("state", "=", "pending")])
```

## `with_delay` and `delayable` API

The core delay API is **compatible**:

```python
# Simple delay (unchanged)
record.with_delay(channel="export", priority=15).do_export()

# Explicit delayable (unchanged)
record.delayable(channel="export", eta=3600).do_export().delay()
```

### Supported Options

| Option | Supported | Notes |
|---|---|---|
| `channel` | Yes | Flat string, not hierarchical path |
| `priority` | Yes | Lower = higher priority. Default: 10 |
| `eta` | Yes | `datetime`, `timedelta`, or seconds (`int`) |
| `max_retries` | Yes | 0 = infinite. Default: 5 |
| `description` | Yes | Stored in the `name` field on the job |
| `identity_key` | Yes | String or callable |

`scheduled_at` is accepted as an alias for `eta`.

## `group`, `chain`, `on_done`

Core graph composition is compatible:

```python
from odoo.addons.job_worker.delay import group, chain

g = group(
    record1.delayable().do_work(),
    record2.delayable().do_work(),
)
g.delay()

c = chain(
    record1.delayable().step_one(),
    record2.delayable().step_two(),
)
c.delay()

main = record.delayable().do_heavy_work()
callback = record.delayable().notify_done()
main.on_done(callback)
main.delay()
```

Chain dependencies are tracked via `parent_id` and `graph_uuid` fields on the
job record. Child jobs start in `waiting` state and move to `pending` when
their parent completes.

`job_worker` also adds `on_error(*delayables)` (on `Delayable`, `DelayableGroup`,
and `DelayableChain`) for failure-path callbacks — see
[Job Graphs](job-graphs.md#error-callbacks-with-on_error). It is auto-cancelled when
the parent succeeds and runs only when the parent fails.

!!! info "Group callbacks"
    `group().on_done(callback)` creates a wait-for-all barrier — the callback
    starts in `waiting` and transitions to `pending` only after every group
    member completes. If any member fails permanently, the callback also fails.

!!! note "Chain limitation"
    `chain()` sequencing is enforced for `Delayable` steps. Putting a `group()`
    inside `chain()` does not create a wait-for-all barrier.

## `identity_key`

Works the same way — string or callable:

```python
from odoo.addons.job_worker.job import identity_exact

record.with_delay(identity_key="export_partner_42").do_export()
record.with_delay(identity_key=identity_exact).do_export()
```

**Callables receive a `Delayable` object** (not a `Job` object as in OCA).
The `Delayable` exposes: `model_name`, `method_name`, `args`, `kwargs`, and
`recordset` (with `.ids`).

## `RetryableJobError`

Same constructor signature:

```python
from odoo.addons.job_worker.exception import RetryableJobError

raise RetryableJobError("Service not ready", seconds=120, ignore_retry=True)
```

Differences from OCA:

- **No `retry_pattern` on functions** — use `RetryableJobError(seconds=N)` to
  control retry delay, or rely on exponential backoff
  (`10 * 2^(attempt-1)` seconds, capped at 3600)
- **`FailedJobError` is not special-cased** — it follows the same retry flow as
  other non-`RetryableJobError` exceptions, based on `max_retries`

## Testing with `trap_jobs`

The API differs from OCA's version:

=== "OCA (before)"

    ```python
    from odoo.addons.queue_job.tests.common import trap_jobs

    with trap_jobs() as trap:
        record.with_delay().do_export()
        trap.assert_enqueued_job(
            self.env["my.model"].do_export,  # bound method
        )
    ```

=== "job_worker (after)"

    ```python
    from odoo.addons.job_worker.tests.common import trap_jobs

    with trap_jobs(self.env) as trap:
        record.with_delay().do_export()
        trap.assert_enqueued_job(
            "my.model",       # model name string
            "do_export",      # method name string
        )
    ```

| Aspect | OCA | job_worker |
|---|---|---|
| Context manager arg | `trap_jobs()` (no args) | `trap_jobs(env)` (pass environment) |
| `assert_enqueued_job` | Bound method object | `model` and `method` strings |
| `properties` param | Dict of job properties | Use `**extra_filters` kwargs |
| Job storage | Jobs NOT written to DB | Jobs ARE written to DB |
| `JobMixin` | Mixin class for test cases | Not provided |

## Field Name Changes

`job_worker` provides computed alias fields for backward compatibility:

| OCA field name | job_worker field name | Alias provided |
|---|---|---|
| `date_created` | `create_date` | Yes |
| `date_enqueued` | `create_date` | No |
| `date_started` | `started_at` | Yes |
| `date_done` | `completed_at` | Yes |
| `date_cancelled` | `cancelled_at` | Yes |
| `exec_time` | `duration` | Yes |
| `eta` | `scheduled_at` | Yes |
| `retry` | `attempts` | Yes |

Both old and new names work in searches and reads:

```python
# Both work
jobs = self.env["queue.job"].search([("date_done", ">", cutoff)])
jobs = self.env["queue.job"].search([("completed_at", ">", cutoff)])
```

## Synchronous Execution Bypass

```bash
# Environment variable
export QUEUE_JOB__NO_DELAY=1
```

```python
# Context key
record.with_context(queue_job__no_delay=True).with_delay().do_export()
```

Compatible with OCA's `TEST_QUEUE_JOB_NO_DELAY` pattern. If your test harness
sets `QUEUE_JOB__NO_DELAY`, it will work.

## `_job_store_values` Hook

The hook signature differs:

=== "OCA"

    ```python
    def _job_store_values(self, job):
        """Receives a Job object."""
        return {"company_id": job.company.id}
    ```

=== "job_worker"

    ```python
    def _job_store_values(self, job_vals):
        """Receives the vals dict for queue.job.create()."""
        values = super()._job_store_values(job_vals)
        values["company_id"] = self.env.company.id
        return values
    ```

## Related Actions

OCA uses `queue.job.function` configuration to define related actions.
`job_worker` provides `open_related_action()` as a built-in method that reads
`model_name` and `record_ids` from the payload and opens the appropriate view.
No configuration is needed.

## Removed Features

| Feature | Replacement |
|---|---|
| `_patch_job_auto_delay` | Call `with_delay()` explicitly |
| `queue.job.channel` (hierarchical) | Flat channel strings + `queue.limit` |
| `queue.job.function` (whitelist) | Any method can be delayed |
| `retry_pattern` on functions | `RetryableJobError(seconds=N)` |
| `@job` decorator | Not needed — any method can be delayed |
| `JobSerialized` field | Standard `fields.Json` |
| `_job_prepare_context_before_enqueue` | Context is not stored in payload |

## Step-by-Step Checklist

For each module being migrated:

- [ ] **Manifest:** Change `"queue_job"` to `"job_worker"` in `depends`
- [ ] **Python imports:** Replace all `odoo.addons.queue_job` with `odoo.addons.job_worker`
- [ ] **Channel XML:** Delete all `queue.job.channel` records; create `queue.limit` records as needed
- [ ] **Function XML:** Delete all `queue.job.function` records
- [ ] **State references:** Replace `"enqueued"` with `"pending"` and `"wait_dependencies"` with `"waiting"`
- [ ] **View references:** Update any `ref="queue_job...."` external IDs
- [ ] **retry_pattern:** Migrate to `RetryableJobError(seconds=N)` inside the job method
- [ ] **Test imports:** Update to `job_worker.tests.common` and adjust `trap_jobs()` usage
- [ ] **identity_key callables:** Verify custom identity functions work with `Delayable` objects
- [ ] **Context keys:** Replace `TEST_QUEUE_JOB_NO_DELAY` with `QUEUE_JOB__NO_DELAY`
- [ ] **Run tests:** Install `job_worker` alongside your module and run the full test suite
