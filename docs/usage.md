# Enqueueing Jobs

## `with_delay()`

The simplest way to enqueue a job. Captures the method call and enqueues it
immediately:

```python
partner.with_delay(priority=5, channel="exports").write({"name": "Queued"})
```

Returns the created `queue.job` record.

### Supported Options

| Option | Type | Default | Description |
|---|---|---|---|
| `channel` | `str` | `"root"` | Channel name for throttling |
| `priority` | `int` | `10` | Lower number = higher priority |
| `eta` | `datetime`, `timedelta`, or `int` | `None` | Earliest execution time (int = seconds from now) |
| `max_retries` | `int` | `5` | Maximum retry attempts (0 = infinite) |
| `description` | `str` | `None` | Human-readable description (stored in `name` field) |
| `identity_key` | `str` or callable | `None` | Deduplication key |
| `timeout` | `int` | `0` | Per-job timeout in seconds (0 = no timeout) |

## `delayable()`

Explicit form that separates method capture from enqueuing. Required for building
[job graphs](job-graphs.md):

```python
delayable = partner.delayable(priority=5, channel="exports")
delayable.write({"name": "Queued"})
job = delayable.delay()
```

Accepts the same options as `with_delay()`.

!!! note "`scheduled_at` alias"
    `scheduled_at` is accepted as an alias for `eta`. Both set the earliest execution
    time. Do not pass conflicting values for both.

## Direct API

The `enqueue()` model method on `queue.job` provides full control:

```python
job = env["queue.job"].enqueue(
    model_name="res.partner",
    method_name="write",
    record_ids=[partner.id],
    args=[{"name": "Updated by queue"}],
    kwargs={},
    channel="root",
    priority=10,
    max_retries=5,
)
```

## Scheduling

Use `eta` (or `scheduled_at`) to delay execution:

```python
from datetime import timedelta

# Execute after 1 hour
partner.with_delay(eta=timedelta(hours=1)).send_reminder()

# Execute at a specific time
from datetime import datetime
partner.with_delay(eta=datetime(2025, 6, 1, 9, 0)).send_reminder()

# Execute after 300 seconds
partner.with_delay(eta=300).send_reminder()
```

Jobs with a future `scheduled_at` remain in `pending` state but are invisible to
workers until the scheduled time arrives.

## Job Identity and Deduplication

Use `identity_key` to prevent duplicate jobs. If a job with the same `identity_key`
already exists in `pending`, `waiting`, or `started` state, the existing job is
returned instead of creating a new one.

### String Key

```python
partner.with_delay(
    identity_key=f"partner:{partner.id}:sync"
).sync_to_external_system()
```

### Callable Key

Pass a function that receives a `Delayable` and returns a string:

```python
from odoo.addons.job_worker.job import identity_exact

partner.with_delay(identity_key=identity_exact).sync_to_external_system()
```

`identity_exact` hashes `(model_name, method_name, record_ids, args, kwargs)` into a
SHA-1 digest. This means two calls with identical arguments produce the same key.

!!! info "Enforcement"
    Deduplication is enforced by a partial unique index on `identity_key` for active
    job states (`waiting`, `pending`, `started`). Once a job completes, fails, or is
    cancelled, a new job with the same key can be created.

### Custom Identity Functions

Custom identity functions receive a `Delayable` object with these attributes:

- `model_name` — Target model name (e.g., `"res.partner"`)
- `method_name` — Target method name (e.g., `"write"`)
- `args` — Positional arguments
- `kwargs` — Keyword arguments
- `recordset` — The target recordset (access `.ids` for record IDs)

```python
def identity_by_partner(delayable):
    return f"sync:{delayable.recordset.ids[0]}"

partner.with_delay(identity_key=identity_by_partner).sync_to_external_system()
```

## Splitting Recordsets

Use `split()` to break a large recordset into chunked jobs:

```python
delayable = records.delayable(channel="export")
delayable.do_export()
chunked_group = delayable.split(100)  # 100 records per chunk
chunked_group.delay()
```

By default `split()` returns a `DelayableGroup` (parallel execution). Pass
`chain=True` to get a `DelayableChain` (sequential execution):

```python
chunked_chain = delayable.split(100, chain=True)
chunked_chain.delay()
```

## Job Execution Context

Each job stores the execution context at enqueue time:

- **`user_id`** — The user who enqueued the job
- **`company_id`** — The active company at enqueue time

The worker reconstructs this context before executing the job method, so the method
runs with the same user, company, language, and timezone as the original caller.

## Synchronous Execution

For testing or emergency scenarios, force jobs to execute immediately (in the current
transaction) instead of going through the worker:

### Environment Variable

```bash
export QUEUE_JOB__NO_DELAY=1
```

### Context Key

```python
record.with_context(queue_job__no_delay=True).with_delay().do_work()
```

When either is set, `delay()` still creates the job record in the database but
immediately calls `run_now()` on it.

## Customizing Job Records

Override `_job_store_values()` on your model to inject custom field values into
job records at creation time:

```python
class MyModel(models.Model):
    _inherit = "my.model"

    def _job_store_values(self, job_vals):
        values = super()._job_store_values(job_vals)
        values["company_id"] = self.env.company.id
        return values
```

The hook receives the `vals` dictionary that will be passed to `queue.job.create()`.
