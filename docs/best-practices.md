# Best Practices

## Idempotency

Design job methods to be safely re-executed. Jobs may be retried after transient
failures or worker crashes, so the same method call can happen more than once.

```python
# Idempotent: checking state before acting
def sync_to_external(self):
    for record in self:
        if record.external_id:
            external_api.update(record.external_id, record._serialize())
        else:
            ext_id = external_api.create(record._serialize())
            record.external_id = ext_id
```

!!! warning "Non-idempotent patterns to avoid"
    - Sending emails without checking a "sent" flag
    - Incrementing counters without deduplication
    - Creating records without checking for duplicates

## Error Handling

### RetryableJobError

Raise `RetryableJobError` for transient failures that should be retried:

```python
from odoo.addons.job_worker.exception import RetryableJobError

def call_external_api(self):
    try:
        response = requests.post(API_URL, json=self._serialize())
        response.raise_for_status()
    except requests.ConnectionError:
        raise RetryableJobError(
            "API unreachable",
            seconds=120,        # retry in 120 seconds
            ignore_retry=True,  # don't count toward max_retries
        )
```

### FailedJobError

Use `FailedJobError` as a semantic marker for permanent/business failures:

```python
from odoo.addons.job_worker.exception import FailedJobError

def process_payment(self):
    if not self.payment_token:
        raise FailedJobError("No payment token configured")
```

Current behavior note: `QueueWorker` does not special-case `FailedJobError`; it
follows the normal non-`RetryableJobError` retry logic.

### Unhandled Exceptions

Any unhandled exception (including `FailedJobError`) causes the job to be retried
with exponential backoff
(10s, 20s, 40s, ... up to `max_backoff_seconds`). After `max_retries` attempts,
the job moves to `failed` state.

## Retries

### Exponential Backoff

The default retry schedule is exponential: `10 * 2^(attempt - 1)` seconds, capped at
`max_backoff_seconds` (default: 3600 seconds = 1 hour).

| Attempt | Delay |
|---|---|
| 1 | 10s |
| 2 | 20s |
| 3 | 40s |
| 4 | 80s |
| 5 | 160s |

### Custom Retry Delay

Override the backoff for specific errors:

```python
raise RetryableJobError("Rate limited", seconds=300)
```

### Infinite Retries

Set `max_retries=0` to retry indefinitely:

```python
record.with_delay(max_retries=0).sync_critical_data()
```

## Timeouts

Set per-job timeouts to prevent runaway jobs:

```python
# Timeout after 5 minutes
record.with_delay(timeout=300).generate_large_report()
```

When a job exceeds its timeout, the worker marks it with
`TimeoutJobError: ...` in `exc_info`, then applies the standard retry logic.

!!! info "Timeout enforcement"
    Timeouts are enforced via heartbeat monitoring in a separate thread. The heartbeat
    thread checks if the job has exceeded its timeout and marks it for retry or failure.

## Channel Design

### Separate by Resource Type

Use distinct channels for different workloads to prevent one type from starving
another:

```python
record.with_delay(channel="email").send_notification()
record.with_delay(channel="export").generate_report()
record.with_delay(channel="api_sync").push_to_external()
```

### Rate Limiting for External APIs

Use `rate_limit` to respect external API quotas:

```xml
<record id="limit_api_sync" model="queue.limit">
    <field name="name">api_sync</field>
    <field name="limit">3</field>
    <field name="rate_limit">2</field>  <!-- 2 calls/second -->
</record>
```

### Priority Levels

Use priority to ensure critical jobs run first:

```python
# Critical: run before everything else
record.with_delay(priority=1).process_payment()

# Normal: default priority
record.with_delay(priority=10).sync_inventory()

# Low: background maintenance
record.with_delay(priority=100).cleanup_temp_files()
```

## Testing with `trap_jobs`

Use the `trap_jobs` context manager to test job enqueueing without running a worker:

```python
from odoo.addons.job_worker.tests.common import trap_jobs

class TestMyModule(TransactionCase):
    def test_job_is_enqueued(self):
        with trap_jobs(self.env) as trap:
            self.partner.with_delay(channel="export").do_export()

            trap.assert_jobs_count(1)
            trap.assert_enqueued_job(
                "res.partner",    # model name
                "do_export",      # method name
            )

    def test_job_execution(self):
        with trap_jobs(self.env) as trap:
            self.partner.with_delay().update_name()
            trap.perform_enqueued_jobs()

        # Assert side effects after execution
        self.assertEqual(self.partner.name, "Updated")
```

### Available Assertions

| Method | Description |
|---|---|
| `trap.assert_jobs_count(n)` | Assert exactly `n` jobs were enqueued |
| `trap.assert_jobs_count(n, model=..., method=...)` | Assert `n` jobs matching model/method |
| `trap.assert_enqueued_job(model, method)` | Assert a job exists with the given model/method |
| `trap.perform_enqueued_jobs()` | Execute all trapped jobs synchronously |

### Multi-Transaction Testing

Use `external_env` for tests that need to observe job state across transactions:

```python
from odoo.addons.job_worker.tests.common import external_env

with external_env(self.env) as (cr, env):
    jobs = env["queue.job"].search([("state", "=", "pending")])
    cr.commit()
```

## General Guidelines

1. **Keep jobs small** — Break large operations into chunks using `split()` or manual batching
2. **Use identity keys** — Prevent duplicate jobs for the same logical operation
3. **Set appropriate `max_retries`** — Don't retry forever unless the operation is critical
4. **Use descriptive channels** — Makes monitoring and throttling straightforward
5. **Log inside jobs** — Use `_logger` for observability; tracebacks are captured automatically on failure
6. **Test with `trap_jobs`** — Verify jobs are enqueued with the correct parameters
