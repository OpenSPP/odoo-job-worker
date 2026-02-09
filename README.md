# Job Worker

High-performance, SQL-driven background job engine for Odoo.

> **Pre-Release Software** — This project is under active development and has not yet undergone the level of testing and review required for production use. APIs, field names, and behaviors may change without notice.

**[Documentation](https://openspp.github.io/odoo-job-worker)** | **[Migration Guide](https://openspp.github.io/odoo-job-worker/migration/)**

## Description

This repository provides three Odoo modules for background job processing:

- **job_worker** — Core queue engine with a persistent `queue.job` model, SQL pull worker (`FOR UPDATE SKIP LOCKED`) with PostgreSQL `LISTEN/NOTIFY` wakeups, channel-level concurrency and rate limiting, and developer APIs compatible with `with_delay()` / `delayable()` patterns.
- **job_worker_demo** — Interactive demo companion for exploring the queue system.
- **job_worker_monitor** — Dashboard, metrics, and alerting for queue operations.

## Requirements

- Odoo 19.0
- PostgreSQL 12+ (including PostgreSQL 18)

**PostgreSQL 18 Note:** Version 18 has stricter serialization checks under `REPEATABLE READ` isolation. This module uses `READ COMMITTED` isolation for heartbeat and status updates to avoid serialization failures while maintaining correct concurrent behavior.

## Installation

1. Place the `job_worker`, `job_worker_demo`, and `job_worker_monitor` directories in your Odoo addons path.
2. Update the apps list and install `Job Worker`.

```bash
odoo -d <db_name> \
  --addons-path=/path/to/odoo/addons,/path/to/job-worker-modules \
  -i job_worker \
  --stop-after-init
```

## Configuration

### Start a Worker Process

The worker class is `odoo.addons.job_worker.cli.worker.QueueWorker`.

Basic launcher (recommended as a dedicated process/service):

```python
# run_worker.py
import odoo
from odoo.tools import config

from odoo.addons.job_worker.cli.worker import QueueWorker

config.parse_config([
    "-c", "/etc/odoo/odoo.conf",
    "-d", "<db_name>",
])

odoo.service.server.load_server_wide_modules()
registry = odoo.modules.registry.Registry(config["db_name"])

worker = QueueWorker(config["db_name"])
worker.run()
```

Then run:

```bash
python run_worker.py
```

Operational notes:
- Worker listens on channel `queue_job_wake_up`.
- Jobs are recovered if stale (`started` + old/missing heartbeat).
- Retry backoff is exponential: 10s, 20s, 40s, ... until `max_retries`.

### Channel Throttling (`queue.limit`)

Configure per-channel limits using model `queue.limit`:
- `limit`: max concurrent jobs.
- `rate_limit`: jobs/second (`0` means unlimited).

Configure in the UI under the Queue menus provided by the module.

### Security Roles

- **Queue Job User**: view and operate their own jobs.
- **Queue Job Manager**: includes user rights, can view all jobs, and can configure channels.

## Usage

### Direct API

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

### `with_delay()` shortcut

```python
job = partner.with_delay(priority=5, channel="exports").write({"name": "Queued"})
```

### `delayable()` explicit scheduling

```python
delayable = partner.delayable(priority=5, channel="exports").write({"name": "Queued"})
job = delayable.delay()
```

`eta` and `scheduled_at` are supported aliases for scheduling. If both are provided, they must match.

### Job Identity and Deduplication

Use `identity_key` to collapse duplicate active jobs:

```python
job = env["queue.job"].enqueue(
    model_name="res.partner",
    method_name="write",
    record_ids=[partner.id],
    args=[{"name": "Queued once"}],
    kwargs={},
    identity_key=f"partner:{partner.id}:write_name",
)
```

If a job with the same `identity_key` already exists in `waiting`, `pending`, or `started`, the existing job is returned.

### Job Execution Context

Each job stores:
- `user_id`: execution user.
- `company_id`: execution company.

Worker execution builds context from job metadata (user/company/lang/tz), then executes method calls in that context.

### Operations and Troubleshooting

- Requeue failed jobs from the UI (`button_requeue`) to reset attempts/error and wake workers.
- Use list bulk actions on `Queue Jobs > Jobs` for:
  - Requeue selected jobs
  - Set selected jobs to done
  - Set selected jobs to failed
- Inspect fields: `state`, `attempts`, `max_retries`, `exc_info`, `heartbeat`, `worker_id`.
- If jobs are not running:
  1. Confirm worker process is running.
  2. Confirm `scheduled_at` is not in the future.
  3. Check channel limits (`queue.limit`).
  4. Check PostgreSQL connectivity and logs.

## Development and Tests

Run the test suite using Docker:

```bash
bash docker/run_tests.sh
```

## Bug Tracker

Bugs are tracked on [GitHub Issues](https://github.com/OpenSPP/odoo-job-worker/issues).

## Credits

### Authors

- [OpenSPP](https://openspp.org)

### License

This project is licensed under the [LGPL-3.0](LICENSE).
