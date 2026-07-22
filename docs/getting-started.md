# Getting Started

## Requirements

- **Odoo 19.0**
- **PostgreSQL 12+** (including PostgreSQL 18)
- Python 3.11+

## Installation

Place the `job_worker` directory (and optionally `job_worker_monitor`, `job_worker_demo`)
in your Odoo addons path, then install:

```bash
odoo -d <db_name> \
  --addons-path=/path/to/odoo/addons,/path/to/job-worker-modules \
  -i job_worker \
  --stop-after-init
```

Or install via the Odoo UI: **Apps > Search "Job Worker" > Install**.

## Start a Worker

The runner is a standalone process that discovers your databases, pulls jobs, and
executes them. Start it with the bundled launcher:

```bash
python job_worker_runner.py -c /etc/odoo/odoo.conf
```

Or invoke it as a module:

```bash
python -m odoo.addons.job_worker.cli -c /etc/odoo/odoo.conf
```

Both entry points start the multi-database `QueueJobRunner`, which supervises a
worker per database with `job_worker` installed.

See [Deployment](deployment.md) for production setup with Docker, systemd, and the
container healthcheck.

## Enqueue Your First Job

In any Odoo server action, cron, or Python shell:

### Using `with_delay()`

```python
partner = env["res.partner"].browse(1)
job = partner.with_delay(priority=5, channel="root").write({"name": "Hello from the queue!"})
```

The method call is captured and stored as a job record. The worker picks it up and
executes `partner.write({"name": "Hello from the queue!"})` in the background.

### Using `delayable()`

```python
partner = env["res.partner"].browse(1)
delayable = partner.delayable(priority=5, channel="root")
delayable.write({"name": "Hello from the queue!"})
job = delayable.delay()
```

The `delayable()` form is useful when you need to compose jobs into
[graphs](job-graphs.md) before enqueuing.

### Using the Direct API

```python
job = env["queue.job"].enqueue(
    model_name="res.partner",
    method_name="write",
    record_ids=[1],
    args=[{"name": "Hello from the queue!"}],
    kwargs={},
    channel="root",
    priority=5,
)
```

## Monitor Jobs

Navigate to **Queue Jobs > Jobs** in the Odoo menu to see job states, errors, and
execution details. Install `job_worker_monitor` for a dashboard with metrics and
alerts.

## Next Steps

- [Enqueueing Jobs](usage.md) — Full API reference for `with_delay()`, `delayable()`, scheduling, and deduplication
- [Job Graphs](job-graphs.md) — Compose parallel groups and sequential chains
- [Configuration](configuration.md) — Channel throttling, worker parameters, and Odoo settings
- [Deployment](deployment.md) — Production setup guides
