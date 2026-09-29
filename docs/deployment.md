# Deployment

## Worker Process

The runner is a standalone Python process that connects to your Odoo databases and
continuously pulls and executes jobs. It is the recommended way to run jobs in
production.

### Runner (recommended)

Use one of the two bundled entry points. Both start the multi-database
`QueueJobRunner` via `QueueJobRunner.from_environ_or_config()`, so runner settings
are read from the environment (see [Configuration](configuration.md)).

```bash
# Standalone launcher script
python job_worker_runner.py -c /etc/odoo/odoo.conf

# Or invoke the runner as a module
python -m odoo.addons.job_worker.cli -c /etc/odoo/odoo.conf
```

The runner:

- Discovers databases with `job_worker` installed
- Spawns a `QueueWorker` thread per database
- Monitors thread health and restarts crashed workers
- Quarantines databases with repeated failures
- Uses PostgreSQL advisory locks to prevent duplicate runners
- Writes a heartbeat file for container health checks (see below)

### Single-Database Worker (advanced)

If you need to run a single unsupervised worker for one database — for example in a
constrained container — you can instantiate `QueueWorker` directly:

```python
# run_worker.py
import odoo
from odoo.tools import config

from odoo.addons.job_worker.cli.worker import QueueWorker

config.parse_config([
    "-c", "/etc/odoo/odoo.conf",
    "-d", "production",
])

odoo.service.server.load_server_wide_modules()
registry = odoo.modules.registry.Registry(config["db_name"])

worker = QueueWorker(config["db_name"])
worker.run()
```

!!! warning "No supervision or heartbeat"
    A bare `QueueWorker` is not restarted on crash and does **not** write the
    heartbeat file the container healthcheck relies on. Prefer the runner unless you
    have a specific reason not to.

## Signals

| Signal | Behavior |
|---|---|
| `SIGTERM` | Stops taking new jobs, waits up to `join_timeout_seconds` (30s) per database — one database after another — for running jobs to finish, then exits. A job still running at that point is abandoned: its row stays `started` until its heartbeat goes stale, and is then reclaimed with an attempt counted |
| `SIGINT` | Same as `SIGTERM` |

## Docker Compose

Example `docker-compose.yml` for running the job worker alongside Odoo. This is a
generic illustration; the repository's own `docker/docker-compose.yml` is a
test harness, not a production template.

```yaml
services:
  postgres:
    image: postgres:18-alpine
    environment:
      POSTGRES_DB: odoo
      POSTGRES_USER: odoo
      POSTGRES_PASSWORD: odoo
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "odoo"]
      interval: 5s
      timeout: 5s
      retries: 5

  odoo:
    image: your-odoo-image:19.0
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "8069:8069"
    volumes:
      - ./addons:/mnt/extra-addons
    environment:
      - HOST=postgres
      - USER=odoo
      - PASSWORD=odoo

  job-worker:
    image: your-odoo-image:19.0
    depends_on:
      postgres:
        condition: service_healthy
    volumes:
      - ./addons:/mnt/extra-addons
    environment:
      - HOST=postgres
      - USER=odoo
      - PASSWORD=odoo
    command: python /mnt/extra-addons/odoo-job-worker/job_worker_runner.py -c /etc/odoo/odoo.conf
    restart: unless-stopped

volumes:
  pgdata:
```

!!! tip "Scaling workers"
    Run multiple `job-worker` containers to increase throughput. Each worker acquires
    jobs independently via `FOR UPDATE SKIP LOCKED`, so there is no double-execution
    risk.

## Health checks

The `QueueJobRunner` has no HTTP endpoint, so an orchestrator cannot probe it with
an HTTP healthcheck. Instead the runner writes a small **heartbeat file** on every
healthy iteration of its supervisor loop, and the bundled
`job_worker_healthcheck.py` script reports whether that file is fresh.

A stale heartbeat means one of:

- the runner process is gone (the file is never refreshed),
- the supervisor loop has stopped iterating (hung / deadlocked), or
- the worker fleet is degraded — the runner deliberately withholds the heartbeat
  while any database is quarantined (its worker has crashed past the failure
  threshold) **or** stuck in database-error recovery for longer than
  `database_unhealthy_after_seconds` (default 300s). Database errors no longer
  crash worker threads or arm the quarantine — the worker recovers in place — so
  this degraded signal is how a database that is unreachable or permanently
  broken surfaces. It clears on its own once a full healthy cycle completes.

Because the degraded signal only reaches an orchestrator through this script,
wire `job_worker_healthcheck.py` as the container healthcheck in every
deployment — without it a worker stuck in recovery looks healthy from outside.

The script imports only the standard library (it does **not** bootstrap Odoo), so
it is fast enough to run as a container `HEALTHCHECK`:

```yaml
  job-worker:
    image: your-odoo-image:19.0
    command: python /mnt/extra-addons/odoo-job-worker/job_worker_runner.py -c /etc/odoo/odoo.conf
    healthcheck:
      test: ["CMD-SHELL", "python /mnt/extra-addons/odoo-job-worker/job_worker_healthcheck.py || exit 1"]
      interval: 30s
      timeout: 10s
      start_period: 60s
      retries: 3
    # Docker's default is 10s, then SIGKILL — shorter than the runner's 30s
    # wait for running jobs (see Signals).
    stop_grace_period: 60s
    restart: unless-stopped
```

Both the writer and the checker resolve the same settings from the environment:

| Variable | Default | Purpose |
|---|---|---|
| `JOB_WORKER_HEARTBEAT_FILE` | `/tmp/job_worker_heartbeat` | Path of the heartbeat file |
| `JOB_WORKER_HEARTBEAT_MAX_AGE` | `60` | Seconds before the heartbeat is considered stale |

!!! note "Runner-only"
    The heartbeat is emitted by the multi-database `QueueJobRunner`. A bare
    single-database `QueueWorker` (see [Single-Database Worker](#single-database-worker-advanced))
    does not write a heartbeat file.

## Upgrading modules while the worker runs

A job worker usually runs beside Odoo (a "sidecar"), and keeps running while an
install or upgrade (`-i` / `-u`) runs in another container. It must not take
part in that upgrade. Loading an Odoo registry is not read-only: it runs every
model's `_register_hook` (some modules write data there), it resumes a
half-finished upgrade if it finds `base.partially_updated_database`, and when it
fails it calls Odoo's `reset_modules_state()`. That last one marks the upgrading
process's `to upgrade` modules `installed` in the middle of its work.

### What the worker does

For each database, the runner checks an **upgrade gate** every few seconds. It
uses one small query on its own connection, and needs no registry. The gate is
**closed** while:

| Reason in the log | Meaning |
|---|---|
| `modules-in-transition` | A module is `to install`, `to upgrade` or `to remove`. Odoo's own cron runner refuses to run under the same condition |
| `partial-update-flag` | `base.partially_updated_database` is set: a previous upgrade stopped part-way |
| `version-skew` | An installed module's version on this worker's addons path differs from the one recorded in the database. Either new code is waiting for its `-u`, or this worker was not redeployed |
| `probe-blocked` | The check itself hit its lock timeout. An upgrade holding a schema lock is the usual cause |

While the gate is closed, the worker:

- does **not** load or reload its registry;
- does **not** take new jobs;
- keeps heartbeating jobs that are already running;
- resumes by itself when the gate opens, reloading the upgraded registry at once.

The runner process also **never resets module states**. If one of its registry
loads fails, it logs that it did not reset them instead.

!!! warning "Running jobs keep running"
    The gate stops the worker from *starting* work during an upgrade. A job that
    was already running when the upgrade began keeps running, and keeps its
    locks: an upgrade's `ALTER TABLE` can queue behind it, and every query on
    that table then queues behind the `ALTER`. For a clean upgrade, stop the
    worker first (below).

### Recommended upgrade procedure

1. Stop the worker. `docker compose stop job-worker` sends SIGTERM and waits
   `stop_grace_period`.
2. Run the upgrade as a one-shot: `odoo -d <db> -u <modules> --stop-after-init --no-http`.
3. Start Odoo, then start the worker.

If your orchestration can't guarantee that order, the gate is what keeps a
still-running or restarting worker out of the upgrade's way.

### Health while paused

A pause is healthy for an ordinary upgrade window. Once it lasts longer than
`JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER` (default 3600s), the runner logs an
ERROR and stops refreshing its heartbeat, so the healthcheck reports unhealthy.
A pause that never ends means one of two things:

- an upgrade that was interrupted, leaving modules in `to upgrade`. Re-run the
  upgrade from the upgrading process;
- code deployed without its `-u`. Run it.

The worker deliberately never resets module states itself.

### Settings

| Variable | Default | Description |
|---|---|---|
| `JOB_WORKER_UPGRADE_GATE` | `1` | `0` disables the gate entirely |
| `JOB_WORKER_UPGRADE_GATE_VERSION_CHECK` | `1` | `0` skips the code-vs-database version check (the other checks stay on) |
| `JOB_WORKER_UPGRADE_GATE_INTERVAL` | `5` | Seconds between checks |
| `JOB_WORKER_UPGRADE_PAUSE_UNHEALTHY_AFTER` | `3600` | Seconds a pause may last before the worker reports unhealthy; `0` never does |

### "Transient module states were reset"

This line is logged by Odoo itself, by whichever process called
`reset_modules_state()`:

- **The web/upgrade container:** its own install or upgrade failed and it
  cleaned up after itself; or its cron threads found modules stuck in
  `to …` states for more than 5 hours.
- **Never the job worker.** It logs *"NOT resetting transient module states"*
  instead.

## systemd Service

Create a systemd unit for the worker:

```ini
# /etc/systemd/system/odoo-job-worker.service
[Unit]
Description=Odoo Job Worker
After=postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=odoo
Group=odoo
ExecStart=/usr/bin/python3 /opt/odoo/addons/odoo-job-worker/job_worker_runner.py -c /etc/odoo/odoo.conf
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable odoo-job-worker
sudo systemctl start odoo-job-worker
```

Monitor logs:

```bash
sudo journalctl -u odoo-job-worker -f
```

## Production Checklist

- [ ] Worker process is managed by a process supervisor (systemd, Docker, etc.)
- [ ] `job_worker_healthcheck.py` is the container healthcheck, and `stop_grace_period` is at least 45s
- [ ] Module upgrades stop the worker first, or rely on the upgrade gate (see [Upgrading modules while the worker runs](#upgrading-modules-while-the-worker-runs))
- [ ] `restart` policy is configured for automatic recovery
- [ ] Channel concurrency limits are set appropriately via `queue.limit` records
- [ ] Job retention period is configured (`job_worker.done_job_retention_days`)
- [ ] PostgreSQL connection pool is sized for worker concurrency
- [ ] `job_worker_monitor` is installed for dashboard and alerts
- [ ] Log aggregation is set up for worker process output
