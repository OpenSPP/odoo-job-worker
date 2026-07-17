# Deployment

## Worker Process

The worker is a standalone Python process that connects to your Odoo database and
continuously pulls and executes jobs.

### Single-Database Worker

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

### Multi-Database Runner

For environments with multiple Odoo databases, use the `QueueJobRunner` which
auto-discovers databases and manages per-database worker threads:

```python
# run_runner.py
import odoo
from odoo.tools import config

from odoo.addons.job_worker.cli.runner import QueueJobRunner

config.parse_config(["-c", "/etc/odoo/odoo.conf"])

runner = QueueJobRunner()
runner.run()
```

The runner:

- Discovers databases with `job_worker` installed
- Spawns a `QueueWorker` thread per database
- Monitors thread health and restarts crashed workers
- Quarantines databases with repeated failures
- Uses PostgreSQL advisory locks to prevent duplicate runners

## Signals

| Signal | Behavior |
|---|---|
| `SIGTERM` | Graceful shutdown — finishes running jobs, then exits |
| `SIGINT` | Same as `SIGTERM` |

## Docker Compose

Example `docker-compose.yml` for running the job worker alongside Odoo:

```yaml
services:
  postgres:
    image: postgres:16-alpine
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
    command: >
      python -c "
      import odoo;
      from odoo.tools import config;
      config.parse_config(['-c', '/etc/odoo/odoo.conf', '-d', 'odoo']);
      odoo.service.server.load_server_wide_modules();
      odoo.modules.registry.Registry(config['db_name']);
      from odoo.addons.job_worker.cli.worker import QueueWorker;
      QueueWorker(config['db_name']).run()
      "
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
  threshold), so a persistently broken database surfaces as unhealthy rather than
  flapping after each rediscovery.

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
    restart: unless-stopped
```

Both the writer and the checker resolve the same settings from the environment:

| Variable | Default | Purpose |
|---|---|---|
| `JOB_WORKER_HEARTBEAT_FILE` | `/tmp/job_worker_heartbeat` | Path of the heartbeat file |
| `JOB_WORKER_HEARTBEAT_MAX_AGE` | `60` | Seconds before the heartbeat is considered stale |

!!! note "Runner-only"
    The heartbeat is emitted by the multi-database `QueueJobRunner`. A bare
    single-database `QueueWorker` (as shown in the example above) does not write a
    heartbeat file.

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
ExecStart=/usr/bin/python3 /opt/odoo/run_worker.py
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
- [ ] `restart` policy is configured for automatic recovery
- [ ] Channel concurrency limits are set appropriately via `queue.limit` records
- [ ] Job retention period is configured (`job_worker.done_job_retention_days`)
- [ ] PostgreSQL connection pool is sized for worker concurrency
- [ ] `job_worker_monitor` is installed for dashboard and alerts
- [ ] Log aggregation is set up for worker process output
