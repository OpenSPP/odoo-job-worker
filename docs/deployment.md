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
