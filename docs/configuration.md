# Configuration

## Worker Parameters

The `QueueWorker` class accepts these parameters:

| Parameter | Default | Description |
|---|---|---|
| `db_name` | (required) | Database name to process jobs for |
| `concurrency` | `2` | Number of concurrent job execution threads |
| `poll_timeout` | `5` | Seconds to wait for `NOTIFY` before polling |
| `stale_after_seconds` | `60` | Jobs without a heartbeat for this long are considered stale and eligible for recovery |
| `heartbeat_interval_seconds` | `15` | How often the worker sends heartbeats for running jobs |
| `max_backoff_seconds` | `3600` | Maximum retry delay (exponential backoff caps here) |

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `QUEUE_JOB_CONCURRENCY` | `2` | Worker thread pool size when using `QueueJobRunner.from_environ_or_config()` |
| `QUEUE_JOB__NO_DELAY` | (unset) | Set to `1` to force synchronous job execution |

## Runner Parameters

The `QueueJobRunner` supervises workers across multiple databases.

| Parameter | Default | Description |
|---|---|---|
| `database_names` | (auto-discover) | List of databases to process; `None` means auto-discover |
| `discovery_interval_seconds` | `300` | Seconds between database discovery cycles |
| `maximum_consecutive_failures` | `5` | Failures within the window before a database is quarantined |
| `failure_window_seconds` | `300` | Time window used for failure counting |
| `join_timeout_seconds` | `30` | Seconds to wait for a worker thread to stop |

The runner uses PostgreSQL advisory locks to prevent duplicate runners on the same
databases.

## Channel Throttling

Channels are flat string tags assigned to jobs (e.g., `"root"`, `"exports"`,
`"email"`). Configure per-channel limits using the `queue.limit` model.

### Via the UI

Navigate to **Queue Jobs > Configuration > Channel Limits** to create/edit channel
limits. If `job_worker_monitor` is installed, you can also use **Queue Jobs >
Channels**.

### Via XML Data

```xml
<record id="limit_exports" model="queue.limit">
    <field name="name">exports</field>
    <field name="limit">5</field>
    <field name="rate_limit">2</field>
</record>
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `Char` | (required) | Channel name — must match the `channel` string used in jobs |
| `limit` | `Integer` | `1` | Maximum concurrent jobs in this channel |
| `rate_limit` | `Integer` | `0` | Maximum jobs started per second (`0` = unlimited) |

!!! info "Default concurrency"
    If no `queue.limit` record exists for a channel, the default concurrency is **1**
    (one job at a time). Set `limit` to `0` to block the channel entirely.

### Examples

```xml
<!-- High-throughput channel: 10 concurrent, no rate limit -->
<record id="limit_bulk" model="queue.limit">
    <field name="name">bulk</field>
    <field name="limit">10</field>
    <field name="rate_limit">0</field>
</record>

<!-- Rate-limited API channel: 3 concurrent, 1 per second -->
<record id="limit_api" model="queue.limit">
    <field name="name">api</field>
    <field name="limit">3</field>
    <field name="rate_limit">1</field>
</record>

<!-- Paused channel: no jobs will run -->
<record id="limit_paused" model="queue.limit">
    <field name="name">maintenance</field>
    <field name="limit">0</field>
</record>
```

## Odoo Configuration

### System Parameters

| Key | Default | Description |
|---|---|---|
| `job_worker.done_job_retention_days` | `30` | Days to keep completed/failed/cancelled jobs before auto-deletion |

Set via **Settings > Technical > Parameters > System Parameters** or:

```python
env["ir.config_parameter"].set_param("job_worker.done_job_retention_days", "90")
```

### odoo.conf

No special `odoo.conf` settings are required. The worker reads the standard Odoo
configuration for database connection parameters.

Example minimal `odoo.conf` for a worker:

```ini
[options]
db_host = localhost
db_port = 5432
db_user = odoo
db_password = odoo
db_name = production
addons_path = /opt/odoo/addons,/opt/extra-addons
```

## Security Roles

Two security groups control access to queue functionality:

| Role | Permissions |
|---|---|
| **Queue Job User** | View and operate own jobs |
| **Queue Job Manager** | All user permissions + view all jobs + create/edit `queue.limit` configuration |

Assign roles via **Settings > Users > [User] > Other > Queue Jobs**.
