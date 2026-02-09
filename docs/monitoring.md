# Monitoring

The `job_worker_monitor` module extends the core `job_worker` with a dashboard,
metrics collection, and configurable alert rules.

## Installation

```bash
odoo -d <db_name> \
  --addons-path=/path/to/odoo/addons,/path/to/job-worker-modules \
  -i job_worker_monitor \
  --stop-after-init
```

`job_worker_monitor` depends on `job_worker` and `mail` (for alert notifications).

## Dashboard

Navigate to **Queue Jobs > Dashboard** for a real-time overview of your queue:

- **System health** — queue depth, active workers, failed jobs
- **Performance** — failure rate (1h), throughput (1h), P95 duration (1h)
- **Needs attention** — stuck jobs, aged pending, retry exhausted, top failure type
- **Drill-down actions** — each KPI opens a filtered `queue.job` list

## Metrics

The module collects metrics via a scheduled cron job (`queue.job.metric`). Metrics
include:

| Metric | Description |
|---|---|
| Jobs Completed | Count of `done` jobs in the snapshot window |
| Jobs Failed | Count of `failed` jobs in the snapshot window |
| Average Duration | Average duration of completed jobs |
| P95 Duration | 95th percentile duration of completed jobs |
| Queue Depth | Current `pending` + `waiting` jobs |
| Active Workers | Distinct workers with fresh heartbeats |

### Duration Buckets (Job View)

Jobs are categorized by execution time using `queue.job.duration_bucket`:

| Bucket | Duration |
|---|---|
| `instant` | < 1 second |
| `fast` | 1–10 seconds |
| `moderate` | 10–60 seconds |
| `slow` | 1–5 minutes |
| `very_slow` | > 5 minutes |

## Alert Rules

Configure alert rules at **Queue Jobs > Alert Rules** to get notified about queue
problems:

- **Failed job threshold** — Alert when failed job count exceeds a limit
- **Queue depth threshold** — Alert when pending jobs exceed a limit
- **Stale job detection** — Alert when started jobs have old heartbeats
- **P95 duration threshold** — Alert when tail latency degrades
- **Average wait time threshold** — Alert when jobs wait too long before start

Alerts are delivered via the Odoo `mail` module (email notifications, Odoo inbox).

## Operational Tips

### Investigating Failed Jobs

1. Navigate to **Queue Jobs > Jobs** and filter by `State = Failed`
2. Open a failed job to see the `Exception Info` field with the full traceback
3. Use **Requeue** to retry the job (resets attempts and clears the error)
4. Use **Open Related Record** to jump to the target record

### Bulk Operations

The job list view supports bulk actions:

- **Requeue Selected** — Reset selected jobs to `pending`
- **Set to Done** — Mark selected jobs as completed
- **Set to Failed** — Mark selected jobs as failed

### Troubleshooting

If jobs are not running:

1. **Confirm the worker process is running** — Check systemd/Docker logs
2. **Check `scheduled_at`** — Jobs with a future `scheduled_at` wait until that time
3. **Check channel limits** — Verify `queue.limit` records allow sufficient concurrency
4. **Check PostgreSQL connectivity** — Worker needs a direct database connection
5. **Inspect heartbeats** — Stale heartbeats indicate worker crashes; jobs will be auto-recovered
