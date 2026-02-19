import logging
from datetime import timedelta

from odoo import api, fields, models

_logger = logging.getLogger(__name__)


class QueueJobMetric(models.Model):
    _name = "queue.job.metric"
    _description = "Queue Job Metric Snapshot"
    _order = "snapshot_at DESC"

    snapshot_at = fields.Datetime(
        string="Snapshot At",
        required=True,
        index=True,
    )
    channel = fields.Char(
        string="Channel",
        required=True,
        index=True,
    )
    period = fields.Selection(
        [
            ("5min", "5 Minutes"),
            ("1hour", "1 Hour"),
            ("1day", "1 Day"),
        ],
        string="Period",
        required=True,
        index=True,
    )
    jobs_completed = fields.Integer(string="Jobs Completed")
    jobs_failed = fields.Integer(string="Jobs Failed")
    average_duration = fields.Float(string="Average Duration (s)")
    p95_duration = fields.Float(string="P95 Duration (s)")
    queue_depth = fields.Integer(string="Queue Depth")
    active_workers = fields.Integer(string="Active Workers")

    @api.model
    def _collect_5min_snapshot(self):
        """Collect 5-minute metric snapshots from the live queue_job table.

        Called by a scheduled action every 5 minutes.
        """
        now = fields.Datetime.now()
        window_start = now - timedelta(minutes=5)

        # Flush pending ORM writes so raw SQL sees current state
        self.env["queue.job"].flush_model()

        # Completed/failed job stats per channel in the last 5 minutes
        self.env.cr.execute(
            """
            SELECT
                COALESCE(channel, 'root') AS channel,
                COUNT(*) FILTER (WHERE state = 'done') AS completed,
                COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                COALESCE(
                    AVG(duration) FILTER (WHERE state = 'done' AND duration > 0),
                    0
                ) AS average_duration,
                COALESCE(
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration)
                    FILTER (WHERE state = 'done' AND duration > 0),
                    0
                ) AS p95_duration
            FROM queue_job
            WHERE completed_at >= %s AND completed_at <= %s
            GROUP BY channel
            """,
            (window_start, now),
        )
        completed_stats = {
            row[0]: {
                "completed": row[1],
                "failed": row[2],
                "average_duration": row[3],
                "p95_duration": row[4],
            }
            for row in self.env.cr.fetchall()
        }

        # Queue depth and active workers per channel
        self.env.cr.execute(
            """
            SELECT
                COALESCE(channel, 'root') AS channel,
                COUNT(*) FILTER (WHERE state IN ('pending', 'waiting')) AS queue_depth,
                COUNT(DISTINCT worker_id)
                    FILTER (WHERE state = 'started'
                            AND heartbeat > NOW() - INTERVAL '60 seconds')
                    AS active_workers
            FROM queue_job
            WHERE state IN ('pending', 'waiting', 'started')
            GROUP BY channel
            """
        )
        live_stats = {
            row[0]: {"queue_depth": row[1], "active_workers": row[2]}
            for row in self.env.cr.fetchall()
        }

        # Merge all channels
        all_channels = set(completed_stats.keys()) | set(live_stats.keys())
        vals_list = []
        for channel in all_channels:
            cs = completed_stats.get(channel, {})
            ls = live_stats.get(channel, {})
            vals_list.append(
                {
                    "snapshot_at": now,
                    "channel": channel,
                    "period": "5min",
                    "jobs_completed": cs.get("completed", 0),
                    "jobs_failed": cs.get("failed", 0),
                    "average_duration": cs.get("average_duration", 0),
                    "p95_duration": cs.get("p95_duration", 0),
                    "queue_depth": ls.get("queue_depth", 0),
                    "active_workers": ls.get("active_workers", 0),
                }
            )

        if vals_list:
            self.create(vals_list)
            _logger.info(
                "Collected 5-min metric snapshots for %d channels", len(vals_list)
            )

    @api.model
    def _rollup_hourly(self):
        """Roll up 5-min snapshots into hourly aggregates.

        Called by a scheduled action every hour.  Deletes 5-min rows
        older than 7 days.
        """
        now = fields.Datetime.now()
        hour_start = now.replace(minute=0, second=0, microsecond=0)
        window_start = hour_start - timedelta(hours=1)

        self.env.cr.execute(
            """
            SELECT
                channel,
                SUM(jobs_completed) AS completed,
                SUM(jobs_failed) AS failed,
                CASE WHEN SUM(jobs_completed) > 0
                     THEN SUM(average_duration * jobs_completed) / SUM(jobs_completed)
                     ELSE 0 END AS average_duration,
                -- MAX of sub-period P95s is an upper-bound approximation;
                -- true percentile would require raw data.
                MAX(p95_duration) AS p95_duration,
                AVG(queue_depth) AS queue_depth,
                MAX(active_workers) AS active_workers
            FROM queue_job_metric
            WHERE period = '5min'
              AND snapshot_at >= %s AND snapshot_at < %s
            GROUP BY channel
            """,
            (window_start, hour_start),
        )
        rows = self.env.cr.fetchall()
        vals_list = []
        for row in rows:
            vals_list.append(
                {
                    "snapshot_at": hour_start,
                    "channel": row[0],
                    "period": "1hour",
                    "jobs_completed": row[1],
                    "jobs_failed": row[2],
                    "average_duration": row[3],
                    "p95_duration": row[4],
                    "queue_depth": int(row[5]),
                    "active_workers": row[6],
                }
            )
        if vals_list:
            self.create(vals_list)

        # Clean up old 5-min rows
        cutoff = now - timedelta(days=7)
        self.search([("period", "=", "5min"), ("snapshot_at", "<", cutoff)]).unlink()

    @api.model
    def _rollup_daily(self):
        """Roll up hourly snapshots into daily aggregates.

        Called by a scheduled action once per day.  Deletes hourly rows
        older than 90 days.
        """
        now = fields.Datetime.now()
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        window_start = day_start - timedelta(days=1)

        self.env.cr.execute(
            """
            SELECT
                channel,
                SUM(jobs_completed) AS completed,
                SUM(jobs_failed) AS failed,
                CASE WHEN SUM(jobs_completed) > 0
                     THEN SUM(average_duration * jobs_completed) / SUM(jobs_completed)
                     ELSE 0 END AS average_duration,
                -- MAX of sub-period P95s is an upper-bound approximation
                MAX(p95_duration) AS p95_duration,
                AVG(queue_depth) AS queue_depth,
                MAX(active_workers) AS active_workers
            FROM queue_job_metric
            WHERE period = '1hour'
              AND snapshot_at >= %s AND snapshot_at < %s
            GROUP BY channel
            """,
            (window_start, day_start),
        )
        rows = self.env.cr.fetchall()
        vals_list = []
        for row in rows:
            vals_list.append(
                {
                    "snapshot_at": day_start,
                    "channel": row[0],
                    "period": "1day",
                    "jobs_completed": row[1],
                    "jobs_failed": row[2],
                    "average_duration": row[3],
                    "p95_duration": row[4],
                    "queue_depth": int(row[5]),
                    "active_workers": row[6],
                }
            )
        if vals_list:
            self.create(vals_list)

        # Clean up old hourly rows
        cutoff = now - timedelta(days=90)
        self.search([("period", "=", "1hour"), ("snapshot_at", "<", cutoff)]).unlink()
