import ast
from datetime import timedelta

from odoo import api, fields, models


class QueueJobDashboard(models.TransientModel):
    _name = "queue.job.dashboard"
    _description = "Queue Job Dashboard KPI"

    name = fields.Char(string="KPI Name")
    value = fields.Char(string="Value")
    value_number = fields.Float(string="Numeric Value")
    color = fields.Integer(string="Color")
    severity = fields.Selection(
        [
            ("success", "Success"),
            ("warning", "Warning"),
            ("danger", "Danger"),
            ("info", "Info"),
        ],
        string="Severity",
    )
    subtitle = fields.Char(string="Subtitle")
    group_label = fields.Char(string="Group")
    action_domain = fields.Char(string="Action Domain")
    action_context = fields.Char(string="Action Context")
    sequence = fields.Integer(string="Sequence")

    # ------------------------------------------------------------------
    # SQL query helpers
    # ------------------------------------------------------------------
    def _query_queue_depth(self):
        """Count pending + waiting jobs."""
        self.env.cr.execute(
            """
            SELECT COUNT(*)
            FROM queue_job
            WHERE state IN ('pending', 'waiting')
            """
        )
        return self.env.cr.fetchone()[0]

    def _query_active_workers(self):
        """Count distinct workers with fresh heartbeat on started jobs."""
        self.env.cr.execute(
            """
            SELECT COUNT(DISTINCT worker_id)
            FROM queue_job
            WHERE state = 'started'
              AND heartbeat > NOW() - INTERVAL '60 seconds'
            """
        )
        return self.env.cr.fetchone()[0]

    def _query_failed_jobs(self):
        """Count outstanding failed jobs."""
        self.env.cr.execute(
            """
            SELECT COUNT(*)
            FROM queue_job
            WHERE state = 'failed'
            """
        )
        return self.env.cr.fetchone()[0]

    def _query_failure_rate(self, since):
        """Return (failed_count, total_count) for jobs completed since `since`."""
        self.env.cr.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE state = 'failed') AS failed,
                COUNT(*) FILTER (WHERE state IN ('done', 'failed')) AS total
            FROM queue_job
            WHERE completed_at >= %s
            """,
            (since,),
        )
        row = self.env.cr.fetchone()
        return row[0], row[1]

    def _query_throughput(self, since):
        """Return (count, average_duration) for done jobs since `since`."""
        self.env.cr.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(AVG(duration) FILTER (WHERE duration > 0), 0)
            FROM queue_job
            WHERE state = 'done'
              AND completed_at >= %s
            """,
            (since,),
        )
        row = self.env.cr.fetchone()
        return row[0], row[1]

    def _query_p95_duration(self, since):
        """Return the 95th percentile duration for done jobs since `since`."""
        self.env.cr.execute(
            """
            SELECT COALESCE(
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration), 0
            )
            FROM queue_job
            WHERE state = 'done'
              AND completed_at >= %s
              AND duration > 0
            """,
            (since,),
        )
        return self.env.cr.fetchone()[0]

    def _query_stuck_jobs(self):
        """Count started jobs with heartbeat >5 minutes stale."""
        self.env.cr.execute(
            """
            SELECT COUNT(*)
            FROM queue_job
            WHERE state = 'started'
              AND heartbeat < NOW() - INTERVAL '5 minutes'
            """
        )
        return self.env.cr.fetchone()[0]

    def _query_aged_pending(self):
        """Count pending/waiting jobs created >1 hour ago."""
        self.env.cr.execute(
            """
            SELECT COUNT(*)
            FROM queue_job
            WHERE state IN ('pending', 'waiting')
              AND create_date < NOW() - INTERVAL '1 hour'
            """
        )
        return self.env.cr.fetchone()[0]

    def _query_retry_exhausted(self):
        """Count failed jobs with attempts >= max_retries."""
        self.env.cr.execute(
            """
            SELECT COUNT(*)
            FROM queue_job
            WHERE state = 'failed'
              AND attempts >= max_retries
            """
        )
        return self.env.cr.fetchone()[0]

    def _query_top_failure(self):
        """Return (exc_name, count) for the most common exception among failed jobs."""
        self.env.cr.execute(
            """
            SELECT exc_name, COUNT(*) AS count
            FROM queue_job
            WHERE state = 'failed'
              AND exc_name IS NOT NULL
              AND exc_name != ''
            GROUP BY exc_name
            ORDER BY count DESC
            LIMIT 1
            """
        )
        row = self.env.cr.fetchone()
        if row:
            return row[0], row[1]
        return "", 0

    # ------------------------------------------------------------------
    # KPI computation
    # ------------------------------------------------------------------
    @api.model
    def _compute_kpis(self):
        """Compute dashboard KPI cards and return as a list of dicts."""
        # Flush ORM cache so raw SQL queries see pending writes
        self.env.flush_all()
        now = fields.Datetime.now()
        one_hour_ago = now - timedelta(hours=1)

        # System Health group
        queue_depth = self._query_queue_depth()
        active_workers = self._query_active_workers()
        failed_jobs = self._query_failed_jobs()

        # Performance group
        failed_count, total_count = self._query_failure_rate(one_hour_ago)
        failure_rate = (failed_count / total_count * 100) if total_count > 0 else 0.0
        throughput, average_duration = self._query_throughput(one_hour_ago)
        p95_duration = self._query_p95_duration(one_hour_ago)

        # Needs Attention group
        stuck_jobs = self._query_stuck_jobs()
        aged_pending = self._query_aged_pending()
        retry_exhausted = self._query_retry_exhausted()
        top_failure_name, top_failure_count = self._query_top_failure()

        return [
            # --- System Health (seq 1-3) ---
            {
                "name": "Queue Depth",
                "value": str(queue_depth),
                "value_number": queue_depth,
                "severity": (
                    "danger"
                    if queue_depth > 200
                    else ("warning" if queue_depth > 50 else "success")
                ),
                "subtitle": "pending + waiting",
                "group_label": "System Health",
                "action_domain": "[('state', 'in', ['pending', 'waiting'])]",
                "sequence": 1,
            },
            {
                "name": "Active Workers",
                "value": str(active_workers),
                "value_number": active_workers,
                "severity": "success" if active_workers > 0 else "danger",
                "subtitle": "with fresh heartbeat",
                "group_label": "System Health",
                "action_domain": "[('state', '=', 'started')]",
                "sequence": 2,
            },
            {
                "name": "Failed Jobs",
                "value": str(failed_jobs),
                "value_number": failed_jobs,
                "severity": (
                    "danger"
                    if failed_jobs > 5
                    else ("warning" if failed_jobs > 0 else "success")
                ),
                "subtitle": "needs manual action" if failed_jobs > 0 else "",
                "group_label": "System Health",
                "action_domain": "[('state', '=', 'failed')]",
                "sequence": 3,
            },
            # --- Performance (seq 4-6) ---
            {
                "name": "Failure Rate (1h)",
                "value": f"{failure_rate:.1f}%",
                "value_number": failure_rate,
                "severity": (
                    "danger"
                    if failure_rate > 10
                    else ("warning" if failure_rate > 2 else "success")
                ),
                "subtitle": (
                    f"{failed_count} of {total_count} jobs"
                    if total_count > 0
                    else "no completed jobs"
                ),
                "group_label": "Performance",
                "action_domain": "[('state', '=', 'failed')]",
                "sequence": 4,
            },
            {
                "name": "Throughput (1h)",
                "value": f"{throughput} jobs",
                "value_number": throughput,
                "severity": "info",
                "subtitle": f"avg {average_duration:.1f}s",
                "group_label": "Performance",
                "action_domain": "[('state', '=', 'done')]",
                "sequence": 5,
            },
            {
                "name": "P95 Duration (1h)",
                "value": f"{p95_duration:.1f}s",
                "value_number": p95_duration,
                "severity": (
                    "danger"
                    if p95_duration > 120
                    else ("warning" if p95_duration > 30 else "success")
                ),
                "subtitle": "95th percentile",
                "group_label": "Performance",
                "action_domain": "[('state', '=', 'done')]",
                "sequence": 6,
            },
            # --- Needs Attention (seq 7-10) ---
            {
                "name": "Stuck Jobs",
                "value": str(stuck_jobs),
                "value_number": stuck_jobs,
                "severity": "danger" if stuck_jobs > 0 else "success",
                "subtitle": ("heartbeat >5min stale" if stuck_jobs > 0 else ""),
                "group_label": "Needs Attention",
                "action_domain": (
                    f"[('state', '=', 'started'),"
                    f" ('heartbeat', '<',"
                    f" '{fields.Datetime.to_string(now - timedelta(minutes=5))}')]"
                ),
                "sequence": 7,
            },
            {
                "name": "Aged Pending",
                "value": str(aged_pending),
                "value_number": aged_pending,
                "severity": (
                    "danger"
                    if aged_pending > 10
                    else ("warning" if aged_pending > 0 else "success")
                ),
                "subtitle": "created >1h ago" if aged_pending > 0 else "",
                "group_label": "Needs Attention",
                "action_domain": (
                    f"[('state', 'in', ['pending', 'waiting']),"
                    f" ('create_date', '<',"
                    f" '{fields.Datetime.to_string(now - timedelta(hours=1))}')]"
                ),
                "sequence": 8,
            },
            {
                "name": "Retry Exhausted",
                "value": str(retry_exhausted),
                "value_number": retry_exhausted,
                "severity": "danger" if retry_exhausted > 0 else "success",
                "subtitle": ("needs manual action" if retry_exhausted > 0 else ""),
                "group_label": "Needs Attention",
                "action_domain": "[('is_retry_exhausted', '=', True)]",
                "sequence": 9,
            },
            {
                "name": "Top Failure",
                "value": top_failure_name if top_failure_name else "None",
                "value_number": top_failure_count,
                "severity": "danger" if top_failure_count > 0 else "success",
                "subtitle": (
                    f"{top_failure_count} occurrences" if top_failure_count > 0 else ""
                ),
                "group_label": "Needs Attention",
                "action_domain": (
                    f"[('state', '=', 'failed'),"
                    f" ('exc_name', '=', '{top_failure_name}')]"
                    if top_failure_name
                    else "[('state', '=', 'failed')]"
                ),
                "sequence": 10,
            },
        ]

    def _refresh_kpis(self):
        """Delete stale KPI records and create fresh ones."""
        self.sudo().search([]).unlink()
        kpis = self._compute_kpis()
        for kpi in kpis:
            self.create(kpi)

    def action_open_jobs(self):
        """Open queue.job list filtered by this card's domain."""
        self.ensure_one()
        domain = ast.literal_eval(self.action_domain or "[]")
        action = {
            "type": "ir.actions.act_window",
            "name": self.name,
            "res_model": "queue.job",
            "view_mode": "list,form",
            "domain": domain,
            "target": "current",
        }
        if self.action_context:
            action["context"] = ast.literal_eval(self.action_context)
        return action

    @api.model
    def web_search_read(
        self,
        domain=None,
        specification=None,
        offset=0,
        limit=None,
        order=None,
        count_limit=None,
    ):
        """Override to auto-populate KPIs on kanban/list load."""
        self._refresh_kpis()
        return super().web_search_read(
            domain=domain,
            specification=specification,
            offset=offset,
            limit=limit,
            order=order,
            count_limit=count_limit,
        )

    @api.model
    def web_read_group(self, **kwargs):
        """Override to auto-populate KPIs when kanban is grouped."""
        self._refresh_kpis()
        return super().web_read_group(**kwargs)
