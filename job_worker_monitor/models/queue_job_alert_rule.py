import logging
from datetime import timedelta

from odoo import api, fields, models

_logger = logging.getLogger(__name__)

METRIC_SELECTIONS = [
    ("failed_count", "Failed Job Count"),
    ("queue_depth", "Queue Depth"),
    ("stale_workers", "Stale Workers"),
    ("p95_duration", "P95 Duration (s)"),
    ("average_wait_time", "Average Wait Time (s)"),
]

OPERATOR_SELECTIONS = [
    (">", "Greater Than"),
    (">=", "Greater or Equal"),
    ("<", "Less Than"),
    ("<=", "Less or Equal"),
]


class QueueJobAlertRule(models.Model):
    _name = "queue.job.alert.rule"
    _description = "Queue Job Alert Rule"
    _inherit = ["mail.thread"]

    name = fields.Char(string="Rule Name", required=True)
    active = fields.Boolean(string="Active", default=True)
    metric = fields.Selection(
        METRIC_SELECTIONS,
        string="Metric",
        required=True,
    )
    channel = fields.Char(
        string="Channel",
        help="Optional channel filter. Leave blank for all channels.",
    )
    operator = fields.Selection(
        OPERATOR_SELECTIONS,
        string="Operator",
        required=True,
        default=">",
    )
    threshold = fields.Float(string="Threshold", required=True)
    evaluation_window_minutes = fields.Integer(
        string="Evaluation Window (min)",
        default=5,
    )
    cooldown_minutes = fields.Integer(
        string="Cooldown (min)",
        default=60,
    )
    notification_user_ids = fields.Many2many(
        "res.users",
        string="Notify Users",
    )
    last_triggered_at = fields.Datetime(
        string="Last Triggered",
        readonly=True,
    )
    last_value = fields.Float(
        string="Last Measured Value",
        readonly=True,
    )

    def _evaluate_metric(self):
        """Compute the current value of this rule's metric."""
        self.ensure_one()
        window_start = fields.Datetime.now() - timedelta(
            minutes=self.evaluation_window_minutes
        )
        channel_filter = ""
        params = [window_start]
        if self.channel:
            channel_filter = "AND channel = %s"
            params.append(self.channel)

        if self.metric == "failed_count":
            query = f"""
                SELECT COUNT(*)
                FROM queue_job
                WHERE state = 'failed'
                  AND completed_at >= %s
                  {channel_filter}
            """
        elif self.metric == "queue_depth":
            query = f"""
                SELECT COUNT(*)
                FROM queue_job
                WHERE state IN ('pending', 'waiting')
                  {channel_filter}
            """
            params = []
            if self.channel:
                params = [self.channel]
        elif self.metric == "stale_workers":
            stale_threshold = fields.Datetime.now() - timedelta(seconds=60)
            query = """
                SELECT COUNT(DISTINCT worker_id)
                FROM queue_job
                WHERE state = 'started'
                  AND (heartbeat IS NULL OR heartbeat < %s)
            """
            params = [stale_threshold]
            if self.channel:
                query += " AND channel = %s"
                params.append(self.channel)
        elif self.metric == "p95_duration":
            query = f"""
                SELECT COALESCE(
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration), 0
                )
                FROM queue_job
                WHERE state = 'done'
                  AND duration > 0
                  AND completed_at >= %s
                  {channel_filter}
            """
        elif self.metric == "average_wait_time":
            # Average time from create_date to started_at for recently started jobs
            query = f"""
                SELECT COALESCE(
                    AVG(EXTRACT(EPOCH FROM (started_at - create_date))), 0
                )
                FROM queue_job
                WHERE started_at IS NOT NULL
                  AND started_at >= %s
                  {channel_filter}
            """
        else:
            return 0.0

        self.env["queue.job"].flush_model()
        self.env.cr.execute(query, params)
        result = self.env.cr.fetchone()
        return float(result[0]) if result and result[0] is not None else 0.0

    def _check_threshold(self, value):
        """Return True if the threshold is breached."""
        self.ensure_one()
        if self.operator == ">":
            return value > self.threshold
        elif self.operator == ">=":
            return value >= self.threshold
        elif self.operator == "<":
            return value < self.threshold
        elif self.operator == "<=":
            return value <= self.threshold
        return False

    def _send_notification(self, value):
        """Post a notification message on the rule's chatter."""
        self.ensure_one()
        channel_label = self.channel or "all channels"
        metric_label = dict(METRIC_SELECTIONS).get(self.metric, self.metric)
        body = (
            f"<p><strong>Alert: {self.name}</strong></p>"
            f"<p>Metric <em>{metric_label}</em> "
            f"on channel <em>{channel_label}</em> "
            f"is {self.operator} {self.threshold}.</p>"
            f"<p>Current value: <strong>{value:.2f}</strong></p>"
        )
        partner_ids = self.notification_user_ids.mapped("partner_id").ids
        self.message_post(
            body=body,
            subject=f"Queue Alert: {self.name}",
            partner_ids=partner_ids,
            message_type="notification",
            subtype_xmlid="mail.mt_note",
        )

    @api.model
    def _evaluate_all_rules(self):
        """Evaluate all active alert rules. Called by cron every 5 minutes."""
        rules = self.search([("active", "=", True)])
        now = fields.Datetime.now()
        for rule in rules:
            try:
                value = rule._evaluate_metric()
                rule.last_value = value
                if rule._check_threshold(value):
                    # Check cooldown
                    if rule.last_triggered_at:
                        cooldown_end = rule.last_triggered_at + timedelta(
                            minutes=rule.cooldown_minutes
                        )
                        if now < cooldown_end:
                            continue
                    rule.last_triggered_at = now
                    rule._send_notification(value)
                    _logger.warning(
                        "Alert rule '%s' triggered: %s %s %s (value=%.2f)",
                        rule.name,
                        rule.metric,
                        rule.operator,
                        rule.threshold,
                        value,
                    )
            except Exception:
                _logger.exception(
                    "Error evaluating alert rule '%s' (id=%s)", rule.name, rule.id
                )

    def action_test_rule(self):
        """Button action to test a rule immediately."""
        self.ensure_one()
        value = self._evaluate_metric()
        triggered = self._check_threshold(value)
        message = (
            f"Test result: value={value:.2f}, threshold={self.threshold}, "
            f"operator={self.operator}, triggered={triggered}"
        )
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": f"Alert Test: {self.name}",
                "message": message,
                "type": "warning" if triggered else "info",
                "sticky": False,
            },
        }
