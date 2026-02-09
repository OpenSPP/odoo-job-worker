from odoo import fields, models


class QueueLimit(models.Model):
    _inherit = "queue.limit"

    current_running = fields.Integer(
        string="Running Jobs",
        compute="_compute_health",
    )
    current_pending = fields.Integer(
        string="Pending Jobs",
        compute="_compute_health",
    )
    current_failed = fields.Integer(
        string="Failed Jobs",
        compute="_compute_health",
    )
    utilization_percent = fields.Float(
        string="Utilization (%)",
        compute="_compute_health",
    )

    def _compute_health(self):
        if not self.ids:
            return

        self.env.cr.execute(
            """
            SELECT channel,
                   COUNT(*) FILTER (WHERE state = 'started') AS running,
                   COUNT(*) FILTER (WHERE state = 'pending') AS pending,
                   COUNT(*) FILTER (WHERE state = 'failed') AS failed
            FROM queue_job
            WHERE channel IN %s
              AND state IN ('started', 'pending', 'failed')
            GROUP BY channel
            """,
            (tuple(self.mapped("name")),),
        )
        stats = {row[0]: row[1:] for row in self.env.cr.fetchall()}

        for record in self:
            running, pending, failed = stats.get(record.name, (0, 0, 0))
            record.current_running = running
            record.current_pending = pending
            record.current_failed = failed
            if record.limit > 0:
                record.utilization_percent = (running / record.limit) * 100.0
            else:
                record.utilization_percent = 0.0
