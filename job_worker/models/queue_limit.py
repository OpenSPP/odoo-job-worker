from odoo import fields, models


class QueueLimit(models.Model):
    _name = "queue.limit"
    _description = "Queue Channel Throttling Configuration"

    name = fields.Char(
        string="Channel Name",
        required=True,
        index=True,
        help="e.g. 'root', 'export', 'mail'",
    )
    limit = fields.Integer(
        string="Concurrency Limit",
        default=1,
        help="Max concurrent jobs allowed. Default 1.",
        required=True,
    )
    rate_limit = fields.Integer(
        string="Rate Limit (RPS)", default=0, help="Max jobs per second (0 = no limit)."
    )

    _name_uniq = models.Constraint("UNIQUE(name)", "Channel name must be unique!")
    _limit_non_negative = models.Constraint(
        'CHECK("limit" >= 0)', "Concurrency limit must be >= 0."
    )
    _rate_limit_non_negative = models.Constraint(
        "CHECK(rate_limit >= 0)", "Rate limit must be >= 0."
    )
