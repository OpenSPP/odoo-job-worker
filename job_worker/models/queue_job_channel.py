from odoo import fields, models


class QueueJobChannel(models.Model):
    _name = "queue.job.channel"
    _description = "Queue Job Channel"
    _order = "name"
    _log_access = False

    name = fields.Char(required=True, index=True)

    _name_uniq = models.Constraint("UNIQUE(name)", "Channel name must be unique.")
