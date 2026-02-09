from odoo import fields, models


class QueueJobFunction(models.Model):
    _name = "queue.job.function"
    _description = "Queue Job Function"
    _order = "name"
    _log_access = False

    name = fields.Char(required=True, index=True)

    _name_uniq = models.Constraint("UNIQUE(name)", "Function name must be unique.")
