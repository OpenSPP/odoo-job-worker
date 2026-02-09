import json
import re

from odoo import api, fields, models


class QueueJob(models.Model):
    _inherit = "queue.job"

    is_retry_exhausted = fields.Boolean(
        string="Retry Exhausted",
        compute="_compute_is_retry_exhausted",
        store=True,
        index=True,
        help="True when job has failed and exhausted all retry attempts.",
    )

    model_name = fields.Char(
        string="Model",
        compute="_compute_model_method",
        store=True,
        index=True,
    )
    method_name = fields.Char(
        string="Method",
        compute="_compute_model_method",
        store=True,
        index=True,
    )
    exc_name = fields.Char(
        string="Exception Type",
        compute="_compute_exc_name",
        store=True,
        index=True,
    )

    @api.depends("state", "attempts", "max_retries")
    def _compute_is_retry_exhausted(self):
        for record in self:
            record.is_retry_exhausted = (
                record.state == "failed" and record.attempts >= record.max_retries
            )

    @api.depends("payload")
    def _compute_model_method(self):
        for record in self:
            payload = record.payload
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    payload = {}
            if isinstance(payload, dict):
                record.model_name = payload.get("model", "")
                record.method_name = payload.get("method", "")
            else:
                record.model_name = ""
                record.method_name = ""

    @api.depends("exc_info")
    def _compute_exc_name(self):
        for record in self:
            if record.exc_info:
                # Extract the exception class from the last line of the traceback
                # e.g. "ValueError: some message" -> "ValueError"
                lines = record.exc_info.strip().splitlines()
                if lines:
                    last_line = lines[-1]
                    match = re.match(r"^(\w+(?:\.\w+)*):", last_line)
                    if match:
                        record.exc_name = match.group(1)
                    else:
                        record.exc_name = last_line[:100]
                else:
                    record.exc_name = ""
            else:
                record.exc_name = ""
