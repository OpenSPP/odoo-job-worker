from odoo import fields, models

from ..delay import Delayable, DelayableRecordset


class Base(models.AbstractModel):
    _inherit = "base"

    def with_delay(
        self,
        priority=None,
        eta=None,
        max_retries=None,
        description=None,
        channel=None,
        identity_key=None,
        scheduled_at=None,
        timeout=None,
    ):
        """Upstream-compatible shortcut API that enqueues on method call."""
        if (
            eta is not None
            and scheduled_at is not None
            and fields.Datetime.to_datetime(eta)
            != fields.Datetime.to_datetime(scheduled_at)
        ):
            raise ValueError(
                "Provide either eta or scheduled_at, not conflicting values for both."
            )
        if eta is None and scheduled_at is not None:
            eta = scheduled_at
        return DelayableRecordset(
            self,
            priority=priority,
            eta=eta,
            max_retries=max_retries,
            description=description,
            channel=channel,
            identity_key=identity_key,
            timeout=timeout,
        )

    def delayable(
        self,
        priority=None,
        eta=None,
        max_retries=None,
        description=None,
        channel=None,
        identity_key=None,
        scheduled_at=None,
        timeout=None,
    ):
        """Upstream-compatible explicit delay API requiring a final ``delay()``."""
        if (
            eta is not None
            and scheduled_at is not None
            and fields.Datetime.to_datetime(eta)
            != fields.Datetime.to_datetime(scheduled_at)
        ):
            raise ValueError(
                "Provide either eta or scheduled_at, not conflicting values for both."
            )
        if eta is None and scheduled_at is not None:
            eta = scheduled_at
        return Delayable(
            self,
            priority=priority,
            eta=eta,
            max_retries=max_retries,
            description=description,
            channel=channel,
            identity_key=identity_key,
            timeout=timeout,
        )

    def _job_store_values(self, job_vals):
        """Hook for models to inject extra values into the job record.

        Override this method on a model to add custom fields to jobs
        that target that model.  The returned dict is merged into the
        ``create()`` vals.

        :param dict job_vals: the vals dict that will be used to create
            the ``queue.job`` record.
        :returns: dict of extra field values (may be empty).
        """
        return {}
