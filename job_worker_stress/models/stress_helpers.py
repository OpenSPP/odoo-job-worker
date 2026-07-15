"""Test-only job bodies for stress scenarios.

Loaded only when the ``job_worker_stress`` addon is installed (which
should never happen in production). Methods are callable via the
queue.job ``enqueue()`` API by passing
``model_name="job.worker.stress.helper"``.
"""

import time

from odoo import api, fields, models

from odoo.addons.job_worker.exception import RetryableJobError


class JobWorkerStressCounter(models.Model):
    """Per-key call counter for ``retry_once_then_succeed``.

    A separate table keeps the test-only state isolated from
    ``queue.job``. Cleared between scenarios by ``setUp`` calls in the
    Tier 1 / Tier 2 harnesses.
    """

    _name = "job.worker.stress.counter"
    _description = "Stress test call counter"

    key = fields.Char(required=True, index=True)
    count = fields.Integer(default=0)

    _key_uniq = models.Constraint("UNIQUE(key)", "Counter key must be unique.")


class JobWorkerStressHelper(models.AbstractModel):
    """Methods callable as job bodies in stress scenarios.

    Abstract because there is no per-record state — the helpers are
    invoked as class methods via ``self.env[model].method()``.
    """

    _name = "job.worker.stress.helper"
    _description = "Stress test job bodies"

    @api.model
    def sleep_for(self, seconds):
        """Block the calling thread for ``seconds``. Used by S4/S7/S8."""
        time.sleep(float(seconds))
        return float(seconds)

    @api.model
    def fail_always(self, reason="stress fail"):
        """Raise a plain Exception. Used to drive failure paths."""
        raise RuntimeError(reason)

    @api.model
    def retry_once_then_succeed(self, key):
        """First call raises ``RetryableJobError(seconds=0)``; second succeeds.

        Used by S6 (retry storm). The counter row is created on the
        first attempt and incremented on the second; the value returned
        is the final count (always 2 for the happy path).

        State persists across attempts because:
        - First attempt: create counter (count=1), then raise.
          run_now()'s RetryableJobError branch does NOT roll back the
          cursor — it just sets state=pending + scheduled_at. The
          counter row is committed alongside the state change.
        - Second attempt: counter exists, increment to 2, return.
        """
        Counter = self.env["job.worker.stress.counter"]
        existing = Counter.search([("key", "=", key)], limit=1)
        if not existing:
            Counter.create({"key": key, "count": 1})
            raise RetryableJobError(
                f"stress retry_once_then_succeed first attempt for {key}",
                seconds=0,
            )
        existing.count += 1
        return existing.count
