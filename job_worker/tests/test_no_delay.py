import os

from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestMustRunWithoutDelay(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_context_key_triggers_immediate_execution(self):
        """Jobs run synchronously when queue_job__no_delay is in context."""
        partner = self.env["res.partner"].create({"name": "Before"})
        job = (
            partner.with_context(queue_job__no_delay=True)
            .with_delay()
            .write({"name": "After"})
        )
        self.assertEqual(job.state, "done")
        partner.invalidate_recordset()
        self.assertEqual(partner.name, "After")

    def test_env_var_triggers_immediate_execution(self):
        """Jobs run synchronously when QUEUE_JOB__NO_DELAY env var is set."""
        partner = self.env["res.partner"].create({"name": "Before Env"})
        os.environ["QUEUE_JOB__NO_DELAY"] = "1"
        try:
            job = partner.with_delay().write({"name": "After Env"})
            self.assertEqual(job.state, "done")
            partner.invalidate_recordset()
            self.assertEqual(partner.name, "After Env")
        finally:
            del os.environ["QUEUE_JOB__NO_DELAY"]

    def test_normal_delay_does_not_run_immediately(self):
        """Without bypass, jobs stay in pending state."""
        partner = self.env["res.partner"].create({"name": "Stay Pending"})
        job = partner.with_delay().write({"name": "Should Not Run"})
        self.assertEqual(job.state, "pending")
        partner.invalidate_recordset()
        self.assertEqual(partner.name, "Stay Pending")

    def test_delayable_api_with_context_key(self):
        """The explicit delayable API also respects no-delay bypass."""
        partner = (
            self.env["res.partner"]
            .with_context(queue_job__no_delay=True)
            .create({"name": "Delayable Before"})
        )
        job = partner.delayable().write({"name": "Delayable After"}).delay()
        self.assertEqual(job.state, "done")
        partner.invalidate_recordset()
        self.assertEqual(partner.name, "Delayable After")
