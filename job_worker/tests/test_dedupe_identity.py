from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestDedupeIdentity(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _enqueue(self, key):
        return self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            identity_key=key,
        )

    def test_identity_key_idempotent_enqueues_same_pending_job(self):
        key = "identity_key_idempotent"
        first = self._enqueue(key)
        second = self._enqueue(key)
        self.assertEqual(first.id, second.id)

    def test_done_jobs_with_same_identity_key_are_allowed(self):
        """The partial unique index only covers pending/started states.
        Multiple done jobs with the same identity_key are allowed."""
        key = "identity_done_collision"
        first = self._enqueue(key)
        first.write({"state": "done"})

        second = self._enqueue(key)
        self.assertEqual(second.state, "pending")
        second.write({"state": "done"})
        self.assertEqual(second.state, "done")

    def test_two_pending_jobs_with_same_identity_key_are_rejected(self):
        """The partial unique index prevents two pending jobs with the
        same identity_key from existing at the same time."""
        key = "identity_pending_collision"
        first = self._enqueue(key)
        self.assertEqual(first.state, "pending")
        # The enqueue deduplication returns the existing job
        second = self._enqueue(key)
        self.assertEqual(first.id, second.id)
