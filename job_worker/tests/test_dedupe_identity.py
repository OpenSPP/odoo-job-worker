from odoo.exceptions import ValidationError
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


@tagged("post_install", "-at_install")
class TestIdentityKeyEdgeCases(TransactionCase):
    """Probe identity key deduplication for edge cases."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_identity_key_empty_string_treated_as_no_key(self):
        """Empty string identity_key is normalized to None, so
        multiple jobs with identity_key="" should each get created
        independently (no dedup)."""
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "empty key 1"}],
            kwargs={},
            identity_key="",
            channel="empty_key",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "empty key 2"}],
            kwargs={},
            identity_key="",
            channel="empty_key",
        )
        self.assertNotEqual(job1.id, job2.id)
        # identity_key should be normalized to None
        self.assertFalse(job1.identity_key)
        self.assertFalse(job2.identity_key)

    def test_identity_key_none_treated_as_no_key(self):
        """Explicit None identity_key should allow multiple independent
        jobs without deduplication."""
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "none key 1"}],
            kwargs={},
            identity_key=None,
            channel="none_key",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "none key 2"}],
            kwargs={},
            identity_key=None,
            channel="none_key",
        )
        self.assertNotEqual(job1.id, job2.id)
        self.assertFalse(job1.identity_key)
        self.assertFalse(job2.identity_key)

    def test_identity_key_very_long_string(self):
        """An extremely long identity key should be accepted."""
        long_key = "x" * 10000
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "long key"}],
            kwargs={},
            identity_key=long_key,
            channel="long_key",
        )
        self.assertEqual(job.identity_key, long_key)

    def test_identity_key_dedup_across_channels(self):
        """Identity key deduplication should work across different channels."""
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "cross channel 1"}],
            kwargs={},
            identity_key="cross_channel_key",
            channel="channel_a",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "cross channel 2"}],
            kwargs={},
            identity_key="cross_channel_key",
            channel="channel_b",
        )
        # Identity key is global, not per-channel
        self.assertEqual(job1.id, job2.id)

    def test_identity_key_requeue_with_existing_active_dedup(self):
        """Requeueing a failed job with identity_key when another active job
        has the same key should be blocked by the constraint."""
        self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "dedup requeue 1"}],
            kwargs={},
            identity_key="requeue_dedup_key",
            channel="requeue_dedup",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "dedup requeue 2"}],
            kwargs={},
            channel="requeue_dedup",
        )
        job2.write({"state": "failed"})

        # Directly writing a conflicting identity_key should raise
        with self.assertRaises(ValidationError):
            job2.write({"identity_key": "requeue_dedup_key"})

    def test_identity_key_dedup_returns_existing_on_enqueue(self):
        """Enqueueing with a duplicate identity_key should return the
        existing job (not create a new one)."""
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "constraint 1"}],
            kwargs={},
            identity_key="constraint_key",
            channel="constraint",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "constraint 2"}],
            kwargs={},
            identity_key="constraint_key",
            channel="constraint",
        )
        self.assertEqual(job1.id, job2.id)


@tagged("post_install", "-at_install")
class TestRequeueIdentityKeyConflict(TransactionCase):
    """Probe button_requeue's identity_key skip logic."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_requeue_skipped_when_active_duplicate_exists(self):
        """button_requeue should skip a failed job if another active job
        with the same identity_key already exists."""
        # Create job1 with identity_key (pending/active)
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "active dup"}],
            kwargs={},
            identity_key="requeue_skip_key",
            channel="requeue_skip",
        )
        self.assertEqual(job1.state, "pending")

        # Create job2 with a unique identity_key, then fail it
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "failed dup"}],
            kwargs={},
            identity_key="requeue_skip_key_temp",
            channel="requeue_skip",
        )
        # Fail job2 first (moves it to terminal state)
        job2.write({"state": "failed"})
        # Now update identity_key via SQL to match job1
        # (the partial unique index allows this because job2 is in "failed"
        # state, which is not covered by the index)
        self.env.cr.execute(
            "UPDATE queue_job SET identity_key = %s WHERE id = %s",
            ("requeue_skip_key", job2.id),
        )
        self.env.invalidate_all()

        job2.button_requeue()
        # job2 should remain failed because job1 is active with same key
        self.assertEqual(job2.state, "failed")

    def test_requeue_proceeds_when_no_active_duplicate(self):
        """button_requeue should proceed when no active duplicate exists."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "sole job"}],
            kwargs={},
            identity_key="requeue_sole_key",
            channel="requeue_sole",
        )
        job.write({"state": "failed"})
        job.button_requeue()
        self.assertEqual(job.state, "pending")
