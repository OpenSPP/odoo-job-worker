import json
from datetime import timedelta
from unittest.mock import patch

from odoo import fields
from odoo.tests.common import TransactionCase, tagged

from ..delay import chain, group
from ..job import identity_exact


@tagged("post_install", "-at_install")
class TestQueueJobEnqueue(TransactionCase):
    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def _payload(self, job):
        if isinstance(job.payload, str):
            return json.loads(job.payload)
        return job.payload

    def test_with_delay_creates_expected_job(self):
        partner = self.env["res.partner"].create({"name": "Before"})
        eta = fields.Datetime.now().replace(microsecond=0) + timedelta(hours=1)

        job = partner.with_delay(
            priority=3,
            max_retries=4,
            channel="exports",
            eta=eta,
            identity_key="partner_write_1",
        ).write({"name": "After"})

        payload = self._payload(job)
        self.assertEqual(job.state, "pending")
        self.assertEqual(job.priority, 3)
        self.assertEqual(job.max_retries, 4)
        self.assertEqual(job.channel, "exports")
        self.assertEqual(job.identity_key, "partner_write_1")
        self.assertEqual(fields.Datetime.to_datetime(job.scheduled_at), eta)
        self.assertEqual(fields.Datetime.to_datetime(job.eta), eta)
        self.assertEqual(payload["model"], "res.partner")
        self.assertEqual(payload["method"], "write")
        self.assertEqual(payload["ids"], partner.ids)
        self.assertEqual(payload["args"], [{"name": "After"}])
        self.assertEqual(payload["kwargs"], {})

    def test_delayable_requires_explicit_delay(self):
        partner = self.env["res.partner"].create({"name": "Before Delayable"})
        before_count = self.Job.search_count([])
        delayable = partner.delayable(priority=1).write({"name": "After Delayable"})
        self.assertIsNotNone(delayable)
        self.assertEqual(self.Job.search_count([]), before_count)

        job = delayable.delay()
        self.assertEqual(self.Job.search_count([]), before_count + 1)
        payload = self._payload(job)
        self.assertEqual(payload["method"], "write")
        self.assertEqual(payload["ids"], partner.ids)

    def test_default_max_retries_matches_upstream(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Default Retries"}],
            kwargs={},
        )
        self.assertEqual(job.max_retries, 5)

    def test_enqueue_accepts_eta_seconds(self):
        start = fields.Datetime.now()
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "ETA Seconds"}],
            kwargs={},
            eta=60,
        )
        self.assertTrue(
            fields.Datetime.to_datetime(job.scheduled_at)
            >= start + timedelta(seconds=50)
        )

    def test_enqueue_rejects_conflicting_eta_and_scheduled_at(self):
        now = fields.Datetime.now().replace(microsecond=0)
        with self.assertRaisesRegex(ValueError, "conflicting values"):
            self.Job.enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", self.env.user.id)]],
                kwargs={},
                eta=now,
                scheduled_at=now + timedelta(minutes=1),
            )

    def test_enqueue_accepts_matching_eta_and_scheduled_at(self):
        when = fields.Datetime.now().replace(microsecond=0) + timedelta(minutes=5)
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            eta=when,
            scheduled_at=when,
        )
        self.assertEqual(fields.Datetime.to_datetime(job.scheduled_at), when)

    def test_with_delay_rejects_conflicting_eta_and_scheduled_at(self):
        partner = self.env["res.partner"].create({"name": "Conflict ETA"})
        now = fields.Datetime.now().replace(microsecond=0)

        with self.assertRaisesRegex(ValueError, "conflicting values"):
            partner.with_delay(
                eta=now,
                scheduled_at=now + timedelta(minutes=1),
            ).write({"name": "Should Not Enqueue"})

    def test_enqueue_accepts_negative_eta_seconds(self):
        before = fields.Datetime.now()
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            eta=-30,
        )
        self.assertTrue(fields.Datetime.to_datetime(job.scheduled_at) <= before)

    def test_enqueue_accepts_eta_as_string(self):
        eta_str = "2026-01-01 12:34:56"
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            eta=eta_str,
        )
        self.assertEqual(
            fields.Datetime.to_datetime(job.scheduled_at),
            fields.Datetime.to_datetime(eta_str),
        )

    def test_enqueue_with_company_id_none_context_creates_job_without_company(self):
        job = self.Job.with_context(company_id=None).enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel="no_company_context",
        )

        self.assertFalse(job.company_id)

    def test_enqueue_defaults_channel_to_root_when_none(self):
        job = self.Job.enqueue(
            model_name="res.users",
            method_name="search",
            record_ids=[],
            args=[[("id", "=", self.env.user.id)]],
            kwargs={},
            channel=None,
        )
        self.assertEqual(job.channel, "root")

    def test_identity_key_callable_is_supported(self):
        partner = self.env["res.partner"].create({"name": "Identity Callable"})
        first = partner.with_delay(identity_key=identity_exact).write(
            {"name": "Callable"}
        )
        second = partner.with_delay(identity_key=identity_exact).write(
            {"name": "Callable"}
        )
        self.assertEqual(first.id, second.id)

    def test_group_and_chain_helpers_enqueue_jobs(self):
        first_partner = self.env["res.partner"].create({"name": "Group A"})
        second_partner = self.env["res.partner"].create({"name": "Group B"})
        third_partner = self.env["res.partner"].create({"name": "Chain C"})
        fourth_partner = self.env["res.partner"].create({"name": "Chain D"})
        before_count = self.Job.search_count([])

        group(
            first_partner.delayable().write({"name": "Group A Done"}),
            second_partner.delayable().write({"name": "Group B Done"}),
        ).delay()
        chain(
            third_partner.delayable().write({"name": "Chain C Done"}),
            fourth_partner.delayable().write({"name": "Chain D Done"}),
        ).delay()

        self.assertEqual(self.Job.search_count([]), before_count + 4)

    def test_enqueue_serializes_complex_arguments(self):
        partner = self.env["res.partner"].create({"name": "Argument Partner"})

        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Created by queue", "owner": self.env.user}],
            kwargs={"related": partner},
            channel="root",
        )
        payload = self._payload(job)

        self.assertEqual(payload["args"][0]["owner"]["__type__"], "odoo_recordset")
        self.assertEqual(payload["args"][0]["owner"]["model"], "res.users")
        self.assertEqual(payload["kwargs"]["related"]["__type__"], "odoo_recordset")
        self.assertEqual(payload["kwargs"]["related"]["model"], "res.partner")
        self.assertEqual(payload["kwargs"]["related"]["ids"], partner.ids)

    def test_enqueue_emits_notify(self):
        with patch.object(self.env.cr, "execute", wraps=self.env.cr.execute) as execute:
            self.Job.enqueue(
                model_name="res.users",
                method_name="search",
                record_ids=[],
                args=[[("id", "=", self.env.user.id)]],
                kwargs={},
                channel="root",
            )

        notify_calls = [
            call
            for call in execute.call_args_list
            if call.args
            and isinstance(call.args[0], str)
            and "NOTIFY queue_job_wake_up" in call.args[0]
        ]
        self.assertTrue(notify_calls, "Expected a NOTIFY queue_job_wake_up query")

    def test_identity_key_returns_existing_pending_job(self):
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Dedup A"}],
            kwargs={},
            identity_key="dedup_key_pending",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Dedup B"}],
            kwargs={},
            identity_key="dedup_key_pending",
        )

        self.assertEqual(job1.id, job2.id)

    def test_identity_key_returns_existing_started_job(self):
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Dedup Started A"}],
            kwargs={},
            identity_key="dedup_key_started",
        )
        job1.write({"state": "started", "heartbeat": fields.Datetime.now()})

        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Dedup Started B"}],
            kwargs={},
            identity_key="dedup_key_started",
        )

        self.assertEqual(job1.id, job2.id)

    def test_payload_display_is_formatted_json_string(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Payload Display"}],
            kwargs={},
        )
        self.assertIsInstance(job.payload_display, str)
        parsed = json.loads(job.payload_display)
        self.assertEqual(parsed["model"], "res.partner")
        self.assertEqual(parsed["method"], "create")
        self.assertIn(
            "\n", job.payload_display, "Should be pretty-printed with newlines"
        )

    def test_payload_display_empty_when_no_payload(self):
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Empty Display Test"}],
            kwargs={},
        )
        # Simulate an edge case: force payload to falsy
        job.write({"payload": False})
        self.assertEqual(job.payload_display, "")

    def test_result_captured_on_run_now(self):
        """run_now() captures the method's return value in the result field."""
        partner = self.env["res.partner"].create({"name": "Result Test"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Result After"}],
            kwargs={},
        )
        job.run_now()
        self.assertEqual(job.state, "done")
        # res.partner.write() returns True
        self.assertTrue(job.result)

    def test_result_display_formats_json(self):
        """result_display computed field produces formatted JSON."""
        partner = self.env["res.partner"].create({"name": "Display Test"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Display After"}],
            kwargs={},
        )
        job.run_now()
        self.assertIsInstance(job.result_display, str)
        self.assertTrue(len(job.result_display) > 0)
        parsed = json.loads(job.result_display)
        self.assertIsNotNone(parsed)

    def test_result_none_when_method_returns_none(self):
        """Void methods store no result (falsy)."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="flush_recordset",
            record_ids=[],
            args=[],
            kwargs={},
        )
        job.run_now()
        self.assertEqual(job.state, "done")
        self.assertFalse(job.result)
        self.assertEqual(job.result_display, "")

    def test_result_none_after_failed_job(self):
        """Failed jobs keep result as falsy."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="method_does_not_exist",
            record_ids=[],
            args=[],
            kwargs={},
        )
        try:
            job.run_now()
        except AttributeError:
            pass
        job.invalidate_recordset()
        self.assertEqual(job.state, "failed")
        self.assertFalse(job.result)

    def test_result_handles_non_serializable(self):
        """Non-serializable results fall back to repr."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Non Serializable Result"}],
            kwargs={},
        )
        job.run_now()
        # res.partner.create() returns a recordset — serialized via JobEncoder
        self.assertIsNotNone(job.result)

    def test_result_truncates_large_values(self):
        """Results exceeding 64KB are truncated with metadata."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Truncate Test"}],
            kwargs={},
        )
        job.run_now()
        # Simulate a large result via _serialize_result directly
        large_data = {"data": "x" * 100_000}
        result = job._serialize_result(large_data)
        self.assertTrue(result.get("__truncated__"))
        self.assertIn("__repr__", result)
        self.assertIn("__size_bytes__", result)

    def test_identity_key_allows_new_job_once_previous_done(self):
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Done Dedup A"}],
            kwargs={},
            identity_key="dedup_key_done",
        )
        job1.write({"state": "done"})

        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Done Dedup B"}],
            kwargs={},
            identity_key="dedup_key_done",
        )

        self.assertNotEqual(job1.id, job2.id)
        self.assertEqual(job2.state, "pending")


@tagged("post_install", "-at_install")
class TestPayloadValidation(TransactionCase):
    """Probe payload handling for dangerous or malformed inputs."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_nonexistent_model_raises_valueerror_at_enqueue(self):
        """enqueue() raises ValueError when model_name doesn't exist."""
        with self.assertRaises(ValueError) as ctx:
            self.Job.enqueue(
                model_name="nonexistent.model.xyz",
                method_name="do_stuff",
                record_ids=[],
                args=[],
                kwargs={},
                channel="bad_model",
            )
        self.assertIn("nonexistent.model.xyz", str(ctx.exception))

    def test_nonexistent_method_in_payload_raises_on_run(self):
        """A payload referencing a non-existent method should fail on execution.
        run_now() marks the job as failed and re-raises the exception."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="absolutely_nonexistent_method_xyz",
            record_ids=[],
            args=[],
            kwargs={},
            channel="bad_method",
        )
        try:
            job.run_now()
            self.fail("run_now() should raise when method does not exist")
        except AttributeError:
            pass
        job.invalidate_recordset()
        self.assertEqual(job.state, "failed")
        self.assertIn("AttributeError", job.exc_info or "")

    def test_private_method_in_payload(self):
        """A payload targeting a private method (underscore prefix) should
        still execute -- the system does not filter method names."""
        partner = self.env["res.partner"].create({"name": "Private Method"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="_compute_display_name",
            record_ids=partner.ids,
            args=[],
            kwargs={},
            channel="private",
        )
        # This is an observation test: private methods are callable.
        # If the system should block private methods, this test documents
        # that it currently does NOT.
        job.run_now()
        self.assertEqual(job.state, "done")

    def test_empty_model_raises_valueerror_at_enqueue(self):
        """enqueue() raises ValueError when model_name is empty."""
        with self.assertRaises(ValueError) as ctx:
            self.Job.enqueue(
                model_name="",
                method_name="",
                record_ids=[],
                args=[],
                kwargs={},
                channel="empty",
            )
        self.assertIn("does not exist", str(ctx.exception))

    def test_payload_with_nonexistent_record_ids(self):
        """Payload with record IDs that don't exist should fail on run_now."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=[999999999],
            args=[{"name": "ghost"}],
            kwargs={},
            channel="ghost_ids",
        )
        with self.assertRaises(ValueError) as ctx:
            job.run_now()
        self.assertIn("not found", str(ctx.exception))

    def test_payload_with_mixed_existing_and_missing_ids(self):
        """Payload with some valid and some invalid IDs should fail."""
        partner = self.env["res.partner"].create({"name": "Exists"})
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=[partner.id, 999999999],
            args=[{"name": "mixed"}],
            kwargs={},
            channel="mixed_ids",
        )
        with self.assertRaises(ValueError) as ctx:
            job.run_now()
        self.assertIn("999999999", str(ctx.exception))

    def test_payload_with_empty_record_ids_list(self):
        """Empty record_ids should call the method on the model class."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "Created Without IDs"}],
            kwargs={},
            channel="empty_ids",
        )
        job.run_now()
        self.assertEqual(job.state, "done")
        created = self.env["res.partner"].search(
            [("name", "=", "Created Without IDs")], limit=1
        )
        self.assertTrue(created)

    def test_payload_with_none_record_ids(self):
        """None record_ids should be treated like empty."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=None,
            args=[{"name": "None IDs Partner"}],
            kwargs={},
            channel="none_ids",
        )
        job.run_now()
        self.assertEqual(job.state, "done")


@tagged("post_install", "-at_install")
class TestEtaNormalizationEdgeCases(TransactionCase):
    """Probe eta/scheduled_at normalization for edge cases."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_eta_as_timedelta(self):
        """ETA as a timedelta should be resolved relative to now."""
        from datetime import timedelta as td

        before = fields.Datetime.now()
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "timedelta eta"}],
            kwargs={},
            eta=td(hours=1),
            channel="eta_td",
        )
        scheduled = fields.Datetime.to_datetime(job.scheduled_at)
        self.assertGreaterEqual(scheduled, before + td(minutes=59))

    def test_eta_as_zero(self):
        """ETA as 0 seconds should schedule approximately now."""
        before = fields.Datetime.now()
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "zero eta"}],
            kwargs={},
            eta=0,
            channel="eta_zero",
        )
        scheduled = fields.Datetime.to_datetime(job.scheduled_at)
        diff = abs((scheduled - before).total_seconds())
        self.assertLess(diff, 5)

    def test_eta_as_float(self):
        """ETA as a float (fractional seconds) should work."""
        before = fields.Datetime.now()
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "float eta"}],
            kwargs={},
            eta=0.5,
            channel="eta_float",
        )
        scheduled = fields.Datetime.to_datetime(job.scheduled_at)
        diff = abs((scheduled - before).total_seconds())
        self.assertLess(diff, 5)

    def test_normalize_eta_with_none(self):
        """_normalize_eta with None should return None."""
        result = self.Job._normalize_eta(None)
        self.assertIsNone(result)


@tagged("post_install", "-at_install")
class TestEnqueueEdgeCases(TransactionCase):
    """Probe enqueue API for boundary conditions."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_enqueue_with_zero_priority(self):
        """Priority 0 should be valid and highest priority."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "priority 0"}],
            kwargs={},
            priority=0,
            channel="priority",
        )
        self.assertEqual(job.priority, 0)

    def test_enqueue_with_negative_priority(self):
        """Negative priority should be accepted (higher than 0)."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "priority negative"}],
            kwargs={},
            priority=-100,
            channel="neg_priority",
        )
        self.assertEqual(job.priority, -100)

    def test_enqueue_with_very_large_priority(self):
        """Very large priority should be accepted (lowest priority)."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "priority huge"}],
            kwargs={},
            priority=999999999,
            channel="huge_priority",
        )
        self.assertEqual(job.priority, 999999999)

    def test_enqueue_with_negative_timeout(self):
        """Negative timeout should be stored (timeout=0 means no timeout,
        but negative is ambiguous)."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "negative timeout"}],
            kwargs={},
            timeout=-1,
            channel="neg_timeout",
        )
        self.assertEqual(job.timeout, -1)

    def test_enqueue_with_very_long_channel_name(self):
        """Very long channel name should be accepted."""
        long_channel = "c" * 5000
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "long channel"}],
            kwargs={},
            channel=long_channel,
        )
        self.assertEqual(job.channel, long_channel)

    def test_enqueue_with_special_characters_in_channel(self):
        """Special characters in channel name should be accepted."""
        channel = "root/sub-channel.v2 (test) [special]"
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "special channel"}],
            kwargs={},
            channel=channel,
        )
        self.assertEqual(job.channel, channel)

    def test_enqueue_stores_user_and_company(self):
        """Enqueued jobs should capture the current user and company."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "user check"}],
            kwargs={},
            channel="user",
        )
        self.assertEqual(job.user_id.id, self.env.user.id)
        self.assertEqual(job.company_id.id, self.env.company.id)

    def test_enqueue_uuid_is_unique(self):
        """Each enqueued job should get a unique UUID."""
        job1 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "uuid 1"}],
            kwargs={},
            channel="uuid",
        )
        job2 = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "uuid 2"}],
            kwargs={},
            channel="uuid",
        )
        self.assertNotEqual(job1.uuid, job2.uuid)
        self.assertTrue(job1.uuid)
        self.assertTrue(job2.uuid)


@tagged("post_install", "-at_install")
class TestEnqueueValidation(TransactionCase):
    """Probe enqueue validation and edge cases not covered elsewhere."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_conflicting_eta_and_scheduled_at_raises(self):
        """Providing both eta and scheduled_at with different values
        should raise ValueError."""
        now = fields.Datetime.now()
        with self.assertRaises(ValueError) as ctx:
            self.Job.enqueue(
                model_name="res.partner",
                method_name="create",
                record_ids=[],
                args=[{"name": "conflict"}],
                kwargs={},
                eta=now + timedelta(hours=1),
                scheduled_at=now - timedelta(hours=1),
                channel="conflict_eta",
            )
        self.assertIn("eta", str(ctx.exception).lower())

    def test_eta_as_negative_timedelta(self):
        """ETA as a negative timedelta should schedule in the past."""
        before = fields.Datetime.now()
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "negative td eta"}],
            kwargs={},
            eta=timedelta(hours=-1),
            channel="neg_td_eta",
        )
        scheduled = fields.Datetime.to_datetime(job.scheduled_at)
        self.assertLess(scheduled, before)


@tagged("post_install", "-at_install")
class TestMethodNameValidation(TransactionCase):
    """Verify that dangerous method names are blocked at enqueue time."""

    def setUp(self):
        super().setUp()
        self.Job = self.env["queue.job"]

    def test_dunder_init_blocked(self):
        """__init__ should be blocked from job queue execution."""
        with self.assertRaises(ValueError) as ctx:
            self.Job.enqueue(
                model_name="res.partner",
                method_name="__init__",
                record_ids=[],
                args=[],
                kwargs={},
                channel="dunder",
            )
        self.assertIn("Dunder method", str(ctx.exception))

    def test_dunder_del_blocked(self):
        """__del__ should be blocked from job queue execution."""
        with self.assertRaises(ValueError) as ctx:
            self.Job.enqueue(
                model_name="res.partner",
                method_name="__del__",
                record_ids=[],
                args=[],
                kwargs={},
                channel="dunder",
            )
        self.assertIn("Dunder method", str(ctx.exception))

    def test_dunder_getattr_blocked(self):
        """__getattr__ should be blocked from job queue execution."""
        with self.assertRaises(ValueError) as ctx:
            self.Job.enqueue(
                model_name="res.partner",
                method_name="__getattr__",
                record_ids=[],
                args=[],
                kwargs={},
                channel="dunder",
            )
        self.assertIn("Dunder method", str(ctx.exception))

    def test_single_underscore_allowed(self):
        """Single-underscore private methods should still be allowed
        (they are legitimate Odoo methods like _compute_*)."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="_compute_display_name",
            record_ids=[],
            args=[],
            kwargs={},
            channel="single_underscore",
        )
        self.assertTrue(job)
        self.assertEqual(job.state, "pending")

    def test_regular_method_allowed(self):
        """Regular public methods should be allowed."""
        job = self.Job.enqueue(
            model_name="res.partner",
            method_name="create",
            record_ids=[],
            args=[{"name": "allowed"}],
            kwargs={},
            channel="regular_method",
        )
        self.assertTrue(job)
        self.assertEqual(job.state, "pending")
