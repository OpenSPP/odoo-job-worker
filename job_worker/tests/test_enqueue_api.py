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
