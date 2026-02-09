import json
import uuid
from datetime import date, datetime

from odoo.tests.common import TransactionCase, tagged

from ..models.job_serialization import JobDecoder, JobEncoder


@tagged("post_install", "-at_install")
class TestJobSerialization(TransactionCase):
    def test_encoder_handles_recordset_and_temporal_types(self):
        partner = self.env["res.partner"].create({"name": "Encoder Partner"})
        payload = {
            "record": partner,
            "list": [partner, date(2024, 1, 2), datetime(2024, 1, 2, 3, 4, 5)],
            "nested": {"when": datetime(2025, 5, 6, 7, 8, 9)},
        }

        encoded = json.dumps(payload, cls=JobEncoder)
        decoded = json.loads(encoded)

        self.assertEqual(decoded["record"]["__type__"], "odoo_recordset")
        self.assertEqual(decoded["record"]["model"], "res.partner")
        self.assertEqual(decoded["record"]["ids"], partner.ids)
        self.assertEqual(decoded["list"][1]["__type__"], "date_isoformat")
        self.assertEqual(decoded["list"][1]["value"], "2024-01-02")
        self.assertEqual(decoded["list"][2]["__type__"], "datetime_isoformat")
        self.assertEqual(decoded["list"][2]["value"], "2024-01-02T03:04:05")
        self.assertEqual(decoded["nested"]["when"]["__type__"], "datetime_isoformat")
        self.assertEqual(decoded["nested"]["when"]["value"], "2025-05-06T07:08:09")

    def test_decoder_rebuilds_recordset(self):
        partner = self.env["res.partner"].create({"name": "Decoder Partner"})
        raw = json.dumps(
            {"__type__": "odoo_recordset", "model": "res.partner", "ids": partner.ids}
        )

        decoded = json.loads(raw, cls=JobDecoder, env=self.env)

        self.assertEqual(decoded, partner)

    def test_decoder_rebuilds_nested_recordsets(self):
        partner = self.env["res.partner"].create({"name": "Decoder Nested Partner"})
        raw = json.dumps(
            {
                "args": [
                    {
                        "__type__": "odoo_recordset",
                        "model": "res.partner",
                        "ids": partner.ids,
                    }
                ],
                "kwargs": {
                    "target": {
                        "__type__": "odoo_recordset",
                        "model": "res.partner",
                        "ids": partner.ids,
                    }
                },
            }
        )

        decoded = json.loads(raw, cls=JobDecoder, env=self.env)

        self.assertEqual(decoded["args"][0], partner)
        self.assertEqual(decoded["kwargs"]["target"], partner)

    def test_encoder_recordset_includes_user_and_safe_context(self):
        partner = self.env["res.partner"].create({"name": "Encoder Context Partner"})
        partner_ctx = partner.with_context(
            lang="it_IT",
            tz="Europe/Rome",
            active_test=False,
            foo="bar",
        )

        encoded = json.dumps(partner_ctx, cls=JobEncoder)
        decoded = json.loads(encoded)

        self.assertEqual(decoded["__type__"], "odoo_recordset")
        self.assertIn("uid", decoded)
        self.assertIn("su", decoded)
        self.assertIn("context", decoded)
        self.assertEqual(decoded["uid"], self.env.uid)
        self.assertEqual(decoded["context"].get("lang"), "it_IT")
        self.assertEqual(decoded["context"].get("tz"), "Europe/Rome")
        self.assertEqual(decoded["context"].get("active_test"), False)
        self.assertNotIn("foo", decoded["context"])

    def test_decoder_recordset_restores_user_and_context(self):
        system_group = self.env.ref("base.group_system")
        company = self.env.company
        suffix = uuid.uuid4().hex[:8]
        run_user = (
            self.env["res.users"]
            .with_context(no_reset_password=True)
            .create(
                {
                    "name": "Decoder Context User",
                    "login": f"decoder_context_user_{suffix}",
                    "email": f"decoder_context_user_{suffix}@example.com",
                    "lang": "en_US",
                    "tz": "Europe/Brussels",
                    "company_id": company.id,
                    "company_ids": [(6, 0, [company.id])],
                    "group_ids": [(6, 0, [system_group.id])],
                }
            )
        )
        partner = self.env["res.partner"].create({"name": "Decoder Context Partner"})
        raw = json.dumps(
            {
                "__type__": "odoo_recordset",
                "model": "res.partner",
                "ids": partner.ids,
                "uid": run_user.id,
                "su": False,
                "context": {"lang": "it_IT", "tz": "Europe/Rome", "active_test": False},
            }
        )

        decoded = json.loads(raw, cls=JobDecoder, env=self.env)

        self.assertEqual(decoded.env.uid, run_user.id)
        self.assertEqual(decoded.env.context.get("lang"), "it_IT")
        self.assertEqual(decoded.env.context.get("tz"), "Europe/Rome")
        self.assertEqual(decoded.env.context.get("active_test"), False)

    def test_decoder_raises_for_unknown_model(self):
        raw = json.dumps(
            {"__type__": "odoo_recordset", "model": "x_unknown.model", "ids": [1]}
        )

        with self.assertRaisesRegex(ValueError, "not found"):
            json.loads(raw, cls=JobDecoder, env=self.env)

    def test_decoder_raises_for_deleted_recordset(self):
        partner = self.env["res.partner"].create({"name": "Deleted Decoder Partner"})
        partner_id = partner.id
        partner.unlink()
        raw = json.dumps(
            {"__type__": "odoo_recordset", "model": "res.partner", "ids": [partner_id]}
        )

        with self.assertRaisesRegex(ValueError, "Record\\(s\\).*not found"):
            json.loads(raw, cls=JobDecoder, env=self.env)

    def test_decoder_raises_for_partially_missing_recordset(self):
        partner_a = self.env["res.partner"].create({"name": "Partial A"})
        partner_b = self.env["res.partner"].create({"name": "Partial B"})
        missing_id = partner_b.id
        partner_b.unlink()
        raw = json.dumps(
            {
                "__type__": "odoo_recordset",
                "model": "res.partner",
                "ids": [partner_a.id, missing_id],
            }
        )

        with self.assertRaisesRegex(ValueError, "Record\\(s\\).*not found"):
            json.loads(raw, cls=JobDecoder, env=self.env)

    def test_run_now_marks_failed_when_target_records_are_missing(self):
        partner = self.env["res.partner"].create({"name": "Run Now Missing"})
        job = self.env["queue.job"].enqueue(
            model_name="res.partner",
            method_name="write",
            record_ids=partner.ids,
            args=[{"name": "Should Not Apply"}],
            kwargs={},
        )
        partner.unlink()

        with self.assertRaisesRegex(ValueError, "Record\\(s\\).*not found"):
            job.run_now()

        job.invalidate_recordset()
        self.assertEqual(job.state, "failed")
        self.assertIn("not found", job.exc_info or "")

    def test_roundtrip_preserves_datetime_and_date_types(self):
        raw = json.dumps(
            {
                "when": datetime(2026, 1, 2, 3, 4, 5),
                "on": date(2026, 1, 2),
            },
            cls=JobEncoder,
        )

        decoded = json.loads(raw, cls=JobDecoder, env=self.env)

        self.assertIsInstance(decoded["when"], datetime)
        self.assertIsInstance(decoded["on"], date)
        self.assertEqual(decoded["when"], datetime(2026, 1, 2, 3, 4, 5))
        self.assertEqual(decoded["on"], date(2026, 1, 2))
