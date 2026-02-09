import json
from datetime import date, datetime

from odoo import models as odoo_models
from odoo.tests.common import TransactionCase, tagged

from ..models.job_serialization import JobDecoder, JobEncoder


@tagged("post_install", "-at_install")
class TestSerializationRoundtrip(TransactionCase):
    def _assert_roundtrip(self, original, decoded):
        if isinstance(original, odoo_models.BaseModel):
            self.assertEqual(decoded._name, original._name)
            self.assertEqual(decoded.ids, original.ids)
            return
        if isinstance(original, datetime):
            self.assertIsInstance(decoded, datetime)
            self.assertEqual(decoded, original)
            return
        if isinstance(original, date):
            self.assertIsInstance(decoded, date)
            self.assertEqual(decoded, original)
            return
        if isinstance(original, list):
            self.assertEqual(len(decoded), len(original))
            for orig_item, dec_item in zip(original, decoded, strict=False):
                self._assert_roundtrip(orig_item, dec_item)
            return
        if isinstance(original, dict):
            self.assertEqual(set(decoded.keys()), set(original.keys()))
            for key in original:
                self._assert_roundtrip(original[key], decoded[key])
            return
        self.assertEqual(decoded, original)

    def test_roundtrip_payload_matrix(self):
        partner = self.env["res.partner"].create({"name": "Roundtrip Partner"})
        user = self.env.user
        cases = [
            {
                "simple_nested": {
                    "a": 1,
                    "b": [True, None, "txt"],
                    "c": {"d": date(2026, 1, 2), "e": datetime(2026, 1, 2, 3, 4, 5)},
                }
            },
            {
                "recordset_mix": {
                    "owner": user,
                    "partners": [partner, {"p2": partner}],
                    "meta": {"model": "res.partner", "ids": partner.ids},
                }
            },
            {
                "job_like_payload": {
                    "model": "res.partner",
                    "method": "write",
                    "ids": partner.ids,
                    "args": [{"name": "RT"}],
                    "kwargs": {"actor": user, "when": datetime(2026, 2, 1, 11, 12, 13)},
                }
            },
        ]

        for payload in cases:
            with self.subTest(payload=list(payload.keys())[0]):
                encoded = json.dumps(payload, cls=JobEncoder)
                decoded = json.loads(encoded, cls=JobDecoder, env=self.env)
                self._assert_roundtrip(payload, decoded)
