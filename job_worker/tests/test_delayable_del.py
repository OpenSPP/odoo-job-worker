from unittest.mock import patch

from odoo.tests.common import TransactionCase, tagged

from ..delay import Delayable


@tagged("post_install", "-at_install")
class TestDelayableDel(TransactionCase):
    def test_del_warns_when_not_delayed(self):
        """A delayable configured but never delayed emits a warning."""
        partner = self.env["res.partner"].create({"name": "Del Test"})
        delayable = partner.delayable().write({"name": "forgotten"})
        self.assertIsNotNone(delayable._job_method)
        self.assertIsNone(delayable._generated_job)

        with patch("odoo.addons.job_worker.delay._logger") as mock_logger:
            delayable.__del__()
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args
            self.assertIn("never delayed", call_args[0][0])

    def test_del_silent_when_delayed(self):
        """A properly delayed delayable does not emit a warning."""
        partner = self.env["res.partner"].create({"name": "Delayed OK"})
        delayable = partner.delayable().write({"name": "ok"})
        delayable.delay()
        self.assertIsNotNone(delayable._generated_job)

        with patch("odoo.addons.job_worker.delay._logger") as mock_logger:
            delayable.__del__()
            mock_logger.warning.assert_not_called()

    def test_del_silent_when_no_method_set(self):
        """A fresh delayable with no method set does not warn."""
        partner = self.env["res.partner"].create({"name": "No Method"})
        delayable = partner.delayable()
        self.assertIsNone(delayable._job_method)

        with patch("odoo.addons.job_worker.delay._logger") as mock_logger:
            delayable.__del__()
            mock_logger.warning.assert_not_called()

    def test_del_does_not_raise_on_stale_env(self):
        """__del__ must not raise even if environment is stale."""
        delayable = Delayable.__new__(Delayable)
        delayable._generated_job = None
        delayable._job_method = "write"
        # Simulate stale recordset by setting it to a broken value
        delayable.recordset = None
        # Must not raise
        delayable.__del__()
