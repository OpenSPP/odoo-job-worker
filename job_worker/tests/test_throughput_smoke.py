import time
import uuid

from odoo import SUPERUSER_ID, api
from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestThroughputSmoke(TransactionCase):
    def test_process_jobs_throughput_smoke(self):
        total_jobs = 20
        channel = f"throughput_{uuid.uuid4().hex[:8]}"

        with self.env.registry.cursor() as setup_cr:
            setup_env = api.Environment(setup_cr, SUPERUSER_ID, {})
            setup_env["queue.job"].search(
                [("state", "in", ["pending", "started"])]
            ).write({"state": "done", "heartbeat": False, "worker_id": False})
            for _ in range(total_jobs):
                setup_env["queue.job"].enqueue(
                    model_name="res.users",
                    method_name="search",
                    record_ids=[],
                    args=[[("id", "=", setup_env.user.id)]],
                    kwargs={},
                    channel=channel,
                )
            setup_cr.commit()

        started = time.monotonic()
        done = 0
        while done < total_jobs:
            with self.env.registry.cursor() as run_cr:
                run_env = api.Environment(run_cr, SUPERUSER_ID, {})
                batch = run_env["queue.job"].search(
                    [("channel", "=", channel), ("state", "=", "pending")],
                    limit=10,
                )
                if not batch:
                    break
                batch.run_now()
                done = run_env["queue.job"].search_count(
                    [("channel", "=", channel), ("state", "=", "done")]
                )
                run_cr.commit()
        elapsed = time.monotonic() - started

        with self.env.registry.cursor() as verify_cr:
            verify_env = api.Environment(verify_cr, SUPERUSER_ID, {})
            done = verify_env["queue.job"].search_count(
                [("channel", "=", channel), ("state", "=", "done")]
            )
        self.assertEqual(done, total_jobs)
        self.assertLess(
            elapsed, 30, f"throughput smoke exceeded threshold: {elapsed:.2f}s"
        )
