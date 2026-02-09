import logging
import random
import time

from odoo import fields, models

_logger = logging.getLogger(__name__)


class DemoTask(models.Model):
    _name = "demo.task"
    _description = "Demo Task"

    name = fields.Char(
        required=True,
        default=lambda self: self.env["ir.sequence"].next_by_code("demo.task"),
    )
    state = fields.Selection(
        [
            ("draft", "Draft"),
            ("queued", "Queued"),
            ("done", "Done"),
        ],
        default="draft",
        readonly=True,
    )
    log = fields.Text(readonly=True)
    completed_at = fields.Datetime(readonly=True)

    def _append_log(self, message):
        """Append a timestamped entry to the log field."""
        now = fields.Datetime.now()
        entry = f"[{now}] {message}"
        for record in self:
            existing = record.log or ""
            record.log = f"{existing}{entry}\n"

    def _mark_done(self):
        """Set state to done and record completion time."""
        now = fields.Datetime.now()
        for record in self:
            record.state = "done"
            record.completed_at = now

    # --- Queued methods (called by the worker) ---

    def process_basic(self):
        """Set state=done, write to log. Sleeps briefly to be visible in the UI."""
        time.sleep(2)
        self._append_log("process_basic completed")
        self._mark_done()
        return {"status": "completed", "task_id": self.id}

    def process_slow(self, duration=3):
        """Sleep then set done (demonstrates throttling)."""
        time.sleep(duration)
        self._append_log(f"process_slow completed (duration={duration})")
        self._mark_done()

    def process_unreliable(self):
        """Raise 70% of the time (demonstrates retries)."""
        if random.random() < 0.7:
            raise RuntimeError("Simulated transient failure in process_unreliable")
        self._append_log("process_unreliable completed successfully")
        self._mark_done()

    def process_always_fails(self):
        """Always raise (demonstrates terminal failure)."""
        raise RuntimeError("This task always fails by design")

    def process_step_one(self):
        """Write to log but do NOT set done (chain step 1)."""
        time.sleep(3)
        self._append_log("process_step_one completed")
        return {"step": 1, "task_id": self.id}

    def process_step_two(self):
        """Write to log AND set done (chain step 2)."""
        time.sleep(3)
        self._append_log("process_step_two completed")
        self._mark_done()
        return {"step": 2, "task_id": self.id}

    # --- UI button actions (enqueue from UI) ---

    def action_enqueue_basic(self):
        """Enqueue process_basic on the demo channel."""
        for record in self:
            record.with_delay(channel="demo").process_basic()
            record.state = "queued"

    def action_enqueue_throttled(self):
        """Enqueue process_slow on the demo_throttled channel."""
        for record in self:
            record.with_delay(channel="demo_throttled").process_slow()
            record.state = "queued"

    def action_enqueue_high_priority(self):
        """Enqueue process_basic with priority=1."""
        for record in self:
            record.with_delay(channel="demo", priority=1).process_basic()
            record.state = "queued"

    def action_enqueue_low_priority(self):
        """Enqueue process_basic with priority=20."""
        for record in self:
            record.with_delay(channel="demo", priority=20).process_basic()
            record.state = "queued"

    def action_enqueue_delayed(self):
        """Enqueue process_basic with a 60-second delay."""
        for record in self:
            record.with_delay(channel="demo", eta=60).process_basic()
            record.state = "queued"

    def action_enqueue_deduplicated(self):
        """Enqueue process_basic with identity_key for deduplication."""
        for record in self:
            record.with_delay(
                channel="demo",
                identity_key=f"demo_task_{record.id}",
            ).process_basic()
            record.state = "queued"

    def action_enqueue_retry_demo(self):
        """Enqueue process_unreliable with max_retries=5."""
        for record in self:
            record.with_delay(channel="demo", max_retries=5).process_unreliable()
            record.state = "queued"

    def action_enqueue_always_fails(self):
        """Enqueue process_always_fails with max_retries=2."""
        for record in self:
            record.with_delay(channel="demo", max_retries=2).process_always_fails()
            record.state = "queued"

    def action_enqueue_chain(self):
        """Enqueue a two-step chain: step_one then step_two."""
        for record in self:
            step_one = record.delayable(channel="demo").process_step_one()
            step_two = record.delayable(channel="demo").process_step_two()
            step_one.on_done(step_two)
            step_one.delay()
            record.state = "queued"

    def action_create_bulk(self):
        """Create a kitchen-sink batch of mixed job types."""
        from odoo.addons.job_worker.delay import chain, group

        DemoTask = self.env["demo.task"]

        # --- 5 basic jobs on demo channel, varied priorities ---
        for i, priority in enumerate([1, 5, 10, 15, 20], start=1):
            task = DemoTask.create({"name": f"Basic p={priority} #{i}"})
            task.with_delay(channel="demo", priority=priority).process_basic()
            task.state = "queued"

        # --- 3 slow jobs on the throttled channel (different durations) ---
        for i, duration in enumerate([2, 5, 8], start=1):
            task = DemoTask.create({"name": f"Slow {duration}s #{i}"})
            task.with_delay(channel="demo_throttled").process_slow(duration=duration)
            task.state = "queued"

        # --- 4 bulk jobs (rate-limited channel) ---
        for i in range(1, 5):
            task = DemoTask.create({"name": f"Bulk #{i}"})
            task.with_delay(channel="demo_bulk").process_basic()
            task.state = "queued"

        # --- 2 two-step chains (parent → waiting child) ---
        for i in range(1, 3):
            task = DemoTask.create({"name": f"Chain #{i}"})
            step_one = task.delayable(channel="demo").process_step_one()
            step_two = task.delayable(channel="demo").process_step_two()
            step_one.on_done(step_two)
            step_one.delay()
            task.state = "queued"

        # --- 1 three-step chain using chain() ---
        task_chain = DemoTask.create({"name": "Long Chain"})
        chain(
            task_chain.delayable(channel="demo").process_step_one(),
            task_chain.delayable(channel="demo").process_basic(),
            task_chain.delayable(channel="demo").process_step_two(),
        ).delay()
        task_chain.state = "queued"

        # --- 1 group of 3 parallel jobs ---
        task_group = DemoTask.create({"name": "Parallel Group"})
        group(
            task_group.delayable(channel="demo", priority=1).process_basic(),
            task_group.delayable(channel="demo", priority=5).process_basic(),
            task_group.delayable(channel="demo", priority=10).process_basic(),
        ).delay()
        task_group.state = "queued"

        # --- 2 unreliable jobs (will retry) ---
        for i in range(1, 3):
            task = DemoTask.create({"name": f"Unreliable #{i}"})
            task.with_delay(channel="demo", max_retries=5).process_unreliable()
            task.state = "queued"

        # --- 1 always-fails job ---
        task_fail = DemoTask.create({"name": "Always Fails"})
        task_fail.with_delay(channel="demo", max_retries=2).process_always_fails()
        task_fail.state = "queued"

        # --- 1 delayed job (starts 30s from now) ---
        task_delayed = DemoTask.create({"name": "Delayed 30s"})
        task_delayed.with_delay(channel="demo", eta=30).process_basic()
        task_delayed.state = "queued"

    def action_enqueue_selected(self):
        """Enqueue all selected tasks (from list view multi-select)."""
        for record in self:
            if record.state == "draft":
                record.with_delay(channel="demo").process_basic()
                record.state = "queued"

    def action_reset_to_draft(self):
        """Reset task to draft state, clearing execution data."""
        for record in self:
            record.write(
                {
                    "state": "draft",
                    "log": False,
                    "completed_at": False,
                }
            )
