import datetime
import json
import logging
import traceback
from uuid import uuid4

from psycopg2 import IntegrityError

from odoo import api, fields, models
from odoo.exceptions import ValidationError

from ..exception import RetryableJobError
from ..job import DEFAULT_MAX_RETRIES, DEFAULT_PRIORITY, DEFAULT_TIMEOUT

_logger = logging.getLogger(__name__)


class QueueJob(models.Model):
    _name = "queue.job"
    _description = "Queue Job"
    _order = "priority ASC, scheduled_at ASC NULLS LAST, id ASC"

    payload = fields.Json(
        string="Execution Payload",
        required=True,
        help="Full execution context: model, method, args, kwargs.",
    )

    state = fields.Selection(
        [
            ("waiting", "Waiting"),
            ("pending", "Pending"),
            ("started", "Started"),
            ("done", "Done"),
            ("failed", "Failed"),
            ("cancelled", "Cancelled"),
        ],
        string="State",
        default="pending",
        index=True,
        required=True,
    )

    priority = fields.Integer(
        string="Priority",
        default=DEFAULT_PRIORITY,
        index=True,
        help="Lower is higher priority.",
    )
    scheduled_at = fields.Datetime(
        string="Scheduled At",
        index=True,
        help="Job visible only after this time.",
    )
    eta = fields.Datetime(
        string="Execute only after",
        compute="_compute_eta",
        inverse="_inverse_eta",
        search="_search_eta",
    )

    channel = fields.Char(
        string="Channel",
        index=True,
        help="String tag for throttling (e.g., 'root', 'export').",
    )

    attempts = fields.Integer(string="Attempts", default=0, readonly=True)
    retry = fields.Integer(
        string="Current try",
        compute="_compute_retry",
        inverse="_inverse_retry",
        search="_search_retry",
    )
    max_retries = fields.Integer(
        string="Max Retries",
        default=DEFAULT_MAX_RETRIES,
        help="A value of 0 means infinite retries.",
    )
    timeout = fields.Integer(
        string="Timeout (seconds)",
        default=DEFAULT_TIMEOUT,
        help="Maximum execution time per attempt. 0 means no timeout.",
    )

    exc_info = fields.Text(string="Exception Info", readonly=True)
    heartbeat = fields.Datetime(
        string="Heartbeat",
        index=True,
        help="Last time the worker updated this job.",
    )
    worker_id = fields.Char(
        string="Worker ID",
        index=True,
        help="ID of the worker processing this job.",
    )

    identity_key = fields.Char(
        string="Identity Key",
        index=True,
        help="Hash to prevent duplicate jobs.",
    )

    started_at = fields.Datetime(
        string="Started At",
        readonly=True,
        help="Timestamp when the worker began executing this job.",
    )
    completed_at = fields.Datetime(
        string="Completed At",
        readonly=True,
        help="Timestamp when job reached done or failed state.",
    )
    cancelled_at = fields.Datetime(
        string="Cancelled At",
        readonly=True,
        help="Timestamp when job was cancelled.",
    )

    # Aliases for OCA queue_job field names
    date_created = fields.Datetime(
        string="Created Date",
        compute="_compute_date_created",
        search="_search_date_created",
    )
    date_started = fields.Datetime(
        string="Start Date",
        compute="_compute_date_started",
        inverse="_inverse_date_started",
        search="_search_date_started",
    )
    date_done = fields.Datetime(
        string="Date Done",
        compute="_compute_date_done",
        inverse="_inverse_date_done",
        search="_search_date_done",
    )
    exec_time = fields.Float(
        string="Execution Time",
        compute="_compute_exec_time",
        inverse="_inverse_exec_time",
        search="_search_exec_time",
    )
    date_cancelled = fields.Datetime(
        string="Date Cancelled",
        compute="_compute_date_cancelled",
        inverse="_inverse_date_cancelled",
        search="_search_date_cancelled",
    )
    duration = fields.Float(
        string="Duration (seconds)",
        readonly=True,
        help="Execution time in seconds (completed_at - started_at).",
    )

    result = fields.Json(
        string="Result",
        readonly=True,
        help="Return value from job execution, if any.",
    )
    result_display = fields.Text(
        string="Result (formatted)",
        compute="_compute_result_display",
    )

    parent_id = fields.Many2one(
        "queue.job",
        string="Parent Job",
        readonly=True,
        index=True,
        ondelete="set null",
        help="Previous step in a chain. Set by on_done() for linear chains.",
    )
    child_ids = fields.One2many(
        "queue.job",
        "parent_id",
        string="Dependent Jobs",
        readonly=True,
    )
    graph_uuid = fields.Char(
        string="Graph UUID",
        readonly=True,
        index=True,
        help="Groups related jobs that were enqueued together.",
    )
    dependency_job_ids = fields.Json(
        string="Dependency Job IDs",
        readonly=True,
        help="List of job IDs that must all complete before this job runs. "
        "Used by group().on_done() for multi-parent barriers.",
    )
    pending_dependency_count = fields.Integer(
        string="Pending Dependencies",
        default=0,
        readonly=True,
        help="Number of parent jobs that must still complete. "
        "Job transitions from waiting to pending when this reaches 0.",
    )
    run_on_failure = fields.Boolean(
        string="Run on Failure",
        default=False,
        readonly=True,
        index=True,
        help="When True, this job runs only if its parent / dependencies "
        "fail. On success it is auto-cancelled. Used by on_error() callbacks "
        "to perform cleanup (e.g. clearing locks) regardless of outcome.",
    )

    payload_display = fields.Text(
        string="Payload (formatted)",
        compute="_compute_payload_display",
    )

    model_name = fields.Char(
        string="Model Name",
        compute="_compute_payload_fields",
        store=True,
        index=True,
    )
    method_name = fields.Char(
        string="Method Name",
        compute="_compute_payload_fields",
        store=True,
        index=True,
    )
    func_string = fields.Char(
        string="Function",
        compute="_compute_payload_fields",
        store=True,
    )
    record_ids = fields.Json(
        string="Record IDs",
        compute="_compute_payload_fields",
        store=True,
    )
    name = fields.Char(string="Description")

    channel_id = fields.Many2one(
        "queue.job.channel",
        string="Channel (filter)",
        compute="_compute_channel_id",
        store=True,
        index=True,
    )
    function_id = fields.Many2one(
        "queue.job.function",
        string="Function (filter)",
        compute="_compute_function_id",
        store=True,
        index=True,
    )
    duration_bucket = fields.Selection(
        [
            ("instant", "< 1s"),
            ("fast", "1 – 10s"),
            ("moderate", "10 – 60s"),
            ("slow", "1 – 5 min"),
            ("very_slow", "> 5 min"),
        ],
        string="Duration",
        compute="_compute_duration_bucket",
        store=True,
        index=True,
    )

    uuid = fields.Char(
        string="UUID",
        readonly=True,
        index=True,
        default=lambda self: str(uuid4()),
        copy=False,
    )

    user_id = fields.Many2one(
        "res.users", string="User", default=lambda self: self.env.user
    )
    company_id = fields.Many2one(
        "res.company",
        string="Company",
        default=lambda self: self.env.company,
    )

    def init(self):
        self.env.cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS queue_job_identity_key_active_uniq
            ON queue_job (identity_key)
            WHERE state IN ('waiting', 'pending', 'started')
        """)
        self.env.cr.execute("""
            CREATE INDEX IF NOT EXISTS queue_job_completed_at_idx
            ON queue_job (completed_at) WHERE completed_at IS NOT NULL
        """)
        self.env.cr.execute("""
            CREATE INDEX IF NOT EXISTS queue_job_channel_completed_idx
            ON queue_job (channel, completed_at) WHERE completed_at IS NOT NULL
        """)
        self.env.cr.execute("""
            CREATE INDEX IF NOT EXISTS queue_job_dependency_ids_gin
            ON queue_job USING gin (dependency_job_ids jsonb_path_ops)
            WHERE dependency_job_ids IS NOT NULL
        """)

    def _compute_display_name(self):
        for record in self:
            payload = record.payload
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    payload = None
            if isinstance(payload, dict):
                model = payload.get("model", "?")
                method = payload.get("method", "?")
                ids = payload.get("ids", [])
                record.display_name = f"{model}.{method} {ids}"
            else:
                record.display_name = f"Job #{record.id}"

    _MAX_RESULT_SIZE = 65_536  # 64 KB

    @api.depends("result")
    def _compute_result_display(self):
        for record in self:
            if record.result:
                result = record.result
                if isinstance(result, str):
                    try:
                        result = json.loads(result)
                    except (json.JSONDecodeError, ValueError):
                        pass
                record.result_display = json.dumps(result, indent=2)
            else:
                record.result_display = ""

    def _serialize_result(self, result):
        """Serialize a job return value to JSON-safe form."""
        if result is None:
            return None
        try:
            from .job_serialization import JobEncoder

            serialized = json.dumps(result, cls=JobEncoder)
            if len(serialized) > self._MAX_RESULT_SIZE:
                return {
                    "__truncated__": True,
                    "__repr__": repr(result)[:1024],
                    "__size_bytes__": len(serialized),
                }
            return json.loads(serialized)
        except Exception:
            try:
                return {"__repr__": repr(result)[:1024]}
            except Exception:
                return {"__repr__": "<unserializable>"}

    @api.depends("payload")
    def _compute_payload_display(self):
        for record in self:
            if record.payload:
                payload = record.payload
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (json.JSONDecodeError, ValueError):
                        pass
                record.payload_display = json.dumps(payload, indent=2)
            else:
                record.payload_display = ""

    @api.depends("payload")
    def _compute_payload_fields(self):
        for record in self:
            payload = record.payload
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except (json.JSONDecodeError, ValueError):
                    payload = None
            if isinstance(payload, dict):
                model = payload.get("model", "")
                method = payload.get("method", "")
                ids = payload.get("ids", [])
                record.model_name = model
                record.method_name = method
                record.record_ids = ids
                record.func_string = f"{model}.{method}({ids})"
            else:
                record.model_name = False
                record.method_name = False
                record.record_ids = False
                record.func_string = False

    def _get_or_create_lookup(self, model_name, name, cache):
        """Find or create a lookup record, handling concurrent inserts."""
        Model = self.env[model_name].sudo()
        if name not in cache:
            record = Model.search([("name", "=", name)], limit=1)
            if not record:
                try:
                    with self.env.cr.savepoint():
                        record = Model.create({"name": name})
                        record.flush_recordset()
                except IntegrityError:
                    record = Model.search([("name", "=", name)], limit=1)
            cache[name] = record.id if record else False
        return cache[name]

    @api.depends("channel")
    def _compute_channel_id(self):
        cache = {}
        for record in self:
            if not record.channel:
                record.channel_id = False
                continue
            record.channel_id = self._get_or_create_lookup(
                "queue.job.channel", record.channel, cache
            )

    @api.depends("model_name", "method_name")
    def _compute_function_id(self):
        cache = {}
        for record in self:
            if not record.model_name or not record.method_name:
                record.function_id = False
                continue
            name = f"{record.model_name}.{record.method_name}"
            record.function_id = self._get_or_create_lookup(
                "queue.job.function", name, cache
            )

    @api.depends("duration")
    def _compute_duration_bucket(self):
        for record in self:
            duration = record.duration
            if duration is None or duration is False:
                record.duration_bucket = False
            elif duration < 1:
                record.duration_bucket = "instant"
            elif duration < 10:
                record.duration_bucket = "fast"
            elif duration < 60:
                record.duration_bucket = "moderate"
            elif duration < 300:
                record.duration_bucket = "slow"
            else:
                record.duration_bucket = "very_slow"

    @api.constrains("identity_key", "state")
    def _check_identity_key_state_unique(self):
        for record in self:
            if not record.identity_key:
                continue
            duplicate = self.search(
                [
                    ("id", "!=", record.id),
                    ("identity_key", "=", record.identity_key),
                    ("state", "in", ["waiting", "pending", "started"]),
                ],
                limit=1,
            )
            if duplicate:
                raise ValidationError(
                    "Job with this identity key already exists in the same state."
                )

    @api.depends("scheduled_at")
    def _compute_eta(self):
        for record in self:
            record.eta = record.scheduled_at

    def _inverse_eta(self):
        for record in self:
            record.scheduled_at = record.eta

    def _search_eta(self, operator, value):
        return [("scheduled_at", operator, value)]

    @api.depends("attempts")
    def _compute_retry(self):
        for record in self:
            record.retry = record.attempts

    def _inverse_retry(self):
        for record in self:
            record.attempts = record.retry

    def _search_retry(self, operator, value):
        return [("attempts", operator, value)]

    # Alias: date_created -> create_date
    @api.depends("create_date")
    def _compute_date_created(self):
        for record in self:
            record.date_created = record.create_date

    def _search_date_created(self, operator, value):
        return [("create_date", operator, value)]

    # Alias: date_started -> started_at
    @api.depends("started_at")
    def _compute_date_started(self):
        for record in self:
            record.date_started = record.started_at

    def _inverse_date_started(self):
        for record in self:
            record.started_at = record.date_started

    def _search_date_started(self, operator, value):
        return [("started_at", operator, value)]

    # Alias: date_done -> completed_at
    @api.depends("completed_at")
    def _compute_date_done(self):
        for record in self:
            record.date_done = record.completed_at

    def _inverse_date_done(self):
        for record in self:
            record.completed_at = record.date_done

    def _search_date_done(self, operator, value):
        return [("completed_at", operator, value)]

    # Alias: exec_time -> duration
    @api.depends("duration")
    def _compute_exec_time(self):
        for record in self:
            record.exec_time = record.duration

    def _inverse_exec_time(self):
        for record in self:
            record.duration = record.exec_time

    def _search_exec_time(self, operator, value):
        return [("duration", operator, value)]

    # Alias: date_cancelled -> cancelled_at
    @api.depends("cancelled_at")
    def _compute_date_cancelled(self):
        for record in self:
            record.date_cancelled = record.cancelled_at

    def _inverse_date_cancelled(self):
        for record in self:
            record.cancelled_at = record.date_cancelled

    def _search_date_cancelled(self, operator, value):
        return [("cancelled_at", operator, value)]

    def _release_dependents(self):
        """Release multi-parent dependents (group barriers) when this job completes.

        on_done dependents (run_on_failure=False) transition waiting → pending
        once their dependency count reaches zero. on_error dependents
        (run_on_failure=True) are cancelled instead, since no failure occurred.
        """
        self.env.flush_all()
        for job in self:
            self.env.cr.execute(
                """
                UPDATE queue_job
                SET pending_dependency_count = pending_dependency_count - 1,
                    state = CASE
                        WHEN pending_dependency_count - 1 <= 0
                             AND run_on_failure
                            THEN 'cancelled'
                        WHEN pending_dependency_count - 1 <= 0
                            THEN 'pending'
                        ELSE state
                    END,
                    cancelled_at = CASE
                        WHEN pending_dependency_count - 1 <= 0
                             AND run_on_failure
                            THEN NOW()
                        ELSE cancelled_at
                    END,
                    write_date = NOW()
                WHERE state = 'waiting'
                  AND dependency_job_ids IS NOT NULL
                  AND dependency_job_ids @> (%s)::jsonb
                """,
                (json.dumps([job.id]),),
            )
        self.env.invalidate_all()

    def _fail_dependents(self):
        """Cascade failure to multi-parent dependents.

        on_done dependents (run_on_failure=False) cascade to failed.
        on_error dependents (run_on_failure=True) are promoted to pending so
        their failure-handler runs.
        """
        self.env.flush_all()
        for job in self:
            self.env.cr.execute(
                """
                UPDATE queue_job
                SET state = CASE
                        WHEN run_on_failure THEN 'pending'
                        ELSE 'failed'
                    END,
                    exc_info = CASE
                        WHEN run_on_failure THEN exc_info
                        ELSE %s
                    END,
                    write_date = NOW()
                WHERE state = 'waiting'
                  AND dependency_job_ids IS NOT NULL
                  AND dependency_job_ids @> (%s)::jsonb
                """,
                (f"Parent job {job.id} failed", json.dumps([job.id])),
            )
        self.env.invalidate_all()

    def _cancel_dependents(self):
        """Cascade cancellation to multi-parent dependents.

        A cancelled job never runs, so it produces neither success nor failure:
        ``_release_dependents`` will never decrement its dependents' counters and
        ``_fail_dependents`` never fires for it. Left alone, every dependent waits
        on a condition that can no longer occur.

        Cancellation propagates to BOTH dispositions, unlike failure. on_error
        dependents are deliberately NOT promoted to pending: their handler exists
        to react to a failure, and nothing failed here. Running it on a
        cancellation would invent an error that never happened.
        """
        self.env.flush_all()
        cancelled_ids = []
        for job in self:
            self.env.cr.execute(
                """
                UPDATE queue_job
                SET state = 'cancelled',
                    cancelled_at = NOW(),
                    write_date = NOW()
                WHERE state = 'waiting'
                  AND dependency_job_ids IS NOT NULL
                  AND dependency_job_ids @> (%s)::jsonb
                RETURNING id
                """,
                (json.dumps([job.id]),),
            )
            cancelled_ids.extend(row[0] for row in self.env.cr.fetchall())
        self.env.invalidate_all()
        if cancelled_ids:
            # Recurse across BOTH axes, or the strand this method closes simply
            # reappears one hop away — reachable through the public API:
            # ``group(a, b).on_done(chain(x, y))`` cancels the chain head ``x``
            # here and leaves ``y`` (its ``parent_id`` child) waiting forever,
            # and a nested group's inner barrier strands the same way.
            #
            # Termination is guaranteed without a visited set: every transition
            # is waiting -> cancelled and the predicates match only ``waiting``
            # rows, so each job can be walked at most once and even cyclic
            # dependency data cannot loop.
            newly = self.browse(cancelled_ids)
            for job in newly:
                job._cascade_children_on_parent_cancelled()
            newly._cancel_dependents()

    def _cascade_children_on_parent_cancelled(self):
        """Cancel waiting ``parent_id`` descendants when this parent is cancelled.

        Distinct from both sibling cascades, because a cancelled parent is neither
        outcome the chain was wired for:

        * ``_cascade_children_on_parent_success`` promotes on_done children to
          ``pending`` — wrong here, since the parent never ran and its successor
          must not run either;
        * ``_cascade_children_on_parent_failure`` marks them ``failed`` with an
          ``exc_info`` naming a failure that did not occur.

        Both dispositions therefore cancel, and the walk continues downward: a
        cancelled child never runs either, so it cannot cascade on its own behalf
        and its descendants would be orphaned in ``waiting`` exactly as before.
        """
        self.ensure_one()
        now = fields.Datetime.now()
        queue = list(self.child_ids.filtered(lambda c: c.state == "waiting"))
        while queue:
            job = queue.pop(0)
            job.write({"state": "cancelled", "cancelled_at": now})
            queue.extend(job.child_ids.filtered(lambda c: c.state == "waiting"))
            # Cross to the dependency axis too: a barrier waiting on this chain
            # link would otherwise strand when the chain is cancelled upstream.
            job._cancel_dependents()

    def button_requeue(self):
        for job in self:
            if job.identity_key:
                duplicate = self.search(
                    [
                        ("id", "!=", job.id),
                        ("identity_key", "=", job.identity_key),
                        ("state", "in", ["waiting", "pending", "started"]),
                    ],
                    limit=1,
                )
                if duplicate:
                    continue
            job.write(
                {
                    "state": "pending",
                    "attempts": 0,
                    "exc_info": False,
                    "result": False,
                    "scheduled_at": False,
                    "worker_id": False,
                    "heartbeat": False,
                    "started_at": False,
                    "completed_at": False,
                    "cancelled_at": False,
                    "duration": False,
                }
            )
            self.env.cr.execute("NOTIFY queue_job_wake_up")

    def button_set_to_done(self):
        now = fields.Datetime.now()
        for job in self:
            vals = {"state": "done", "completed_at": now}
            if job.started_at:
                vals["duration"] = (
                    now - fields.Datetime.to_datetime(job.started_at)
                ).total_seconds()
            job.write(vals)
        self._release_dependents()

    def button_set_to_failed(self):
        now = fields.Datetime.now()
        for job in self:
            vals = {"state": "failed", "completed_at": now}
            if job.started_at:
                vals["duration"] = (
                    now - fields.Datetime.to_datetime(job.started_at)
                ).total_seconds()
            job.write(vals)
            job._cascade_children_on_parent_failure()
        self._fail_dependents()

    def _cascade_children_on_parent_success(self):
        """Release waiting parent_id descendants when this parent completes.

        on_done descendants (run_on_failure=False) move waiting → pending and
        will cascade themselves when they later run.
        on_error descendants (run_on_failure=True) are cancelled (no failure
        occurred). Cancelled jobs never run, so we walk the chain and
        propagate cancellation to grandchildren — otherwise they would stay
        stuck in 'waiting' forever.
        """
        self.ensure_one()
        now = fields.Datetime.now()
        # BFS over jobs that transition to a *terminal* state without running.
        # Jobs we transition to 'pending' are not enqueued for further walks
        # because they will run normally and cascade themselves.
        cancelled_queue = []
        for child in self.child_ids.filtered(lambda c: c.state == "waiting"):
            if child.run_on_failure:
                child.write({"state": "cancelled", "cancelled_at": now})
                cancelled_queue.append(child)
            else:
                child.state = "pending"
        while cancelled_queue:
            job = cancelled_queue.pop(0)
            for grandchild in job.child_ids.filtered(lambda c: c.state == "waiting"):
                # Parent (the cancelled job) never produced success or failure,
                # so neither on_done nor on_error firing is appropriate. Cancel
                # downward.
                grandchild.write({"state": "cancelled", "cancelled_at": now})
                cancelled_queue.append(grandchild)

    def _cascade_children_on_parent_failure(self):
        """Cascade waiting parent_id descendants when this parent fails.

        on_done descendants (run_on_failure=False) cascade to failed and the
        chain is walked so grandchildren also fail (the cascade-failed job
        never runs to trigger its own descendants).
        on_error descendants (run_on_failure=True) are promoted to pending —
        they will run normally and cascade themselves when they finish.
        """
        self.ensure_one()
        # BFS over jobs that transition to 'failed' without running. Jobs we
        # transition to 'pending' (on_error) will run normally and cascade
        # themselves, so we don't enqueue them.
        failed_queue = []
        for child in self.child_ids.filtered(lambda c: c.state == "waiting"):
            if child.run_on_failure:
                child.state = "pending"
            else:
                child.state = "failed"
                child.exc_info = f"Parent job {self.id} failed"
                failed_queue.append(child)
        while failed_queue:
            job = failed_queue.pop(0)
            for grandchild in job.child_ids.filtered(lambda c: c.state == "waiting"):
                if grandchild.run_on_failure:
                    grandchild.state = "pending"
                else:
                    grandchild.state = "failed"
                    grandchild.exc_info = f"Parent job {job.id} failed"
                    failed_queue.append(grandchild)

    def button_cancelled(self):
        """Cancel these jobs, and everything left waiting on them.

        The cascade is not optional bookkeeping. A cancelled job never reaches
        ``_release_dependents`` (only completion calls it) and never reaches
        ``_fail_dependents`` (only failure does), so anything waiting on it waits
        on an event that can no longer happen. For a ``group(...).on_done(barrier)``
        that is permanent: the barrier holds ``pending_dependency_count > 0``
        forever, in a state no failure surface looks at — it is not ``failed``, not
        ``cancelled``, and will never become ``pending``.

        It also blocks its own recovery. ``enqueue`` dedupes ``identity_key``
        against ``('waiting', 'pending', 'started')``, so re-dispatching the graph
        returns the stranded barrier instead of a fresh one, and the new wave is
        wired onto a barrier that already carries an undecrementable dependency.
        """
        now = fields.Datetime.now()
        # Skip jobs that already reached a terminal state. Without this a call on
        # a ``done`` group member would cancel a HEALTHY barrier: the barrier's
        # ``dependency_job_ids`` still lists that member (``_release_dependents``
        # decrements the counter but never removes the id), so the ``@>``
        # predicate matches and the cascade fires while the other members are
        # still running. Before the cascade existed this call was a harmless
        # no-op, which is why the guard was not needed until now.
        cancellable = self.filtered(
            lambda job: job.state not in ("done", "failed", "cancelled")
        )
        for job in cancellable:
            job.write({"state": "cancelled", "cancelled_at": now})
            job._cascade_children_on_parent_cancelled()
        cancellable._cancel_dependents()

    def open_related_action(self):
        """Open the record(s) targeted by this job."""
        self.ensure_one()
        record_ids = self.payload.get("ids", []) if self.payload else []
        if not record_ids:
            return False
        action = {
            "type": "ir.actions.act_window",
            "res_model": self.model_name,
            "view_mode": "form",
        }
        if len(record_ids) == 1:
            action["res_id"] = record_ids[0]
        else:
            action["domain"] = [("id", "in", record_ids)]
            action["view_mode"] = "list,form"
        return action

    def run_now(self):
        """Execute the job immediately in the current transaction."""
        from .job_serialization import JobDecoder

        for job in self:
            start_time = fields.Datetime.now()
            job.started_at = start_time
            try:
                raw_payload = json.dumps(job.payload)
                payload = json.loads(raw_payload, cls=JobDecoder, env=self.env)
                model_name = payload["model"]
                method_name = payload["method"]
                args = payload.get("args", [])
                kwargs = payload.get("kwargs", {})
                record_ids = payload.get("ids") or []

                if record_ids:
                    records = self.env[model_name].browse(record_ids)
                    existing_ids = set(records.exists().ids)
                    missing_ids = [
                        record_id
                        for record_id in record_ids
                        if record_id not in existing_ids
                    ]
                    if missing_ids:
                        raise ValueError(
                            f"Record(s) {missing_ids} not found in model {model_name}"
                        )
                    result = getattr(records, method_name)(*args, **kwargs)
                else:
                    result = getattr(self.env[model_name], method_name)(*args, **kwargs)

                try:
                    job.result = job._serialize_result(result)
                except Exception:
                    pass
                job.state = "done"
                end_time = fields.Datetime.now()
                job.completed_at = end_time
                job.duration = (end_time - start_time).total_seconds()
                job._cascade_children_on_parent_success()
                # Release multi-parent dependents (group barriers)
                job._release_dependents()
            except RetryableJobError as err:
                job.exc_info = traceback.format_exc()
                if not err.ignore_retry:
                    job.attempts += 1
                if err.seconds is not None:
                    delay_seconds = err.seconds
                else:
                    delay_seconds = min(
                        10 * (2 ** max(job.attempts - 1, 0)),
                        3600,
                    )
                job.scheduled_at = fields.Datetime.now() + datetime.timedelta(
                    seconds=delay_seconds
                )
                job.state = "pending"
            except Exception:
                job.state = "failed"
                job.exc_info = traceback.format_exc()
                end_time = fields.Datetime.now()
                job.completed_at = end_time
                job.duration = (end_time - start_time).total_seconds()
                job._cascade_children_on_parent_failure()
                # Cascade failure to multi-parent dependents (group barriers)
                job._fail_dependents()
                raise

    @api.model
    def _normalize_eta(self, eta):
        if eta is None:
            return None
        if isinstance(eta, datetime.timedelta):
            return fields.Datetime.now() + eta
        if isinstance(eta, int | float):
            return fields.Datetime.now() + datetime.timedelta(seconds=eta)
        return fields.Datetime.to_datetime(eta)

    @api.model
    def enqueue(
        self,
        model_name,
        method_name,
        record_ids,
        args,
        kwargs,
        channel="root",
        priority=None,
        max_retries=None,
        scheduled_at=None,
        eta=None,
        description=None,
        identity_key=None,
        parent_id=None,
        graph_uuid=None,
        timeout=None,
        dependency_job_ids=None,
        run_on_failure=False,
    ):
        """Public API to enqueue a job with queue_job-compatible options."""
        from .job_serialization import JobEncoder

        if not model_name or model_name not in self.env:
            raise ValueError(f"Model {model_name!r} does not exist in the registry")
        if method_name and method_name.startswith("__") and method_name.endswith("__"):
            raise ValueError(
                f"Dunder method {method_name!r} is not allowed in job queue"
            )
        identity_key = identity_key or None

        if eta is None and scheduled_at is not None:
            eta = scheduled_at
        if (
            eta is not None
            and scheduled_at is not None
            and fields.Datetime.to_datetime(eta)
            != fields.Datetime.to_datetime(scheduled_at)
        ):
            raise ValueError(
                "Provide either eta or scheduled_at, not conflicting values for both."
            )

        priority = DEFAULT_PRIORITY if priority is None else priority
        max_retries = DEFAULT_MAX_RETRIES if max_retries is None else max_retries
        timeout = DEFAULT_TIMEOUT if timeout is None else timeout
        channel = channel or "root"

        if identity_key:
            existing = self.search(
                [
                    ("identity_key", "=", identity_key),
                    ("state", "in", ["waiting", "pending", "started"]),
                ],
                limit=1,
            )
            if existing:
                return existing

        payload = {
            "model": model_name,
            "method": method_name,
            "ids": list(record_ids or []),
            "args": list(args or []),
            "kwargs": dict(kwargs or {}),
        }
        payload_serialized = json.loads(json.dumps(payload, cls=JobEncoder))
        has_explicit_company_none = (
            "company_id" in self.env.context
            and self.env.context.get("company_id") is None
        )
        company_id = False if has_explicit_company_none else self.env.company.id

        vals = {
            "payload": payload_serialized,
            "state": "waiting" if (parent_id or dependency_job_ids) else "pending",
            "priority": priority,
            "channel": channel,
            "max_retries": max_retries,
            "timeout": timeout,
            "scheduled_at": self._normalize_eta(eta),
            "identity_key": identity_key,
            "company_id": company_id,
            "parent_id": parent_id,
            "graph_uuid": graph_uuid,
            "name": description or False,
            "dependency_job_ids": dependency_job_ids,
            "pending_dependency_count": (
                len(dependency_job_ids) if dependency_job_ids else 0
            ),
            "run_on_failure": bool(run_on_failure),
        }
        store_values = self.env[model_name]._job_store_values(vals)
        vals.update(store_values)
        try:
            with self.env.cr.savepoint():
                job = self.create(vals)
        except IntegrityError:
            if not identity_key:
                raise
            existing = self.search(
                [
                    ("identity_key", "=", identity_key),
                    ("state", "in", ["waiting", "pending", "started"]),
                ],
                limit=1,
            )
            if existing:
                return existing
            raise
        self.env.cr.execute("NOTIFY queue_job_wake_up")
        return job

    @api.autovacuum
    def _gc_old_jobs(self):
        """Delete done/failed/cancelled jobs older than the configured retention.

        Reads the retention from ir.config_parameter key
        ``job_worker.done_job_retention_days`` (default 30).  Deletes in
        chunks of 1000 to avoid long-held locks.
        """
        icp = self.env["ir.config_parameter"].sudo()
        retention_days = int(icp.get_param("job_worker.done_job_retention_days", "30"))
        cutoff = fields.Datetime.now() - datetime.timedelta(days=retention_days)
        chunk_size = 1000
        while True:
            jobs = self.sudo().search(
                [
                    "|",
                    "&",
                    ("state", "in", ["done", "failed"]),
                    ("completed_at", "<=", cutoff),
                    "&",
                    ("state", "=", "cancelled"),
                    ("cancelled_at", "<=", cutoff),
                ],
                limit=chunk_size,
            )
            if not jobs:
                break
            count = len(jobs)
            jobs.unlink()
            _logger.info(
                "Autovacuum: deleted %d old queue jobs (cutoff=%s)", count, cutoff
            )
            if count < chunk_size:
                break
            self.env.cr.commit()
