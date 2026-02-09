from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import patch

from odoo import SUPERUSER_ID, api, fields


@contextmanager
def external_env(env, uid=SUPERUSER_ID):
    """Open an isolated cursor/environment pair for multi-transaction tests."""
    with env.registry.cursor() as cr:
        yield cr, api.Environment(cr, uid, {})


def assert_dt_between(testcase, value, lower, upper):
    value_dt = fields.Datetime.to_datetime(value)
    lower_dt = fields.Datetime.to_datetime(lower)
    upper_dt = fields.Datetime.to_datetime(upper)
    testcase.assertTrue(
        lower_dt <= value_dt <= upper_dt,
        f"{value_dt!r} was expected between {lower_dt!r} and {upper_dt!r}",
    )


def now_with_slack(seconds=5):
    now = fields.Datetime.now()
    return now - timedelta(seconds=seconds), now + timedelta(seconds=seconds)


class TrapJobs:
    """Captures enqueued jobs without writing to the database.

    Usage::

        with trap_jobs(env) as trap:
            partner.with_delay().write({"name": "test"})
            trap.assert_jobs_count(1)
            trap.assert_enqueued_job("res.partner", "write")
            trap.perform_enqueued_jobs()
    """

    def __init__(self):
        self.enqueued_jobs = []

    def assert_jobs_count(self, count, model=None, method=None):
        """Assert that exactly *count* jobs were trapped.

        Optionally filter by *model* and/or *method*.
        """
        filtered = self._filter(model, method)
        actual = len(filtered)
        if actual != count:
            descriptions = [
                f"  {j['model_name']}.{j['method_name']}" for j in self.enqueued_jobs
            ]
            raise AssertionError(
                f"Expected {count} job(s)"
                + (f" for {model}.{method}" if model or method else "")
                + f", found {actual}.\nTrapped jobs:\n"
                + "\n".join(descriptions)
            )

    def assert_enqueued_job(
        self, model, method, args=None, kwargs=None, **extra_filters
    ):
        """Assert a specific job exists among the trapped jobs.

        Returns the matching job spec.
        """
        for job_spec in self.enqueued_jobs:
            if job_spec["model_name"] != model:
                continue
            if job_spec["method_name"] != method:
                continue
            if args is not None and list(job_spec.get("args", [])) != list(args):
                continue
            if kwargs is not None and dict(job_spec.get("kwargs", {})) != dict(kwargs):
                continue
            match = True
            for key, value in extra_filters.items():
                if job_spec.get(key) != value:
                    match = False
                    break
            if match:
                return job_spec
        descriptions = [
            f"  {j['model_name']}.{j['method_name']}({j.get('args', [])}, "
            f"{j.get('kwargs', {})})"
            for j in self.enqueued_jobs
        ]
        raise AssertionError(
            f"No trapped job matching {model}.{method}"
            + (f" args={args}" if args is not None else "")
            + (f" kwargs={kwargs}" if kwargs is not None else "")
            + "\nTrapped jobs:\n"
            + "\n".join(descriptions)
        )

    def perform_enqueued_jobs(self):
        """Execute all trapped jobs synchronously."""
        for job_spec in self.enqueued_jobs:
            job = job_spec["job_record"]
            job.run_now()

    def _filter(self, model=None, method=None):
        result = self.enqueued_jobs
        if model:
            result = [j for j in result if j["model_name"] == model]
        if method:
            result = [j for j in result if j["method_name"] == method]
        return result


@contextmanager
def trap_jobs(env):
    """Context manager that patches ``QueueJob.enqueue()`` to capture jobs.

    Jobs are still written to the database (so they get valid IDs and
    computed fields), but they remain in ``pending`` state and are
    collected in the returned :class:`TrapJobs` instance for assertions.

    Usage::

        with trap_jobs(env) as trap:
            partner.with_delay().write({"name": "hello"})
            trap.assert_jobs_count(1)
    """
    trap = TrapJobs()
    QueueJob = type(env["queue.job"])
    original_enqueue = QueueJob.enqueue

    @api.model
    def patched_enqueue(self, **kwargs):
        job = original_enqueue(self, **kwargs)
        trap.enqueued_jobs.append(
            {
                "job_record": job,
                "model_name": kwargs.get("model_name", ""),
                "method_name": kwargs.get("method_name", ""),
                "record_ids": kwargs.get("record_ids", []),
                "args": kwargs.get("args", []),
                "kwargs": kwargs.get("kwargs", {}),
                "priority": kwargs.get("priority"),
                "channel": kwargs.get("channel"),
                "eta": kwargs.get("eta"),
                "max_retries": kwargs.get("max_retries"),
                "description": kwargs.get("description"),
                "identity_key": kwargs.get("identity_key"),
                "timeout": kwargs.get("timeout"),
            }
        )
        return job

    with patch.object(QueueJob, "enqueue", patched_enqueue):
        yield trap
