class JobError(Exception):
    """Base exception for job-related errors."""


class FailedJobError(JobError):
    """Raised when a job has permanently failed."""


class RetryableJobError(JobError):
    """Raised inside a job to request a retry.

    :param str msg: human-readable error message
    :param int seconds: fixed retry delay (overrides exponential backoff).
        Falls back to exponential backoff when ``None``.
    :param bool ignore_retry: when ``True`` the current attempt is not
        counted toward ``max_retries``.
    """

    def __init__(self, msg, seconds=None, ignore_retry=False):
        super().__init__(msg)
        self.seconds = seconds
        self.ignore_retry = ignore_retry


class TransientRegistryError(RetryableJobError):
    """Raised when the job's model is absent from the worker's registry.

    Almost always a transient module install/upgrade window: the worker
    reloads the registry every ``registry_check_interval`` (via
    ``registry.check_signaling``), after which the model resolves. Retried
    *without* counting the attempt so a job enqueued during an upgrade waits
    the reload out instead of exhausting ``max_retries`` and failing
    permanently — but bounded by a wall-clock age cap so a genuinely removed
    or renamed model still fails eventually rather than looping forever.
    """


class TimeoutJobError(JobError):
    """Raised when a job exceeds its configured timeout duration."""
