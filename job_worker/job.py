import hashlib

DEFAULT_PRIORITY = 10
DEFAULT_MAX_RETRIES = 5
DEFAULT_TIMEOUT = 0

WAITING = "waiting"
PENDING = "pending"
STARTED = "started"
DONE = "done"
FAILED = "failed"
CANCELLED = "cancelled"

STATES = [
    (WAITING, "Waiting"),
    (PENDING, "Pending"),
    (STARTED, "Started"),
    (DONE, "Done"),
    (FAILED, "Failed"),
    (CANCELLED, "Cancelled"),
]


def identity_exact_hasher(job_):
    """Prepare hasher object for identity_exact."""
    hasher = hashlib.sha1()
    hasher.update(job_.model_name.encode("utf-8"))
    hasher.update(job_.method_name.encode("utf-8"))
    hasher.update(str(sorted(job_.recordset.ids)).encode("utf-8"))
    hasher.update(str(job_.args).encode("utf-8"))
    hasher.update(str(sorted(job_.kwargs.items())).encode("utf-8"))
    return hasher


def identity_exact(job_):
    """Identity function using model, method and all arguments as key."""
    return identity_exact_hasher(job_).hexdigest()
