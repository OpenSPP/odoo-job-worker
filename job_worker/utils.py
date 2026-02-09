import os


def must_run_without_delay(env):
    """Return ``True`` when jobs should execute synchronously.

    Checked in two places:

    * Environment variable ``QUEUE_JOB__NO_DELAY``
    * Context key ``queue_job__no_delay``
    """
    if os.environ.get("QUEUE_JOB__NO_DELAY"):
        return True
    if env.context.get("queue_job__no_delay"):
        return True
    return False
