import unittest

from ..exception import FailedJobError, JobError, RetryableJobError, TimeoutJobError


class TestExceptionHierarchy(unittest.TestCase):
    def test_job_error_is_exception(self):
        self.assertTrue(issubclass(JobError, Exception))

    def test_failed_job_error_is_job_error(self):
        self.assertTrue(issubclass(FailedJobError, JobError))

    def test_retryable_job_error_is_job_error(self):
        self.assertTrue(issubclass(RetryableJobError, JobError))

    def test_retryable_defaults(self):
        err = RetryableJobError("oops")
        self.assertEqual(str(err), "oops")
        self.assertIsNone(err.seconds)
        self.assertFalse(err.ignore_retry)

    def test_retryable_with_seconds(self):
        err = RetryableJobError("retry me", seconds=30)
        self.assertEqual(err.seconds, 30)
        self.assertFalse(err.ignore_retry)

    def test_retryable_with_ignore_retry(self):
        err = RetryableJobError("soft fail", ignore_retry=True)
        self.assertIsNone(err.seconds)
        self.assertTrue(err.ignore_retry)

    def test_retryable_with_all_params(self):
        err = RetryableJobError("full", seconds=60, ignore_retry=True)
        self.assertEqual(str(err), "full")
        self.assertEqual(err.seconds, 60)
        self.assertTrue(err.ignore_retry)

    def test_failed_job_error_message(self):
        err = FailedJobError("permanent failure")
        self.assertEqual(str(err), "permanent failure")

    def test_retryable_caught_as_job_error(self):
        with self.assertRaises(JobError):
            raise RetryableJobError("catch me")

    def test_failed_caught_as_job_error(self):
        with self.assertRaises(JobError):
            raise FailedJobError("catch me too")

    def test_timeout_job_error_is_job_error(self):
        self.assertTrue(issubclass(TimeoutJobError, JobError))

    def test_timeout_job_error_is_not_retryable(self):
        self.assertFalse(issubclass(TimeoutJobError, RetryableJobError))

    def test_timeout_job_error_message(self):
        err = TimeoutJobError("Job exceeded 300s timeout")
        self.assertEqual(str(err), "Job exceeded 300s timeout")

    def test_timeout_caught_as_job_error(self):
        with self.assertRaises(JobError):
            raise TimeoutJobError("timed out")
