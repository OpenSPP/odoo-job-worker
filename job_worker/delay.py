import datetime
import logging
import uuid as _uuid

from .job import DEFAULT_MAX_RETRIES, DEFAULT_PRIORITY, DEFAULT_TIMEOUT
from .utils import must_run_without_delay

_logger = logging.getLogger(__name__)


class Delayable:
    """Minimal upstream-compatible delayable object."""

    _properties = (
        "priority",
        "eta",
        "max_retries",
        "description",
        "channel",
        "identity_key",
        "timeout",
    )

    def __init__(
        self,
        recordset,
        priority=None,
        eta=None,
        max_retries=None,
        description=None,
        channel=None,
        identity_key=None,
        timeout=None,
    ):
        self.recordset = recordset
        self.priority = priority
        self.eta = eta
        self.max_retries = max_retries
        self.description = description
        self.channel = channel
        self.identity_key = identity_key
        self.timeout = timeout
        self._job_method = None
        self._job_args = ()
        self._job_kwargs = {}
        self._generated_job = None
        self._next_delayables = []
        self._graph_uuid = None
        self._parent_job_id = None
        self._dependency_job_ids = None

    def __del__(self):
        try:
            if self._generated_job is None and self._job_method is not None:
                _logger.warning(
                    "Delayable for %s.%s was configured but never delayed. "
                    "Did you forget to call .delay()?",
                    self.recordset._name,
                    self._job_method,
                )
        except Exception:
            pass

    @property
    def model_name(self):
        return self.recordset._name

    @property
    def method_name(self):
        return self._job_method

    @property
    def args(self):
        return self._job_args

    @property
    def kwargs(self):
        return self._job_kwargs

    def _set_from_dict(self, properties):
        for key, value in properties.items():
            if key not in self._properties:
                raise ValueError(f"No property {key}")
            setattr(self, key, value)

    def set(self, *args, **kwargs):
        if args:
            self._set_from_dict(*args)
        self._set_from_dict(kwargs)
        return self

    def _store_args(self, *args, **kwargs):
        self._job_args = args
        self._job_kwargs = kwargs
        return self

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        self._job_method = name
        return self._store_args

    def _normalized_eta(self):
        eta = self.eta
        if eta is None:
            return None
        if isinstance(eta, datetime.timedelta):
            return datetime.datetime.now() + eta
        if isinstance(eta, int | float):
            return datetime.datetime.now() + datetime.timedelta(seconds=eta)
        return eta

    def _resolved_identity_key(self):
        if callable(self.identity_key):
            return self.identity_key(self)
        return self.identity_key

    def delay(self):
        if self._generated_job is not None:
            return self._generated_job

        if not self._job_method:
            raise ValueError("No method set on the Delayable")

        # Only generate graph_uuid for multi-job graphs
        graph_uuid = self._graph_uuid
        if not graph_uuid and self._next_delayables:
            graph_uuid = str(_uuid.uuid4())

        self._generated_job = self.recordset.env["queue.job"].enqueue(
            model_name=self.recordset._name,
            method_name=self._job_method,
            record_ids=self.recordset.ids,
            args=self._job_args,
            kwargs=self._job_kwargs,
            priority=self.priority if self.priority is not None else DEFAULT_PRIORITY,
            max_retries=(
                self.max_retries
                if self.max_retries is not None
                else DEFAULT_MAX_RETRIES
            ),
            eta=self._normalized_eta(),
            channel=self.channel or "root",
            description=self.description,
            identity_key=self._resolved_identity_key(),
            parent_id=self._parent_job_id,
            graph_uuid=graph_uuid,
            timeout=self.timeout if self.timeout is not None else DEFAULT_TIMEOUT,
            dependency_job_ids=self._dependency_job_ids,
        )
        for next_delayable in self._next_delayables:
            next_delayable._graph_uuid = graph_uuid
            next_delayable._parent_job_id = self._generated_job.id
            next_delayable.delay()
        if must_run_without_delay(self.recordset.env):
            self._generated_job.run_now()
        return self._generated_job

    def split(self, size, chain=False):
        """Split the recordset into chunks and return a group or chain.

        :param int size: maximum number of records per chunk.
        :param bool chain: when ``True`` return a :class:`DelayableChain`
            instead of a :class:`DelayableGroup`.
        :returns: :class:`DelayableGroup` or :class:`DelayableChain`
        """
        ids = self.recordset.ids
        model = self.recordset.browse
        chunks = [ids[i : i + size] for i in range(0, len(ids), size)]
        sub_delayables = []
        for chunk_ids in chunks:
            sub = Delayable(
                model(chunk_ids),
                priority=self.priority,
                eta=self.eta,
                max_retries=self.max_retries,
                description=self.description,
                channel=self.channel,
                identity_key=self.identity_key,
                timeout=self.timeout,
            )
            if self._job_method:
                sub._job_method = self._job_method
                sub._job_args = self._job_args
                sub._job_kwargs = self._job_kwargs
            sub_delayables.append(sub)
        if chain:
            return DelayableChain(*sub_delayables)
        return DelayableGroup(*sub_delayables)

    def on_done(self, *delayables):
        self._next_delayables.extend(delayables)
        return self


class DelayableGroup:
    """Upstream-compatible group wrapper.

    All contained delayables are enqueued together.
    """

    def __init__(self, *delayables):
        self._delayables = list(delayables)
        self._next_delayables = []

    def on_done(self, *delayables):
        self._next_delayables.extend(delayables)
        return self

    def delay(self):
        graph_uuid = str(_uuid.uuid4())
        member_jobs = []
        for delayable in self._delayables:
            delayable._graph_uuid = graph_uuid
            job = delayable.delay()
            member_jobs.append(job.id)
        for next_delayable in self._next_delayables:
            next_delayable._graph_uuid = graph_uuid
            next_delayable._dependency_job_ids = list(member_jobs)
            next_delayable.delay()


class DelayableChain:
    """Upstream-compatible chain wrapper.

    All jobs are enqueued immediately. Sequential execution
    is not enforced; use on_done() for explicit ordering.
    """

    def __init__(self, *delayables):
        self._delayables = list(delayables)
        self._next_delayables = []

    def on_done(self, *delayables):
        self._next_delayables.extend(delayables)
        return self

    def delay(self):
        graph_uuid = str(_uuid.uuid4())
        previous_job = None
        for delayable in self._delayables:
            delayable._graph_uuid = graph_uuid
            if previous_job:
                delayable._parent_job_id = previous_job.id
            previous_job = delayable.delay()
        for next_delayable in self._next_delayables:
            next_delayable._graph_uuid = graph_uuid
            if previous_job:
                next_delayable._parent_job_id = previous_job.id
            next_delayable.delay()


class DelayableRecordset:
    """Shortcut object used by ``with_delay()``."""

    def __init__(
        self,
        recordset,
        priority=None,
        eta=None,
        max_retries=None,
        description=None,
        channel=None,
        identity_key=None,
        timeout=None,
    ):
        self.delayable = Delayable(
            recordset,
            priority=priority,
            eta=eta,
            max_retries=max_retries,
            description=description,
            channel=channel,
            identity_key=identity_key,
            timeout=timeout,
        )

    @property
    def recordset(self):
        return self.delayable.recordset

    def __getattr__(self, name):
        def _delay_delayable(*args, **kwargs):
            getattr(self.delayable, name)(*args, **kwargs)
            return self.delayable.delay()

        return _delay_delayable

    def __str__(self):
        return (
            f"DelayableRecordset({self.delayable.recordset._name}"
            f"{getattr(self.delayable.recordset, '_ids', '')})"
        )

    __repr__ = __str__


def group(*delayables):
    return DelayableGroup(*delayables)


def chain(*delayables):
    return DelayableChain(*delayables)
