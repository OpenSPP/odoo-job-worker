# Changelog — job_worker

Version history for the `job_worker` module. Entries are keyed by the module
version in `__manifest__.py`, with the corresponding repository release tag
noted where one exists.

## 19.0.1.3.0 — unreleased

### Changed

- **Cancelling a job now cascades to everything waiting on it.** Previously
  `button_cancelled` wrote `state = 'cancelled'` and cascaded nothing, so a
  cancelled job's dependents waited on an event that could no longer occur:
  `_release_dependents` runs only on completion and `_fail_dependents` only on
  failure. For a `group(...).on_done(barrier)` that was permanent and silent —
  the barrier held `pending_dependency_count > 0` in `waiting` forever, was
  never pruned by `_gc_old_jobs` (which retires only terminal rows), and blocked
  its own recovery, because `enqueue`'s `identity_key` dedupe matches `waiting`
  and so handed a re-dispatched graph the stranded barrier instead of a fresh
  one ([#32](https://github.com/OpenSPP/odoo-job-worker/pull/32)).

  **This reverses a previously documented behaviour.** Two tests asserted the
  orphaning and described it as an explicit design choice; both are updated
  rather than removed, each with a docstring explaining the reversal.
  Cancellation propagates as *cancellation*, not failure: `on_error` dependents
  are **not** promoted to `pending`, because their handler exists to react to a
  failure and a cancelled job never failed. Consumers that relied on a
  cancelled job's dependents remaining runnable — e.g. "cancel this member,
  then requeue it to let the graph finish" — must switch to re-dispatching the
  graph, which the `identity_key` exit above now permits.

## 19.0.1.2.0 — unreleased

### Fixed

- Group barriers whose dependencies live in another job graph are now released
  and cascaded correctly. Re-triggering an operation while its first run was
  still in flight (an `identity_key` match returning an existing job) produced
  barriers stuck in `waiting` forever; the redundant `graph_uuid` predicate has
  been dropped from all release/cascade paths ([#17](https://github.com/OpenSPP/odoo-job-worker/pull/17)).
- The worker now honours `run_on_failure`: `on_error()` callbacks run after real
  failures and are cancelled after success. Previously the worker's cascade
  paths inverted the contract in both directions — handlers ran after
  *successful* jobs and never ran after failures. The worker now delegates to
  the model's cascade logic where an ORM environment exists, removing the
  duplication that allowed the drift ([#18](https://github.com/OpenSPP/odoo-job-worker/pull/18)).
- A timed-out job is no longer re-dispatched while its execution thread may
  still be running. The row is held `started` under the original worker's claim
  and becomes eligible again when the thread returns (cleanly or by raising) or
  its heartbeat goes stale — bounding, though not eliminating, concurrent
  double-execution. Retry pacing is preserved via `scheduled_at`, and retry
  counting converges on the reclaim path, which appends to `exc_info` so the
  timeout diagnosis survives. Jobs that set `timeout` must be idempotent or
  avoid committing internally ([#19](https://github.com/OpenSPP/odoo-job-worker/pull/19)).
- The stall watchdog no longer kills a healthy worker that is waiting on its own
  long-running job. Waiting on an active job now counts as progress; a genuinely
  hung worker (blocked inside acquire/heartbeat/select) is still recycled
  ([#21](https://github.com/OpenSPP/odoo-job-worker/pull/21)).
- A database error in the worker's main loop or during registry load no longer
  kills the worker thread — the worker recovers in place (drops the session,
  backs off exponentially, takes a fresh one) — and database errors never count
  toward the quarantine threshold. Previously an aborted transaction
  (`InFailedSqlTransaction`) escaped the narrower `OperationalError` guards,
  crash-looped the thread, and quarantined the entire database. The guard is
  `psycopg2.Error`, which also covers `InterfaceError`
  ([#22](https://github.com/OpenSPP/odoo-job-worker/pull/22)).
- `test_heartbeat_loop_prevents_stale_reacquire` deflaked and strengthened: the
  probe now waits on the observed heartbeat refresh instead of a fixed sleep
  with a ~0.1s margin ([#20](https://github.com/OpenSPP/odoo-job-worker/pull/20)).

### Added

- Jobs orphaned by a stall-watchdog kill are stamped with `WorkerStalledError`
  before the process exits, making a watchdog kill distinguishable from an OOM
  kill on the job row. In a multi-database process every database's in-flight
  rows are stamped; bystander databases get a variant naming the database that
  actually stalled ([#19](https://github.com/OpenSPP/odoo-job-worker/pull/19)).
- Degraded-health signal: a database stuck in database-error recovery past
  `database_unhealthy_after_seconds` (default 300s) withholds the runner
  heartbeat, failing the container healthcheck. Requires
  `job_worker_healthcheck.py` wired as the container healthcheck; see the
  deployment docs ([#22](https://github.com/OpenSPP/odoo-job-worker/pull/22)).
- New tuning knobs (constructor-only for now):
  `database_error_backoff_seconds` / `database_error_backoff_cap_seconds` on the
  worker, and `registry_load_backoff_seconds` /
  `registry_load_backoff_cap_seconds` / `database_unhealthy_after_seconds` on
  the runner ([#22](https://github.com/OpenSPP/odoo-job-worker/pull/22)).

### Changed

- `_handle_timeout` no longer decides retry-vs-permanent-failure; that decision
  (and the cascade to dependents) lives solely in the reclaim path
  ([#19](https://github.com/OpenSPP/odoo-job-worker/pull/19)).
- The quarantine threshold (`maximum_consecutive_failures`) now counts only
  genuine worker crashes — non-database exceptions — since database errors are
  recovered in place ([#22](https://github.com/OpenSPP/odoo-job-worker/pull/22)).

## 19.0.1.1.0 — released as [2026.07](https://github.com/OpenSPP/odoo-job-worker/releases/tag/2026.07) (2026-07-23)

Initial public release: SQL-driven pull-architecture job queue engine with
per-job heartbeats, stale-job recovery, channel throttling, job graphs
(chains, groups, `on_done`/`on_error` callbacks), per-attempt timeouts, and a
standalone supervisor (`job_worker_runner.py`) with database discovery,
quarantine, and a file-based container healthcheck.
