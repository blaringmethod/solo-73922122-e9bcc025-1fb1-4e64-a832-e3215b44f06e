"""Background worker: claims batches/items and computes point-in-time income.

Concurrency model
-----------------
Every API process runs one worker loop against the shared database.
- A PENDING (or lease-expired RUNNING) batch is claimed with a row lock
  (SELECT ... FOR UPDATE SKIP LOCKED), so exactly one worker owns a batch.
- Items are claimed in chunks the same way. A worker renews the leases with a
  heartbeat; if it dies, another process reclaims the expired leases and
  continues with the *same frozen input*. DONE items are never recomputed.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

import asyncpg

from . import config

logger = logging.getLogger("income.worker")


class InjectedChunkError(RuntimeError):
    """Raised by the demo/test fault-injection hook."""


def classify_error(exc: BaseException) -> tuple[str, str]:
    """Map an exception to (error_category, short summary).

    The category is a stable vocabulary used by the diagnostics API; the
    summary is the exception's own message, truncated to keep the row small.
    Subclass checks are ordered most-specific first: integrity/data errors are
    PostgresError subclasses and would otherwise be swallowed by the generic
    SQL branch.
    """
    if isinstance(exc, InjectedChunkError):
        category = "injected_failure"
    elif isinstance(exc, asyncpg.QueryCanceledError):
        category = "timeout"
    elif isinstance(exc, (asyncpg.ConnectionDoesNotExistError,
                          asyncpg.ConnectionFailureError,
                          asyncpg.CannotConnectNowError,
                          ConnectionError)):
        # Connection errors are PostgresError subclasses in asyncpg, so this
        # branch must precede the generic SQL one.
        category = "connection_error"
    elif isinstance(exc, (asyncpg.ForeignKeyViolationError,
                          asyncpg.UniqueViolationError,
                          asyncpg.NotNullViolationError,
                          asyncpg.DataError,
                          asyncpg.NumericValueOutOfRangeError)):
        category = "data_error"
    elif isinstance(exc, asyncpg.PostgresError):
        # Every other server-side SQL failure (syntax, schema, internal...).
        category = "sql_error"
    else:
        category = "internal_error"
    try:
        message = str(exc)
    except Exception:  # some asyncpg exceptions raise from __str__ with no args
        message = repr(exc)
    if not message:
        message = repr(exc)
    summary = f"{type(exc).__name__}: {message}"[:500]
    return category, summary


# ---------------------------------------------------------------------------
# The point-in-time reconstruction, expressed in one SQL statement per chunk.
#
# For every claimed application a:
#   1. Restrict event versions to those known at application time AND already
#      ingested when the batch was accepted (frozen_ingest_at) -> reconstruct
#      exactly the versions visible at application time.
#   2. Per event, keep the newest visible version (DISTINCT ON ... ORDER BY
#      known_time DESC) -> later revisions of the same event are applied,
#      including withdrawals.
#   3. Drop withdrawn versions and events whose business_time is after the
#      application time; pick the remaining event with the latest
#      business_time, ties broken by the smallest event_id.
#   4. Left join so applications with no usable event stay empty.
# ---------------------------------------------------------------------------
_COMPUTE_CHUNK_SQL = """
WITH picked AS (
    SELECT bi.batch_id, bi.application_id, bi.item_seq
    FROM batch_items bi
    WHERE bi.batch_id = $1
      AND bi.status = 'RUNNING'
      AND bi.worker_id = $2
      AND bi.lease_expires_at > now()
    ORDER BY bi.item_seq
    LIMIT $3
),
visible_versions AS (
    SELECT
        p.application_id,
        p.item_seq,
        cev.event_id,
        cev.known_time,
        cev.business_time,
        cev.income,
        cev.withdrawn,
        cev.ingest_batch_id AS source_ingest_batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY p.application_id, cev.event_id
            ORDER BY cev.known_time DESC
        ) AS rn
    FROM picked p
    JOIN applications app ON app.application_id = p.application_id
    JOIN credit_event_versions cev
      ON cev.customer_id = app.customer_id
     AND cev.known_time <= app.application_time
     AND cev.ingested_at <= (SELECT frozen_ingest_at FROM batches WHERE batch_id = p.batch_id)
),
current_versions AS (
    SELECT * FROM visible_versions WHERE rn = 1
),
candidates AS (
    SELECT
        application_id,
        item_seq,
        event_id,
        known_time,
        business_time,
        income,
        source_ingest_batch_id,
        ROW_NUMBER() OVER (
            PARTITION BY application_id
            ORDER BY business_time DESC, event_id ASC
        ) AS cand_rn
    FROM current_versions
    WHERE withdrawn = FALSE
      AND business_time <= (
          SELECT application_time FROM applications ap
          WHERE ap.application_id = current_versions.application_id
      )
),
best AS (
    SELECT * FROM candidates WHERE cand_rn = 1
),
inserted AS (
    INSERT INTO batch_results
        (batch_id, application_id, customer_id, application_time, income,
         source_event_id, source_version_known_time, source_ingest_batch_id,
         source_business_time, item_seq)
    SELECT
        p.batch_id, p.application_id, app.customer_id, app.application_time,
        b.income, b.event_id, b.known_time, b.source_ingest_batch_id,
        b.business_time, p.item_seq
    FROM picked p
    JOIN applications app ON app.application_id = p.application_id
    LEFT JOIN best b ON b.application_id = p.application_id
    ON CONFLICT (batch_id, application_id) DO NOTHING
    RETURNING batch_id, application_id
)
UPDATE batch_items bi
SET status = 'DONE', worker_id = NULL, lease_expires_at = NULL
FROM inserted i
WHERE bi.batch_id = i.batch_id AND bi.application_id = i.application_id;
"""

_CLAIM_BATCH_SQL = """
UPDATE batches b
SET status = 'RUNNING',
    worker_id = $1,
    lease_expires_at = now() + make_interval(secs => $2)
FROM (
    SELECT batch_id
    FROM batches
    WHERE status = 'PENDING'
       OR (status = 'RUNNING' AND lease_expires_at < now())
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
) c
WHERE b.batch_id = c.batch_id
RETURNING b.batch_id;
"""

_CLAIM_ITEMS_SQL = """
UPDATE batch_items
SET status = 'RUNNING', worker_id = $2,
    lease_expires_at = now() + make_interval(secs => $3),
    attempts = attempts + 1
WHERE (batch_id, application_id) IN (
    SELECT batch_id, application_id
    FROM batch_items
    WHERE batch_id = $1
      AND (status = 'PENDING' OR (status = 'RUNNING' AND lease_expires_at < now()))
    ORDER BY item_seq
    FOR UPDATE SKIP LOCKED
    LIMIT $4
)
RETURNING application_id;
"""

# After a failed chunk transaction the rows are still RUNNING with an active
# lease; hand them back so another claim cycle (or worker) can retry them.
_RELEASE_CHUNK_SQL = """
UPDATE batch_items
SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
WHERE batch_id = $1 AND status = 'RUNNING' AND worker_id = $2;
"""

_MAX_ATTEMPTS_SQL = """
SELECT max(attempts) AS max_attempts
FROM batch_items WHERE batch_id = $1 AND status <> 'DONE';
"""

_HEARTBEAT_SQL = """
UPDATE batches
SET lease_expires_at = now() + make_interval(secs => $2)
WHERE batch_id = $1 AND status = 'RUNNING' AND worker_id = $3;
"""
_HEARTBEAT_ITEMS_SQL = """
UPDATE batch_items
SET lease_expires_at = now() + make_interval(secs => $2)
WHERE batch_id = $1 AND status = 'RUNNING' AND worker_id = $3;
"""

_RECLAIM_ORPHAN_ITEMS_SQL = """
UPDATE batch_items
SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
WHERE batch_id = $1 AND status = 'RUNNING' AND lease_expires_at < now();
"""

_COMPLETE_SQL = """
UPDATE batches b
SET status = 'COMPLETED',
    processed_items = d.done_count,
    worker_id = NULL,
    lease_expires_at = NULL,
    error = NULL
FROM (
    SELECT count(*) AS done_count
    FROM batch_items WHERE batch_id = $1 AND status = 'DONE'
) d
WHERE b.batch_id = $1
  AND NOT EXISTS (
      SELECT 1 FROM batch_items WHERE batch_id = $1 AND status <> 'DONE'
  )
RETURNING b.batch_id;
"""

_FAIL_SQL = """
UPDATE batches
SET status = 'FAILED', error = $2
WHERE batch_id = $1;
"""

# Persist the failure record BEFORE the failed items are released for retry.
# Keeping it in a separate committed transaction means the record survives the
# failed chunk's rollback, worker exits and lease takeover.
_INSERT_FAILURE_SQL = """
INSERT INTO chunk_failures
    (failure_id, batch_id, application_ids, attempt_number, error_category,
     error_summary, worker_id, status)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'OPEN');
"""

# A success only resolves failures whose applications are all DONE. Partial
# overlap (a chunk can contain several applications) leaves the row OPEN.
_RESOLVE_FAILURES_SQL = """
UPDATE chunk_failures cf
SET status = 'RESOLVED', resolved_at = now()
WHERE cf.batch_id = $1
  AND cf.status = 'OPEN'
  AND NOT EXISTS (
      SELECT 1
      FROM unnest(cf.application_ids) AS f(application_id)
      LEFT JOIN batch_items bi
        ON bi.batch_id = cf.batch_id AND bi.application_id = f.application_id
      WHERE bi.status IS DISTINCT FROM 'DONE'
  );
"""

# Fault-injection hook (demo/test only, gated by config.FAULT_INJECTION_ENABLED).
_FAULT_LOOKUP_SQL = """
SELECT application_id, fail_times, stall_seconds
FROM fault_injections
WHERE application_id = ANY($1)
"""

_FAULT_DECREMENT_SQL = """
UPDATE fault_injections
SET fail_times = greatest(fail_times - 1, 0)
WHERE application_id = ANY($1)
"""


class Worker:
    def __init__(self, pool: asyncpg.Pool, worker_id: str) -> None:
        self.pool = pool
        self.worker_id = worker_id
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._current_batch: uuid.UUID | None = None
        self._heartbeat_task: asyncio.Task | None = None
        # Demo/test fault injection pauses heartbeats while a chunk is stalled,
        # reproducing the lease-expiry state left by a SIGKILLed worker.
        self._heartbeat_paused = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"worker-{self.worker_id}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        logger.info("worker %s started", self.worker_id)
        while not self._stop.is_set():
            try:
                worked = await self._tick()
            except Exception:  # keep the worker alive through transient errors
                logger.exception("worker tick failed")
                worked = False
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=config.WORK_POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass
        logger.info("worker %s stopped", self.worker_id)

    async def _tick(self) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(_CLAIM_BATCH_SQL, self.worker_id, config.LEASE_SECONDS)
        if row is None:
            return False
        batch_id = row["batch_id"]
        self._current_batch = batch_id
        logger.info("worker %s claimed batch %s", self.worker_id, batch_id)
        self._heartbeat_task = asyncio.create_task(self._heartbeat(batch_id))
        try:
            await self._process_batch(batch_id)
        finally:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat_task = None
            self._current_batch = None
        return True

    async def _heartbeat(self, batch_id: uuid.UUID) -> None:
        while True:
            await asyncio.sleep(config.HEARTBEAT_SECONDS)
            if self._heartbeat_paused:
                # A demo stall intentionally freezes this worker like a crash.
                continue
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        _HEARTBEAT_SQL, batch_id, config.LEASE_SECONDS, self.worker_id
                    )
                    await conn.execute(
                        _HEARTBEAT_ITEMS_SQL, batch_id, config.LEASE_SECONDS, self.worker_id
                    )
            except Exception:
                logger.exception("heartbeat failed for batch %s", batch_id)

    async def _process_batch(self, batch_id: uuid.UUID) -> None:
        # Items leased by a dead worker are put back before we claim anything.
        async with self.pool.acquire() as conn:
            await conn.execute(_RECLAIM_ORPHAN_ITEMS_SQL, batch_id)

        while True:
            async with self.pool.acquire() as conn:
                claimed = await conn.fetch(
                    _CLAIM_ITEMS_SQL,
                    batch_id, self.worker_id, config.LEASE_SECONDS, config.CHUNK_SIZE,
                )
            if not claimed:
                break
            app_ids = [r["application_id"] for r in claimed]
            try:
                await self._maybe_inject_failure(batch_id, app_ids)
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute(
                            _COMPUTE_CHUNK_SQL, batch_id, self.worker_id, config.CHUNK_SIZE
                        )
                # Success: mark every OPEN failure fully covered by DONE items
                # as resolved, in the same logical step as the completion.
                async with self.pool.acquire() as conn:
                    await conn.execute(_RESOLVE_FAILURES_SQL, batch_id)
                if config.CHUNK_DELAY_SECONDS:
                    await asyncio.sleep(config.CHUNK_DELAY_SECONDS)
            except Exception as exc:
                # The failed transaction rolled back, so the items remain
                # RUNNING under our lease. Persist the failure record and only
                # then hand the items back for re-claim. Both the persisted
                # `attempts` counter and chunk_failures survive worker exits.
                logger.exception("chunk failed for batch %s", batch_id)
                category, summary = classify_error(exc)
                max_attempts = await self._record_failure_and_release(
                    batch_id, app_ids, category, summary
                )
                if max_attempts is not None and max_attempts >= config.MAX_CHUNK_ATTEMPTS:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            _FAIL_SQL, batch_id,
                            f"an item failed after {max_attempts} attempts; "
                            f"see /api/v1/batches/{batch_id}/diagnostics",
                        )
                    return
                await asyncio.sleep(0.5)
                continue

        async with self.pool.acquire() as conn:
            completed = await conn.fetchrow(_COMPLETE_SQL, batch_id)
        if completed:
            logger.info("batch %s COMPLETED", batch_id)
        else:
            # Should not happen: items are leased but not done. Another worker
            # will reclaim them; this batch stays RUNNING until its lease
            # expires and is picked up again.
            logger.warning("batch %s not completable yet; releasing for reclaim", batch_id)

    async def _record_failure_and_release(
        self,
        batch_id: uuid.UUID,
        app_ids: list[str],
        category: str,
        summary: str,
    ) -> int | None:
        """Persist the failure row and release the failed chunk.

        Stale-owner guard: the worker's batch lease must still be live. If this
        worker was stalled past its lease and another process took over, the
        late error is discarded so it can never overwrite the takeover's
        diagnostics or disturb the new owner's items.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Lease check is done in SQL against the database clock so the
                # two API processes need no synchronised system clock.
                owner = await conn.fetchrow(
                    """SELECT worker_id,
                              (lease_expires_at IS NOT NULL
                               AND lease_expires_at > now()) AS lease_live
                         FROM batches WHERE batch_id = $1
                       FOR UPDATE""",
                    batch_id,
                )
                still_owner = (
                    owner is not None
                    and owner["worker_id"] == self.worker_id
                    and owner["lease_live"]
                )
                if not still_owner:
                    logger.warning(
                        "worker %s lost batch %s before recording its failure; "
                        "dropping the stale error (owner now %s)",
                        self.worker_id, batch_id,
                        owner["worker_id"] if owner else None,
                    )
                    return None
                attempt_number = await conn.fetchval(
                    """SELECT max(attempts) FROM batch_items
                        WHERE batch_id = $1 AND application_id = ANY($2)""",
                    batch_id, app_ids,
                )
                await conn.execute(
                    _INSERT_FAILURE_SQL,
                    uuid.uuid4(), batch_id, app_ids, attempt_number,
                    category, summary, self.worker_id,
                )
                await conn.execute(_RELEASE_CHUNK_SQL, batch_id, self.worker_id)
                max_attempts = await conn.fetchval(_MAX_ATTEMPTS_SQL, batch_id)
        return max_attempts

    async def _maybe_inject_failure(self, batch_id: uuid.UUID, app_ids: list[str]) -> None:
        """Demo/test hook backed by the fault_injections table.

        For every listed application with fail_times > 0 the chunk fails once;
        a positive stall_seconds first lets this worker's leases expire (with
        heartbeats paused) so another process takes over before the late error
        arrives — exercising the stale-writer guard.
        """
        if not config.FAULT_INJECTION_ENABLED:
            return
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(_FAULT_LOOKUP_SQL, app_ids)
            active = [r for r in rows if r["fail_times"] > 0]
            if not active:
                return
            stall = max(r["stall_seconds"] for r in active)
            await conn.execute(_FAULT_DECREMENT_SQL,
                               [r["application_id"] for r in active])
        if stall > 0:
            logger.warning(
                "FAULT INJECTION: worker %s stalling %ss on batch %s; leases will expire",
                self.worker_id, stall, batch_id,
            )
            self._heartbeat_paused = True
            try:
                async with self.pool.acquire() as conn:
                    # Reproduce the exact post-SIGKILL state: leases in the past.
                    await conn.execute(
                        "UPDATE batches SET lease_expires_at = now() - interval '1 second' "
                        "WHERE batch_id = $1 AND worker_id = $2",
                        batch_id, self.worker_id,
                    )
                    await conn.execute(
                        "UPDATE batch_items SET lease_expires_at = now() - interval '1 second' "
                        "WHERE batch_id = $1 AND worker_id = $2",
                        batch_id, self.worker_id,
                    )
                await asyncio.sleep(stall)
            finally:
                self._heartbeat_paused = False
        raise InjectedChunkError(
            f"injected chunk failure for applications {sorted(r['application_id'] for r in active)}"
        )
