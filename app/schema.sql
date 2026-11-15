-- ============================================================================
-- Point-in-time income backfill — PostgreSQL schema
-- All timestamps are stored as timestamptz (UTC).
-- ============================================================================

-- Imported loan applications ------------------------------------------------
CREATE TABLE IF NOT EXISTS applications (
    application_id   TEXT PRIMARY KEY,
    customer_id      TEXT NOT NULL,
    application_time TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingest_batch_id  UUID NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_app_customer ON applications (customer_id, application_time);

-- Credit bureau change log ---------------------------------------------------
-- One row = one known version of one event.
-- (event_id, known_time) is the natural version key and is unique.
CREATE TABLE IF NOT EXISTS credit_event_versions (
    event_id        TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    business_time   TIMESTAMPTZ NOT NULL,   -- UTC business time of the change
    known_time      TIMESTAMPTZ NOT NULL,   -- UTC time at which we learned it
    income          INTEGER,                -- NULL when withdrawn = true
    withdrawn       BOOLEAN NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    ingest_batch_id UUID NOT NULL,
    PRIMARY KEY (event_id, known_time)
);
CREATE INDEX IF NOT EXISTS idx_cev_customer_lookup
    ON credit_event_versions (customer_id, business_time, event_id, known_time);

-- Audit of every import batch ------------------------------------------------
CREATE TABLE IF NOT EXISTS import_batches (
    ingest_batch_id UUID PRIMARY KEY,
    import_kind     TEXT NOT NULL CHECK (import_kind IN ('applications', 'credit_events')),
    row_count       INTEGER NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('accepted', 'rejected')),
    reason          TEXT,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Generation batches (immutable work units) ----------------------------------
CREATE TABLE IF NOT EXISTS batches (
    batch_id          UUID PRIMARY KEY,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    frozen_ingest_at  TIMESTAMPTZ NOT NULL,  -- visibility fence
    status            TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    total_items       INTEGER NOT NULL,
    processed_items   INTEGER NOT NULL DEFAULT 0,
    worker_id         TEXT,
    lease_expires_at  TIMESTAMPTZ,
    error             TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_work ON batches (status, lease_expires_at);

-- Snapshot of the input of each batch ----------------------------------------
CREATE TABLE IF NOT EXISTS batch_items (
    batch_id        UUID NOT NULL REFERENCES batches (batch_id),
    application_id  TEXT NOT NULL,
    item_seq        BIGINT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'RUNNING', 'DONE')),
    worker_id       TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id, application_id),
    UNIQUE (batch_id, item_seq)
);
CREATE INDEX IF NOT EXISTS idx_batch_items_claim
    ON batch_items (batch_id, status, lease_expires_at);

-- Final results; only exposed once the batch is COMPLETED --------------------
CREATE TABLE IF NOT EXISTS batch_results (
    batch_id        UUID NOT NULL,
    application_id  TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    application_time TIMESTAMPTZ NOT NULL,
    income          INTEGER,
    source_event_id TEXT,
    source_version_known_time TIMESTAMPTZ,
    source_ingest_batch_id UUID,
    source_business_time TIMESTAMPTZ,
    item_seq        BIGINT NOT NULL,
    PRIMARY KEY (batch_id, application_id)
);
CREATE INDEX IF NOT EXISTS idx_batch_results_page
    ON batch_results (batch_id, item_seq);

-- ---------------------------------------------------------------------------
-- Chunk-level failure diagnostics
--
-- One row per failed chunk attempt. The row is written *before* the failed
-- items are handed back for retry, and worker restarts never delete it.
-- A successful retry flips the matching row from OPEN to RESOLVED.
--
-- status:
--   OPEN      not retried successfully yet (items PENDING, RUNNING or the
--             batch FAILED); the diagnostics endpoint derives RETRYING for
--             rows whose items are currently being recomputed.
--   RESOLVED  every application referenced by the failure has since reached
--             DONE, i.e. a later successful attempt overwrote the failure.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chunk_failures (
    failure_id     UUID PRIMARY KEY,
    batch_id       UUID NOT NULL REFERENCES batches (batch_id),
    application_ids TEXT[] NOT NULL,           -- items covered by the failed chunk
    attempt_number INTEGER NOT NULL,           -- max attempts seen on those items
    error_category TEXT NOT NULL CHECK (
        error_category IN ('data_error', 'sql_error', 'connection_error',
                           'timeout', 'injected_failure', 'internal_error')
    ),
    error_summary  TEXT NOT NULL,
    worker_id      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN', 'RESOLVED')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_chunk_failures_batch
    ON chunk_failures (batch_id, created_at);

-- ---------------------------------------------------------------------------
-- Demo / test-only fault injection. When FAULT_INJECTION_ENABLED is on, a
-- worker claiming one of these applications fails the chunk the configured
-- number of times before succeeding. Never used by production code paths.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fault_injections (
    application_id   TEXT PRIMARY KEY,
    fail_times       INTEGER NOT NULL CHECK (fail_times >= 0),
    stall_seconds    REAL NOT NULL DEFAULT 0,  -- freeze the worker past its lease
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
