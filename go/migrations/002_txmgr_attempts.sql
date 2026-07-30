-- Upgrade path for databases initialized before the P2-A durable-attempt tx
-- manager and the executor receive-failure budget landed. review-0715 evolved
-- 001_initial_schema.sql in place, which the checksum guard in Store.Migrate
-- rejects on any already-initialized database; 001 is therefore pinned to its
-- originally-applied bytes and this migration carries the delta. A fresh
-- database applies 001 + 002 and ends at the same schema either way (modulo
-- physical column order and auto-generated constraint names).

-- Every lzReceive failure hash charged against a job's retry budget. Insert
-- success is the counting condition, so a crash-replayed receipt or lagging
-- LzReceiveAlert for an already-counted hash can never charge the budget twice.
CREATE TABLE IF NOT EXISTS executor_receive_failures (
  guid BYTEA NOT NULL REFERENCES executor_jobs(guid) ON DELETE CASCADE,
  tx_hash BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (guid, tx_hash)
);

-- Durable-attempt model (P2-A): the outbox owns the logical task and nonce;
-- each physical signed transaction is an immutable row in tx_attempts, and the
-- active attempt is the single source of truth for the current hash, gas, and
-- fees (read queries project them from the join; nothing is mirrored here).
-- The per-row request-side gas caps and mirrored tx_hash move to tx_attempts;
-- for historical mined rows the hash survives in receipt_tx_hash.
ALTER TABLE tx_outbox
  DROP COLUMN gas_limit,
  DROP COLUMN max_fee_per_gas,
  DROP COLUMN max_priority_fee_per_gas,
  DROP COLUMN tx_hash;

ALTER TABLE tx_outbox
  ADD COLUMN active_attempt_id BIGINT,
  -- Signing lease so only one worker instance signs a new attempt for this row.
  ADD COLUMN lease_token UUID,
  ADD COLUMN lease_until TIMESTAMPTZ,
  -- When status = 'held', held_reason names why the signer lane is blocked.
  ADD COLUMN held_reason TEXT
    CHECK (held_reason IS NULL OR held_reason IN
      ('nonce_reconcile_required', 'reprice_required', 'manual',
       'nonce_consumed_externally', 'broadcast_exhausted')),
  -- Operator cancel intent (txretry cancel-nonce). It persists until the final
  -- receipt terminalization: every send/replacement entry point must refuse to
  -- advance the original task while it is set, and only cancel attempts fly.
  ADD COLUMN cancel_requested_at TIMESTAMPTZ,
  -- Persistent receipt resolution, derived once under the row locks when a
  -- confirmation-depth receipt is first observed. The workflow application and
  -- the terminal finalizer both consume exactly this pinned outcome/attempt, so
  -- a cancel request racing the receipt pipeline cannot make them diverge, and
  -- a crash between them replays the same resolution.
  ADD COLUMN receipt_outcome TEXT
    CHECK (receipt_outcome IS NULL OR receipt_outcome IN
      ('confirmed', 'receipt_failed', 'canceled')),
  ADD COLUMN receipt_attempt_id BIGINT,
  -- Consecutive pre-sign failures (estimate, fee quote, or signing) since the
  -- last successfully persisted attempt, counted only while the row holds a
  -- nonce under a signing lease. At the cap the lane is held for manual review
  -- instead of falling back to the destructive failed/requeue path, which would
  -- release the nonce and wedge the signer.
  ADD COLUMN pre_sign_failure_count INTEGER NOT NULL DEFAULT 0
    CHECK (pre_sign_failure_count >= 0),
  ADD COLUMN next_sign_at TIMESTAMPTZ,
  -- Receipt polling fairness cursor: the poller visits non-terminal rows oldest
  -- poll first so one receiptless attempt cannot starve the others of the batch.
  ADD COLUMN last_receipt_poll_at TIMESTAMPTZ,
  -- Operator-requested same-nonce replacement (txretry replace); cleared when the
  -- replacement attempt is persisted.
  ADD COLUMN replace_requested_at TIMESTAMPTZ,
  ADD CHECK ((receipt_outcome IS NULL) = (receipt_attempt_id IS NULL)),
  ADD CHECK ((status = 'held') = (held_reason IS NOT NULL)),
  ADD CHECK ((lease_token IS NULL) = (lease_until IS NULL));

CREATE TABLE IF NOT EXISTS tx_attempts (
  id BIGSERIAL PRIMARY KEY,
  outbox_id BIGINT NOT NULL REFERENCES tx_outbox(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('original', 'replacement', 'cancel')),
  nonce BIGINT NOT NULL CHECK (nonce >= 0),
  tx_type SMALLINT NOT NULL CHECK (tx_type IN (0, 2)),
  tx_hash BYTEA NOT NULL CHECK (octet_length(tx_hash) = 32),
  raw_tx BYTEA NOT NULL CHECK (octet_length(raw_tx) > 0),
  gas_limit NUMERIC NOT NULL CHECK (gas_limit > 0),
  max_fee_per_gas NUMERIC NOT NULL CHECK (max_fee_per_gas > 0),
  max_priority_fee_per_gas NUMERIC
    CHECK (
      (tx_type = 0 AND max_priority_fee_per_gas IS NULL)
      OR (tx_type = 2 AND max_priority_fee_per_gas > 0)
    ),
  state TEXT NOT NULL
    CHECK (state IN ('signed', 'submitted', 'ambiguous', 'rejected', 'mined')),
  send_error_class TEXT
    CHECK (send_error_class IS NULL OR send_error_class IN
      ('accepted', 'ambiguous', 'nonce_too_low', 'nonce_too_high',
       'underpriced', 'retryable_env', 'definitive')),
  send_error TEXT,
  signing_token UUID NOT NULL,
  broadcast_count INTEGER NOT NULL DEFAULT 0 CHECK (broadcast_count >= 0),
  broadcast_lease_token UUID,
  broadcast_lease_until TIMESTAMPTZ,
  next_broadcast_at TIMESTAMPTZ,
  last_broadcast_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tx_hash),
  UNIQUE (outbox_id, id),
  UNIQUE (outbox_id, signing_token),
  CHECK ((broadcast_lease_token IS NULL) = (broadcast_lease_until IS NULL))
);

-- The active attempt must belong to the same outbox row (composite FK). The
-- constraint is deferrable so an attempt insert and the active-pointer switch
-- can happen in one transaction.
ALTER TABLE tx_outbox
  ADD CONSTRAINT tx_outbox_active_attempt_fk
  FOREIGN KEY (id, active_attempt_id)
  REFERENCES tx_attempts (outbox_id, id)
  DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX IF NOT EXISTS idx_tx_attempts_outbox ON tx_attempts(outbox_id);
CREATE INDEX IF NOT EXISTS idx_tx_attempts_broadcast_candidate
  ON tx_attempts(next_broadcast_at, id)
  WHERE state IN ('signed', 'ambiguous');
CREATE INDEX IF NOT EXISTS idx_tx_outbox_receipt_poll
  ON tx_outbox(chain_eid, signer_id, last_receipt_poll_at ASC NULLS FIRST, id)
  WHERE active_attempt_id IS NOT NULL
    AND status NOT IN ('confirmed', 'failed');

-- Nonce reconciliation scheduling: one instance claims the signer lane for a
-- confirmed-block NonceAt pass, and the backoff keeps held rows from hitting
-- the RPC on every manager pass.
ALTER TABLE tx_nonce_cursors
  ADD COLUMN reconcile_lease_token UUID,
  ADD COLUMN reconcile_lease_until TIMESTAMPTZ,
  ADD COLUMN next_reconcile_at TIMESTAMPTZ,
  ADD CHECK ((reconcile_lease_token IS NULL) = (reconcile_lease_until IS NULL));
