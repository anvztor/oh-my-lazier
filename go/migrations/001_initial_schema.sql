CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chains (
  eid INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  chain_id BIGINT NOT NULL,
  endpoint_address BYTEA NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  paused BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pathways (
  id BIGSERIAL PRIMARY KEY,
  src_eid INTEGER NOT NULL REFERENCES chains(eid),
  dst_eid INTEGER NOT NULL REFERENCES chains(eid),
  src_oapp BYTEA NOT NULL,
  dst_oapp BYTEA NOT NULL,
  send_lib BYTEA NOT NULL,
  receive_lib BYTEA NOT NULL,
  open_executor BYTEA NOT NULL,
  open_dvn BYTEA NOT NULL,
  price_feed BYTEA NOT NULL,
  destination_open_dvn BYTEA NOT NULL,
  max_message_size INTEGER NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  paused BOOLEAN NOT NULL DEFAULT false,
  UNIQUE(src_eid, dst_eid, src_oapp, dst_oapp)
);

CREATE TABLE IF NOT EXISTS packets (
  guid BYTEA PRIMARY KEY,
  src_eid INTEGER NOT NULL REFERENCES chains(eid),
  dst_eid INTEGER NOT NULL REFERENCES chains(eid),
  nonce NUMERIC NOT NULL,
  sender BYTEA NOT NULL,
  receiver BYTEA NOT NULL,
  send_lib BYTEA NOT NULL,
  src_tx_hash BYTEA NOT NULL,
  src_block_number BIGINT NOT NULL,
  src_log_index INTEGER NOT NULL,
  encoded_packet BYTEA NOT NULL,
  packet_header BYTEA NOT NULL,
  message BYTEA NOT NULL,
  payload_hash BYTEA NOT NULL,
  options BYTEA NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(src_eid, dst_eid, sender, receiver, nonce)
);

CREATE TABLE IF NOT EXISTS executor_jobs (
  guid BYTEA PRIMARY KEY REFERENCES packets(guid),
  assigned BOOLEAN NOT NULL DEFAULT false,
  assigned_fee NUMERIC,
  status TEXT NOT NULL,
  commit_tx_hash BYTEA,
  receive_tx_hash BYTEA,
  last_error TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every lzReceive failure hash charged against a job's retry budget. Insert
-- success is the counting condition, so a crash-replayed receipt or lagging
-- LzReceiveAlert for an already-counted hash can never charge the budget twice.
CREATE TABLE IF NOT EXISTS executor_receive_failures (
  guid BYTEA NOT NULL REFERENCES executor_jobs(guid) ON DELETE CASCADE,
  tx_hash BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (guid, tx_hash)
);

CREATE TABLE IF NOT EXISTS dvn_jobs (
  guid BYTEA PRIMARY KEY REFERENCES packets(guid),
  assigned BOOLEAN NOT NULL DEFAULT false,
  assigned_fee NUMERIC,
  confirmations_required BIGINT NOT NULL,
  status TEXT NOT NULL,
  verify_tx_hash BYTEA,
  quorum_result JSONB,
  last_error TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_packet_skips (
  role TEXT NOT NULL,
  src_eid INTEGER NOT NULL REFERENCES chains(eid),
  dst_eid INTEGER NOT NULL REFERENCES chains(eid),
  nonce NUMERIC NOT NULL,
  sender BYTEA NOT NULL,
  receiver BYTEA NOT NULL,
  guid BYTEA NOT NULL,
  src_tx_hash BYTEA NOT NULL,
  src_block_number BIGINT NOT NULL,
  src_log_index INTEGER NOT NULL,
  reason TEXT NOT NULL,
  worker BYTEA,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(role, src_eid, dst_eid, sender, receiver, nonce)
);

CREATE TABLE IF NOT EXISTS tx_outbox (
  id BIGSERIAL PRIMARY KEY,
  chain_eid INTEGER NOT NULL REFERENCES chains(eid),
  purpose TEXT NOT NULL,
  guid BYTEA,
  to_address BYTEA NOT NULL,
  calldata BYTEA NOT NULL,
  value NUMERIC NOT NULL DEFAULT 0,
  nonce BIGINT,
  signer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  -- Records the winning attempt's hash at terminalization so receipt facts stay
  -- self-contained audit evidence.
  receipt_tx_hash BYTEA,
  receipt_status INTEGER,
  receipt_block_number BIGINT,
  receipt_gas_used NUMERIC,
  receipt_effective_gas_price NUMERIC,
  receipt_gas_cost_dst_wei NUMERIC,
  receipt_gas_cost_src_wei NUMERIC,
  receipt_observed_at TIMESTAMPTZ,
  receipt_cost_priced_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0,
  failure_kind TEXT,
  next_retry_at TIMESTAMPTZ,
  retry_of_id BIGINT REFERENCES tx_outbox(id),
  last_error TEXT,
  -- Durable-attempt model (P2-A): the outbox owns the logical task and nonce;
  -- each physical signed transaction is an immutable row in tx_attempts, and the
  -- active attempt is the single source of truth for the current hash, gas, and
  -- fees (read queries project them from the join; nothing is mirrored here).
  active_attempt_id BIGINT,
  -- Signing lease so only one worker instance signs a new attempt for this row.
  lease_token UUID,
  lease_until TIMESTAMPTZ,
  -- When status = 'held', held_reason names why the signer lane is blocked.
  held_reason TEXT
    CHECK (held_reason IS NULL OR held_reason IN
      ('nonce_reconcile_required', 'reprice_required', 'manual',
       'nonce_consumed_externally', 'broadcast_exhausted')),
  -- Operator cancel intent (txretry cancel-nonce). It persists until the final
  -- receipt terminalization: every send/replacement entry point must refuse to
  -- advance the original task while it is set, and only cancel attempts fly.
  cancel_requested_at TIMESTAMPTZ,
  -- Persistent receipt resolution, derived once under the row locks when a
  -- confirmation-depth receipt is first observed. The workflow application and
  -- the terminal finalizer both consume exactly this pinned outcome/attempt, so
  -- a cancel request racing the receipt pipeline cannot make them diverge, and
  -- a crash between them replays the same resolution.
  receipt_outcome TEXT
    CHECK (receipt_outcome IS NULL OR receipt_outcome IN
      ('confirmed', 'receipt_failed', 'canceled')),
  receipt_attempt_id BIGINT,
  CHECK ((receipt_outcome IS NULL) = (receipt_attempt_id IS NULL)),
  -- Consecutive pre-sign failures (estimate, fee quote, or signing) since the
  -- last successfully persisted attempt, counted only while the row holds a
  -- nonce under a signing lease. At the cap the lane is held for manual review
  -- instead of falling back to the destructive failed/requeue path, which would
  -- release the nonce and wedge the signer.
  pre_sign_failure_count INTEGER NOT NULL DEFAULT 0
    CHECK (pre_sign_failure_count >= 0),
  next_sign_at TIMESTAMPTZ,
  -- Receipt polling fairness cursor: the poller visits non-terminal rows oldest
  -- poll first so one receiptless attempt cannot starve the others of the batch.
  last_receipt_poll_at TIMESTAMPTZ,
  -- Operator-requested same-nonce replacement (txretry replace); cleared when the
  -- replacement attempt is persisted.
  replace_requested_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((status = 'held') = (held_reason IS NOT NULL)),
  CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);

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

CREATE TABLE IF NOT EXISTS tx_nonce_cursors (
  chain_eid INTEGER NOT NULL REFERENCES chains(eid),
  signer_id TEXT NOT NULL,
  next_nonce BIGINT NOT NULL CHECK (next_nonce >= 0),
  -- Nonce reconciliation scheduling: one instance claims the signer lane for a
  -- confirmed-block NonceAt pass, and the backoff keeps held rows from hitting
  -- the RPC on every manager pass.
  reconcile_lease_token UUID,
  reconcile_lease_until TIMESTAMPTZ,
  next_reconcile_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(chain_eid, signer_id),
  CHECK ((reconcile_lease_token IS NULL) = (reconcile_lease_until IS NULL))
);

CREATE TABLE IF NOT EXISTS indexer_cursors (
  chain_eid INTEGER NOT NULL REFERENCES chains(eid),
  stream TEXT NOT NULL,
  last_block BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(chain_eid, stream)
);

CREATE INDEX IF NOT EXISTS idx_packets_status ON packets(status);
CREATE INDEX IF NOT EXISTS idx_packets_source_position ON packets(src_eid, src_block_number, src_log_index);
CREATE INDEX IF NOT EXISTS idx_executor_jobs_status_retry ON executor_jobs(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_dvn_jobs_status_retry ON dvn_jobs(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_tx_outbox_status_chain ON tx_outbox(status, chain_eid, id);
CREATE INDEX IF NOT EXISTS idx_tx_outbox_unpriced_receipt
  ON tx_outbox(chain_eid, purpose, updated_at, id)
  WHERE receipt_gas_cost_dst_wei IS NOT NULL
    AND receipt_gas_cost_src_wei IS NULL;
CREATE INDEX IF NOT EXISTS idx_tx_outbox_failed_retry
  ON tx_outbox(status, chain_eid, signer_id, next_retry_at, id)
  WHERE status = 'failed';
CREATE UNIQUE INDEX IF NOT EXISTS idx_tx_outbox_chain_signer_nonce
  ON tx_outbox(chain_eid, signer_id, nonce)
  WHERE nonce IS NOT NULL;
