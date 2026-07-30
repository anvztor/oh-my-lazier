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
  gas_limit NUMERIC,
  max_fee_per_gas NUMERIC,
  max_priority_fee_per_gas NUMERIC,
  nonce BIGINT,
  signer_id TEXT NOT NULL,
  status TEXT NOT NULL,
  tx_hash BYTEA,
  -- Mirrors tx_hash for mined rows so receipt facts stay self-contained audit evidence.
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
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tx_nonce_cursors (
  chain_eid INTEGER NOT NULL REFERENCES chains(eid),
  signer_id TEXT NOT NULL,
  next_nonce BIGINT NOT NULL CHECK (next_nonce >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(chain_eid, signer_id)
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
