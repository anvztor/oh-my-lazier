-- Cancel retry pacing is tracked separately from the operator's cancel request
-- time: deferrals (fee-cap blocks, receipts awaiting confirmation depth,
-- pre-sign failures) push cancel_defer_until forward while cancel_requested_at
-- stays immutable, so the cancel age surfaced by stats and readiness reflects
-- how long the operator has actually been waiting.
ALTER TABLE tx_outbox
    ADD COLUMN cancel_defer_until timestamptz;
