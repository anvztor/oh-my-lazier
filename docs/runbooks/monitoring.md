# Monitoring Checklist

The first-phase worker exposes HTTP health and Prometheus text metrics from `metrics.listen_address`.

Endpoints:

- `/healthz`: process liveness only. It returns `200` without touching Postgres.
- `/readyz`: readiness check. It returns `200` only when the worker can read the DB stats snapshot and the DB-backed readiness rules pass.
- `/metrics`: Prometheus text metrics derived from durable Postgres state plus process-local indexer polling and worker-loop retry status.

Required scrape target:

```text
http://<worker-host>:9090/metrics
```

Required alert rules are tracked in `docs/monitoring/prometheus-alerts.yml`.
`npm run check:runbooks` verifies that the documented high-signal alerts remain
present and linked back to this runbook.

Required alerts:

- `LazWorkerReadinessFailed`: `/readyz` returns non-200 for more than two scrape intervals; page.
- `LazChainPaused`: `laz_chain_paused == 1` for any chain; page immediately. This means chain-wide quorum safety logic paused the worker path.
- `LazPathwayPaused`: `laz_pathway_paused == 1` for any pathway; page immediately. This means packet-level receipt/log conflict safety logic paused a pathway.
- `LazDVNQuorumConflict`: `laz_dvn_jobs_total{status="QUORUM_CONFLICT"} > 0`; page immediately and inspect source RPC providers before unpausing.
- `LazDVNReorgDetected`: `laz_dvn_jobs_total{status="REORG_DETECTED"} > 0`; page if it persists past the next confirmation loop; inspect source RPC providers and source transaction receipts.
- `LazPacketManualReview`: `laz_packets_total{status="MANUAL_REVIEW"} > 0`; ticket within one business day; page if count increases during migration.
- `LazExecutorReceiveFailed`: `laz_executor_jobs_total{status="LZ_RECEIVE_FAILED"} > 0`; ticket and inspect destination `LzReceiveAlert` logs. A failed outbox receipt may be automatically cloned for retry and restore the job to `LZ_RECEIVE_TX_ENQUEUED`; persistent `LZ_RECEIVE_FAILED` means the retry path has not yet cleared the workflow state. The executor re-broadcasts a reverting `lzReceive` at most five times before parking the job in `MANUAL_REVIEW`, so a receiver that reverts at execution cannot drain the executor signer indefinitely.
- `LazWorkerManualReview`: `laz_executor_jobs_total{status="MANUAL_REVIEW"} > 0` or `laz_dvn_jobs_total{status="MANUAL_REVIEW"} > 0`; ticket and block migration approval until reviewed.
- `LazTxOutboxFailed`: `laz_tx_outbox_total{status="failed",retry_state="exhausted"} > 0`; ticket; page if exhausted failure count increases for active migration chains. Rows with `retry_state="retrying"` are still under txmgr automatic retry, and rows with `retry_state="superseded"` either already have a retry child or the workflow advanced past the stale failure.
- `LazWorkerFeeNegativeMargin`: `laz_worker_fee_negative_margin_jobs > 0`; page after five minutes. This means mined worker transaction gas cost, converted to source-chain native wei, exceeds the source-chain assignment fee for at least one job.
- `LazWorkerFeeReconciliationPending`: `laz_worker_fee_unpriced_receipts > 0`; ticket after fifteen minutes. Check pricing source health and `fee_accounting` loop logs; tx receipt status has already been recorded and is not blocked by pricing failures.
- `LazSignerLowNativeBalance`: `laz_signer_native_balance_wei < laz_signer_min_native_balance_wei`; page after five minutes. Fund the affected worker signer before queued or replacement transactions exhaust their configured fee caps.
- `LazIndexerPollFailing`: `laz_indexer_poll_success == 0` for a configured indexer; page after the failure persists across polling intervals.
- `LazIndexerCursorStalled`: missing `laz_indexer_cursor_last_block` movement for an enabled chain over the expected polling window; page if the chain is actively used.

Active worker status paths:

- DVN active verification: `ASSIGNED -> WAITING_CONFIRMATIONS -> QUORUM_CHECKING -> READY_TO_VERIFY -> VERIFY_TX_ENQUEUED -> VERIFIED`.
- Executor delivery: `ASSIGNED -> WAITING_DVN_VERIFICATION -> VERIFIABLE -> COMMIT_TX_ENQUEUED -> COMMITTED -> EXECUTABLE -> LZ_RECEIVE_TX_ENQUEUED -> DELIVERED`.
- Shadow DVN verification stops at `WOULD_VERIFY`; it must not enqueue `dvn_verify` transactions.
- Destination-chain reconciliation can skip transaction enqueue and move jobs forward when matching on-chain completion is already observable. During database rebuild or historical replay, `PacketVerified`, `PacketDelivered`, `LzReceiveAlert`, and `PayloadVerified` events can fill the corresponding executor or DVN status and tx hash even when the local outbox row no longer exists.
- Destination cursors keep enabled-pathway events pending while the local source packet is absent. They advance past missing source packets only when the source indexer recorded a skipped-assignment tombstone, such as a disabled pathway, unexpected source worker, or a packet sent through a different send library. Source indexing filters the raw PacketV1 route against configured pathways before strict packet decoding, so unrelated OApps sharing the same send library cannot block a source stream.
- `QUORUM_CONFLICT`, `REORG_DETECTED`, `MANUAL_REVIEW`, persistent `LZ_RECEIVE_FAILED`, and `tx_outbox.status="failed"` with `retry_state="exhausted"` are operator-action states, not healthy terminal states.

Before any migration approval, run the DB-backed readiness gate and attach the JSON output:

```bash
go run ./go/cmd/readinesscheck -config <worker.yaml> -format json
```

The readiness gate fails if an enabled chain is paused, an enabled pathway between enabled chains is paused, an active chain has exhausted failed `tx_outbox` rows, a packet/job for an enabled service is in a manual-review or failed/conflict/reorg state, or an enabled pathway's required role-specific indexer cursor is missing or has not advanced past block `0`. Executor-enabled processes require `executor_source` on the source chain and `executor_destination` on the destination chain. DVN-enabled processes require `dvn_source` on the source chain and `dvn_destination` on the destination chain.

Migration dashboard panels:

- Chain enabled/paused status by `eid` and `name`.
- Pathway paused status by `src_eid` and `dst_eid`.
- Packet count by pathway and status.
- Executor job count by status.
- DVN job count by status.
- Tx outbox count by chain, status, and retry state.
- Mined receipt gas cost by chain and purpose: `laz_tx_receipt_gas_cost_dst_wei`.
- Worker fee revenue, actual gas cost, gross margin, negative-margin jobs, and unpriced receipts by role and pathway: `laz_worker_fee_revenue_src_wei`, `laz_worker_fee_actual_gas_cost_src_wei`, `laz_worker_fee_gross_margin_src_wei`, `laz_worker_fee_negative_margin_jobs`, and `laz_worker_fee_unpriced_receipts`.
- Signer native balance, configured minimum native balance, balance poll success, last success timestamp, and last error timestamp by chain and signer.
- Indexer cursor last block by chain and stream: `executor_source`, `executor_destination`, `dvn_source`, and `dvn_destination`.
- Indexer poll success, last poll duration, observed head, confirmed block upper bound, and last error timestamp by chain.
- Worker loop retry count and last retry timestamp by loop name.

Operational assumptions:

- Packet, job, outbox, receipt gas-cost, worker fee, pause, and cursor metrics are derived from committed DB state, so a worker restart should not reset that visibility. Indexer poll status, signer balance status, and worker loop retry metrics are process-local and reset on restart.
- Worker binaries default to `-log-level info`. Run with `-log-level debug` during investigations to include normal skip/defer reasons without changing durable state.
- The worker defaults to `-indexer-progress-log-interval 1m`. Set it to `0` to disable periodic indexer progress `Info` logs; per-poll details remain available with `-log-level debug`, and continuous progress should be monitored through `/metrics`.
- Worker logs emit `Info` entries for durable state changes and transaction enqueue/broadcast/receipt milestones, `Warn` entries for conflict, reorg, receipt failure, signing failure, and broadcast failure paths, and `Debug` entries for normal skip/defer reasons that do not change state.
- Indexer logs emit at most one throttled `Info` `indexer progress` entry per chain per interval, aggregating advanced streams, block range, lag, processed item counts, and duration. Per-stream `indexer stream advanced` entries and per-poll `indexer poll completed` summaries are `Debug`. Per-event entries identify source assignments and destination reconciliation events by `guid`, `src_eid`, `dst_eid`, and `tx_hash`.
- Tx manager logs identify nonce bootstrap/claim, signing, broadcast, receipt confirmation/failure, mined gas usage, actual destination-chain gas cost, and retry enqueue by `tx_outbox_id` or `id`, `chain_eid`, `signer`, and `purpose`; they must not include calldata, signatures, keystore contents, or raw secret-bearing config.
- RPC failures and quorum conflicts identify configured endpoints only as `provider[n]`, where `n` is the zero-based configuration order. Logs and persisted job errors must not include complete RPC URLs or their credentials, paths, or query values.
- Pricing logs identify `eid`, source name, primary/sanity role, and failure category. Deviation rejections also include `deviation_bps`. They must not include market-data base URLs, RPC URLs, API keys, or secret-bearing configuration. An unhealthy primary, no healthy declared sanity source, or any healthy sanity deviation stops the whole price-update cycle before enqueue; there is no sanity fallback.
- The `fee_accounting` loop converts mined worker receipt gas costs to source-chain native wei. Pricing-source errors leave affected receipts pending and visible through `laz_worker_fee_unpriced_receipts`; they do not revert tx receipt confirmation or job state transitions.
- `services.executor.enabled` and `services.dvn.enabled` are process-level switches. A deployment that runs only one role should page on that role's streams and job states, while the other role's durable cursors may be absent in that process.
- Txmgr automatically retries failed outbox rows with classified failure kinds for up to five attempts. It also automatically replaces broadcast rows that have no receipt after `tx_manager.stale_broadcast_replacement_after_seconds` seconds, keeping the nonce and using the existing replacement fee bump while still respecting configured fee caps. `txretry` remains the manual override for exhausted rows or operator-reviewed replacement, but it is no longer the default path for ordinary failed rows or ordinary pending replacements.
- If Postgres-backed stats are temporarily unavailable, `/metrics` still exposes process-local indexer and worker loop retry metrics and sets `laz_metrics_db_snapshot_available 0`; `/readyz` remains unavailable until the DB-backed readiness snapshot succeeds.
- `/healthz` is only a liveness probe. Use `/readyz` and `/metrics` for operational readiness and alerting.
- Do not unpause a chain or pathway until the conflict source is identified and the latest `inspect:lz-config` output still matches the intended migration config.
- A deterministic active-DVN destination config mismatch moves the affected job to `MANUAL_REVIEW` and pauses that pathway atomically. Other pathways continue processing; clear the drift and review the recorded `last_error` before unpausing.
- A deterministic executor delivery build failure, including unsupported persisted executor options observed after a permissionless commit, moves the job and packet to `MANUAL_REVIEW` with `last_error` instead of retrying indefinitely.
