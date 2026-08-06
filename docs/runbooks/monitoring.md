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

- `LazWorkerReadinessFailed`: `/readyz` returns non-200 for more than two scrape intervals; page. Readiness also fails while any active-chain signer lane is parked `held(manual)`, `held(nonce_consumed_externally)`, `held(broadcast_exhausted)`, or reprice-held past the automatic replacement cap (reported as the synthetic `reprice_exhausted` reason) — those holds block every higher nonce for the signer until the operator resolves them with `txretry`; the self-healing `reprice_required` (below the cap) and `nonce_reconcile_required` holds and fresh pending cancels do not fail readiness — except a `reprice_required` hold or a pending cancel older than fifteen minutes, which means the mandatory bump cannot land (typically the configured fee cap blocks it, so the automatic path can neither converge nor escalate) and also fails readiness. The cancel age counts from the immutable `cancel_requested_at`; deferrals only move the separate pacing column, so a fee-cap-stuck cancel cannot hide behind a perpetually young age.
- `LazChainPaused`: `laz_chain_paused == 1` for any chain; page immediately. This means chain-wide quorum safety logic paused the worker path. A chain removed from configuration keeps its pause bit in the database as a safety state for a possible re-enable, but its gauge reports 0 — readiness and the durable loops ignore disabled records, so their retained pause must not page.
- `LazPathwayPaused`: `laz_pathway_paused == 1` for any pathway; page immediately. This means packet-level receipt/log conflict safety logic paused a pathway. Disabled pathways likewise report 0.
- Disabled scopes are invisible to packet, job, and outbox statistics: when a chain or pathway is removed from configuration, its retained packets, executor/DVN jobs, and outbox rows (a pricing row follows its send chain) drop out of the `laz_packets_total`, `laz_executor_jobs_total`, `laz_dvn_jobs_total`, and `laz_tx_outbox_total` gauges and out of `/readyz`, so historical `MANUAL_REVIEW`, failed, or exhausted rows of a removed scope neither page nor hold readiness at 503. Paused scopes stay fully visible — a pause is temporary safety, not removal. Held signer-lane gauges are the exception: a held row physically blocks the shared signer lane on its chain, so it stays visible while its chain is enabled.
- `LazDVNQuorumConflict`: `laz_dvn_jobs_total{status="QUORUM_CONFLICT"} > 0`; page immediately and inspect source RPC providers before unpausing.
- `LazDVNReorgDetected`: `laz_dvn_jobs_total{status="REORG_DETECTED"} > 0`; page if it persists past the next confirmation loop; inspect source RPC providers and source transaction receipts.
- `LazPacketManualReview`: `laz_packets_total{status="MANUAL_REVIEW"} > 0`; ticket within one business day; page if count increases during migration.
- `LazExecutorReceiveFailed`: `laz_executor_jobs_total{status="LZ_RECEIVE_FAILED"} > 0`; ticket and inspect destination `LzReceiveAlert` logs. A failed outbox receipt may be automatically cloned for retry and restore the job to `LZ_RECEIVE_TX_ENQUEUED`; persistent `LZ_RECEIVE_FAILED` means the retry path has not yet cleared the workflow state. The executor re-broadcasts a reverting `lzReceive` at most five times before parking the job in `MANUAL_REVIEW`, so a receiver that reverts at execution cannot drain the executor signer indefinitely. Each failing transaction hash is charged against that budget exactly once, even when the same failure is reported by both the receipt applier and the destination `LzReceiveAlert` observer or replayed after a crash.
- `LazWorkerManualReview`: `laz_executor_jobs_total{status="MANUAL_REVIEW"} > 0` or `laz_dvn_jobs_total{status="MANUAL_REVIEW"} > 0`; ticket and block migration approval until reviewed.
- `LazTxOutboxFailed`: `laz_tx_outbox_total{status="failed",retry_state="exhausted"} > 0`; ticket; page if exhausted failure count increases for active migration chains. Rows with `retry_state="retrying"` are still under txmgr automatic retry, and rows with `retry_state="superseded"` either already have a retry child, had the workflow advance past the stale failure, are completed operator resolutions (a mined cancel or a resolved externally consumed nonce), or are failed pricing rows — terminal by design, because their calldata carries a time-bound observation and the bot supersedes them with a fresh one; the pricing snapshot-age alerts own that outcome.
- `LazWorkerFeeNegativeMargin`: `laz_worker_fee_negative_margin_jobs > 0`; page after five minutes. This means mined worker transaction gas cost, converted to source-chain native wei, exceeds the source-chain assignment fee for at least one job.
- `LazWorkerFeeReconciliationPending`: `laz_worker_fee_unpriced_receipts > 0`; ticket after fifteen minutes. Check pricing source health and `fee_accounting` loop logs; tx receipt status has already been recorded and is not blocked by pricing failures.
- `LazSignerLowNativeBalance`: `laz_signer_native_balance_wei < laz_signer_min_native_balance_wei`; page after five minutes. Fund the affected worker signer before queued or replacement transactions exhaust their configured fee caps.
- `LazRPCProviderConflict`: `laz_rpc_provider_status{status="conflict"} == 1` or `laz_rpc_provider_log_conflict == 1` or `laz_rpc_provider_state_conflict == 1` for five minutes; page. A conflicting provider disagrees with the majority quorum on canonical headers, on confirmed log windows, or on comparable state reads (`eth_call`, `eth_getCode`, gas estimates outside the bounded set, pending nonces). The worker keeps making progress while a strict majority still agrees, so this alert is the only signal that a configured endpoint is forked, corrupted, or hostile; remove or replace the endpoint before it can become part of a majority.
- `LazRPCQuorumUnavailable`: `2 * count by (chain_eid, job, instance) (laz_rpc_provider_status{status="unavailable"}) >= count by (chain_eid, job, instance) (laz_rpc_provider_status)` for five minutes; page. The grouping keeps each scrape target separate: every worker instance has its own provider set, and merging instances would let a healthy instance mask another's lost majority. Once half or more of a chain's configured providers are unavailable, the fixed strict majority (`q = floor(N/2) + 1`) can no longer form and all quorum reads for that chain stop fail-closed. This alert covers deployments without indexers (for example pricing-only), where `LazIndexerPollFailing` cannot fire; restore enough independent providers to re-establish the majority.
- `LazPricingSnapshotNearStale`: `laz_pricing_snapshot_time_to_stale_seconds < 300`; page immediately — time-to-stale only decreases between writes, so any hold time would consume the warning window. The age comes from the on-chain `updatedAt` sampled each pricing cycle — never receipt times, so a confirmed batch whose entries were superseded (skipped) on chain cannot fake freshness. Once the snapshot crosses its `staleAfter` cutoff, every quote for the pathway reverts fail-closed; check the pricing loop, the pending write for the feed, and the signer lane.
- `LazPricingPendingStalled`: `laz_pricing_pending_oldest_age_seconds > 300`; page immediately. A pending pricing transaction gates its feed against new snapshots (one write in flight per feed), so one stuck behind a wedged signer lane or a fee cap lets the on-chain price age toward the cutoff. The threshold is half the validated 600-second freshness margin, so escalation lands while the snapshot still has headroom even when the write started at the very end of the schedule. Readiness also fails for a pending pricing transaction older than five minutes. Recover the lane with `txretry` (replace or cancel-nonce); the bot rebuilds from a fresh observation on its next cycle — failed pricing rows are never re-signed because their calldata carries a time-bound market observation.
- `LazIndexerPollFailing`: `laz_indexer_poll_success == 0` with a non-zero `laz_indexer_failure_since_timestamp_seconds`; page only after the failure sequence outlasts the configured poll interval and persists for another five minutes. A successful retry clears the failure sequence before it pages. With quorum reads this also fires when fewer than a strict majority of configured RPC providers respond, or when no log-window majority exists; the indexer cursor stalls fail-closed rather than ingesting unverified logs.
- `LazIndexerPollStalled`: compare `laz_indexer_last_poll_timestamp_seconds`, or `laz_indexer_start_timestamp_seconds` before the first completion, with twice `laz_indexer_poll_interval_seconds`; page after the condition persists for five minutes. This catches an initial or later poll that stops completing without treating regularly completed failed polls as a stall.

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
- Indexer cursor last block by chain and stream in `laz_indexer_cursor_last_block`: `executor_source`, `executor_destination`, `dvn_source`, and `dvn_destination`.
- Indexer configured poll interval, loop start timestamp, poll success, current failure start, last completed poll, last success, last poll duration, observed head, confirmed block upper bound, and last error timestamp by chain: `laz_indexer_poll_interval_seconds`, `laz_indexer_start_timestamp_seconds`, `laz_indexer_poll_success`, `laz_indexer_failure_since_timestamp_seconds`, `laz_indexer_last_poll_timestamp_seconds`, `laz_indexer_last_success_timestamp_seconds`, `laz_indexer_last_poll_duration_seconds`, `laz_indexer_observed_head_block`, `laz_indexer_confirmed_to_block`, and `laz_indexer_last_error_timestamp_seconds`.
- RPC provider quorum classification by chain and redacted provider id: `laz_rpc_provider_status` (`healthy`, `lagging`, `conflict`, `unavailable`) and the sticky per-provider log-conflict flag `laz_rpc_provider_log_conflict`. Reported after every indexer poll and every signer balance poll; the balance poll runs its own head quorum probe before reporting, so deployments without indexers (for example pricing-only) still feed the provider conflict and quorum-unavailable alerts with live classifications rather than the startup snapshot.
- Worker loop retry count and last retry timestamp by loop name.

Operational assumptions:

- Packet, job, outbox, receipt gas-cost, worker fee, pause, and cursor metrics are derived from committed DB state, so a worker restart should not reset that visibility. Indexer poll status, signer balance status, and worker loop retry metrics are process-local and reset on restart.
- Worker binaries default to `-log-level info`. Run with `-log-level debug` during investigations to include normal skip/defer reasons without changing durable state.
- The worker defaults to `-indexer-progress-log-interval 1m`. Set it to `0` to disable periodic indexer progress `Info` logs; per-poll details remain available with `-log-level debug`, and continuous progress should be monitored through `/metrics`.
- Worker logs emit `Info` entries for durable state changes and transaction enqueue/broadcast/receipt milestones, `Warn` entries for conflict, reorg, receipt failure, signing failure, and broadcast failure paths, and `Debug` entries for normal skip/defer reasons that do not change state.
- Indexer logs emit at most one throttled `Info` `indexer progress` entry per chain per interval, aggregating advanced streams, block range, lag, processed item counts, and duration. Per-stream `indexer stream advanced` entries and per-poll `indexer poll completed` summaries are `Debug`. Per-event entries identify source assignments and destination reconciliation events by `guid`, `src_eid`, `dst_eid`, and `tx_hash`.
- Tx manager logs identify nonce bootstrap/claim, signing, broadcast, receipt confirmation/failure, mined gas usage, actual destination-chain gas cost, and retry enqueue by `tx_outbox_id` or `id`, `chain_eid`, `signer`, and `purpose`; they must not include calldata, signatures, keystore contents, or raw secret-bearing config.
- RPC failures and quorum conflicts identify configured endpoints only as `provider[n]`, where `n` is the zero-based configuration order. Logs and persisted job errors must not include complete RPC URLs or their credentials, paths, or query values.
- RPC head, log, account-nonce, and receipt-canonicality reads require a fixed strict majority of the configured providers (`q = floor(N/2) + 1` over all configured endpoints, not over the currently reachable subset). The nonce read feeds nonce reconciliation, which parks signer lanes and advances the durable nonce cursor, so a single fabricated `eth_getTransactionCount` answer can never drive it: without a majority value the reconciliation round is skipped and durable state stays untouched. A minority provider that forks or serves divergent logs is classified `conflict` (head) or flagged by `laz_rpc_provider_log_conflict` (logs, sticky until an agreeing log window clears it) without stopping the worker; when tip candidates diverge and the head vote only succeeds at a lower height, every provider whose tip is above that verified height is classified `unavailable` for the round, so single-source latest reads and sends never route to an unverified fork descendant; losing the majority itself stops reads fail-closed and surfaces as failed polls and stalled cursors, never as ingesting unverified data.
- Configured RPC endpoints for one chain must come from independent failure domains (different infrastructure operators). Pointing multiple configured URLs at the same backend or vendor satisfies the count but not the safety assumption: a single compromised or forked backend could then form a "majority" on its own.
- Pricing logs identify `eid`, source name, primary/sanity role, and failure category. Source-sanity rejections include `deviation_bps` and remain governed by `max_deviation_bps`; an unhealthy primary, no healthy declared sanity source, or any healthy sanity deviation stops the whole price-update cycle before enqueue, with no sanity fallback. Normal write suppression logs `skipped price snapshot update` at `Debug` with the source EID, destination EID, feed, computed `deviation_bps`, seconds since the last write, `min_update_deviation_bps`, and `heartbeat_seconds`. On startup, the bot bootstraps those per-pathway values from the source-chain OpenPriceFeed instead of forcing a cold write. Verify `stale_after_seconds` exceeds `heartbeat_seconds + interval_seconds` by at least 600 seconds (the validated freshness margin). Pricing logs must not include market-data base URLs, RPC URLs, API keys, or secret-bearing configuration.
- The `fee_accounting` loop converts mined worker receipt gas costs to source-chain native wei. Pricing-source errors leave affected receipts pending and visible through `laz_worker_fee_unpriced_receipts`; they do not revert tx receipt confirmation or job state transitions.
- `services.executor.enabled` and `services.dvn.enabled` are process-level switches. A deployment that runs only one role should page on that role's streams and job states, while the other role's durable cursors may be absent in that process.
- `LazTxOutboxOrphaned`: `laz_tx_outbox_orphaned_total > 0`; page immediately. The signing path writes `active_attempt_id` and the send status in one statement, so a `signed` or `broadcast` row with no active attempt is a broken invariant rather than a transient state. Such a row is invisible to receipt polling (which joins the active attempt), a `signed` one keeps its nonce and blocks every higher nonce for that signer, and a `broadcast` one leaves its workflow job enqueued forever; none of this self-heals. The known producer is applying the `002_txmgr_attempts.sql` schema upgrade while rows were in flight — see the pre-upgrade drain requirement in [mainnet-readiness.md](mainnet-readiness.md). Recover each row with `txretry -action cancel-nonce -id <tx_outbox id>`, which signs a same-nonce noop self transfer, parks the owning job as `MANUAL_REVIEW`, and unlocks the lane; a bare nonce-holding row with no attempt is explicitly cancelable. Locate the rows with `laz_tx_outbox_orphaned_total` and `laz_tx_outbox_orphaned_oldest_age_seconds` (labels: chain, signer, status; disabled chains are excluded, matching readiness), or:

```sql
SELECT id, chain_eid, signer_id, status, nonce, updated_at
FROM tx_outbox
WHERE status IN ('signed', 'broadcast') AND active_attempt_id IS NULL
ORDER BY chain_eid, signer_id, nonce;
```

  Readiness fails with `orphaned_outbox_row` for every affected active chain while any row remains.
- Txmgr automatically retries failed outbox rows with classified failure kinds for up to five attempts. Broadcast outcomes never destroy send state: an unrecognized or transport-level send error is treated as possibly accepted, the persisted raw is replayed with bounded backoff, and receipt polling covers every persisted attempt hash. Stale broadcast rows are automatically replaced after `tx_manager.stale_broadcast_replacement_after_seconds` seconds, and an underpriced (reprice-held) row is automatically repriced after a one-minute cooldown — both keep the nonce, bump the fee at least 10% over the latest attempt, respect the configured caps, and stop at the automatic replacement cap. At that cap an accepted broadcast row simply stays under receipt polling, while a still-underpriced row remains reprice-held and surfaces as the synthetic `reprice_exhausted` held reason, which fails readiness until the operator replaces or cancels it. A `nonce too low` hold is reconciled automatically each minute against the chain's confirmed account nonce: a still-unspent nonce resumes broadcasting (unless its attempt's broadcast budget is already exhausted — then it parks as `held(broadcast_exhausted)`, because resuming would strand it outside both the replay pipeline and the lower-nonce barrier); a nonce the chain consumed with no receipt on any of our attempts parks as `held(nonce_consumed_externally)` with the observed evidence (this indicates the single-broadcaster assumption was violated — treat it as a key-management incident) and fast-forwards the local nonce cursor so fresh work signs past the consumed range. A row in `status = held` blocks all higher nonces for its signer and needs operator action: `txretry -action replace` authorizes one more replacement for a broadcast, reprice-held, or `broadcast_exhausted` row (a replacement is a fresh attempt with a fresh replay budget for the same intent); `txretry -action cancel-nonce` abandons any nonce-holding row with a same-nonce noop self transfer (the owning job parks as MANUAL_REVIEW, the row terminates as `failed/canceled`, and the lane unlocks) and clears any pending replace request — re-requesting cancel on an already-canceling row is what authorizes one cancel fee bump; `txretry -action resolve-external-nonce -resolution retry|abandon` terminates an externally consumed row and either clones a fresh task or parks the job. Watch `laz_tx_outbox_held_total` and `laz_tx_outbox_held_oldest_age_seconds` (labels: chain, signer, reason — including the synthetic `cancel_requested`) for stuck lanes.
- If Postgres-backed stats are temporarily unavailable, `/metrics` still exposes process-local indexer and worker loop retry metrics and sets `laz_metrics_db_snapshot_available 0`; `/readyz` remains unavailable until the DB-backed readiness snapshot succeeds.
- `/healthz` is only a liveness probe. Use `/readyz` and `/metrics` for operational readiness and alerting.
- Do not unpause a chain or pathway until the conflict source is identified and the latest `inspect:lz-config` output still matches the intended migration config.
- Pause semantics on the send side: once a pause or config-disable commits, the worker adds no new nonce for that scope — work selection stops offering the affected jobs, executor/DVN/pricing transaction enqueues are refused, queued outbox rows are held back from signing, and automatic failure retries — receipt-failed clones and estimate-revert requeues alike — are deferred (their failure metadata and attempt budget stay untouched) until the scope is active again — except the lzReceive purpose, whose failed outbox row is finalized while the `LZ_RECEIVE_FAILED` job keeps carrying the retry budget; the deliverer re-enqueues it once the pathway is active. A packet's scope is its exact pathway plus both endpoint chains; a pricing transaction's scope is its send chain. The bounded set of transactions that already held a nonce before the pause still converges: they are broadcast, replaced, repriced, reconciled, and receipt-polled to a terminal state, because freezing them would wedge the shared signer lane. Operator cancel (`txretry -action cancel-nonce`) and nonce reconciliation stay available while paused — they repair the lane rather than add spend.
- A deterministic active-DVN destination config mismatch moves the affected job to `MANUAL_REVIEW` and pauses that pathway atomically. Other pathways continue processing; clear the drift and review the recorded `last_error` before unpausing.
- A deterministic executor delivery build failure, including unsupported persisted executor options observed after a permissionless commit, moves the job and packet to `MANUAL_REVIEW` with `last_error` instead of retrying indefinitely.

After resolving and reviewing an active-DVN destination config mismatch, reset the selected job and its pathway together. Supply the complete 64-character GUID without the `0x` prefix. Resetting to `ASSIGNED` makes the worker re-run destination validation, source-confirmation waiting, and source receipt quorum validation rather than trusting the earlier review state. A remaining mismatch returns the job and pathway to manual review atomically.

```bash
psql "$DATABASE_URL" -v guid='<64-character-guid>' <<'SQL'
BEGIN;
WITH target AS (
  SELECT packet.guid, packet.src_eid, packet.dst_eid, packet.sender, packet.receiver
  FROM packets AS packet
  JOIN dvn_jobs AS job ON job.guid = packet.guid
  WHERE packet.guid = decode(:'guid', 'hex') AND job.status = 'MANUAL_REVIEW'
  FOR UPDATE OF packet, job
), resumed AS (
  UPDATE dvn_jobs AS job
  SET status = 'ASSIGNED', quorum_result = NULL, last_error = NULL,
      retry_count = 0, next_retry_at = NULL, updated_at = now()
  FROM target
  WHERE job.guid = target.guid
  RETURNING job.guid
), unpaused AS (
  UPDATE pathways AS pathway
  SET paused = false
  FROM target, resumed
  WHERE pathway.src_eid = target.src_eid
    AND pathway.dst_eid = target.dst_eid
    AND pathway.src_oapp = target.sender
    AND pathway.dst_oapp = target.receiver
  RETURNING pathway.id
)
SELECT (SELECT count(*) FROM resumed) AS resumed_jobs,
       (SELECT count(*) FROM unpaused) AS unpaused_pathways,
       (SELECT count(*) FROM resumed) = 1
         AND (SELECT count(*) FROM unpaused) = 1 AS recovery_ok
\gset
\echo resumed_jobs=:resumed_jobs unpaused_pathways=:unpaused_pathways
\if :recovery_ok
  COMMIT;
\else
  ROLLBACK;
  \warn 'DVN recovery changed an unexpected number of rows; transaction rolled back'
  \quit 1
\endif
SQL
```

The transaction commits only when `resumed_jobs = 1` and `unpaused_pathways = 1`; otherwise it rolls back and exits nonzero. After a successful reset, run the readiness check and watch the selected GUID through the DVN states.
