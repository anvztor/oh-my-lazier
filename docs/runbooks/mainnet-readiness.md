# Mainnet Readiness Runbook

This runbook is the final review index before any mainnet deployment proposal. Phase 1 remains EVM-only and must not use self-only DVN.

## Required Inputs

- Validated worker config for the target environment.
- `configdiff` output from the last approved config to the proposed config.
- `configcheck` output proving the proposed worker config matches live chain state.
- LayerZero address refresh output from `npm run check:lz-addresses`.
- DB-backed readiness output from `go run ./go/cmd/readinesscheck -config <worker.yaml> -format json`.
- `inspect:lz-config` output for every configured direction.
- Signer inventory and key-management review.
- Price bot runbook evidence for the latest OpenPriceFeed shared price snapshot update and worker fee-model check.
- Rate-limit and pause review for every OFT pathway.
- Monitoring dashboard and alert proof.
- Runbook review output from `npm run check:runbooks`.
- Security review report with no open critical findings.

## Non-Negotiable Phase 1 Scope

- Ethereum Sepolia <-> Hoodi rehearsal must complete before mainnet.
- No `composeMsg`.
- No `lzCompose`.
- No native drop.
- No ordered execution.
- No non-EVM chain support.
- Worker chain configs must declare `family: evm`.
- No self-only DVN.
- Required DVNs must include OpenDVN and at least one independent external DVN.
  LayerZero Labs DVN is an optional external DVN choice, not a required
  provider; deployment profiles can opt into repo-known metadata with
  `chains[].includeLayerZeroLabsDVN` when it exists for the local chain.
- Confirmations must be explicitly configured per chain and match the approved LayerZero ULN configuration.

## Contract Script Boundary

All contract-script entrypoints run through the npm `hardhat run --no-compile`
wrappers. Each online single-chain invocation must provide a Hardhat
`--network` and a strict, command-specific JSON envelope through
`OML_SCRIPT_PARAMS`. The envelope and profile may contain only public
operational inputs. RPC credentials, private keys, keystore passwords, and API
keys must come from Hardhat configuration variables/keystore or the automation
secret store.

Every transaction-capable command requires an explicit top-level `apply`.
Archive the `apply: false` plan with the ticket before authorizing
`apply: true`. Interactive operation confirms the full write set once; non-TTY
operation must use `confirmation: "approved"`. The actual Hardhat signer must
match the approved owner/delegate recorded by the command input.

## Review Sequence

1. Run `make check`.
2. Run `go run ./go/cmd/configdiff -from <approved.yaml> -to <proposed.yaml>`.
3. Run `go run ./go/cmd/configcheck -config <proposed.yaml> -format json`.
4. Run `OML_SCRIPT_PARAMS=<inspect-params.json> npm run inspect:lz-config -- --network <network>` for each configured direction and archive stdout.
5. Complete `docs/runbooks/key-management.md`.
6. Complete `docs/runbooks/price-bot.md`.
7. Complete `docs/runbooks/rate-limit.md`.
8. Confirm `docs/runbooks/monitoring.md` dashboard and alerts are active.
9. Run `npm run check:runbooks` and archive output.
10. Run `go run ./go/cmd/readinesscheck -config <worker.yaml> -format json` and archive output.
11. Complete security review and resolve all critical findings.
12. Confirm rollback steps for Executor and DVN config are documented with previous config values.
13. Confirm canary transfer amount, sender account, recipient account, minimum recipient balance, signer, owner, and operator contacts.
14. Run `OML_SCRIPT_PARAMS=<record-params.json> npm run check:migration-evidence`, where the file contains `{ "input": { "migrationEvidence": "<record.json>" } }`.
15. Approve the migration ticket only after every artifact above is attached.

## Go / Worker Checks

Required commands:

```bash
go test ./...
go test ./go/internal/signer/keystore ./go/internal/signer/kms -count=1
go test ./go/internal/config ./go/internal/configdiff ./go/cmd/configdiff -count=1
go test ./go/internal/configcheck ./go/cmd/configcheck -count=1
go test ./go/internal/metrics ./go/internal/db ./go/internal/app -count=1
go test ./go/internal/readiness ./go/cmd/readinesscheck -count=1
npm run check:runbooks
make security-check
```

Required runtime checks:

- `/healthz` returns `200`.
- `/readyz` returns `200`.
- `/metrics` exposes chain pause, pathway pause, packet, executor, DVN, tx outbox, signer native balance, indexer cursor, and indexer polling metrics.
- No chain or pathway is paused before the migration begins.
- No active-chain tx outbox row is stuck in `failed` with `retry_state="exhausted"`.
- Every active transaction signer reports `laz_signer_native_balance_wei >= laz_signer_min_native_balance_wei`.
- For deployments with `services.dvn.enabled: true`, no DVN job is stuck in `READY_TO_VERIFY` or `VERIFY_TX_ENQUEUED` beyond the expected tx manager polling and confirmation window.
- For deployments with `services.executor.enabled: true`, no executor job is stuck in `WAITING_DVN_VERIFICATION`, `VERIFIABLE`, `COMMIT_TX_ENQUEUED`, `COMMITTED`, `EXECUTABLE`, or `LZ_RECEIVE_TX_ENQUEUED` beyond the expected source/destination confirmation window.
- Every enabled pathway has advanced indexer cursors for the roles enabled in that process: executor requires `executor_source` and `executor_destination`; DVN requires `dvn_source` and `dvn_destination`.
- `go run ./go/cmd/readinesscheck -config <worker.yaml>` exits successfully.

### Schema upgrade: drain the send side before applying 002

`002_txmgr_attempts.sql` moves the transaction hash, gas limit, and fee caps
off `tx_outbox` and onto the new `tx_attempts` rows, dropping those columns.
It does not backfill an attempt for a row that is already in flight, so a
`signed` or `broadcast` row that survives the upgrade loses its only hash and
gets no `active_attempt_id`. Receipt polling joins the active attempt, so such
a row is never polled or replaced even if its transaction lands, and a
`signed` row keeps its nonce and blocks that signer lane.

Before upgrading an already-initialized database across 002:

- Stop the worker, then confirm no `tx_outbox` row on any active chain is in
  `signed` or `broadcast`:

```sql
SELECT chain_eid, signer_id, status, count(*)
FROM tx_outbox
WHERE status IN ('signed', 'broadcast')
GROUP BY 1, 2, 3;
```

- If any row is listed, restart the worker and let receipt polling terminalize
  it (or resolve it with `txretry`) before applying the migration. A fresh
  database is unaffected: it applies 001 + 002 with an empty outbox.

## Contract / LayerZero Checks

Required commands:

```bash
npm run typecheck
npx hardhat test solidity --no-compile
OML_SCRIPT_PARAMS=<inspect-params.json> npm run inspect:lz-config -- --network <network>
```

Required state:

- OFT peers are configured in both directions.
- OpenExecutor and OpenDVN allow only intended SendLib addresses.
- Worker pathway config is enabled only for approved OApps.
- Endpoint executor config points to each pathway's configured `source_workers.open_executor` only after the executor migration step.
- Source SendUln config matches each pathway's configured `send_required_dvns` exactly (the pathway's `source_workers.open_dvn` plus every approved independently operated DVN, no extras) only during the DVN join step.
- Destination ReceiveUln config matches each pathway's configured `receive_required_dvns` exactly (the pathway's `destination_workers.open_dvn` plus every approved independently operated DVN, no extras) only during the DVN join step.
- Destination OpenDVN authorizes the active destination `tx_roles.dvn` signer before active DVN mode is enabled.
- Optional DVNs are explicitly disabled for the first-phase required-DVN migration.
- Source OpenPriceFeed `priceSnapshot(dstEid)` is fresh, the pricing signer is an authorized PriceFeed submitter, OpenExecutor/OpenDVN `priceFeed()` both point to the configured feed, and each worker's `feeModel(dstEid)` matches the approved price evidence. Same-native pathways document the 1:1 route; cross-asset pathways document the selected primary, explicit freshness limit, optional sanity set, and deviation threshold. Chainlink and Uniswap are required only when explicitly referenced, and Uniswap may only be sanity.

## Rollback Approval

The rollback section of the migration ticket must include:

- previous Executor config
- previous send ULN config
- previous receive ULN config
- `OML_SCRIPT_PARAMS=<rollback-params.json> npm run configure:lz-rollback -- --network <network>` output with top-level `apply: false`, showing the exact rollback `setConfig` batches
- restored Executor/ULN config check after rollback
- canary transfer evidence after rollback
- owner account able to pause/unpause the affected OApp/OFT pathway
- signer account able to submit worker transactions
- `go run ./go/cmd/draincheck -config <worker.yaml> -src-eid <src> -dst-eid <dst> -format json` output for the affected pathway
- manual retry plan for verified but undelivered packets when `verified_but_undelivered_count` is non-zero, using txmgr automatic retry and `tx_manager.stale_broadcast_replacement_after_seconds` pending replacement first. Run `go run ./go/cmd/txretry -config <worker.yaml> -action retry-failed|replace -id <tx_outbox_id>` only after automatic retry is exhausted or an operator override is approved. Reprice-held (underpriced) rows recover automatically after a one-minute cooldown until the automatic replacement cap; `replace` records an operator replacement request on a broadcast or reprice-held row, resets its signing-failure budget, and authorizes one replacement past the automatic cap and cooldowns; the tx manager then signs a same-nonce replacement attempt at the greater of the fresh RPC suggestion and a 10% bump over the previous signed fee (deferring instead of exceeding the configured cap) and broadcasts the persisted raw. `retry-failed` requeues estimate-gas failures in place; only receipt-failed rows are cloned to keep mined failure evidence, and lzReceive receipt retries are skipped when the executor workflow has already advanced. Rows in `status = held` with reason `nonce_reconcile_required` are reconciled automatically against the confirmed chain nonce (read under the fixed provider majority; a round without a majority nonce is skipped); reason `broadcast_exhausted` (the active attempt spent its replay budget without an accepted send) recovers with `replace` or `cancel-nonce`; reason `manual` needs `txretry -action cancel-nonce` (abandon the task attempt and free the nonce with a noop; a lane whose active attempt is already a cancel instead gets one bumped cancel authorized by re-running `cancel-nonce` — `replace` does not accept manual holds); reason `nonce_consumed_externally` needs `txretry -action resolve-external-nonce -resolution retry` (terminate and clone a fresh task) or `-resolution abandon` (terminate and park the job for manual review).

## Rejection Criteria

Reject mainnet readiness if:

- any critical security finding remains open
- self-only DVN is proposed
- required DVNs do not include an independent external DVN
- confirmations are missing, zero, or do not match the approved LayerZero ULN configuration
- signer inventory or rollback signer is missing
- price snapshot evidence is missing, stale, or not produced through the approved pricing path
- rate-limit capacity/refill is not documented per pathway
- monitoring alerts are not active
- `make security-check` fails
- config diff output is missing or unreviewed
- canary amount, sender, recipient, receipt, or recipient balance evidence is missing
- `npm run check:migration-evidence` fails for the migration ticket record
- `go run ./go/cmd/readinesscheck -config <worker.yaml>` reports any issue
