# oh-my-lazier

Self-hosted LayerZero V2 Executor and DVN worker stack.

The repo contains:

- Solidity contracts for `TestOFT`, worker-scoped `OpenPriceFeed`, `OpenExecutor`, `OpenDVN`, worker options, access control, submitter-managed shared price snapshots, and worker fee models.
- TypeScript scripts for deployment, LayerZero config, canaries, local E2E, ABI generation, runbook checks, and migration evidence checks.
- Go worker services for config validation, indexing, executor delivery, DVN verification, pricing, tx management, readiness, and metrics.
- Docker Compose setups for local Postgres, integration dependencies, and the dual-Anvil E2E.

## Layout

```text
contracts/contracts/   Solidity contracts
contracts/scripts/     Deployment, inspection, validation, and E2E scripts
contracts/test/        Solidity tests
go/                    Go worker, CLI, DB, config, and runtime packages
config/                Example worker config
docs/                  Runbooks, deployment policy, security records, and monitoring
```

## Requirements

- Node.js 26+
- Go 1.26+
- Docker, for integration/E2E/smoke targets
- Foundry `forge`, for Solidity formatting checks
- `golangci-lint`, for `make check`

Install dependencies:

```bash
npm install
```

## Main Commands

The Makefile intentionally exposes only repository-level gates and CI/doc
entrypoints. Use the underlying `npm`, `go`, `gofmt`, `forge`, or `docker`
commands directly for narrow local loops.

```bash
make check            # compile, typecheck, ABI drift checks, tests, docs checks, lint, format checks
make test-integration # Docker Compose Postgres plus Rustack KMS integration tests
make security-check   # security review check, npm audit disposition, govulncheck
make docker-smoke     # build worker image and verify its entrypoint
make e2e-local        # local Postgres + LocalStack KMS + two Anvil chains + canary, RBF, multi-send, replay, and fee-withdrawal flow
make e2e-ci           # CI E2E with prestarted services and a prebuilt worker image
```

ABI artifacts are committed under `go/internal/lzabi/abis`, `go/internal/configcheck/abis`, and `go/internal/pricing/abis`.

```bash
make generate-lzabi
make check-lzabi
make generate-pricing-abi
make check-pricing-abi
```

## Worker

The example config is [config/example.yaml](config/example.yaml). Start the default local stack with:

```bash
docker compose up
```

Run the same on-chain config gate used at worker startup:

```bash
go run ./go/cmd/configcheck -config config/example.yaml
```

`DATABASE_URL` may override the configured database URL at runtime. Other config is loaded once at startup; runtime config changes require a process restart.

Useful operator commands:

```bash
go run ./go/cmd/worker -config <worker.yaml> -log-level debug -indexer-progress-log-interval 1m
go run ./go/cmd/configdiff -from <approved.yaml> -to <proposed.yaml>
go run ./go/cmd/readinesscheck -config <worker.yaml> -format json
go run ./go/cmd/pricebot-once -config <worker.yaml> -log-level debug
go run ./go/cmd/draincheck -config <worker.yaml> -src-eid <src> -dst-eid <dst> -format json
go run ./go/cmd/txretry -config <worker.yaml> -action retry-failed|replace|cancel-nonce|resolve-external-nonce [-resolution retry|abandon] -id <tx_outbox_id>
```

Worker binaries default to `-log-level info`. Use `-log-level debug` when investigating normal skip/defer reasons such as indexer caught-up windows, disabled pathways, not-yet-confirmed DVN jobs, or deferred tx manager work.
The long-running worker also defaults to `-indexer-progress-log-interval 1m`, which limits indexer progress `Info` logs to one aggregated line per chain per interval; set it to `0` to disable periodic progress `Info` logs and rely on `/metrics` plus debug logs.
Use `go run ./go/cmd/worker -config <worker.yaml> -skip-onchain-check` only as a long-running worker startup bypass for the on-chain config check. It does not skip local YAML/schema validation, and it does not affect `configcheck` or `pricebot-once`. Pricing chains configured with on-chain sources (Chainlink, Uniswap) still establish their RPC head quorum before source validation, and every remaining configured chain establishes it before the durable loops start, so the bypass never lets a single provider serve unverified startup reads.

## Phase 1 Scope

Phase 1 is EVM-only.

- Worker chain configs must declare `family: evm`.
- Required DVNs are the configured `OpenDVN` plus at least one independent
  external DVN. LayerZero Labs DVN is an optional external DVN choice, not a
  required provider; deployment profiles can opt into the repo-known
  Sepolia/Hoodi address with `chains[].includeLayerZeroLabsDVN`.
- Basic OFT send is supported.
- `composeMsg`, `lzCompose`, native drop, ordered execution, self-only DVN, and non-EVM chains are out of scope.
- Executor options must contain exactly one zero-value executor `lzReceive` option.
- `OpenDVN` rejects non-empty DVN options.
- Shared price snapshots must be fresh.

`OpenExecutor` remains compatible with the pinned nonpayable `ILayerZeroExecutor.assignJob` interface. `OpenExecutor` and `OpenDVN` quote and emit assignment price information while pinned `SendUln302` records returned worker fees in its own ledger. Worker owners withdraw those recorded fees through the worker `withdrawFee(sendLib, recipient, amount)` passthrough for allowed send libraries.

## Runtime Notes

- Startup fails before durable loops if local config is invalid or live chain state does not match the loaded YAML.
- Unsigned worker YAML fields accept only non-negative YAML integer scalars, not decimals or quoted numeric strings. Second-based values used as runtime durations must also fit the worker's duration range.
- Every configured RPC URL must report the configured EVM chain ID.
- Chain head, log, and state reads go through a fixed strict-majority quorum over all configured RPC providers (`q = floor(N/2) + 1` of the configured count, not of the reachable subset). State reads (`eth_call`, `eth_getCode`, pending nonces) vote on comparable results — successes by payload, deterministic reverts by identity — with nil-block calls anchored to the verified canonical block hash. A provider error counts as a deterministic revert only when the provider itself claims one (RPC code 3, a standalone `revert`/`reverted` word, or a `VM execution error.` carrying validated ABI revert bytes), so an opaque diagnostic payload never votes; a revert that wins its vote is marked as the quorum's verdict and terminal transaction handling consumes that marker instead of re-reading provider text. Gas estimates aggregate a bounded set around the upper median on each provider's latest state and reject values past the canonical block gas limit; state-read disagreement is tracked as a separate sticky per-provider conflict dimension and never re-promotes a lagging or forked provider's head status. The canonical head is the highest height a majority has reached with a majority-identical block hash, and indexer log windows require majority-identical log sequences from providers whose snapshot tip covers the window. A disagreeing minority provider is flagged (`conflict` head status, sticky log-conflict metric) without stopping progress; losing the majority stops the affected reads fail-closed. Startup establishes the head quorum before the first on-chain config read. Configured RPC endpoints must come from independent failure domains — duplicating one backend across URLs satisfies the count but silently voids the majority-safety assumption.
- Address fields are parsed as EVM 20-byte hex addresses during config load.
- Worker contract addresses remain required in every pathway config, even when this process runs only one role.
- `services.executor.enabled` and `services.dvn.enabled` default to true when omitted; pricing remains independently controlled.
- Pricing transaction fee caps and minimum signer balances are configured per EID under `pricing.chains[].tx_policy`; `pricing.stale_after_seconds` cannot exceed the OpenPriceFeed one-day maximum. Scheduled snapshots are written only after `pricing.min_update_deviation_bps` movement or `pricing.heartbeat_seconds` elapsed. `pricing.stale_after_seconds` must exceed `pricing.heartbeat_seconds` plus `pricing.interval_seconds` by at least 600 seconds, so a scheduled heartbeat refresh always retains an enqueue/signing/confirmation margin and the pending-write readiness escalation fires while the snapshot still has headroom. Fee caps intentionally have no repository-wide absolute ceiling because they are chain-specific trusted operator inputs and must be approved during config review.
- Before durable loops and database initialization, worker startup performs bounded, concurrent pricing-source identity checks: it probes configured market IDs, requires Chainlink descriptions to match, requires Uniswap pools to expose the configured token pair, and confirms that each configured Uniswap TWAP window has enough observation history. A successful CoinMarketCap or CoinGecko response that omits the requested ID is retried up to three attempts within the source-request timeout; only repeated omission becomes a deterministic configuration mismatch. Stable request/identity mismatches, contract-call reverts during identity checks, an Uniswap `OLD` observation-history revert, empty or malformed ABI responses from otherwise successful identity calls, malformed or non-HTTPS market-data BaseURLs, and missing or malformed secret references fail fast. Market-data HTTP 403/404 responses, non-`OLD` Uniswap observation failures, timeouts, transport/RPC errors, rate limits, and upstream server failures are deferred to the supervised runtime loops, so transient pricing outages do not terminate unrelated worker loops. Every market-data endpoint must use HTTPS, and its client does not follow redirects to another origin.
- Runtime Chainlink reads pin `description`, `decimals`, and `latestRoundData` to one latest block number. Uniswap remains sanity-only and requires a TWAP window of at least 1800 seconds.
- Tx fees are selected at send time by `txmgr`, which estimates gas, reads current RPC fee suggestions, applies configured caps, and signs. A chain with `legacy_transactions: true` always quotes type-0 (legacy) fees, even when its latest header reports a base fee, for mempools that drop EIP-1559 transactions; such chains do not require a priority-fee cap. Every signed transaction is persisted as an immutable `tx_attempts` row before any node sees it: broadcasts, bounded same-raw replays, and same-nonce replacements after `tx_manager.stale_broadcast_replacement_after_seconds` all send previously persisted raw bytes, so an ambiguous send result or a crash mid-broadcast can never lose receipt tracking for a possibly accepted transaction. An underpriced raw is automatically repriced (10% bump under the configured caps) after a one-minute cooldown; a node's deterministic rejection or an exhausted signing or replay budget parks the signer lane as `held` for operator review instead of releasing the nonce, while an accepted broadcast that reaches the automatic replacement cap simply stays under receipt polling. A signer assigns a fresh nonce only when no earlier row is still short of broadcast (nonce-assigned, signed, or held), so one nonce at a time moves through the signing pipeline while already-broadcast nonces converge under receipt polling. A `nonce too low` hold is reconciled automatically against the chain's confirmed account nonce: a still-unspent nonce resumes broadcasting, while an externally consumed nonce parks with evidence (fast-forwarding the local cursor) until the operator resolves it with `txretry resolve-external-nonce`. Any nonce-holding row can be abandoned with `txretry cancel-nonce`, which replaces it with a same-nonce noop self transfer and parks the owning job for manual review. A mined receipt's terminal workflow state is applied only once the receipt is buried under the chain's configured confirmation depth and its block hash is the majority canonical hash at that height, mirroring the indexer, so a short reorg cannot leave a terminal database state for a transaction the chain rolled back.
- A `signed` or `broadcast` outbox row with no active attempt is a broken invariant (the signing path writes both in one statement), so it is detected rather than tolerated: `laz_tx_outbox_orphaned_total` reports it, readiness fails with `orphaned_outbox_row` for that chain, and the operator clears the row with `txretry -action cancel-nonce`. Such a row is invisible to receipt polling and, when `signed`, blocks every higher nonce for its signer. Applying the `002_txmgr_attempts.sql` upgrade while rows are in flight is the known producer; the pre-upgrade drain requirement is in the mainnet-readiness runbook.
- Pausing a chain or pathway (safety logic or operator) is enforced across the whole send side: after the pause commits, no new nonce is added for that scope — work selection, executor/DVN/pricing transaction enqueues, outbox signing, and automatic failure retries all hold back until the scope is active again, and the enqueue/signing decisions take share locks on the pause carriers so a racing pause cannot be missed. Transactions that already held a nonce before the pause converge to a terminal state (broadcast, replacement, reprice, reconciliation, and receipt polling continue), since freezing them would wedge the shared signer lane; `txretry cancel-nonce` remains available under pause to abandon them.
- Worker fee accounting records mined receipt gas usage, converts destination-chain gas cost back into source-chain native wei with the configured pricing sources, and exposes revenue, actual cost, gross margin, negative-margin jobs, and pending reconciliation through `/metrics`.
- Worker metrics expose each active transaction signer's native balance against its configured `min_native_balance_wei` threshold.
- Indexers poll confirmed block windows at each chain's `indexer_poll_interval_seconds` cadence (default 5 seconds) and persist role-specific cursors in Postgres.
- Retryable loop errors are logged and supervised with backoff; non-retryable loop errors stop `App.Run`.

## Maintained Docs

- [contracts/scripts/README.md](contracts/scripts/README.md): deployment, LayerZero config, local E2E, canary, and rollback script usage.
- [docs/deployments/test-oft-policy.md](docs/deployments/test-oft-policy.md): Sepolia/Hoodi TestOFT rehearsal deployment policy.
- [docs/deployments/bsc-testnet.md](docs/deployments/bsc-testnet.md): BSC testnet scope, worker deployment records, and GOAT ↔ BSC lane activation gate.
- [docs/runbooks/mainnet-readiness.md](docs/runbooks/mainnet-readiness.md): required review sequence before mainnet.
- [docs/runbooks/config-diff.md](docs/runbooks/config-diff.md): config review and on-chain config check workflow.
- [docs/runbooks/key-management.md](docs/runbooks/key-management.md), [docs/runbooks/price-bot.md](docs/runbooks/price-bot.md), [docs/runbooks/rate-limit.md](docs/runbooks/rate-limit.md), [docs/runbooks/monitoring.md](docs/runbooks/monitoring.md): operator checklists.
- [docs/security/security-review.md](docs/security/security-review.md) and [docs/security/npm-audit-disposition.md](docs/security/npm-audit-disposition.md): release-readiness security records.
