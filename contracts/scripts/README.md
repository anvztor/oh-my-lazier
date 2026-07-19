# Contract Scripts

These scripts use the compiled Hardhat artifacts and `viem`. They require `npm run compile` before execution. TypeScript scripts accept CLI flags using kebab-case names derived from the previous environment variable names, such as `--rpc-url` for `RPC_URL` and `--test-oft` for TestOFT rehearsal scripts; environment variables remain a fallback for secrets and automation. Deployment uses split Hardhat Ignition modules for rehearsal OApps, worker contracts, OApp/Endpoint config, and worker pathway config; the scripts in this directory handle post-deploy configuration, inspection, evidence checks, canaries, and rollback.

Before any funded testnet migration, confirm the committed LayerZero address list still matches current official metadata:

```bash
npm run check:lz-addresses
```

The check compares protocol contract values encoded in the script against
LayerZero's current `deploymentsV2.json` metadata. External DVN selection is
operator configuration and is not required by this check.

Run the local dual-Anvil E2E from the repository root:

```bash
make e2e-local
```

The local target writes all generated files under `tmp/e2e`, starts disposable
Postgres, LocalStack KMS, isolated Anvil, and worker services through
`docker-compose.e2e.yml`, creates a local `ECC_SECG_P256K1` KMS key, runs
`npm run e2e:deploy-local` to deploy local EndpointV2, SendUln302,
ReceiveUln302, TestOFT, OpenPriceFeed, OpenExecutor, a primary OpenDVN, and a
secondary OpenDVN on both chains, generates a worker keystore, runs
`configcheck`, and
then runs `npm run e2e:run-local`. Chain A uses the generated KMS signer for
executor and active DVN roles; chain B uses the generated keystore signer. The
local runner sends OFT canaries in both directions and then withdraws each
source worker's recorded SendUln302 fee through the worker
`withdrawFee(sendLib, recipient, amount)` passthrough. The runner also calls
`TestOFT.multiSend` on the A -> B direction to emit two
OFT sends in one source transaction, writes
`tmp/e2e/multi-oft-send-indexer.json`, and runs `go/cmd/e2eindexcheck` through
the `make` target to prove the indexer persisted both packet, executor-job, and
DVN-job rows for that single source transaction. After that online indexer
check, the target stops the worker, clears the local E2E Postgres schema, and
runs `go/cmd/e2ereplaycheck` with `tmp/e2e/destination-replay.json` to prove
destination-chain historical `PacketVerified`, `PacketDelivered`, and
`PayloadVerified` events can rebuild delivered executor and verified DVN state
without any local outbox rows. Before clearing the schema, the replay check
requires every configured loopback RPC to report its configured local chain ID.
On one canary direction,
the runner disables destination Anvil automine with `evm_setAutomine`, observes
a pending worker `commitVerification` transaction, waits for txmgr to replace it
with a same-nonce bumped-fee transaction, and then mines the replacement before
continuing delivery assertions. It does not start the price bot; deployment
writes a fresh shared OpenPriceFeed snapshot batch and local worker fee models.
Pinned SendUln302 accounts returned worker fees internally without forwarding
native value to worker `assignJob`; operators withdraw those recorded fees
through the same worker passthrough.
Set `E2E_KEYSTORE_PASSWORD` to override the generated local keystore password;
the same value is passed to the worker container.

CI runs the same deployment and canary scripts through `make e2e-ci`, but
GitHub Actions services own Postgres, LocalStack KMS, and the two Anvil chains. The CI target
does not call Docker Compose or build the worker image; CI builds
`oh-my-lazier-worker:e2e` before invoking it. The target generates host and
container configs under `tmp/e2e`, starts the worker image with host networking,
and points worker readiness at `http://127.0.0.1:9090/readyz`. Override
`E2E_CI_WORKER_IMAGE`, `E2E_CHAIN_A_HOST_RPC_URL`, `E2E_CHAIN_B_HOST_RPC_URL`,
`E2E_CHAIN_A_CONTAINER_RPC_URL`, `E2E_CHAIN_B_CONTAINER_RPC_URL`,
`E2E_HOST_DATABASE_URL`, `E2E_CONTAINER_DATABASE_URL`,
`E2E_KMS_HOST_ENDPOINT`, or `E2E_KMS_CONTAINER_ENDPOINT` only when the service
ports differ from the repository defaults. `make e2e-ci` assumes the Linux
Docker host-network behavior used by GitHub-hosted Ubuntu runners; use
`make e2e-local` for Docker Desktop development.

The active DVN transaction target is the destination OpenDVN, not
ReceiveUln302 directly. `e2e:run-local` requires the worker-submitted primary
OpenDVN verification and a script-submitted secondary OpenDVN verification,
then checks that ReceiveUln302 emitted `PayloadVerified` for both OpenDVNs,
EndpointV2 emitted `PacketDelivered`, the delivery transaction sender matches
the destination chain's configured executor signer, the primary OpenDVN verifier
matches the destination chain's configured DVN signer, and the recipient TestOFT
balance increased. The same run also checks txmgr replacement of one pending
`commitVerification` transaction using the generated local
`tx_manager.stale_broadcast_replacement_after_seconds: 2` setting, plus each
source chain's executor, primary OpenDVN, and secondary OpenDVN fee ledger,
withdrawal events, recipient balance increase, SendUln302 balance decrease, and
zeroed fee ledger. The replay artifact also records the observed destination
commit, receive, and primary-DVN verification transaction hashes for the
multi-send packets so the database rebuild check can assert the persisted hashes
match chain history.

When registry access is unavailable, set `ANVIL_IMAGE` to a compatible local
Foundry image. If `oh-my-lazier-worker:e2e` already exists, set
`E2E_WORKER_UP_FLAGS=--no-build` to reuse it instead of rebuilding the worker
image.

Before approving a deployment or migration evidence record, validate that the
record has attached the required local evidence references:

```bash
npm run check:migration-evidence -- \
  --migration-evidence docs/deployments/sepolia-hoodi/migration-evidence.json
```

The evidence checker verifies that the record includes `make check`, LayerZero
address refresh, DB-backed readiness check, key/price/rate-limit/monitoring/
runbook/security review evidence, that the only phase-1 directions are Ethereum
Sepolia `40161` <-> Hoodi `40449`, that each direction records source and
destination worker contracts, deployment preflight, LayerZero config after,
shared price snapshot evidence tied to the destination EID and freshness window,
drain, canary amount/sender/recipient/minimum balance/receipt/balance-check
evidence, DVN join config with positive `confirmations` and `requiredDVNs`
containing OpenDVN plus an independent external DVN, and DVN verification
evidence tied to the exact payload hash and PacketV1 identity. Records with
`"evidenceType": "migration"` additionally require migration ticket contacts,
config diff artifacts, LayerZero config before snapshots, and rollback evidence
for previous Executor/ULN configs, rollback dry-run output, restored config
check, post-rollback canary, owner pause account, signer account, drain status,
and manual retry plan. Records with `"evidenceType": "deployment"` omit those
migration-only artifacts. LayerZero Labs DVN may be used as the external DVN
for Sepolia/Hoodi rehearsal, but it is not required by the evidence checker;
the worker-side on-chain config check only requires the configured OpenDVN plus
at least one independent external DVN.

Use the profile-driven deployment entrypoint for Sepolia/Hoodi rehearsal and
real external OApp deployments. The profile is the only operator-edited input;
it stores owner, signer addresses, fee caps, gas limits, fee models, OApp mode,
environment variable names for RPC URLs, optional `chains[].externalDVNs` for
arbitrary third-party DVNs, and optional `chains[].includeLayerZeroLabsDVN` for
auto-adding the repo-known LayerZero Labs push DVN. These fields build the
required-DVN set but do not store secret values. Hardhat private key
configuration variables are defined in `hardhat.config.ts`; store
them with `hardhat-keystore` before running state-changing Ignition commands:

```bash
npx hardhat keystore set SEPOLIA_PRIVATE_KEY
npx hardhat keystore set HOODI_PRIVATE_KEY
```

```bash
SEPOLIA_RPC_URL=... \
HOODI_RPC_URL=... \
npm run deploy:profile -- \
  --profile config/deployments/template.json \
  --phase render
```

`mode: "test-oft-rehearsal"` deploys rehearsal `TestOFT` contracts and worker
contracts on both chains, then can configure both OApp/Endpoint and worker
pathways. `mode: "external-oapp"` requires each chain to provide an existing
`oapp` address and never deploys `TestOFT`.

The `render` phase is local-only. It never executes Hardhat Ignition deployment
or configuration transactions. Each run first rewrites the bootstrap deployment
inputs under `tmp/deploy-profile/ignition/parameters` by default:

- `<chain>.open-workers.json` for the `OpenWorkers` module
- `<chain>.test-oft.json` for the `TestOFT` module in
  `test-oft-rehearsal` mode
- `commands.json` and `commands.md` for operator review

After writing those bootstrap files, `render` tries to load existing Ignition
deployment state from `ignition/deployments/<deployment-id>/deployed_addresses.json`.
It requires `OpenWorkers#OpenExecutor`, `OpenWorkers#OpenDVN`, and
`OpenWorkers#OpenPriceFeed`; `test-oft-rehearsal` mode also requires
`TestOFT#TestOFT`. When an older worker deployment state lacks `OpenPriceFeed`,
the renderer reads `OpenExecutor.priceFeed()` and `OpenDVN.priceFeed()` from the
chain and fails unless both match. Set the profile RPC URL environment variables
before rendering from such state.

If the required deployment state is not available yet, `render` stops in
bootstrap-only mode after writing `render-status.json`. Run the deploy phases,
then run `render` again. Once deployment state is present, the second render
writes the direction-specific `OpenWorkersPathwayConfig` and optional
`OAppEndpointConfig` parameter files, `deployment-state.json`, `worker.yaml`,
and verification command inputs. Full render requires the profile RPC URL
environment variables because the generated worker YAML embeds the resolved RPC
URLs.

Only `--apply` executes state-changing Hardhat Ignition commands:

```bash
npm run deploy:profile -- \
  --profile <profile.json> \
  --phase all \
  --apply
```

`--apply` runs Hardhat Ignition with inherited terminal stdio so
`hardhat-keystore` can prompt for the keystore password interactively. Render
and verification commands that write artifacts still capture output to files.
Pass `--build-profile <name>` to forward Hardhat's Ignition build profile to
each state-changing deploy/configure command. Pass `--verify` to forward
Ignition's deployment verification flag; the configured verifier API key, such
as `ETHERSCAN_API_KEY`, must be available to Hardhat:

```bash
npm run deploy:profile -- \
  --profile <profile.json> \
  --phase all \
  --build-profile production \
  --verify \
  --apply
```

Pass `--auto-confirm` to set Hardhat Ignition's confirmation environment
variables for every deploy/configure subprocess and skip repeated
`Confirm deploy to network ...?` prompts:

```bash
npm run deploy:profile -- \
  --profile <profile.json> \
  --phase all \
  --build-profile production \
  --verify \
  --auto-confirm \
  --apply
```

`hardhat-keystore` production passwords are intentionally interactive. For
non-interactive CI or release automation, provide the Hardhat configuration
variables directly in the process environment, such as `SEPOLIA_PRIVATE_KEY`,
`HOODI_PRIVATE_KEY`, and `ETHERSCAN_API_KEY`; environment variables take
precedence over keystore values and avoid the keystore password prompt. Keep
those values in the runner's secret store, not in committed files or shell
history.

Supported phases are `render`, `deploy-test-oft`, `deploy-workers`,
`configure-workers`, `configure-oapp`, `verify`, and `all`. In
`external-oapp` mode, `all --apply` deploys/configures/verifies only the worker
side; OApp peer and Endpoint message-library changes require
`--phase configure-oapp --apply` so production OApp ownership changes are
explicit.

The lower-level Ignition scripts remain available for debugging and staged
rollouts:

```bash
npm run deploy:test-oft
npm run deploy:open-workers
npm run deploy:open-dvn-worker
npm run configure:open-workers-pathway
npm run configure:open-dvn-pathway
npm run configure:oapp-endpoint
```

Use `docs/deployments/test-oft-policy.md` for the approved rehearsal TestOFT
name, symbol, owner, constructor mint, and minting policy. Keep generated files
under `tmp/` out of commits; rendered worker YAML may contain resolved RPC URLs.
When a profile chain omits `startBlockNumber`, the renderer queries that
chain's configured RPC URL and writes the latest block height to
`chains[].start_block_number` in `worker.yaml`. Set `chains[].startBlockNumber`
explicitly only for a fixed historical backfill, including `0` for a full
genesis backfill. If the worker database already has a durable indexer cursor,
that cursor remains authoritative for subsequent worker starts.
Set each profile chain's positive-integer `indexerPollIntervalSeconds`, within
the worker duration range, to emit the per-chain
`chains[].indexer_poll_interval_seconds` polling cadence.

After deployment, check that the deployed contracts are still controlled by the expected operations owner and, when used, that the canary treasury has enough native token and TestOFT balance for the planned transfer:

```bash
npm run check:deployment-preflight -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --test-oft ... \
  --open-executor ... \
  --open-dvn ... \
  --expected-owner ... \
  --min-owner-native-balance 10000000000000000 \
  --canary-treasury ... \
  --min-canary-native-balance 10000000000000000 \
  --min-canary-token-balance 1000000000000000 \
  --expected-total-supply 1000000000000000000000000
```

`--canary-treasury`, `--min-canary-native-balance`,
`--min-canary-token-balance`, and `--expected-total-supply` are optional. In
profile-driven deploys, `chains[].minCanaryTokenBalance` supplies the per-chain
`--min-canary-token-balance` value. Use `0` on Hoodi while checking the initial
zero-supply deployment before any inbound canary mint; raise the Hoodi value
only after a funded Sepolia -> Hoodi canary has minted destination tokens.

Inspect or update one TestOFT pathway pause/rate-limit state during migration:

```bash
npm run oft:pathway -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --test-oft ... \
  --remote-eid 40449 \
  --oft-pathway-action inspect
```

For state-changing actions, include `--private-key`. Supported `--oft-pathway-action` values are `pause-send`, `unpause-send`, `pause-receive`, `unpause-receive`, `drain`, `set-rate-limit`, and `clear-rate-limit`. `drain` sets `capacity=0` and `refillPerSecond=0`; `set-rate-limit` requires `--rate-limit-capacity` and `--rate-limit-refill-per-second`; `clear-rate-limit` removes the configured limiter and returns the pathway to unrestricted send capacity.

```bash
npm run oft:pathway -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --private-key ... \
  --test-oft ... \
  --remote-eid 40449 \
  --oft-pathway-action drain
```

Configure OApp/Endpoint state and worker pathway state as separate phases. The
profile flow writes one parameter file for each side of the boundary, so real
external OApp deployments can deploy/configure workers without implicitly
changing OApp ownership or Endpoint message libraries.

`OAppEndpointConfig` attaches to the source OApp and Endpoint and executes:

- `OApp.setDelegate`
- `OApp.setPeer`
- `EndpointV2.setSendLibrary`
- `EndpointV2.setReceiveLibrary`
- `EndpointV2.setConfig` for SendUln302 `ExecutorConfig` and `UlnConfig`
- `EndpointV2.setConfig` for ReceiveUln302 `UlnConfig`
- `OApp.setEnforcedOptions`

`OpenWorkersPathwayConfig` attaches to the local worker contracts and executes:

- `OpenExecutor.setPriceFeed` and `OpenDVN.setPriceFeed` for the configured shared price feed
- `OpenExecutor.setAllowedSendLib` and `OpenDVN.setAllowedSendLib`
- `OpenExecutor.setPathwayConfig` and `OpenDVN.setPathwayConfig`
- `OpenPriceFeed.setPriceSnapshot` with a `PriceSnapshotUpdate[]` batch
- `OpenExecutor.setFeeModel` and `OpenDVN.setFeeModel`
- `OpenExecutor.withdrawFee` and `OpenDVN.withdrawFee` for allowed send-lib worker fees
- `OpenDVN.setVerifier` for the local verifier signer

`OpenDVNWorker` and `OpenDVNPathwayConfig` are the standalone third-party DVN
variants. `OpenDVNWorker` deploys `OpenPriceFeed` plus `OpenDVN` without an
executor. `OpenDVNPathwayConfig` attaches to that DVN and writes only the DVN
price-feed binding, SendUln allowlist, pathway limits, initial price snapshot,
DVN fee model, and verifier authorization.

`OpenWorkers` deploys `OpenPriceFeed` with explicit `priceFeedSubmitters`.
In profile-driven deployments, `priceFeedSubmitters` is the long-term pricing
signer allowlist and must not include the owner. The renderer adds the owner as
a temporary deployment submitter so `OpenWorkersPathwayConfig` can write the
initial price snapshot, then that module immediately revokes the owner with
`OpenPriceFeed.setSubmitter(owner, false)`. Owner status alone does not submit
future snapshots; the long-term pricing signer must remain in
`priceFeedSubmitters` before enabling the price bot. Existing OpenPriceFeed
deployments that did not authorize the owner cannot be fixed by editing the
profile alone; either redeploy `OpenWorkers`, or have the owner run a reviewed
`OpenPriceFeed.setSubmitter(owner, true)` transaction before rerunning the new
pathway config that writes the initial snapshot and revokes the temporary
authorization.

The profile renderer enables the worker price bot in the generated
`worker.yaml`. Global `pricing.sourceRequestTimeoutSeconds` and
`pricing.maxDeviationBps` default to `10` and `500`; optional CoinMarketCap and
CoinGecko BaseURLs and API-key environment-variable names also live under that
profile block. Runtime timeout and per-source maximum-age seconds must be
positive integers within the worker duration range. Every market-data BaseURL
must use HTTPS, including keyless CoinGecko endpoints; the renderer rejects
malformed or HTTP endpoints before emitting worker YAML. Each chain requires
its own `pricingTxPolicy`; both fee
caps and the minimum-balance threshold must be positive, and the priority-fee
cap must not exceed the total fee cap. Generated profiles always require a
positive priority-fee cap even though the Go loader permits hand-authored
legacy-chain YAML to omit it. Fee caps intentionally have no repository-wide
absolute ceiling because chain gas markets differ, so operators must approve
them per chain. The renderer writes that policy to
`pricing.chains[].tx_policy` so different gas markets do not share fee caps or
minimum-balance thresholds. Chains default to `nativeAssetId: "eth"`,
so the Sepolia/Hoodi testnet profile writes same-native pricing chains that use
destination RPC gas prices directly without requiring any market source. Set a
different lowercase `nativeAssetId` on any chain whose native gas asset differs;
every chain in a cross-asset profile must then declare
`priceSources.primarySource` and the matching source configuration.
CoinMarketCap, CoinGecko, and Chainlink may be
primary sources. `sanitySources` may be omitted or empty, and Chainlink and
Uniswap are optional. Uniswap may only be listed as a sanity source and reads an
approved V3 pool TWAP of at least 1800 seconds through `observe()`; it is not a
required Hoodi dependency and does not use QuoterV2. Chainlink runtime reads pin
feed metadata and round data to one latest block number. The renderer emits only
referenced source blocks and rejects configured but unreferenced sources.

The profile renderer writes worker pathway parameters at
`tmp/deploy-profile/ignition/parameters/sepolia-to-hoodi.open-workers-pathway.json`
and OApp/Endpoint parameters at
`tmp/deploy-profile/ignition/parameters/sepolia-to-hoodi.oapp-endpoint.json`.
The committed `ignition/parameters/*.json` files are static smoke/debug
examples for the new module IDs: `sepolia.json` and `hoodi.json` contain
`OpenWorkers` plus `TestOFT` sections, while the bidirectional
`*.example.json` files contain `OAppEndpointConfig` plus
`OpenWorkersPathwayConfig` sections. Regenerate real deployment parameters from
the profile instead of editing those examples by hand.
Run worker configuration independently:

```bash
SEPOLIA_RPC_URL=... \
npm run configure:open-workers-pathway -- \
  --network sepolia \
  --parameters tmp/deploy-profile/ignition/parameters/sepolia-to-hoodi.open-workers-pathway.json \
  --deployment-id sepolia-open-workers-sepolia-to-hoodi-open-workers-pathway
```

Run OApp/Endpoint configuration only when the OApp owner/delegate has approved
that change:

```bash
SEPOLIA_RPC_URL=... \
npm run configure:oapp-endpoint -- \
  --network sepolia \
  --parameters tmp/deploy-profile/ignition/parameters/sepolia-to-hoodi.oapp-endpoint.json \
  --deployment-id sepolia-open-workers-sepolia-to-hoodi-oapp-endpoint
```

Repeat each command on Hoodi with the corresponding
`hoodi-to-sepolia.*.json` parameter files and `--network hoodi`; Hardhat reads
the Hoodi private key from its keystore-backed configuration variable.

For attach-only debugging, render split parameters from explicit addresses:

```bash
npm run render:oft-pathway-params -- \
  --oapp ... \
  --endpoint 0x6EDCE65403992e310A62460808c4b910D972f10f \
  --remote-eid 40449 \
  --remote-oapp ... \
  --send-uln 0xcc1ae8Cf5D3904Cef3360A9532B477529b177cCE \
  --receive-uln 0xdAf00F5eE2158dD58E0d3857851c432E34A3A851 \
  --open-executor ... \
  --open-dvn ... \
  --price-feed ... \
  --bootstrap-price-submitter <owner> \
  --include-layerzero-labs-dvn \
  --confirmations 12 \
  --executor-max-message-size 10000 \
  --enforced-lz-receive-gas 200000 \
  --max-lz-receive-gas 1000000 \
  --price-snapshot-dst-gas-price-in-src-token 1 \
  --price-snapshot-dst-data-fee-per-byte-in-src-token 0 \
  --price-snapshot-stale-after 1800 \
  --executor-fee-fixed-fee-wei 0 \
  --executor-fee-dst-gas-overhead 50000 \
  --executor-fee-data-size-overhead-bytes 0 \
  --executor-fee-margin-bps 1000 \
  --dvn-fee-fixed-fee-wei 0 \
  --dvn-fee-dst-gas-overhead 150000 \
  --dvn-fee-data-size-overhead-bytes 0 \
  --dvn-fee-margin-bps 1000 > ignition/parameters/sepolia-to-hoodi.split.json
```

`--min-lz-receive-gas` defaults to `--enforced-lz-receive-gas`.
Use `--required-dvns <open-dvn>,<external-dvn>` or `REQUIRED_DVNS` for explicit
full-list control or arbitrary third-party DVNs.
`--include-layerzero-labs-dvn` or `INCLUDE_LAYERZERO_LABS_DVN=true` appends the
repo-known LayerZero Labs push DVN for the local Endpoint/SendUln/ReceiveUln
tuple; if no metadata exists, pass the external DVN explicitly.
`--price-snapshot-updated-at` defaults to the render script's current Unix
timestamp; regenerate parameters shortly before signing so the shared price
snapshot remains fresh for the approved stale window. `--dvn-verifier` defaults
to the Ignition sender, and must match the source-chain `tx_roles.dvn.signer`
used by that chain's OpenDVN verifier authorization.
`DELEGATE` is optional in the render step; when set, it must match the account
that will sign the same configuration run, because the later Endpoint calls
require the signer to be the OApp delegate.

Refresh local OpenExecutor/OpenDVN price inputs or rate limits separately when needed:

```bash
npm run configure:workers -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --private-key ... \
  --test-oft ... \
  --open-executor ... \
  --open-dvn ... \
  --price-feed ... \
  --remote-eid 40449 \
  --send-lib ... \
  --max-message-size 10000 \
  --min-lz-receive-gas 200000 \
  --max-lz-receive-gas 1000000 \
  --price-snapshot-dst-gas-price-in-src-token 1 \
  --price-snapshot-dst-data-fee-per-byte-in-src-token 0 \
  --price-snapshot-stale-after 3600 \
  --executor-fee-fixed-fee-wei 0 \
  --executor-fee-dst-gas-overhead 50000 \
  --executor-fee-data-size-overhead-bytes 0 \
  --executor-fee-margin-bps 1000 \
  --dvn-fee-fixed-fee-wei 0 \
  --dvn-fee-dst-gas-overhead 150000 \
  --dvn-fee-data-size-overhead-bytes 0 \
  --dvn-fee-margin-bps 1000
```

The low-level script flags use fixed-fee terminology to match worker YAML and
deployment profiles. Rendered Ignition parameters and on-chain worker reads
still expose the ABI field name `baseFee`.

For standard pathway setup, `configure:open-workers-pathway` writes the worker
price-feed binding, send-lib allowlist, pathway limits, initial shared price
snapshot, worker fee models, and verifier authorization. `configure:workers`
remains the manual entrypoint for later shared snapshot refreshes, price feed
rotations, fee-model changes, and outbound rate-limit changes. It reads each
worker's current `priceFeed()` and sends `setPriceFeed` only when the configured
`--price-feed` differs. Set `--src-oapp` for external OApp deployments;
rehearsal flows may omit it when `--test-oft` is set. Set
`--rate-limit-capacity` and `--rate-limit-refill-per-second` together to
configure TestOFT outbound rate limiting, which requires `--test-oft`.

Inspect the current LayerZero libraries and message-lib configs for one local direction:

```bash
npm run inspect:lz-config -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --endpoint ... \
  --oapp ... \
  --remote-eid 40449 \
  --send-uln ... \
  --receive-uln ...
```

Save this output before executor or DVN migration. The saved JSON is the rollback input for restoring the previous ExecutorConfig and both SendUln302/ReceiveUln302 UlnConfig values:

```bash
npm run inspect:lz-config -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --endpoint ... \
  --oapp ... \
  --remote-eid 40449 \
  --send-uln ... \
  --receive-uln ... > sepolia-to-hoodi-lz-config.before.json
```

Switch the source send library executor config to OpenExecutor:

```bash
npm run configure:lz-executor -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --private-key ... \
  --endpoint ... \
  --oapp ... \
  --remote-eid 40449 \
  --send-uln ... \
  --open-executor ... \
  --executor-max-message-size 10000
```

Configure ULN302 on one local chain with OpenDVN plus at least one independent
external DVN:

```bash
npm run configure:lz-dvn -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --private-key ... \
  --endpoint ... \
  --oapp ... \
  --remote-eid 40449 \
  --send-uln ... \
  --receive-uln ... \
  --required-dvns <open-dvn>,<external-dvn> \
  --confirmations 12
```

Restore a previously inspected LayerZero config snapshot:

```bash
npm run configure:lz-rollback -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --private-key ... \
  --lz-config-snapshot sepolia-to-hoodi-lz-config.before.json
```

Use `--dry-run` with the same `--lz-config-snapshot` before signing. The dry run validates the snapshot and prints the exact `Endpoint.setConfig` batches and encoded config bytes without requiring RPC or `--private-key`.

`TestOFT` exposes an owner-only post-deploy `mint(address,uint256)` function,
but this repository intentionally does not provide a `mint:oft` script.
Sepolia's approved rehearsal supply is minted by the `TestOFT` Ignition
constructor parameters, and Hoodi supply is created by successful inbound OFT
receive-side minting unless the deployment policy explicitly approves an owner
mint.

Send a basic OFT transfer after the local and remote OFTs are peered and the pathway is configured:

```bash
npm run send:oft -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --private-key ... \
  --test-oft ... \
  --dst-eid 40449 \
  --recipient ... \
  --amount-ld 1000000000000000 \
  --min-amount-ld 1000000000000000
```

`send:oft` calls `quoteSend` first and pays the quoted native fee. By default it sends empty caller `extraOptions` and relies on the pathway's enforced `lzReceiveOption`; this avoids duplicate executor options after the configured-pathway module has set enforced options. Pass `--extra-options 0x...` only for an explicitly approved custom options payload, or `--lz-receive-gas <gas>` only on pathways without enforced lzReceive options. `composeMsg` and `oftCmd` are always empty for the first phase. `send:oft-canary` remains as an alias with canary-specific logging for migration evidence flows.

If `quoteSend` reverts with `PriceSnapshotStale(uint32,uint256,uint256)`, the
source chain's `OpenPriceFeed.priceSnapshot(dstEid)` has passed its
`updatedAt + staleAfter` window. Refresh the source `OpenPriceFeed` snapshot
with `configure:workers` or the price bot, then rerun `check:price-config`
before retrying the OFT send.

`OpenPriceFeed` rejects future `updatedAt` values and `staleAfter` windows above
one day; keep profile and price-bot freshness windows within that contract
limit.

Check the source-chain canary receipt after the send transaction is mined:

```bash
npm run check:oft-canary -- \
  --rpc-url ... \
  --chain-id 11155111 \
  --endpoint ... \
  --source-tx-hash ... \
  --send-lib ... \
  --open-executor ...
```

`check:oft-canary` verifies that the source receipt includes EndpointV2 `PacketSent`, that `PacketSent.sendLibrary` matches `SEND_LIB`, and that SendLib `ExecutorFeePaid.executor` matches `OPEN_EXECUTOR`.

After the destination delivery transaction is known, run the same command against the destination RPC with `DESTINATION_TX_HASH` and optional `DESTINATION_ENDPOINT`:

```bash
npm run check:oft-canary -- \
  --rpc-url ... \
  --chain-id 560048 \
  --endpoint ... \
  --destination-tx-hash ... \
  --destination-endpoint ... \
  --destination-test-oft ... \
  --recipient ... \
  --min-recipient-balance 1000000000000000
```

The destination check requires EndpointV2 `PacketDelivered`, rejects receipts containing `LzReceiveAlert`, and can optionally require the recipient's TestOFT balance to be at least `MIN_RECIPIENT_BALANCE`. `SOURCE_TX_HASH` checks are intended for the source-chain RPC; `DESTINATION_TX_HASH` checks are intended for the destination-chain RPC.

The deployment or migration evidence record must capture each direction's source OpenExecutor/OpenDVN/OpenPriceFeed contracts, destination OpenDVN contract, canary `AMOUNT_LD`, sender account, recipient account, `MIN_RECIPIENT_BALANCE`, source receipt, destination receipt, and recipient balance check. The `priceConfigCheck` object must capture the `DST_EID`, `MAX_PRICE_AGE_SECONDS`, `EXPECTED_STALE_AFTER`, `checkedAt`, shared price-feed `address`, shared `priceSnapshot`, each worker's bound `priceFeed`, and each worker's `feeModel` values from `check:price-config`. The `dvnVerificationReceipt` object must also capture the `EXPECTED_PAYLOAD_HASH`, `EXPECTED_SRC_EID`, `EXPECTED_DST_EID`, `EXPECTED_NONCE`, `EXPECTED_SENDER`, and `EXPECTED_RECEIVER` values used by `check:dvn-verification`. This keeps the approval artifact tied to the exact transfer size, accounts, worker contracts, price freshness, packet, and direction used in the rehearsal.

After DVN join, check a destination-chain verification receipt for both required DVNs:

```bash
npm run check:dvn-verification -- \
  --rpc-url ... \
  --chain-id 560048 \
  --tx-hash ... \
  --receive-uln ... \
  --required-dvns <open-dvn>,<external-dvn> \
  --confirmations 12 \
  --endpoint ...
```

`check:dvn-verification` requires ReceiveUln302 `PayloadVerified` logs for
every address in `REQUIRED_DVNS` with at least `CONFIRMATIONS`. For the active
OpenDVN path, the worker transaction calls `OpenDVN.submitVerification`, and
OpenDVN calls `ReceiveUln302.verify` so the `PayloadVerified.dvn` field is the
OpenDVN address. When `ENDPOINT` is set, it also requires EndpointV2
`PacketVerified` in the same receipt. `EXPECTED_PAYLOAD_HASH` is optional and
filters the checked `PayloadVerified` logs to one payload hash.
Set `EXPECTED_SRC_EID`, `EXPECTED_DST_EID`, `EXPECTED_NONCE`, `EXPECTED_SENDER`, and `EXPECTED_RECEIVER` to also require the `PayloadVerified` PacketV1 header to match the exact migration direction and packet identity.

Run the LayerZero config module or lower-level scripts on both chains with the
local endpoint, local OApp, local message libraries, and local DVN addresses for
each direction. `configure:oapp-endpoint` and `configure:lz-dvn` explicitly set
`optionalDVNCount` to LayerZero's NIL value so default optional DVNs are not
inherited during the first-phase required-DVN migration.
