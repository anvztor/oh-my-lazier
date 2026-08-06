# BSC Testnet Scope and Deployment Records

## Scope

- Network: BSC testnet, chain ID `97`, LayerZero endpoint ID `40102`
  (Hardhat network `bscTestnet`; `deploy:profile` validates this binding).
- Phase 1, EVM only. BSC testnet participates as a destination and return-path
  source for the GOAT ↔ BSC lane.
- Required DVN topology on every pathway touching this network follows the
  repository rule: the pathway's own `OpenDVN` plus at least one independently
  operated DVN, pinned exactly through the worker config's
  `send_required_dvns` / `receive_required_dvns`.
- Approved live testnet execution is limited to worker-contract deployment and
  configuration recorded below. OFT pathway configuration, canary sends, and
  message execution on this lane require their own migration evidence and an
  update to this document before they run.

## Deployment Records

Ignition deployment state is retained under `contracts/ignition/deployments`
as historical evidence. **These addresses are not activatable**: their retained
build-info predates the current `OpenPriceFeed` snapshot semantics (strictly
increasing `updatedAt` with superseded-entry skip), so pointing workers or
pathway configuration at them would run contract behavior the maintained
sources no longer describe. Activating the GOAT ↔ BSC lane requires
redeploying the worker set from the current sources and recording the new
addresses here.

- `migration-bsc-testnet-remote-OpenWorkers` — the rehearsal worker set:

  | Contract | Address |
  |---|---|
  | `OpenExecutor` | `0x250950cD7835998AC59B4444cA8D8983a7f694a7` |
  | `OpenDVN` | `0x598Bbf1B890d61c25A8D942Ce636070c369f1457` |
  | `OpenPriceFeed` | `0x0f71E81c856fE7525397B136a80C98572087B9f1` |

- `migration-bsc-testnet-remote-OpenDVNWorker` — the second DVN worker set
  used for the dual-DVN rehearsal:

  | Contract | Address |
  |---|---|
  | `OpenDVN` | `0xf25b034d63712B5977b4dd82ee46CbfBC9B7Ca79` |
  | `OpenPriceFeed` | `0x04004e611Ffa2F8fa80B9b9752e14ABd61434005` |

Read these records through `@nomicfoundation/ignition-core`
(`listDeployments()` / `status()`), not by parsing `deployed_addresses.json`.

## Counterparty Side

GOAT-side worker deployment records for this lane are not tracked in this
repository. Before the lane is activated, record the GOAT-side evidence and the
pathway configuration (including the exact required DVN sets and per-direction
confirmations) here or in a linked migration-evidence file, mirroring
[sepolia-hoodi](sepolia-hoodi/TEST_RESULT.md).
