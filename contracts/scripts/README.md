# Contract Scripts

Every executable TypeScript entrypoint in this repository is a thin Hardhat
`run` wrapper under `contracts/scripts/commands`. Business logic remains in
import-safe modules under `contracts/scripts` and is tested with Hardhat's Node
test runner. Do not execute those modules directly with `tsx`.

## Common invocation contract

Commands load their business input from the JSON file named by
`OML_SCRIPT_PARAMS`:

```ts
type ScriptRunFile<T> = {
  input: T;
  apply?: boolean;
  confirmation?: "interactive" | "approved";
};
```

Each command has its own strict `input` schema. Unknown envelope or input fields
are rejected. Use these JSON representations:

- integers, amounts, gas values, nonces, and EIDs: unsigned decimal strings
- addresses and hashes: hex strings
- address sets: JSON arrays, not comma-separated strings
- booleans: JSON booleans

The checked-in, non-secret examples are in `config/scripts/examples`. Copy an
example to `tmp/`, replace its public values, and invoke the matching npm
command. For example:

```bash
cp config/scripts/examples/inspect-lz-config.json tmp/inspect-sepolia-to-hoodi.json
OML_SCRIPT_PARAMS=tmp/inspect-sepolia-to-hoodi.json \
  npm run inspect:lz-config -- --network sepolia
```

Commands that take no input, such as `check:runbooks`, may omit
`OML_SCRIPT_PARAMS`; this is equivalent to `{ "input": {} }`.

### Secrets

Never place a private key, RPC URL or credential, keystore password, mnemonic,
API key, bearer token, or other secret in a script parameter file. The common
harness recursively rejects secret-bearing field names before command-specific
validation.

Hardhat network RPC URLs, deployer keys, and verifier API keys come from the
configuration variables declared in `hardhat.config.ts`. Store them with
`hardhat-keystore` for interactive operation or inject the configuration
variables from the automation secret store. Worker/runtime secrets continue to
use the existing infrastructure secret environment variables. Do not commit
generated worker YAML because it contains resolved RPC URLs.

```bash
npx hardhat keystore set SEPOLIA_RPC_URL
npx hardhat keystore set SEPOLIA_PRIVATE_KEY
npx hardhat keystore set HOODI_RPC_URL
npx hardhat keystore set HOODI_PRIVATE_KEY
npx hardhat keystore set ETHERSCAN_API_KEY
```

### Networks and connections

Every online single-network command requires an explicit Hardhat network:

```bash
OML_SCRIPT_PARAMS=<params.json> npm run <command> -- --network <network>
```

The command rejects the implicit `hardhat` network. Read-only commands create a
Hardhat connection with `accounts: "remote"` and expose only a public client.
Write commands obtain their wallet client through the Hardhat Viem integration.
Both paths compare the configured chain ID with the RPC chain ID and close the
connection in `finally`. Every write command compares the selected signer with
its required expected owner/delegate (`input.expectedSigner` for single-network
commands, or the profile/deployment owner for orchestrated commands).

`deploy:profile` is the exception to `--network`: it is a multi-network command
and creates a separate connection for each `chains[].network` in the profile.

### Apply and confirmation

Every command capable of sending a transaction requires the top-level `apply`
field.

- `apply: false` validates input, resolves public file paths, and prints a plan;
  it does not create a write connection or send a transaction.
- `apply: true` with `confirmation: "interactive"` prints the command, network,
  chain ID, actions, and Ignition deployment IDs to stderr before the first
  write, then asks for one confirmation for the whole command.
- Non-TTY execution requires `confirmation: "approved"`; otherwise it fails
  before the first write.

Local ABI, parameter, evidence, and runbook artifact generation is not blocked
by the on-chain apply gate. Machine-readable results remain on stdout; progress,
confirmation, and diagnostic logs go to stderr.

## Command groups

All npm entries use `hardhat run --no-compile`. Commands that need fresh
artifacts run the Hardhat build explicitly; deploy and verification commands use
the same build profile, `production` by default.

### Fixed Ignition modules

These six commands are permanently bound to their named module. The parameter
JSON cannot select an arbitrary module:

| npm command                      | Ignition module            |
| -------------------------------- | -------------------------- |
| `deploy:test-oft`                | `TestOFT`                  |
| `deploy:open-workers`            | `OpenWorkers`              |
| `deploy:open-dvn-worker`         | `OpenDVNWorker`            |
| `configure:oapp-endpoint`        | `OAppEndpointConfig`       |
| `configure:open-workers-pathway` | `OpenWorkersPathwayConfig` |
| `configure:open-dvn-pathway`     | `OpenDVNPathwayConfig`     |

Their common `input` is:

```json
{
  "input": {
    "parameters": "contracts/ignition/parameters/sepolia.json",
    "deploymentId": "sepolia-open-workers",
    "expectedSigner": "0x1111111111111111111111111111111111111111"
  },
  "apply": false,
  "confirmation": "interactive"
}
```

`parameters` is resolved to an absolute path before deployment. With
`apply: true`, the wrapper calls
`connection.ignition.deploy(fixedModule, { parameters, deploymentId,
displayUi: true })`; it never starts `hardhat ignition deploy` as a subprocess.
Existing module IDs, Future IDs, deployment IDs, and tracked journals are part
of the reconciliation contract and must not be renamed.

The three deploy commands (`deploy:test-oft`, `deploy:open-workers`, and
`deploy:open-dvn-worker`) additionally accept `input.verify: true`; source
verification then starts only after the write connection has closed. The three
configuration commands reject the `verify` field because their modules attach
to existing contracts and deploy no source-verifiable contract. Verification
is serialized and is the only retained Ignition CLI subprocess:

```text
hardhat --config hardhat.verify.config.ts --network <network> \
  --build-profile <profile> ignition verify <deployment-id>
```

The dedicated config forces `accounts: "remote"` for HTTP networks. A
non-zero exit or signal fails the command.

### Online read-only commands

The following commands require `OML_SCRIPT_PARAMS` and `--network` but never a
wallet account:

- `inspect:lz-config`
- `check:price-config`
- `check:deployment-preflight`
- `check:dvn-verification`
- `check:oft-canary`
- `oft:pathway` with `input.action: "inspect"`

### Viem write commands

These require `OML_SCRIPT_PARAMS`, `--network`, and an explicit `apply`:

- `configure:workers`
- `configure:lz-executor`
- `configure:lz-dvn`
- `configure:lz-rollback`
- `send:oft`
- `send:oft-canary`
- `oft:pathway` for pause, unpause, drain, clear, and set-rate-limit actions

The reusable operations use `connection.viem`, not Ignition. For a dry-run,
start from the corresponding file in `config/scripts/examples` and keep
`apply: false`.

### Local-only generation and validation

These commands do not create a network connection:

- `render:oft-pathway-params`
- `generate:lzabi` and `check:lzabi`
- `generate:pricing-abi` and `check:pricing-abi`
- `check:lz-addresses`
- `check:migration-evidence`
- `check:npm-audit-disposition`
- `check:security-review`
- `check:runbooks`

Use the evidence checker with its strict envelope:

```bash
OML_SCRIPT_PARAMS=config/scripts/examples/check-migration-evidence.json \
  npm run check:migration-evidence
```

## Ignition deployment state

Application code does not parse Ignition's `deployed_addresses.json`.
`ignition-deployment-state.ts` reads
`<hre.config.paths.ignition>/deployments` through
`@nomicfoundation/ignition-core` `listDeployments()` and `status()`. It checks:

- the expected deployment ID and chain ID
- complete status, with no started, held, timed-out, or failed Future
- every required Future ID
- the Future address and contract name

Only a completely absent deployment ID is a bootstrap condition. Once a
deployment exists, corrupt or incomplete state, a chain mismatch, or a missing
Future is a hard error. In particular, there is no compatibility recovery for
an `OpenWorkers` deployment missing `OpenWorkers#OpenPriceFeed`. Ignition's own
journals and state files remain authoritative and must not be edited or deleted.

## Profile-driven deployment

`deploy:profile` keeps the existing profile stages, deployment IDs, generated
worker config, external-OApp policy, and verification artifacts, but performs
deploy/configure stages directly through the existing Ignition modules. The
profile names each Hardhat network with `chains[].network`; it does not contain
an RPC URL or an environment-variable name for one. Profile objects are strict
at every nested level and reject secret-bearing fields before normalization.

Start from `config/scripts/examples/deploy-profile-render.json` or
`config/scripts/examples/deploy-profile-all.json`. For example, copy the render
input to `tmp/deploy-profile-run.json`:

```json
{
  "input": {
    "profilePath": "config/deployments/template.json",
    "outDir": "tmp/deploy-profile",
    "phase": "render",
    "verifySource": false
  },
  "apply": false,
  "confirmation": "interactive"
}
```

Then run:

```bash
OML_SCRIPT_PARAMS=tmp/deploy-profile-run.json \
  npm run deploy:profile
```

Supported phases are `render`, `deploy-test-oft`, `deploy-workers`,
`configure-workers`, `configure-oapp`, `verify`, and `all`.

- `render` always writes bootstrap parameters and the command plan. If and only
  if a required deployment ID is absent, it writes `render-status.json` and
  exits bootstrap-only. Run the deploy stages and render again.
- `deploy-test-oft`, `deploy-workers`, `configure-workers`, and
  `configure-oapp` send transactions only when `apply: true`.
- `verify` is runtime/read-only verification and uses `apply: false`.
- In `external-oapp` mode, `all` does not modify the external OApp; use the
  explicit `configure-oapp` phase for that owner-authorized operation.
- Set `input.verifySource: true` only with `verify` or `all`. Runtime `verify`
  and block-explorer source verification are separate ordered steps. Source
  verification runs serially after all writes and runtime checks have finished.

For an unattended multi-network apply, change the envelope to `apply: true` and
`confirmation: "approved"`. The harness displays and authorizes all target
networks, chain IDs, operations, and deployment IDs once before the first
write. Generated command envelopes under `<outDir>/commands` default to
`apply: false`; review and explicitly change each one before a staged lower-level
apply.

The profile renderer produces, as applicable:

- Ignition parameter and command-envelope files under `<outDir>/ignition` and
  `<outDir>/commands`
- `commands.json` and `commands.md`
- `deployment-state.json`
- `worker.yaml`
- runtime verification artifacts under `<outDir>/artifacts`

## Local dual-chain E2E

Run the full disposable environment from the repository root:

```bash
make e2e-local
```

The target starts Postgres, LocalStack KMS, and two Anvil chains; creates the
worker keystore; and invokes the Hardhat wrappers using
`config/scripts/e2e-local-deploy.json` and
`config/scripts/e2e-local-run.json`. The deployment uses the
`LocalE2EChain` and `LocalE2EPathway` Ignition modules to deploy EndpointV2,
SendUln302, ReceiveUln302, TestOFT, OpenPriceFeed, OpenExecutor, and stable,
distinct primary and secondary OpenDVN Futures on both chains.

Local Ignition journals live under `tmp/e2e/ignition` via
`OML_IGNITION_DIR`. The normal E2E cleanup removes that directory before the
next Anvil lifecycle, so a restarted chain never reuses stale Ignition state.
Generated module parameters live under `tmp/e2e/ignition-parameters`; every
Ignition deploy call receives the absolute file path, and the same cleanup
removes these files.
The runner retains the two-way canaries, pending-transaction fee bump/RBF,
worker fee withdrawal, multi-send indexing evidence, and destination replay
evidence. Anvil-only RPC methods are issued through
`connection.provider.request()`.

CI uses the same wrappers and isolated state through `make e2e-ci`, with
`config/scripts/e2e-local-run-ci.json` selecting the CI readiness endpoint.
The E2E command input files contain public paths and URLs only; deployer keys,
keystore passwords, KMS settings, database credentials, and other
infrastructure secrets remain in the existing test infrastructure environment.

## Local GOATED regtest

`regtest:deploy` and `regtest:send` reuse the same `LocalE2EChain` and
`LocalE2EPathway` Ignition modules on the `regtest-a`/`regtest-b` Hardhat
networks (chain ids 31337/31338) to stand up the two-chain GOATED topology
with an autonomous two-worker relay: worker 1 runs OpenExecutor plus the
primary OpenDVN, worker 2 runs the secondary OpenDVN, and each worker signs
with its own pre-generated keystore (`go/cmd/e2ekeystore`) and uses its own
database. Both take the strict `OML_SCRIPT_PARAMS` envelope
(`config/scripts/regtest-deploy.json`, `config/scripts/regtest-send.json`):
deployment-affecting settings (`confirmations`, `initialSupply`, `dvnMode`)
are validated envelope fields, while RPC endpoints and the optional
`REGTEST_<A|B>_GAS_PRICE` legacy-fee pin resolve from `REGTEST_*` environment
variables. Ignition journals stay under `tmp/regtest/ignition` via
`OML_IGNITION_DIR`. A gas-price-pinned chain sends every transaction as
type 0 — the deploy and send connections interpose on the provider, and the
generated worker configs set `legacy_transactions: true` so DVN and executor
writes stay legacy too. The initial price snapshots expire after 24 hours and
nothing refreshes them (worker pricing is disabled); rerunning an aged
environment requires recreating `tmp/regtest` (deployments, Ignition
journals, and `ignition-parameters`) and redeploying.
The regtest is a single trust domain (one deployer, shared price feed and
infrastructure): it exercises the 2-of-2 required-DVN protocol flow only and
is not a production topology template. The worker compose stack lives at
`deploy/regtest/docker-compose.regtest.yml`.

## Tests

Script tests live under `contracts/test/nodejs` and import command cores, not
the top-level wrappers:

```bash
npm run typecheck
npm run test:scripts
npx hardhat test solidity --no-compile
npm run check:runbooks
make check
```

`npm run test:scripts` runs only the Hardhat Node test runner and the Solidity
command runs only Solidity tests, so neither suite is executed twice.
