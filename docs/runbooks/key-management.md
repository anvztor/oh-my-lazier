# Key Management Review

This review covers the first-phase worker signer boundary:

- AWS KMS `ECC_SECG_P256K1`
- local geth keystore JSON

## Required Signer Inventory

For each deployment environment, record:

- signer address
- signer backend: `kms` or `keystore`
- allowed chains and purposes: Executor, DVN, price bot
- key owner and emergency contact
- rotation procedure and rollback signer
- funding policy for native gas and configured `min_native_balance_wei`

The configured signer address must match the expected address in worker config and migration tickets. Never infer approval from a successful transaction alone.

Worker config keeps a top-level `signers` inventory. `chains[].tx_roles.executor.signer` must reference a configured signer only when `services.executor.enabled` is true. `chains[].tx_roles.dvn.signer` must reference a configured signer only when `services.dvn.enabled` is true and at least one pathway is `dvn.mode: active`. `pricing.signer` must reference a configured signer only when `pricing.enabled` is true. If executor, active DVN, and pricing are all disabled for this process, `signers` may be empty. Signer IDs are EVM 20-byte hex addresses used as `tx_outbox.signer_id`. Keystore entries may reference only a password environment variable or password file. `keystore.password_env` must match `^[A-Z_][A-Z0-9_]*$`, and `keystore.password_file` must be an absolute path; put the password in the referenced source, never in either reference field. Local validation and password-file read failures reject malformed or unavailable references without echoing their values. Config-diff artifacts always redact password-file values. KMS entries configure key ID, region, address, and optional AWS-compatible endpoint only; AWS credentials are not part of worker YAML and are resolved by the AWS SDK default configuration chain.

## AWS KMS Requirements

The KMS key must be asymmetric `ECC_SECG_P256K1`.

Required controls:

- key policy limits signing access to the worker role and break-glass operators
- CloudTrail or equivalent audit logging is enabled for `kms:Sign`
- deletion is disabled or has a long pending window
- key rotation plan uses a staged worker config change and `configdiff`
- signer address recovery is validated before first use

Implementation evidence:

- `go/internal/config.Config.Validate` rejects unknown signer references only for roles enabled in this process: executor, active DVN, and pricing.
- `go/internal/app.App.txTargets` loads configured signers only when at least one enabled role can submit transactions, and creates one tx manager target per required signer per chain with per-purpose fee policy so executor, DVN, and pricing caps do not overwrite each other when they share a signer.
- `go/internal/db` stores per-chain signer nonce cursors in Postgres. The tx manager initializes a missing cursor from RPC pending nonce on first use, then increments only the local cursor. Automatic failed-transaction retry follows the same cursor: replacement keeps the assigned nonce, while fresh receipt retry consumes the next local nonce. Same-nonce replacement fires automatically in two cases, both signed as durable attempts with the standard 10% fee bump under the configured caps: a broadcast row without a receipt for `tx_manager.stale_broadcast_replacement_after_seconds` seconds, and an underpriced (reprice-held) row after a one-minute cooldown. Both stop at the automatic replacement cap, after which only an explicit `txretry replace` authorizes another attempt. After bootstrap, do not use the same signer account from another broadcaster. If the tx manager parks a row as `held(nonce_consumed_externally)`, the chain's confirmed nonce moved past a locally owned nonce with no receipt on any locally signed attempt: treat it as evidence the account was used outside this worker, audit the key's access immediately, and only then resolve the row with `txretry -action resolve-external-nonce`.
- `go/internal/txmgr.BalanceMonitor` polls every active transaction target signer and exposes `laz_signer_native_balance_wei`, `laz_signer_min_native_balance_wei`, and balance poll status from `/metrics`.
- `go/internal/app.loadKMSAWSConfig` uses the AWS SDK `config.LoadDefaultConfig` path with the configured region.
- `go/internal/signer/kms.Signer.ValidateKey` rejects non-`ECC_SECG_P256K1` keys.
- `go/internal/signer/kms.Signer.SignHash` parses DER signatures, normalizes low-S, and recovers the configured Ethereum address.
- `go/internal/signer/kms.Signer.SignTx` signs configured dynamic-fee and legacy transactions through the same address-recovery boundary.
- `make e2e-local` and `make e2e-ci` create a LocalStack KMS key for chain A while keeping chain B on a local keystore, then assert the destination delivery transaction sender and primary OpenDVN verifier match the configured signer addresses.

## Local Keystore Requirements

Local geth keystore signers are acceptable for testnet and emergency dry runs only unless mainnet approval explicitly allows them.

Required controls:

- keystore JSON is stored outside the repository
- password comes from an environment variable or password file, not config YAML
- password file permissions are restricted to the worker user
- encrypted keystore content and passwords are never logged
- Never log private key material, decrypted keystore content, KMS signatures, or raw secrets.
- keystore signer address is recorded in the signer inventory

Implementation evidence:

- `go/internal/signer/keystore.LoadWithPasswordSource` supports password value, environment variable, or file.
- `go/internal/signer/keystore.ResolvePassword` rejects missing or empty password sources and preserves file-error classification without exposing the configured password-file path.
- `go/internal/signer/keystore.Signer.SignTx` signs with geth's chain-aware latest signer.

## Pre-Migration Checklist

- Run `go test ./go/internal/signer/keystore ./go/internal/signer/kms -count=1`.
- Run `make test-integration` when Docker is available; it starts dedicated Postgres and Rustack containers, runs the DB/tx manager integration tests and the Rustack KMS signing test, and removes the temporary database files on exit.
- Run `make e2e-local` before approving local signer-path changes; it proves both KMS and keystore-backed worker transactions across the bidirectional local OFT canary flow.
- When an AWS-compatible KMS mock is available, run `RUSTACK_KMS_ENDPOINT=<endpoint> make test-kms-rustack`. The target requires `RUSTACK_KMS_ENDPOINT` and runs only the Rustack KMS transaction signing integration test.
- Run `go run ./go/cmd/configdiff -from <current.yaml> -to <proposed.yaml>` and confirm signer changes are expected.
- Confirm worker logs do not include private key material, decrypted keystore JSON, KMS signatures, or raw secrets.
- Confirm each configured signer has native gas above the role's `min_native_balance_wei` threshold on the chains assigned to enabled executor or active DVN purposes, and above `pricing.chains[].tx_policy.min_native_balance_wei` for pricing purposes.
- Confirm break-glass operators can pause OFT sends and worker assignments without needing private key material from the worker host.

## Rotation Procedure

1. Add the new signer to the inventory.
2. Validate KMS key spec or keystore decrypt/sign behavior in a non-production environment.
3. Prepare proposed config with the new signer address.
4. Run `configdiff` and attach output to the change ticket.
5. Fund the new signer for gas.
6. Stop the worker, deploy the config, and restart.
7. Confirm `/readyz`, `/metrics`, and tx outbox progress.
8. Keep the old signer funded AND configured in `signers` until its queued/broadcast transactions are confirmed or explicitly abandoned. For the pricing signer this is enforced at startup: while any old-signer pricing transaction is still pending, the worker keeps a transaction-manager target for that signer so the rows converge, and it refuses to start if the old signer's backend was removed from `signers` too early — those rows would otherwise gate their price feed forever.

## Rejection Criteria

Do not approve mainnet readiness if:

- the signer address is unknown or not in inventory
- a KMS key is not `ECC_SECG_P256K1`
- signer changes are not present in a reviewed `configdiff`
- a local keystore password is committed, printed, or stored in worker YAML
- the rollback signer is unfunded or inaccessible
