# Config Diff Runbook

Use `go run ./go/cmd/configdiff` before testnet migration changes, DVN joins, rollback rehearsals, and any mainnet config proposal.

The command loads both YAML files with validation and defaults, but without environment overrides. This prevents `DATABASE_URL` or deployment environment variables from hiding file-level drift during review.

Both text and JSON output redact database credentials and complete RPC, pricing
market-data, and signer KMS endpoint values. A credential-only change still
appears at `database_url`, `pricing`, `signers[address]`, or the corresponding
`chains[eid]` path, but its before/after values use redaction markers so archived
review artifacts do not retain passwords or API keys. Pricing market-data BaseURLs and signer KMS endpoints
retain only their URL scheme; malformed or opaque non-empty values are
represented only as `[REDACTED]`. The database URL
retains a validated `sslmode` value so TLS policy changes remain visible during
review; all other database query values are omitted. Malformed or opaque
database URLs are also represented only as `[REDACTED]`.
Secret-reference fields (`keystore.password_env`, `coinmarketcap_api_key_env`,
and `coingecko_api_key_env`) must use uppercase environment-variable names;
`keystore.password_file` must be an absolute path. Password-file values are
always represented as `[REDACTED]` in review output, including invalid input.
Config loading rejects malformed references without echoing the supplied value;
the diff projection also replaces any malformed non-empty reference with
`[REDACTED]` as defense in depth. It also rejects non-HTTPS market-data
BaseURLs and reports malformed secret-bearing RPC URLs with a fixed error that
does not include the endpoint.

Text review:

```bash
go run ./go/cmd/configdiff \
  -from config/current.yaml \
  -to config/proposed.yaml
```

JSON artifact:

```bash
go run ./go/cmd/configdiff \
  -from config/current.yaml \
  -to config/proposed.yaml \
  -format json > config-diff.json
```

CI guard:

```bash
go run ./go/cmd/configdiff \
  -from config/current.yaml \
  -to config/proposed.yaml \
  -fail-on-diff
```

Review checklist:

- Confirm both configs validate successfully.
- Confirm unsigned numeric values are non-negative YAML integers rather than decimals or quoted numeric strings; second-based runtime durations must remain within the worker's accepted range.
- Confirm `services.executor.enabled` and `services.dvn.enabled` changes are intentional. These are process-level switches for loops, signer requirements, tx targets, and indexer streams; pathway worker contract addresses still remain required in every config.
- Confirm `tx_manager.stale_broadcast_replacement_after_seconds` changes are intentional.
- Confirm chain `eid`, `family`, `chain_id`, endpoint, transaction roles, and RPC changes are intentional. Phase-1 configs must use `family: evm`.
- Confirm pathway `src_eid`, `dst_eid`, OApp, SendLib, ReceiveLib, source worker contracts, DVN mode, enablement, and max message size changes are intentional.
- Confirm pathway `min_lz_receive_gas` and `max_lz_receive_gas` changes match the OpenExecutor/OpenDVN on-chain pathway settings.
- Confirm signer changes are expected and do not point to unapproved keys. Review same-address backend, KMS key and region, keystore path, and password-source changes; KMS endpoint and password-file values are redacted in the artifact.
- Confirm pricing `native_asset_id`, source request timeout, primary source, declared sanity sources, per-source freshness, market-data BaseURL, pathway-scoped worker fee model, stale threshold, gas spike threshold, and each `pricing.chains[eid].tx_policy` fee cap and signer balance threshold are expected. Pricing fee caps have no repository-wide absolute ceiling and therefore require explicit per-chain operator approval. Market-data BaseURL values are redacted in the artifact. Pathways that share a source worker and destination EID must keep that worker role's fee model identical. Outer transaction gas is estimated by the tx manager at send time.
- For DVN migration, confirm each proposed pathway still uses `pathways[].dvn.mode: shadow` until the explicit active-mode change is approved.
- Keep the text output in the migration ticket and the JSON output as an immutable review artifact.

After diff review, run the on-chain config check before worker restart or price
updates:

```bash
go run ./go/cmd/configcheck -config config/proposed.yaml
```

The check compares the YAML with live chain state, including chain ID, Endpoint
EID, deployed code, OApp peers, send/receive libraries, ULN required DVNs, and
the configured `pathways[].source_workers` pathway configuration. Every
configured RPC URL must return the configured `chain_id` from `eth_chainId`.
Worker startup and `price-once` run the same check before database sync.
The long-running worker can be started with `-skip-onchain-check` only when an
operator intentionally bypasses this on-chain check; local YAML/schema
validation still runs, and `configcheck` plus `pricebot-once` keep the normal
on-chain gate.

The command compares signers by address, chains by `eid`, pricing chains by `eid`,
and pathways by `(src_eid, dst_eid, src_oapp, dst_oapp)` so list reordering does
not create review noise.
