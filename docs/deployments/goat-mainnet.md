# GOAT Mainnet Worker Deployment Records

## Scope

- Networks: GOAT mainnet (chain ID `2345`, EID `30361`), BSC (`56`, `30102`),
  Ethereum (`1`, `30101`), Metis Andromeda (`1088`, `30151`).
- Worker contracts only (`OpenPriceFeed` / `OpenExecutor` / `OpenDVN`).
  No OApp was deployed or modified; the live OFTs keep their current
  DVN/Executor until a migration executed per
  [`docs/runbooks/dvn-executor-migration.md`](../runbooks/dvn-executor-migration.md).
- Deployed 2026-08-19 from `main` @ `b3625b2` build artifacts
  (solc `0.8.35`, `viaIR`, `evmVersion: osaka`). Deployment was direct
  bytecode + constructor-args transactions signed by the deployer EOA;
  no Ignition state is retained for these — this document is the record.
- Required DVN topology follows the repository rule: each pathway's own
  `OpenDVN` plus one independently operated DVN. Operational split:
  DVN-primary, `OpenExecutor`, and the price bot are platform-operated;
  DVN-secondary is independently operated (separate party, separate signer,
  separate worker instance and RPC).

## Ownership Status

- Deployer and current `owner()` of all 20 contracts:
  `0xC0107723DaBaC451FFea07C5362D9462E36273AA` (EOA). Verified on-chain
  per contract on 2026-08-19.
- Ownership transfer to the ops multisig is scheduled to complete **before**
  any pathway `setConfig`. `Ownable` here is one-step: the transfer target
  must be address-verified per chain before sending.
- **Not yet activated**: no `setVerifier` / `setSubmitter` / send-lib
  allowlist / fee-model / pathway configuration has been executed. Feeds have
  no submitters and DVNs have no verifiers yet. Activation requires its own
  migration evidence and an update to this document before it runs.

## Constructor Wiring

- `OpenPriceFeed(owner, submitters=[])` — submitters granted later via
  `setSubmitter`.
- `OpenExecutor(owner, feedW1)` and `OpenDVN-primary(owner, feedW1)` share
  PriceFeed W1; `OpenDVN-secondary(owner, feedW2)` reads PriceFeed W2.

## Deployment Records

### GOAT mainnet — chain ID `2345`, EID `30361`

| Contract | Address | Deploy tx |
|---|---|---|
| `OpenPriceFeed` W1 | `0xfb24fc6ffa67c2dc59321e5db181c6b80a223b87` | `0xb91f24f7bf9193846013e5f3cb44e37b234774afcf2211578f65758e309b0cb1` |
| `OpenExecutor` | `0x1bc581eaa9e95d398af6666fee49918f53faa0f6` | `0x1239440238d27cdc7dff72afbdbf47e4a2a7fb70ca023770ecf710ece5d4f944` |
| `OpenDVN` primary | `0x3bb5f6ced0c09aa4b445d229fd1a0d2ff30766bf` | `0xc41ccc27be41094ddb3e0c47fd3c4c6d19802ec2bfa7befbd6d78e6201b4c8e5` |
| `OpenPriceFeed` W2 | `0x8f116f5fbe6cc13038658ab39b1fcf5a0c941a63` | `0xdf18142690dbfd77c2a333bdba11b3e6127f0367e3a066164747f08de0a57ba0` |
| `OpenDVN` secondary | `0x80ee98468130a2902a791975f71999d69e1f381b` | `0xc8e46995eca948a9c58ef0f2c97117ac5beba6341b3fac1253fa8bd03074e048` |

### BSC — chain ID `56`, EID `30102`

| Contract | Address | Deploy tx |
|---|---|---|
| `OpenPriceFeed` W1 | `0x8d278fd9a4605b1d4d310c3ab8159b801aa83f22` | `0x3e5058ddeb3277153ab9accaaa92676aa06ec7622ccd02badd85ed819f0fa3e1` |
| `OpenExecutor` | `0xfb24fc6ffa67c2dc59321e5db181c6b80a223b87` | `0x35d081683183bccc07bc8a6304919e6aee7eb2072022f50ac9b24ffbb65d3c43` |
| `OpenDVN` primary | `0x1bc581eaa9e95d398af6666fee49918f53faa0f6` | `0x2dab969d9ae8e324b045d4b865befa8e905b5d646c44e373cdb5a97cf43a6210` |
| `OpenPriceFeed` W2 | `0x3bb5f6ced0c09aa4b445d229fd1a0d2ff30766bf` | `0xa48440e10db5b57348e4330ea935237797294f99b5bf9e21a33ce7a2ffd331da` |
| `OpenDVN` secondary | `0x8f116f5fbe6cc13038658ab39b1fcf5a0c941a63` | `0x218846e65f6b452b5faa71f909827279bf90dbddb093fb259eb218442bad3f5c` |

### Ethereum — chain ID `1`, EID `30101`

| Contract | Address | Deploy tx |
|---|---|---|
| `OpenPriceFeed` W1 | `0x39dccbfa9d2cf4e02ac82e7f86dff3180dbca374` | `0xc95d5d0af1d5bd1b3b72e20b316aa7f47489868c79face6d07d2514ca4270f67` |
| `OpenExecutor` | `0x8aa754a0f697b1a6245726fd76c238da6d62cb79` | `0xacf06834daed2e93d755d190d1ba852e2759cd07b10a75ec522b99878291958d` |
| `OpenDVN` primary | `0x812ed64482f9080a6174b86042214c211cfc7c41` | `0x90af80a6ab9d87eb361e8e35e2bfc29e5392a3f18907d70cbf52a8a4804aab79` |
| `OpenPriceFeed` W2 | `0x1f3e21b41955d2ead82fe4f7eb8a679f46e35ba9` | `0xe97894354dcb96bc2bb01395d31aaca26308f52e453faac6ff1ea4c75510eeb3` |
| `OpenDVN` secondary | `0x2c4679e3c6aa4fa64b07b50aab635508f9298e2d` | `0x081ff5f7430fcf6b8fdb717d358f69351246314373fcef9c9944a20a3d25b698` |

### Metis Andromeda — chain ID `1088`, EID `30151`

| Contract | Address | Deploy tx |
|---|---|---|
| `OpenPriceFeed` W1 | `0x8f116f5fbe6cc13038658ab39b1fcf5a0c941a63` | `0x0943cfff0be8a4ee623c027df8ea059933fb5b57a27647799ecc96ca96c1a1ca` |
| `OpenExecutor` | `0x80ee98468130a2902a791975f71999d69e1f381b` | `0x5fdd60e866fb7584991a6779b63dd0262e5c572da508e6341b2967cf5b3ae4ed` |
| `OpenDVN` primary | `0xb8a7734daeabbf917f22b54f15afb2eddfb1096e` | `0xc8ab84b79b4eb6db308efc8852246c62b2ad67dc50f6cd99504feb42d4790c17` |
| `OpenPriceFeed` W2 | `0xa1eb8737f795cbb00fa6ce703903658e3915eda8` | `0xa1ef9ec87e7438341618f0faa9b43e80de64a8d05289377bfe0c44b8ab9f1c45` |
| `OpenDVN` secondary | `0xf2ecaec14f07d2940f54eb934f2fa9a8c540733e` | `0xe34d8e36a6456a797e555145f3b4b81c3a70a8779bf26ba3285ea46b459d8856` |

Note: addresses repeat across chains because they derive from the same
deployer nonce sequence (plain CREATE), offset where a chain started at a
different nonce. Same address on two chains does **not** imply the same role —
always read the per-chain table.

## LayerZero Endpoint Facts (mainnet)

| Chain | EndpointV2 |
|---|---|
| GOAT `30361` | `0x6F475642a6e85809B1c36Fa62763669b1b48DD5B` |
| BSC `30102` | `0x1a44076050125825900e736c501f859c50fE728c` |
| Ethereum `30101` | `0x1a44076050125825900e736c501f859c50fE728c` |
| Metis `30151` | `0x1a44076050125825900e736c501f859c50fE728c` |

## Counterparty Side

Pathway configuration (send/receive libraries, required DVN sets,
per-direction confirmations, enforced options) is tracked in the operations
repository and must be recorded here as migration evidence before the first
mainnet pathway is switched, mirroring
[sepolia-hoodi](sepolia-hoodi/TEST_RESULT.md).
