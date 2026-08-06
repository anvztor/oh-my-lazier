# npm Audit Disposition

This document records the current npm audit disposition for the contract build,
test, and deployment toolchain. It is not final mainnet approval.

## Scope

- `package.json`
- `package-lock.json`
- Hardhat compile and Solidity tests
- TypeScript deployment and LayerZero configuration scripts

## Current Gate

```bash
npm run check:npm-audit-disposition
npm run check:security-review
make security-check
```

`npm run check:npm-audit-disposition` runs `npm audit --audit-level=moderate
--json` and requires:

- zero critical findings
- every high or moderate finding to be present in the recorded disposition set

`make security-check` runs the security review document gate, npm audit
disposition gate, and `govulncheck`.

Current npm audit metadata:

```text
critical: 0
high: 18
moderate: 4
low: 21
total: 43
```

## Remediation Applied

- `@nomicfoundation/hardhat-toolbox-viem` remains a direct dependency.
- `@nomicfoundation/ignition-core = 3.1.7` is a direct dependency for the
  documented deployment-state query workaround.
- `viem` remains a direct pinned dependency for scripts.
- `@chainlink/contracts = 1.5.0` is pinned only as the AggregatorV3 ABI source.
- `@uniswap/v3-core = 1.0.1` is pinned only as the V3 pool ABI source; the retired V3 periphery/QuoterV2 dependency is removed.
- Independent vulnerable transitive packages are pinned through npm overrides:
  - `axios = 1.18.1`
  - `elliptic = 6.6.1`
  - `undici = 6.28.0`
  - `ws = 8.21.0`

These changes remove all critical npm audit findings without changing pinned
LayerZero package versions or removing Hardhat toolbox support.

## Remaining High Findings

| Package                                       | Direct | Source                                      | Disposition                                                           |
| --------------------------------------------- | ------ | ------------------------------------------- | --------------------------------------------------------------------- |
| `@chainlink/contracts-ccip`                   | no     | LayerZero transitive dependency             | Open. Do not auto-downgrade LayerZero packages.                       |
| `@layerzerolabs/lz-evm-messagelib-v2`         | yes    | pinned LayerZero package                    | Open. Required for current LayerZero interfaces.                      |
| `@layerzerolabs/lz-evm-oapp-v2`               | yes    | pinned LayerZero package                    | Open. Required for current OFT base contracts.                        |
| `@nomicfoundation/hardhat-ignition`           | no     | retained Hardhat deployment toolchain       | Open. Aggregate finding inherited from Hardhat and Ignition core.     |
| `@nomicfoundation/hardhat-ignition-viem`      | no     | retained Hardhat deployment toolchain       | Open. Aggregate finding inherited from Hardhat and Ignition core.     |
| `@nomicfoundation/hardhat-keystore`           | no     | retained Hardhat toolbox                    | Open. Aggregate finding inherited from Hardhat.                       |
| `@nomicfoundation/hardhat-network-helpers`    | no     | retained Hardhat toolbox                    | Open. Aggregate finding inherited from Hardhat.                       |
| `@nomicfoundation/hardhat-node-test-runner`   | no     | retained Hardhat toolbox                    | Open. Aggregate finding inherited from Hardhat.                       |
| `@nomicfoundation/hardhat-toolbox-viem`       | yes    | pinned Hardhat V3 toolbox                   | Open. Required by compile, test, Viem, and Ignition workflows.        |
| `@nomicfoundation/hardhat-verify`             | no     | retained Hardhat deployment toolchain       | Open. Inherited from Hardhat and legacy Ethers ABI code.              |
| `@nomicfoundation/hardhat-viem`               | no     | retained Hardhat toolbox                    | Open. Aggregate finding inherited from Hardhat.                       |
| `@nomicfoundation/hardhat-viem-assertions`    | no     | retained Hardhat toolbox                    | Open. Aggregate finding inherited from Hardhat and Viem integration.  |
| `@openzeppelin/contracts`                     | no     | LayerZero and Chainlink dependency graph    | Open. Project contracts directly use pinned OpenZeppelin v5.          |
| `@openzeppelin/contracts-upgradeable`         | no     | LayerZero and Chainlink dependency graph    | Open. Project contracts do not import upgradeable contracts.         |
| `adm-zip`                                     | no     | Hardhat compiler/tooling dependency         | Open. Pinned Hardhat currently exposes no compatible fix.             |
| `hardhat`                                     | yes    | pinned Hardhat 3.9.1                        | Open. Aggregate finding from vulnerable `adm-zip`.                    |
| `lodash-es`                                   | no     | Ignition core transitive dependency         | Open. Required by pinned Ignition core.                               |
| `tmp`                                         | no     | Chainlink ABI package transitive tooling    | Open. Not imported by the Go runtime or project contracts.            |

## Remaining Moderate Findings

The remaining moderate findings are attached to the direct Ignition core state
adapter dependency and transitive tooling bundled by the pinned Chainlink ABI
source:

- `@arbitrum/nitro-contracts`
- `@chainlink/contracts`
- `@nomicfoundation/ignition-core`
- `@offchainlabs/upgrade-executor`

These are not accepted for mainnet by this document. They require either a
compatible upstream fix or explicit approval before mainnet readiness.
`@chainlink/contracts` is used as a pinned source for the AggregatorV3 ABI; the
project does not deploy its bundled contracts. The Go worker embeds the generated
ABI and has no Node.js runtime dependency.

## Release Decision

- Critical npm findings are closed for the current dependency graph.
- High and moderate findings remain open release-readiness items.
- Do not apply npm's suggested LayerZero downgrade automatically; the project
  relies on the currently pinned package interfaces.
- Mainnet readiness requires one of:
  - compatible LayerZero/Hardhat/Chainlink package updates that clear these advisories
  - an explicit security approval accepting the remaining transitive toolchain
    exposure for the planned release

## Verification

```bash
npm run check:npm-audit-disposition
npm run check:security-review
make security-check
make check
git diff --check
```
