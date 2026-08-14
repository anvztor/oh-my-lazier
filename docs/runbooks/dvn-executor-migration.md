# LayerZero V2 EVM Production DVN / Executor Migration Standard

**Document Status:** Production Runbook / Technical Standard  
**Scope:** Production LayerZero V2 EVM OApps, OFTs, ONFTs, and other applications using ULN302  
**Version:** 1.0  
**Date:** 2026-08-14

> This document defines a general production-grade method for migrating the LayerZero V2 security stack. The procedure intentionally does not distinguish between DVN expansion, reduction, partial replacement, full replacement, or changes to required/optional membership, thresholds, or confirmation depth. All such changes are modeled as a controlled transition from an existing security policy to a target security policy.
>
> The terms **MUST / MUST NOT / SHOULD / SHOULD NOT / MAY** are used as normative requirements.

---

## 1. Objectives

This standard is designed to ensure that a LayerZero V2 pathway can be reconfigured without:

1. permanently blocking messages because send and receive policies were updated in an unsafe order;
2. causing the destination to require a DVN that was never assigned a verification job for relevant in-flight packets;
3. losing verifiability or deliverability for in-flight traffic;
4. operating without explicit canary, drain, cutover, rollback, and audit boundaries;
5. conflating DVN security migration with Executor liveness migration;
6. losing consistency across unidirectional, bidirectional, or multi-chain mesh deployments;
7. preventing automation through deployment tooling, multisig workflows, or CI/CD migration planners.

---

## 2. Protocol Behavior and Foundational Facts

For a directional pathway:

```text
Chain A  ───────────────────────────────>  Chain B
source                                      destination
sendConfig                                  receiveConfig
```

The following LayerZero V2 behaviors are fundamental:

- Chain A `sendConfig` determines which DVNs are notified and assigned verification jobs for each outbound packet at send time.
- Chain B `receiveConfig` determines which DVN verification conditions must be satisfied when the packet is delivered.
- Configuration changes are not retroactive and do not automatically assign newly configured DVNs to packets that were already sent.
- The destination receive side does not enforce a particular Executor identity; once a message is verified, `lzReceive(...)` is permissionless.
- The source-side Executor configuration selects the automated execution service that quotes and receives the packet's execution job.
- ULN302 on the source side invokes `assignJob()` for every DVN in `requiredDVNs + optionalDVNs`.

The last property is particularly important for the generalized migration model. `optionalDVNThreshold` is part of the destination verification predicate; it is not a selector that causes the source to assign only K optional DVNs.

Primary references:

- [Migrating from a Single-DVN Configuration](https://docs.layerzero.network/v2/get-started/migrating-from-single-dvn)
- [EVM DVN and Executor Configuration](https://docs.layerzero.network/v2/developers/evm/configuration/dvn-executor-config)
- [SendUlnBase.sol](https://github.com/LayerZero-Labs/LayerZero-v2/blob/main/packages/layerzero-v2/evm/messagelib/contracts/uln/SendUlnBase.sol)

---

## 3. Policy Model

For any directional pathway, define the verification policy:

```text
Policy P = {
    requiredDVNs: R,
    optionalDVNs: O,
    optionalThreshold: K,
    confirmations: C
}
```

Model the Executor independently:

```text
ExecutionPolicy E = {
    executor,
    maxMessageSize
}
```

Define the set of DVNs assigned by the source policy:

```text
Workers(P) = R ∪ O
```

The destination verification condition can be abstracted as:

```text
Verify(P, attestations) :=
    every DVN in R has attested
    AND
    at least K DVNs in O have attested
    AND
    attestation confirmations satisfy C
```

---

## 4. Core Safety Invariant

Every production migration MUST preserve the following invariant:

> For every packet that may still be delivered on the destination, the DVN assignments created when that packet was sent must be sufficient to satisfy the receive policy that is effective when the packet is delivered.

Formally:

```text
For every packet p:

Verify(
    ReceivePolicy(at delivery time),
    AttestationsDerivedFrom(Assignments(at send time))
) == true
```

The unsafe state is:

```text
destination starts requiring DVN-X
              ↑
              │
but an in-flight packet
was never assigned DVN-X at source
```

Waiting longer cannot repair such a packet because the later configuration change does not retroactively create its missing verification job.

Therefore, the fundamental safe sequence is always:

```text
expand source assignments first
→ drain the previous generation
→ change the destination verification predicate
```

This is consistent with LayerZero's official `send-side first → drain in-flight → receive-side` migration guidance.

---

## 5. Generalized Bridge Policy

Let:

```text
OLD = P0
NEW = P1
```

A production migration SHOULD NOT directly execute:

```text
P0 → P1
```

Instead, construct a temporary `BRIDGE` policy:

```text
P0
 ↓
BRIDGE
 ↓
P1
```

### 5.1 Required Bridge Properties

The Bridge MUST satisfy:

```text
Workers(BRIDGE)
    ⊇
Workers(OLD) ∪ Workers(NEW)
```

and:

```text
confirmations(BRIDGE)
    >= max(
        confirmations(OLD),
        confirmations(NEW)
    )
```

This ensures that every packet sent after the Bridge becomes effective can generate the verification inputs required by both the OLD and NEW receive policies.

### 5.2 Recommended Bridge Construction

To minimize unnecessary semantic changes during migration:

```text
Bridge.required =
    OLD.required

Bridge.optional =
    (
        OLD.optional
        ∪ NEW.required
        ∪ NEW.optional
    )
    - OLD.required
```

and:

```text
Bridge.confirmations =
    max(OLD.confirmations, NEW.confirmations)
```

The purpose of the Bridge is to expand **assignment coverage**, not to prematurely express the final destination verification predicate on the source.

### 5.3 Why "Everything Required" Is Not Preferred

Making every OLD and NEW DVN temporarily required can also create the required assignments, but production systems SHOULD generally avoid this because it:

- changes temporary policy semantics;
- creates unnecessary configuration complexity;
- may affect DVN-specific option indexing;
- complicates failure analysis and rollback;
- violates the principle of minimizing unrelated changes during a migration.

---

## 6. Standard Migration State Machine

For a directional pathway `A → B`:

```text
Source A                                 Destination B

Stable OLD
send = OLD             ───────────────>  receive = OLD

Phase 1
send = BRIDGE          ───────────────>  receive = OLD

Phase 2
        [drain OLD generation]

Phase 3
send = BRIDGE          ───────────────>  receive = NEW

Phase 4
send = NEW             ───────────────>  receive = NEW
```

Normative representation:

```text
OLD / OLD
    ↓
BRIDGE / OLD
    ↓ drain
BRIDGE / NEW
    ↓ verify
NEW / NEW
```

The left side is the source send policy; the right side is the destination receive policy.

---

## 7. Phase 0 — Preflight and Snapshot

The following work MUST be completed before any production configuration transaction.

### 7.1 Read Actual On-Chain State

Do not rely solely on `layerzero.config.ts` or an internal configuration database. Record the effective on-chain state:

- EndpointV2;
- OApp address;
- remote EID;
- active Send Library;
- active Receive Library;
- send-side `UlnConfig`;
- receive-side `UlnConfig`;
- send-side `ExecutorConfig`;
- delegate / owner / governance authority.

The operational system must distinguish:

- raw OApp configuration;
- protocol defaults;
- resolved/effective configuration.

Because ULN uses `0` as a default sentinel for several fields, production systems SHOULD explicitly pin security-sensitive values rather than unintentionally inheriting mutable defaults.

### 7.2 Validate New DVNs

Every NEW DVN MUST:

- have a valid deployment on the source chain;
- have the corresponding deployment on the destination chain;
- actively support the specific source → destination pathway;
- operate production-capable off-chain attestation infrastructure;
- have addresses verified through trusted deployment metadata;
- satisfy the required operator, RPC, signing, and infrastructure diversity policy.

A deployed contract does not by itself prove active pathway coverage.

### 7.3 Pre-Build Rollback

Before the first production migration transaction is signed, teams SHOULD prepare:

- rollback calldata;
- multisig transaction bundles;
- the OLD configuration snapshot;
- the Bridge configuration;
- the NEW configuration;
- signer / owner authorization validation;
- emergency pause and recovery procedures.

---

## 8. Phase 1 — Move Source to Bridge

Modify only the source:

```text
A.sendConfig:
    OLD → BRIDGE

B.receiveConfig:
    OLD
```

The destination MUST NOT move to NEW in this phase.

After finality:

1. read the configuration back with `getConfig`;
2. validate DVN membership and confirmations;
3. record the source block number;
4. record the transaction hash;
5. record the outbound nonce / generation boundary where supported.

After this boundary:

```text
OLD generation:
    packets sent before Bridge became effective
    assignments = OLD

BRIDGE generation:
    packets sent after Bridge became effective
    assignments ⊇ OLD ∪ NEW
```

---

## 9. Phase 1.5 — Pre-Cutover Canary

A canary MUST be completed before the destination begins enforcing NEW.

Send a small, low-value packet and verify:

- OLD DVNs are still attesting;
- all NEW required DVNs are attesting;
- a sufficient set of NEW optional DVNs is healthy;
- confirmations match the Bridge configuration;
- commit succeeds;
- execution succeeds;
- the Executor is operating normally;
- LayerZero Scan or internal indexing can correlate the packet, verification, commit, and execution.

This canary proves:

```text
NEW DVNs are producing usable verification
for the Bridge generation
```

It does not yet prove that the NEW receive policy has been activated.

---

## 10. Phase 2 — Drain the OLD Generation

Before the receive policy is changed, all OLD-generation packets MUST be drained.

### 10.1 Preferred Drain Criterion

Production systems SHOULD use:

```text
nonce / GUID based drain
```

Specifically:

```text
∀ packet sent before migration boundary:
    executed
    OR
    explicitly recovered/accounted-for
```

Time-based waiting should be secondary evidence only.

LayerZero's migration guidance recommends at least:

```text
confirmations × source-chain block time
+
delivery buffer
```

and suggests approximately `2 × typical end-to-end delivery time` as a buffer.

### 10.2 Systems with an Outbound Pause

If the OApp supports a governance pause, the preferred sequence is:

```text
pause
→ source = BRIDGE
→ canary
→ drain
→ destination = NEW
→ canary
→ source = NEW
→ resume
```

A pause is not a protocol requirement, but it substantially simplifies generation-boundary management.

---

## 11. Phase 3 — Move Destination to NEW

The destination MAY move from OLD to NEW only when:

- the OLD generation has drained;
- the NEW DVNs are reliably attesting;
- the Bridge is confirmed on-chain;
- rollback transactions are prepared;
- operational monitoring is healthy.

Execute:

```text
B.receiveConfig:
    OLD → NEW
```

The system is now:

```text
A.send = BRIDGE
B.receive = NEW
```

Every Bridge-generation packet has assignments covering both OLD and NEW, allowing the NEW receive predicate to be satisfied.

---

## 12. Phase 3.5 — Post-Cutover Canary

After the receive cutover, another end-to-end canary MUST be executed.

This validates the **NEW security policy itself**:

```text
NEW required attestations
+
NEW optional threshold
+
NEW confirmations
+
commitVerification
+
Endpoint verification
+
lzReceive execution
```

Only after success may the migration proceed to final normalization.

---

## 13. Phase 4 — Normalize Source to NEW

After the post-cutover canary:

```text
A.sendConfig:
    BRIDGE → NEW
```

Final state:

```text
NEW / NEW
```

This restores the long-term symmetric configuration. Production systems SHOULD NOT leave migration-only send/receive asymmetry indefinitely.

---

## 14. Unified Forward Migration Algorithm

All DVN policy changes are represented as:

```text
FROM / FROM
    ↓
BRIDGE / FROM
    ↓
DRAIN
    ↓
BRIDGE / TO
    ↓
CANARY
    ↓
TO / TO
```

where:

```text
Workers(BRIDGE)
    ⊇ Workers(FROM) ∪ Workers(TO)
```

The same algorithm covers:

- adding DVNs;
- removing DVNs;
- replacing one DVN;
- replacing the entire DVN set;
- required ↔ optional changes;
- optional threshold changes;
- increasing or decreasing confirmations.

---

## 15. Common Scenarios

### 15.1 Expansion

```text
OLD = [A, B]
NEW = [A, B, C]
```

State sequence:

```text
[A,B] / [A,B]
→
[A,B,C] / [A,B]
→ drain
[A,B,C] / [A,B,C]
```

If Bridge and NEW are identical, final normalization may be a no-op.

### 15.2 Full Replacement

```text
OLD = [A, B]
NEW = [C, D]
```

State sequence:

```text
[A,B] / [A,B]
→
[A,B,C,D] / [A,B]
→ drain
[A,B,C,D] / [C,D]
→ canary
[C,D] / [C,D]
```

### 15.3 Reduction

```text
OLD = [A, B, C]
NEW = [A, B]
```

The generalized algorithm allows Bridge to equal OLD:

```text
[A,B,C] / [A,B,C]
→ drain
[A,B,C] / [A,B]
→ canary
[A,B] / [A,B]
```

### 15.4 Optional Threshold Only

If OLD and NEW use the same worker set:

```text
Workers(OLD) == Workers(NEW)
```

no new assignment coverage is needed. An automated planner MAY optimize away a no-op Bridge transaction while retaining the required in-flight and cutover safety checks.

---

## 16. Confirmation-Depth Migration

Reliable delivery requires:

```text
sendConfirmations >= receiveConfirmations
```

Therefore:

```text
C_bridge = max(C_old, C_new)
```

### 16.1 Increase

```text
OLD = 15
NEW = 30
```

Sequence:

```text
source:       15 → 30
destination:  15

→ drain

destination:  15 → 30
```

### 16.2 Decrease

```text
OLD = 30
NEW = 15
```

The generalized safe sequence may remain:

```text
source:       30
destination:  30

→ drain

destination:  30 → 15
source:       30 → 15
```

Even where a faster path is possible, a uniform production state machine is often preferable for automation and auditability.

---

## 17. Rollback Standard

Rollback MUST be treated as another controlled migration, not as a blind configuration restore.

### 17.1 Destination Has Not Moved

Current state:

```text
BRIDGE / OLD
```

Safe rollback:

```text
BRIDGE / OLD
→
OLD / OLD
```

Bridge-generation packets already contain the assignments required by OLD.

### 17.2 Destination Is NEW, Source Is Still Bridge

Current state:

```text
BRIDGE / NEW
```

Rollback MUST execute:

```text
1. receive: NEW → OLD
2. send:    BRIDGE → OLD
```

The reverse order MUST NOT be used.

### 17.3 Fully Migrated to NEW

Current state:

```text
NEW / NEW
```

Do not directly move the destination back to OLD. NEW-generation packets may have been assigned only the NEW worker set.

Correct rollback:

```text
NEW / NEW
    ↓
BRIDGE / NEW
    ↓
DRAIN NEW-only generation
    ↓
BRIDGE / OLD
    ↓
OLD / OLD
```

Forward and rollback are symmetric:

```text
Forward:
OLD
→ BRIDGE/OLD
→ BRIDGE/NEW
→ NEW

Rollback:
NEW
→ BRIDGE/NEW
→ BRIDGE/OLD
→ OLD
```

---

## 18. Executor Migration Is a Separate Procedure

DVNs are verification/security dependencies.

Executors are execution/liveness services.

The two SHOULD NOT be migrated in the same production change unless explicitly approved.

### 18.1 Standard Executor Rotation

Let:

```text
E0 = old Executor
E1 = new Executor
```

Procedure:

```text
1. Deploy/start E1
2. Validate fee quoting, assignJob, event ingestion, and destination signer
3. Validate commit and execute workflows
4. Change source ExecutorConfig: E0 → E1
5. Keep E0 processing jobs paid before the cutover
6. Let E1 process jobs created after the cutover
7. Drain the E0 queue
8. Retire E0
```

Executor migration invariant:

```text
old jobs remain serviceable by E0
new jobs are serviceable by E1
```

E0 MUST NOT be retired immediately after the configuration switch.

### 18.2 When Both DVNs and Executor Must Change

Preferred ordering:

```text
Complete DVN migration:
OLD/OLD
→ BRIDGE/OLD
→ BRIDGE/NEW
→ NEW/NEW

Then:
E0 → E1
```

This keeps verification and execution failure domains independently observable and recoverable.

---

## 19. Bidirectional Pathways

`A → B` and `B → A` are separate directional pathways.

A bidirectional deployment therefore owns:

```text
Chain A:
    send(A→B)
    receive(B→A)

Chain B:
    send(B→A)
    receive(A→B)
```

Recommended maintenance-window batching:

```text
Phase 1:
    A.send(A→B) → Bridge_AB
    B.send(B→A) → Bridge_BA

Phase 2:
    drain both directions

Phase 3:
    B.receive(A→B) → New_AB
    A.receive(B→A) → New_BA

Phase 4:
    bidirectional canary

Phase 5:
    A.send(A→B) → New_AB
    B.send(B→A) → New_BA
```

A pathway is not fully migrated merely because one direction is complete.

---

## 20. Multi-Chain Meshes

For a full mesh of N chains:

```text
directional pathways = N × (N - 1)
```

Operational strategies include:

### 20.1 Pathway-by-Pathway

Complete the full migration for each direction independently.

Advantage: minimal blast radius.  
Disadvantage: highest operational overhead.

### 20.2 Per-Source-Chain Batch

For all destinations of one source:

```text
send → Bridge
→ drain
→ partner receives → New
```

Suitable for medium-size meshes.

### 20.3 Mesh-Wide Staged Migration

For a large mesh with strong governance coordination:

```text
all source sends → Bridge
→ global drain
→ all destination receives → New
→ global canary
→ all source sends → New
```

LayerZero's migration guidance similarly describes mesh-wide send-first / receive-later strategies for larger contiguous deployments.

---

## 21. Production Gates and Automated Assertions

Migration tooling SHOULD enforce the following assertions before transaction submission.

### 21.1 Configuration Assertions

- correct send and receive library addresses;
- correct remote EID;
- correct DVN provider identity;
- chain-specific deployment address used for each DVN;
- required and optional arrays strictly ascending;
- no duplicates within each array;
- `requiredDVNCount` equals required array length;
- `optionalDVNCount` equals optional array length;
- `0 < optionalDVNThreshold <= optionalDVNCount` when optional DVNs are enabled;
- resolved policy contains at least one DVN;
- `sendConfirmations >= receiveConfirmations`;
- production policy does not unintentionally inherit mutable defaults.

The current ULN implementation limits each of the required and optional DVN counts to 127.

### 21.2 Bridge Assertions

```text
Workers(Bridge)
    ⊇ Workers(Old) ∪ Workers(New)
```

and:

```text
confirmations(Bridge)
    >= max(old.confirmations, new.confirmations)
```

### 21.3 Cutover Assertions

Before `receive OLD → NEW`:

```text
oldGenerationOutstanding == 0
newDVNsHealthy == true
bridgeReadbackMatchesExpected == true
rollbackPrepared == true
```

### 21.4 Finalization Assertions

Before `send BRIDGE → NEW`:

```text
postCutoverCanary == success
newPolicyAttestations == success
commit == success
execution == success
```

---

## 22. Recommended Software Abstraction

Do not build separate top-level operations such as:

```text
addDVN()
removeDVN()
replaceDVN()
increaseThreshold()
decreaseThreshold()
```

Prefer a single high-level interface:

```ts
migrateSecurityPolicy({
  pathway,
  from,
  to
})
```

The planner computes:

```ts
oldWorkers =
  union(old.requiredDVNs, old.optionalDVNs)

newWorkers =
  union(new.requiredDVNs, new.optionalDVNs)

bridgeWorkers =
  union(oldWorkers, newWorkers)

bridgeConfirmations =
  max(old.confirmations, new.confirmations)
```

and generates:

```text
FROM/FROM
→ BRIDGE/FROM
→ DRAIN
→ BRIDGE/TO
→ CANARY
→ TO/TO
```

with a pre-generated rollback plan:

```text
TO/TO
→ BRIDGE/TO
→ DRAIN
→ BRIDGE/FROM
→ FROM/FROM
```

---

## 23. Recommended MigrationPlan Model

```ts
interface SecurityPolicy {
  confirmations: bigint;
  requiredDVNs: string[];
  optionalDVNs: string[];
  optionalThreshold: number;
}

interface ExecutorPolicy {
  executor: string;
  maxMessageSize: number;
}

interface Pathway {
  srcEid: number;
  dstEid: number;
  srcOApp: string;
  dstOApp: string;
}

interface MigrationPlan {
  pathway: Pathway;

  oldPolicy: SecurityPolicy;
  bridgePolicy: SecurityPolicy;
  newPolicy: SecurityPolicy;

  boundary?: {
    sourceBlock: bigint;
    sourceTxHash: string;
    outboundNonce?: bigint;
  };

  state:
    | "PRECHECK"
    | "SOURCE_BRIDGE"
    | "DRAINING"
    | "DESTINATION_NEW"
    | "POST_CUTOVER_CANARY"
    | "SOURCE_NEW"
    | "COMPLETE"
    | "ROLLING_BACK";
}
```

Recommended retained evidence:

- expected encoded `UlnConfig`;
- expected effective configuration;
- multisig transaction hash;
- execution receipts;
- canary GUID;
- drain reconciliation output;
- rollback calldata.

---

## 24. Standard Production Runbook

```text
PRECHECK
│
├─ snapshot raw + effective configs
├─ validate send/receive libraries
├─ validate DVN addresses
├─ validate pathway coverage
├─ validate operator diversity
├─ construct Bridge
├─ build rollback transactions
│
▼
OPTIONAL PAUSE
│
▼
SOURCE → BRIDGE
│
├─ wait for finality
├─ read back config
├─ record generation boundary
│
▼
PRE-CUTOVER CANARY
│
├─ verify old DVNs
├─ verify new DVNs
├─ verify commit/execution
│
▼
DRAIN OLD GENERATION
│
├─ nonce/GUID reconciliation
├─ zero unresolved old packets
│
▼
DESTINATION → NEW
│
├─ wait for finality
├─ read back config
│
▼
POST-CUTOVER CANARY
│
├─ verify new required DVNs
├─ verify optional threshold
├─ verify confirmations
├─ verify commit
├─ verify execution
│
▼
SOURCE → NEW
│
├─ wait for finality
├─ read back config
│
▼
RESUME
│
▼
OBSERVATION WINDOW
│
▼
RETIRE OLD DVNs / INFRASTRUCTURE
│
▼
EXECUTOR MIGRATION (if required)
```

---

## 25. Change-Control Recommendations

Production organizations SHOULD:

- manage the LayerZero delegate through a multisig and/or timelock;
- coordinate with peer-chain owners before the maintenance window;
- treat every directional pathway as an independently auditable object;
- retain configuration hashes, transaction hashes, GUIDs, and approvals in the change record;
- avoid unrelated MessageLib upgrades during the migration window;
- avoid unrelated OApp business-logic upgrades during DVN migration;
- maintain at least two independently operated DVNs for high-value pathways;
- alert on stalled packets, verification latency, commit latency, execution latency, and Executor signer balances;
- retain OLD DVN / Executor infrastructure until the generations assigned to them are explicitly drained.

---

## 26. Non-Goals and Limitations

This runbook does not cover:

- MessageLib version migration;
- OApp peer migration;
- Endpoint upgrades;
- OApp business-logic upgrades;
- application-level packet idempotency;
- internal consensus/security of a custom DVN;
- source-chain or destination-chain consensus and reorganization risk.

Those changes should have separate runbooks.

---

## 27. Normative Summary

A production-grade LayerZero V2 DVN migration SHOULD be modeled as:

```text
OLD
→ BRIDGE
→ NEW
```

where:

```text
Workers(BRIDGE)
    ⊇ Workers(OLD) ∪ Workers(NEW)
```

and:

```text
confirmations(BRIDGE)
    >= max(
        confirmations(OLD),
        confirmations(NEW)
    )
```

Forward state machine:

```text
OLD / OLD
→ BRIDGE / OLD
→ DRAIN
→ BRIDGE / NEW
→ CANARY
→ NEW / NEW
```

Rollback state machine:

```text
NEW / NEW
→ BRIDGE / NEW
→ DRAIN
→ BRIDGE / OLD
→ OLD / OLD
```

Executor rotation is independent:

```text
start E_new
→ source E_old → E_new
→ drain E_old jobs
→ retire E_old
```

The primary operational rule is:

> **Always cause the future verification dependency to begin producing attestations for new packets before the destination begins enforcing that dependency.**

---

## 28. References

1. LayerZero — Migrating from a Single-DVN Configuration  
   https://docs.layerzero.network/v2/get-started/migrating-from-single-dvn

2. LayerZero — EVM DVN and Executor Configuration  
   https://docs.layerzero.network/v2/developers/evm/configuration/dvn-executor-config

3. LayerZero — Production DVN Configuration  
   https://docs.layerzero.network/v2/concepts/modular-security/production-dvn-configuration

4. LayerZero — Integration Checklist  
   https://docs.layerzero.network/v2/tools/integration-checklist

5. LayerZero — Build and Run Executors  
   https://docs.layerzero.network/v2/workers/off-chain/build-executors

6. LayerZero — Executors  
   https://docs.layerzero.network/v2/concepts/permissionless-execution/executors

7. LayerZero V2 — `SendUlnBase.sol`  
   https://github.com/LayerZero-Labs/LayerZero-v2/blob/main/packages/layerzero-v2/evm/messagelib/contracts/uln/SendUlnBase.sol

8. LayerZero V2 — `UlnBase.sol`  
   https://github.com/LayerZero-Labs/LayerZero-v2/blob/main/packages/layerzero-v2/evm/messagelib/contracts/uln/UlnBase.sol

---

### Note

LayerZero's official documentation currently defines concrete DVN migration procedures, EVM configuration semantics, and production-security recommendations. The `Bridge Policy`, set-based invariant, and generalized forward/rollback state machines defined in this document are production-engineering abstractions derived from those protocol behaviors and source-code semantics; they are not separate on-chain LayerZero protocol objects or official LayerZero feature names.
