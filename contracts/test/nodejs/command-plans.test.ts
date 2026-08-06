import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import type { Address } from "viem";
import { buildConfigureLzDVNPlan } from "../../scripts/configure-lz-dvn.js";
import { buildConfigureLzExecutorPlan } from "../../scripts/configure-lz-executor.js";
import {
  buildConfigureWorkersPlan,
  type ConfigureWorkersInput,
} from "../../scripts/configure-workers.js";
import { buildSendOFTPlan } from "../../scripts/oft-send-runner.js";
import { validateIgnitionCommandFiles } from "../../scripts/commands/ignition-command.js";

const first = address("11");
const second = address("22");

test("LayerZero dry-run plans reject invalid executor and DVN configuration", () => {
  assert.throws(
    () =>
      buildConfigureLzExecutorPlan({
        endpoint: first,
        oapp: second,
        remoteEid: 40449,
        sendUln: address("33"),
        openExecutor: address("44"),
        executorMaxMessageSize: 0x1_0000_0000n,
      }),
    /executorMaxMessageSize exceeds uint32/
  );

  const base = {
    endpoint: first,
    oapp: second,
    remoteEid: 40449,
    sendUln: address("33"),
    receiveUln: address("44"),
    sendConfirmations: 12n,
    receiveConfirmations: 12n,
  };
  assert.throws(
    () => buildConfigureLzDVNPlan({ ...base, requiredDVNs: [address("55")] }),
    /at least two addresses/
  );
  assert.throws(
    () =>
      buildConfigureLzDVNPlan({
        ...base,
        requiredDVNs: [address("55"), address("55")],
      }),
    /duplicate DVN address/
  );
  assert.throws(
    () =>
      buildConfigureLzDVNPlan({
        ...base,
        requiredDVNs: [address("55"), address("66")],
        sendConfirmations: 11n,
        receiveConfirmations: 12n,
      }),
    /send confirmations 11 must be at least receive confirmations 12/
  );
  const asymmetric = buildConfigureLzDVNPlan({
    ...base,
    requiredDVNs: [address("55"), address("66")],
    sendConfirmations: 15n,
    receiveConfirmations: 12n,
  });
  assert.equal(asymmetric.sendUlnConfig.confirmations, 15n);
  assert.equal(asymmetric.receiveUlnConfig.confirmations, 12n);
  assert.notEqual(asymmetric.encodedSendConfig, asymmetric.encodedReceiveConfig);
});

test("OFT send dry-run builds exact options and rejects conflicting input", () => {
  const input = {
    testOFT: first,
    recipient: second,
    dstEid: 40449,
    amountLD: 9_007_199_254_740_993_123n,
    lzReceiveGas: 250_000n,
  };
  const plan = buildSendOFTPlan(input);
  assert.equal(plan.amountLD, input.amountLD);
  assert.equal(plan.minAmountLD, input.amountLD);
  assert.notEqual(plan.sendParam.extraOptions, "0x");

  assert.throws(
    () => buildSendOFTPlan({ ...input, extraOptions: "0x0003" }),
    /must not both be set/
  );
});

test("worker dry-run validates cross-field, width, freshness, and fee constraints", () => {
  const input = workerInput();
  const plan = buildConfigureWorkersPlan(input, { now: 1_000n });
  assert.equal(plan.srcOApp, input.testOFT);
  assert.equal(plan.priceSnapshot.updatedAt, 1_000n);

  assert.throws(
    () =>
      buildConfigureWorkersPlan(
        { ...input, testOFT: undefined, srcOApp: undefined },
        { now: 1_000n }
      ),
    /srcOApp or input\.testOFT is required/
  );
  assert.throws(
    () =>
      buildConfigureWorkersPlan(
        {
          ...input,
          testOFT: undefined,
          srcOApp: address("99"),
          rateLimit: { capacity: 1n, refillPerSecond: 1n },
        },
        { now: 1_000n }
      ),
    /testOFT is required/
  );
  assert.throws(
    () =>
      buildConfigureWorkersPlan(
        {
          ...input,
          priceSnapshot: { ...input.priceSnapshot, staleAfter: 86_401n },
        },
        { now: 1_000n }
      ),
    /between 1 and 86400/
  );
  assert.throws(
    () =>
      buildConfigureWorkersPlan(
        {
          ...input,
          executorFeeModel: {
            ...input.executorFeeModel,
            marginBps: 10_001n,
          },
        },
        { now: 1_000n }
      ),
    /marginBps must not exceed 10000/
  );
});

test("Ignition dry-run validates deployment IDs and parameter files", () => {
  const directory = mkdtempSync(path.join(tmpdir(), "oml-ignition-plan-"));
  const valid = path.join(directory, "valid.json");
  const secret = path.join(directory, "secret.json");
  writeFileSync(valid, JSON.stringify({ OpenWorkers: { owner: first } }));
  writeFileSync(
    secret,
    JSON.stringify({ OpenWorkers: { rpcUrl: "https://example.invalid" } })
  );

  assert.doesNotThrow(() =>
    validateIgnitionCommandFiles(valid, "sepolia-open-workers")
  );
  assert.throws(
    () => validateIgnitionCommandFiles(valid, "../invalid"),
    /invalid Ignition deployment id/
  );
  assert.throws(
    () => validateIgnitionCommandFiles(secret, "sepolia-open-workers"),
    /rpcUrl is not allowed/
  );
});

function workerInput(): ConfigureWorkersInput {
  return {
    testOFT: first,
    openExecutor: address("33"),
    openDVN: address("44"),
    priceFeed: address("55"),
    remoteEid: 40449,
    sendLib: address("66"),
    maxMessageSize: 10_000n,
    minLzReceiveGas: 200_000n,
    maxLzReceiveGas: 1_000_000n,
    priceSnapshot: {
      dstGasPriceInSrcToken: 1n,
      dstDataFeePerByteInSrcToken: 0n,
      staleAfter: 1_800n,
    },
    executorFeeModel: {
      baseFee: 0n,
      dstGasOverhead: 50_000n,
      dataSizeOverheadBytes: 0n,
      marginBps: 1_000n,
    },
    dvnFeeModel: {
      baseFee: 0n,
      dstGasOverhead: 150_000n,
      dataSizeOverheadBytes: 0n,
      marginBps: 1_000n,
    },
  };
}

function address(byte: string): Address {
  return `0x${byte.padStart(2, "0").repeat(20)}` as Address;
}
