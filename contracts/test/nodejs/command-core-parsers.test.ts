import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import { parseDeploymentPreflightCommandInput } from "../../scripts/command-cores/check-deployment-preflight.js";
import { parseCheckDVNVerificationCommandInput } from "../../scripts/command-cores/check-dvn-verification.js";
import { parseMigrationEvidenceCommandInput } from "../../scripts/command-cores/check-migration-evidence.js";
import { parseCheckOFTCanaryCommandInput } from "../../scripts/command-cores/check-oft-canary.js";
import { parsePriceConfigCommandInput } from "../../scripts/command-cores/check-price-config.js";
import { parseConfigureLzDVNCommandInput } from "../../scripts/command-cores/configure-lz-dvn.js";
import { parseConfigureLzExecutorCommandInput } from "../../scripts/command-cores/configure-lz-executor.js";
import {
  parseConfigureLzRollbackCommandInput,
  runConfigureLzRollbackCommand,
} from "../../scripts/command-cores/configure-lz-rollback.js";
import { parseLocalE2EDeployCommandInput } from "../../scripts/command-cores/e2e-local-deploy.js";
import { parseRegtestDeployCommandInput } from "../../scripts/command-cores/regtest-deploy.js";
import { parseRegtestSendCommandInput } from "../../scripts/command-cores/regtest-send.js";
import { parseLocalE2ERunCommandInput } from "../../scripts/command-cores/e2e-local-run.js";
import { parseInspectLzConfigCommandInput } from "../../scripts/command-cores/inspect-lz-config.js";
import { parseOFTPathwayCommandInput } from "../../scripts/command-cores/oft-pathway.js";

const first = address("11");
const second = address("22");
const third = address("33");
const fourth = address("44");
const fifth = address("55");

test("OFT pathway parser enforces action-specific fields", () => {
  const inspect = parseOFTPathwayCommandInput(
    {
      action: "inspect",
      testOFT: first,
      remoteEid: "40449",
    },
    "input"
  );
  assert.equal(inspect.expectedSigner, undefined);
  assert.equal(inspect.rateLimit, undefined);

  assert.throws(
    () =>
      parseOFTPathwayCommandInput(
        {
          action: "pause-send",
          testOFT: first,
          remoteEid: "40449",
        },
        "input"
      ),
    /input\.expectedSigner is required for write actions/
  );
  assert.throws(
    () =>
      parseOFTPathwayCommandInput(
        {
          action: "set-rate-limit",
          testOFT: first,
          remoteEid: "40449",
          expectedSigner: second,
        },
        "input"
      ),
    /input\.rateLimit is required only when action is set-rate-limit/
  );
  assert.throws(
    () =>
      parseOFTPathwayCommandInput(
        {
          action: "inspect",
          testOFT: first,
          remoteEid: "40449",
          rateLimit: { capacity: "1", refillPerSecond: "1" },
        },
        "input"
      ),
    /input\.rateLimit is required only when action is set-rate-limit/
  );

  const setRateLimit = parseOFTPathwayCommandInput(
    {
      action: "set-rate-limit",
      testOFT: first,
      remoteEid: "40449",
      rateLimit: {
        capacity: "9007199254740993123",
        refillPerSecond: "7",
      },
      expectedSigner: second,
    },
    "input"
  );
  assert.equal(setRateLimit.rateLimit?.capacity, 9_007_199_254_740_993_123n);
  assert.equal(setRateLimit.rateLimit?.refillPerSecond, 7n);
});

test("LayerZero configure parsers preserve bigint values and reject loose input", () => {
  const executor = parseConfigureLzExecutorCommandInput(
    {
      endpoint: first,
      oapp: second,
      remoteEid: "40449",
      sendUln: third,
      openExecutor: fourth,
      executorMaxMessageSize: "9007199254740993123",
      expectedSigner: fifth,
    },
    "input"
  );
  assert.equal(executor.executorMaxMessageSize, 9_007_199_254_740_993_123n);

  const dvn = parseConfigureLzDVNCommandInput(
    {
      endpoint: first,
      oapp: second,
      remoteEid: "40449",
      sendUln: third,
      receiveUln: fourth,
      requiredDVNs: [fifth],
      sendConfirmations: "9007199254740993123",
      receiveConfirmations: "12",
      expectedSigner: first,
    },
    "input"
  );
  assert.equal(dvn.sendConfirmations, 9_007_199_254_740_993_123n);
  assert.equal(dvn.receiveConfirmations, 12n);
  assert.deepEqual(dvn.requiredDVNs, [fifth]);

  assert.throws(
    () =>
      parseConfigureLzExecutorCommandInput(
        {
          endpoint: first,
          oapp: second,
          remoteEid: "40449",
          sendUln: third,
          openExecutor: fourth,
          executorMaxMessageSize: "10000",
          expectedSigner: fifth,
          legacyFlag: true,
        },
        "input"
      ),
    /input contains unknown field: legacyFlag/
  );
  assert.throws(
    () =>
      parseConfigureLzDVNCommandInput(
        {
          endpoint: first,
          oapp: second,
          remoteEid: "40449",
          sendUln: third,
          receiveUln: fourth,
          requiredDVNs: `${fifth},${first}`,
          sendConfirmations: "12",
          receiveConfirmations: "12",
          expectedSigner: first,
        },
        "input"
      ),
    /input\.requiredDVNs must be an array/
  );
});

test("read-only command parsers use strict decimal and optional evidence fields", () => {
  const verification = parseCheckDVNVerificationCommandInput(
    {
      txHash: hash("aa"),
      receiveUln: first,
      requiredDVNs: [second, third],
      confirmations: "12",
      expectedSrcEid: "40161",
      expectedDstEid: "40449",
      expectedNonce: "9007199254740993123",
      expectedSender: fourth,
      expectedReceiver: fifth,
    },
    "input"
  );
  assert.equal(verification.expectedNonce, 9_007_199_254_740_993_123n);
  assert.equal(verification.expectedSrcEid, 40161);
  assert.equal(verification.expectedDstEid, 40449);

  const inspection = parseInspectLzConfigCommandInput(
    {
      endpoint: first,
      oapp: second,
      remoteEid: "40449",
      sendUln: third,
      receiveUln: fourth,
    },
    "input"
  );
  assert.equal(inspection.remoteEid, 40449);

  assert.throws(
    () =>
      parseInspectLzConfigCommandInput(
        {
          endpoint: first,
          oapp: second,
          remoteEid: 40449,
          sendUln: third,
          receiveUln: fourth,
        },
        "input"
      ),
    /input\.remoteEid must be a string/
  );
});

test("rollback parser is strict and dry-run requires an explicit network", async () => {
  const input = parseConfigureLzRollbackCommandInput(
    {
      lzConfigSnapshot: "tmp/lz-config.json",
      expectedSigner: first,
    },
    "input"
  );
  assert.equal(input.lzConfigSnapshot, "tmp/lz-config.json");
  assert.throws(
    () =>
      parseConfigureLzRollbackCommandInput(
        {
          lzConfigSnapshot: "tmp/lz-config.json",
          expectedSigner: first,
          dryRun: true,
        },
        "input"
      ),
    /input contains unknown field: dryRun/
  );

  const directory = mkdtempSync(path.join(tmpdir(), "oml-rollback-command-"));
  const parameters = path.join(directory, "parameters.json");
  writeFileSync(
    parameters,
    JSON.stringify({
      input: {
        lzConfigSnapshot: path.join(directory, "missing-snapshot.json"),
        expectedSigner: first,
      },
      apply: false,
    })
  );
  const previous = process.env.OML_SCRIPT_PARAMS;
  process.env.OML_SCRIPT_PARAMS = parameters;
  try {
    await assert.rejects(
      runConfigureLzRollbackCommand({
        globalOptions: {},
      } as HardhatRuntimeEnvironment),
      /a named HTTP network must be selected with --network/
    );
  } finally {
    if (previous === undefined) {
      delete process.env.OML_SCRIPT_PARAMS;
    } else {
      process.env.OML_SCRIPT_PARAMS = previous;
    }
    rmSync(directory, { recursive: true, force: true });
  }
});

test("deployment preflight parser applies zero defaults without losing precision", () => {
  const parsed = parseDeploymentPreflightCommandInput(
    {
      testOFT: first,
      openExecutor: second,
      openDVN: third,
      expectedOwner: fourth,
      expectedTestOFTTotalSupply: "9007199254740993123",
    },
    "input"
  );
  assert.equal(parsed.minOwnerNativeBalance, 0n);
  assert.equal(parsed.minCanaryNativeBalance, 0n);
  assert.equal(parsed.minCanaryTokenBalance, 0n);
  assert.equal(parsed.expectedTestOFTTotalSupply, 9_007_199_254_740_993_123n);

  assert.throws(
    () =>
      parseDeploymentPreflightCommandInput(
        {
          testOFT: first,
          openExecutor: second,
          openDVN: third,
          expectedOwner: fourth,
          minOwnerNativeBalance: 1,
        },
        "input"
      ),
    /input\.minOwnerNativeBalance must be a string/
  );
});

test("canary and price check parsers preserve optional evidence and decimal values", () => {
  const canary = parseCheckOFTCanaryCommandInput(
    {
      endpoint: first,
      sourceTxHash: hash("aa"),
      sendLib: second,
      openExecutor: third,
      destinationTxHash: hash("bb"),
      destinationEndpoint: fourth,
      destinationTestOFT: fifth,
      recipient: first,
      minRecipientBalance: "9007199254740993123",
    },
    "input"
  );
  assert.equal(canary.minRecipientBalance, 9_007_199_254_740_993_123n);
  assert.equal(canary.destinationTxHash, hash("bb"));

  const price = parsePriceConfigCommandInput(
    {
      dstEid: "40449",
      maxPriceAgeSeconds: "9007199254740993123",
      expectedStaleAfter: "1800",
      priceFeed: first,
      openExecutor: second,
      openDVN: third,
    },
    "input"
  );
  assert.equal(price.dstEid, 40449);
  assert.equal(price.maxPriceAgeSeconds, 9_007_199_254_740_993_123n);
  assert.equal(price.expectedStaleAfter, 1800n);

  assert.throws(
    () =>
      parsePriceConfigCommandInput(
        {
          dstEid: "40449",
          maxPriceAgeSeconds: "1800",
          priceFeed: first,
          openExecutor: second,
          openDVN: third,
          rpcUrl: "https://example.invalid",
        },
        "input"
      ),
    /input contains unknown field: rpcUrl/
  );
});

test("regtest command parsers are strict", () => {
  assert.deepEqual(
    parseRegtestDeployCommandInput({ tmpDir: "tmp/regtest" }, "input"),
    {
      tmpDir: "tmp/regtest",
      confirmations: undefined,
      initialSupply: undefined,
      dvnMode: undefined,
    }
  );
  assert.deepEqual(
    parseRegtestDeployCommandInput(
      {
        tmpDir: "tmp/regtest",
        confirmations: "2",
        initialSupply: "1000000",
        dvnMode: "shadow",
      },
      "input"
    ),
    {
      tmpDir: "tmp/regtest",
      confirmations: 2n,
      initialSupply: 1_000_000n,
      dvnMode: "shadow",
    }
  );
  assert.throws(
    () =>
      parseRegtestDeployCommandInput(
        { tmpDir: "tmp/regtest", dvnMode: "off" },
        "input"
      ),
    /input\.dvnMode/
  );
  assert.deepEqual(
    parseRegtestSendCommandInput(
      {
        tmpDir: "tmp/regtest",
        direction: "ba",
        amountLD: "1000000000000000000",
        timeoutMs: "180000",
      },
      "input"
    ),
    {
      tmpDir: "tmp/regtest",
      direction: "ba",
      amountLD: 1_000_000_000_000_000_000n,
      timeoutMs: 180_000,
    }
  );
  assert.throws(
    () =>
      parseRegtestDeployCommandInput(
        { tmpDir: "tmp/regtest", deployerPrivateKey: "not-allowed" },
        "input"
      ),
    /input contains unknown field: deployerPrivateKey/
  );
  assert.throws(
    () =>
      parseRegtestSendCommandInput(
        {
          tmpDir: "tmp/regtest",
          direction: "sideways",
          amountLD: "1",
          timeoutMs: "1000",
        },
        "input"
      ),
    /input\.direction/
  );
});

test("local E2E and migration evidence command parsers are strict", () => {
  assert.deepEqual(
    parseLocalE2EDeployCommandInput({ tmpDir: "tmp/e2e" }, "input"),
    { tmpDir: "tmp/e2e" }
  );
  assert.deepEqual(
    parseLocalE2ERunCommandInput(
      {
        tmpDir: "tmp/e2e",
        workerReadyUrl: "http://127.0.0.1:19090/readyz",
      },
      "input"
    ),
    {
      tmpDir: "tmp/e2e",
      workerReadyUrl: "http://127.0.0.1:19090/readyz",
    }
  );
  assert.deepEqual(
    parseMigrationEvidenceCommandInput(
      { migrationEvidence: "tmp/evidence.json" },
      "input"
    ),
    { migrationEvidence: "tmp/evidence.json" }
  );

  assert.throws(
    () =>
      parseLocalE2EDeployCommandInput(
        { tmpDir: "tmp/e2e", deployerPrivateKey: "not-allowed" },
        "input"
      ),
    /input contains unknown field: deployerPrivateKey/
  );
  assert.throws(
    () =>
      parseLocalE2ERunCommandInput(
        { tmpDir: "tmp/e2e", workerReadyUrl: 9090 },
        "input"
      ),
    /input\.workerReadyUrl must be a string/
  );
  assert.throws(
    () =>
      parseMigrationEvidenceCommandInput(
        { migrationEvidence: "tmp/evidence.json", legacy: true },
        "input"
      ),
    /input contains unknown field: legacy/
  );
});

function address(byte: string): `0x${string}` {
  return `0x${byte.repeat(20)}`;
}

function hash(byte: string): `0x${string}` {
  return `0x${byte.repeat(32)}`;
}
