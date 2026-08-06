import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { mkdirSync } from "node:fs";
import { getAddress } from "viem";
import {
  REGTEST_IGNITION_DEPLOYMENT_IDS,
  assertRegtestIgnitionDirectory,
  buildRegtestChainParameters,
  buildRegtestPathwayParameters,
  regtestWorkerConfig,
  resolveRegtestDeployInput,
  validateRegtestDeployment,
  type RegtestChainDeployment,
  type RegtestDeployment,
  type RegtestWorkerSpec,
} from "../../scripts/regtest-deploy.js";
import { buildRegtestSendPlan } from "../../scripts/regtest-send.js";
import { writeLocalE2EIgnitionParameters } from "../../scripts/e2e-local-deploy.js";
import { persistedPathwayUpdatedAt } from "../../scripts/regtest-deploy.js";
import { decodeUlnConfig } from "../../scripts/lz-config.js";

const deployer = getAddress("0x1111111111111111111111111111111111111111");
const worker1 = getAddress("0x2222222222222222222222222222222222222222");
const worker2 = getAddress("0x3333333333333333333333333333333333333333");

test("regtest Ignition deployment IDs are stable and direction-specific", () => {
  assert.deepEqual(REGTEST_IGNITION_DEPLOYMENT_IDS, {
    chainA: "regtest-chain-a",
    chainB: "regtest-chain-b",
    pathwayAToB: "regtest-pathway-a-to-b",
    pathwayBToA: "regtest-pathway-b-to-a",
  });
});

test("regtest requires isolated Ignition state below its temporary directory", () => {
  assert.doesNotThrow(() =>
    assertRegtestIgnitionDirectory("tmp/regtest/ignition", "tmp/regtest")
  );
  assert.throws(
    () => assertRegtestIgnitionDirectory("contracts/ignition", "tmp/regtest"),
    /regtest requires Hardhat Ignition path/
  );
});

test("resolveRegtestDeployInput reads worker signers from keystores and rejects duplicates", () => {
  const directory = mkdtempSync(path.join(tmpdir(), "oml-regtest-deploy-"));
  writeFileSync(
    path.join(directory, "worker-1-keystore.json"),
    JSON.stringify({ address: worker1.slice(2) })
  );
  writeFileSync(
    path.join(directory, "worker-2-keystore.json"),
    JSON.stringify({ address: worker2.slice(2) })
  );
  const resolved = resolveRegtestDeployInput(
    { tmpDir: directory, confirmations: 2n },
    {
      REGTEST_A_HOST_RPC_URL: "http://127.0.0.1:38545",
      REGTEST_B_HOST_RPC_URL: "http://127.0.0.1:38546",
    }
  );
  assert.equal(resolved.chains[0].name, "regtest-a");
  assert.equal(resolved.chains[1].name, "regtest-b");
  assert.equal(resolved.chains[0].hostRpcUrl, "http://127.0.0.1:38545");
  assert.equal(resolved.confirmations, 2n);
  assert.equal(resolved.dvnMode, "active");
  assert.equal(resolved.workers[0].address, worker1);
  assert.equal(resolved.workers[1].address, worker2);
  assert.equal(resolved.keystorePasswordEnv, "REGTEST_KEYSTORE_PASSWORD");

  writeFileSync(
    path.join(directory, "worker-2-keystore.json"),
    JSON.stringify({ address: worker1.slice(2) })
  );
  assert.throws(
    () => resolveRegtestDeployInput({ tmpDir: directory }, {}),
    /two distinct signer addresses/
  );
});

test("regtest business settings live in the envelope, not the environment", () => {
  const directory = mkdtempSync(path.join(tmpdir(), "oml-regtest-envelope-"));
  writeFileSync(
    path.join(directory, "worker-1-keystore.json"),
    JSON.stringify({ address: worker1.slice(2) })
  );
  writeFileSync(
    path.join(directory, "worker-2-keystore.json"),
    JSON.stringify({ address: worker2.slice(2) })
  );
  // Ambient variables must not change the reviewed deployment.
  const resolved = resolveRegtestDeployInput(
    { tmpDir: directory, dvnMode: "shadow", initialSupply: 5n },
    {
      REGTEST_CONFIRMATIONS: "9",
      REGTEST_INITIAL_SUPPLY: "7",
      REGTEST_DVN_MODE: "active",
    }
  );
  assert.equal(resolved.confirmations, 1n);
  assert.equal(resolved.initialSupply, 5n);
  assert.equal(resolved.dvnMode, "shadow");
  assert.throws(
    () =>
      resolveRegtestDeployInput({ tmpDir: directory, confirmations: 0n }, {}),
    /confirmations must be at least 1/
  );
});

test("resolveRegtestDeployInput restores the legacy gas price knob strictly", () => {
  const directory = mkdtempSync(path.join(tmpdir(), "oml-regtest-gas-"));
  writeFileSync(
    path.join(directory, "worker-1-keystore.json"),
    JSON.stringify({ address: worker1.slice(2) })
  );
  writeFileSync(
    path.join(directory, "worker-2-keystore.json"),
    JSON.stringify({ address: worker2.slice(2) })
  );
  const resolved = resolveRegtestDeployInput(
    { tmpDir: directory },
    { REGTEST_B_GAS_PRICE: "1000000000" }
  );
  assert.equal(resolved.chains[0].gasPrice, undefined);
  assert.equal(resolved.chains[1].gasPrice, 1_000_000_000n);
  assert.throws(
    () =>
      resolveRegtestDeployInput(
        { tmpDir: directory },
        { REGTEST_B_GAS_PRICE: "1gwei" }
      ),
    /REGTEST_B_GAS_PRICE must be an unsigned wei integer/
  );
});

test("persistedPathwayUpdatedAt replays the recorded snapshot timestamp", async () => {
  const directory = mkdtempSync(path.join(tmpdir(), "oml-regtest-params-"));
  assert.equal(
    await persistedPathwayUpdatedAt(directory, "regtest-pathway-a-to-b"),
    undefined
  );
  await writeLocalE2EIgnitionParameters(
    directory,
    "regtest-pathway-a-to-b",
    buildRegtestPathwayParameters(
      chainDeployment("a", 90101),
      chainDeployment("b", 90102),
      1_700_000_123n,
      1n
    )
  );
  assert.equal(
    await persistedPathwayUpdatedAt(directory, "regtest-pathway-a-to-b"),
    1_700_000_123n
  );
  const malformed = path.join(directory, "ignition-parameters");
  mkdirSync(malformed, { recursive: true });
  writeFileSync(path.join(malformed, "regtest-pathway-b-to-a.json"), "{}");
  await assert.rejects(
    () => persistedPathwayUpdatedAt(directory, "regtest-pathway-b-to-a"),
    /no priceSnapshot\.updatedAt bigint/
  );
});

test("regtest pathway parameters authorize each worker's own DVN verifier", () => {
  const source = chainDeployment("a", 90101);
  const destination = chainDeployment("b", 90102);
  const parameters = buildRegtestPathwayParameters(
    source,
    destination,
    1_700_000_000n,
    3n
  ).LocalE2EPathway;
  assert.equal(parameters.primaryDVNVerifier, worker1);
  assert.equal(parameters.secondaryDVNVerifier, worker2);
  assert.equal(parameters.remoteEid, destination.eid);
  const ulnEntry = parameters.receiveConfig[0];
  assert.ok(ulnEntry);
  const decoded = decodeUlnConfig(ulnEntry.config);
  assert.equal(decoded.confirmations, 3n);
  assert.deepEqual(
    [...decoded.requiredDVNs].sort(),
    [source.primaryOpenDVN, source.secondaryOpenDVN].sort()
  );

  const chainParameters = buildRegtestChainParameters(
    source,
    { workers: workers() },
    deployer,
    5n
  ).LocalE2EChain;
  assert.equal(chainParameters.tokenSymbol, "GOATED");
  assert.deepEqual(chainParameters.priceFeedSubmitters, [
    deployer,
    worker1,
    worker2,
  ]);
});

test("regtest worker configs split executor and DVN roles across workers", () => {
  const deployment = regtestDeployment();
  const [workerSpec1, workerSpec2] = workers();
  const config1 = regtestWorkerConfig(
    deployment,
    { keystorePasswordEnv: "REGTEST_KEYSTORE_PASSWORD", dvnMode: "active" },
    workerSpec1,
    "host"
  );
  const config2 = regtestWorkerConfig(
    deployment,
    { keystorePasswordEnv: "REGTEST_KEYSTORE_PASSWORD", dvnMode: "active" },
    workerSpec2,
    "container"
  );
  assert.match(config1, /executor:\n    enabled: true/);
  assert.match(config2, /executor:\n    enabled: false/);
  assert.ok(config1.includes(`open_dvn: "${deployment.chains.a.primaryOpenDVN}"`));
  assert.ok(
    config2.includes(`open_dvn: "${deployment.chains.a.secondaryOpenDVN}"`)
  );
  // Both workers still require the exact two-DVN set on every pathway.
  for (const config of [config1, config2]) {
    assert.ok(config.includes("send_required_dvns:"));
    assert.ok(config.includes(deployment.chains.a.primaryOpenDVN));
    assert.ok(config.includes(deployment.chains.a.secondaryOpenDVN));
  }
  assert.ok(config2.includes("postgres://laz_worker:laz_worker@postgres"));
  // Without a pinned gas price the worker keeps default (EIP-1559) fees.
  assert.ok(!config1.includes("legacy_transactions"));
});

test("a gas-price-pinned chain renders legacy_transactions for the workers", () => {
  const deployment = regtestDeployment();
  deployment.chains.b = { ...deployment.chains.b, gasPrice: 1_000_000_000n };
  const [workerSpec1] = workers();
  const config = regtestWorkerConfig(
    deployment,
    { keystorePasswordEnv: "REGTEST_KEYSTORE_PASSWORD", dvnMode: "active" },
    workerSpec1,
    "host"
  );
  // Only the pinned chain forces legacy transactions; the worker must send
  // type-0 there too or its DVN/executor writes can be dropped.
  const chainBlocks = config.split("  - eid: ");
  const chainA = chainBlocks.find((block) =>
    block.startsWith(`${deployment.chains.a.eid}`)
  );
  const chainB = chainBlocks.find((block) =>
    block.startsWith(`${deployment.chains.b.eid}`)
  );
  assert.ok(chainA !== undefined && chainB !== undefined);
  assert.ok(!chainA.includes("legacy_transactions"));
  assert.match(chainB, /legacy_transactions: true/);
});

test("regtest deployment validation round-trips and send plans stay strict", () => {
  const deployment = regtestDeployment();
  const validated = validateRegtestDeployment(
    JSON.parse(JSON.stringify(deployment))
  );
  assert.deepEqual(validated, deployment);
  assert.throws(
    () =>
      validateRegtestDeployment({
        ...JSON.parse(JSON.stringify(deployment)),
        parameters: { ...deployment.parameters, dvnMode: "off" },
      }),
    /dvnMode must be active or shadow/
  );
  const withGasPrice = JSON.parse(JSON.stringify(deployment)) as {
    chains: { b: Record<string, unknown> };
  };
  withGasPrice.chains.b.gasPrice = "2000000000";
  assert.equal(
    validateRegtestDeployment(withGasPrice).chains.b.gasPrice,
    2_000_000_000n
  );
  withGasPrice.chains.b.gasPrice = "fast";
  assert.throws(
    () => validateRegtestDeployment(withGasPrice),
    /gasPrice must be an unsigned wei integer string/
  );

  const plan = buildRegtestSendPlan(
    { tmpDir: "tmp/regtest", direction: "ba", amountLD: 5n, timeoutMs: 1_000 },
    deployment
  );
  assert.equal(plan.source.network, "regtest-b");
  assert.equal(plan.destination.network, "regtest-a");
  assert.throws(
    () =>
      buildRegtestSendPlan(
        { tmpDir: "tmp/regtest", direction: "ab", amountLD: 0n, timeoutMs: 1 },
        deployment
      ),
    /amountLD must be positive/
  );
});

function workers(): readonly [RegtestWorkerSpec, RegtestWorkerSpec] {
  return [
    {
      index: 1,
      address: worker1,
      metricsAddress: ":9090",
      keystorePaths: {
        host: "tmp/regtest/worker-1-keystore.json",
        container: "/app/tmp/regtest/worker-1-keystore.json",
      },
      databaseURLs: {
        host: "postgres://laz_worker:laz_worker@127.0.0.1:55433/laz_worker_1?sslmode=disable",
        container:
          "postgres://laz_worker:laz_worker@postgres:5432/laz_worker_1?sslmode=disable",
      },
    },
    {
      index: 2,
      address: worker2,
      metricsAddress: ":9091",
      keystorePaths: {
        host: "tmp/regtest/worker-2-keystore.json",
        container: "/app/tmp/regtest/worker-2-keystore.json",
      },
      databaseURLs: {
        host: "postgres://laz_worker:laz_worker@127.0.0.1:55433/laz_worker_2?sslmode=disable",
        container:
          "postgres://laz_worker:laz_worker@postgres:5432/laz_worker_2?sslmode=disable",
      },
    },
  ] as const;
}

function chainDeployment(
  key: "a" | "b",
  eid: number
): RegtestChainDeployment {
  const suffix = key === "a" ? "a" : "b";
  return {
    key,
    name: `regtest-${suffix}`,
    eid,
    chainId: key === "a" ? 31337 : 31338,
    hostRpcUrl: `http://127.0.0.1:1854${key === "a" ? 5 : 6}`,
    containerRpcUrl:
      key === "a" ? "http://anvil-a:8545" : "http://goat-geth:8545",
    endpoint: getAddress(`0x4444444444444444444444444444444444444${key === "a" ? "441" : "442"}`),
    sendUln: getAddress(`0x5555555555555555555555555555555555555${key === "a" ? "551" : "552"}`),
    receiveUln: getAddress(`0x6666666666666666666666666666666666666${key === "a" ? "661" : "662"}`),
    oft: getAddress(`0x7777777777777777777777777777777777777${key === "a" ? "771" : "772"}`),
    priceFeed: getAddress(`0x8888888888888888888888888888888888888${key === "a" ? "881" : "882"}`),
    openExecutor: getAddress(`0x9999999999999999999999999999999999999${key === "a" ? "991" : "992"}`),
    primaryOpenDVN: getAddress(`0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa${key === "a" ? "aa1" : "aa2"}`),
    secondaryOpenDVN: getAddress(`0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb${key === "a" ? "bb1" : "bb2"}`),
    primaryDVNSigner: worker1,
    secondaryDVNSigner: worker2,
    executorSigner: worker1,
  };
}

function regtestDeployment(): RegtestDeployment {
  return {
    generatedAt: "2026-08-03T00:00:00.000Z",
    deployer,
    workers: { worker1, worker2 },
    parameters: {
      confirmations: "1",
      maxMessageSize: 10_000,
      minLzReceiveGas: "100000",
      lzReceiveGas: "250000",
      maxLzReceiveGas: "1000000",
      initialSupply: "1000000000000000000000000",
      dvnMode: "active",
      requiredDVNCount: 2,
    },
    chains: { a: chainDeployment("a", 90101), b: chainDeployment("b", 90102) },
  };
}
