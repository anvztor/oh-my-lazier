import assert from "node:assert/strict";
import test from "node:test";
import type { PublicClient } from "viem";
import {
  CONFIG_TYPE_EXECUTOR,
  CONFIG_TYPE_ULN,
  NIL_DVN_COUNT,
  decodeExecutorConfig,
  decodeUlnConfig,
} from "./lz-config.js";
import {
  buildCommandPlan,
  buildDeploymentState,
  deploymentPreflightArgs,
  extractOpenWorkerContracts,
  isBootstrapStateUnavailable,
  normalizeProfile,
  oappEndpointParameterFile,
  openWorkersParameterFile,
  openWorkersPathwayParameterFile,
  readPriceFeedFromWorkers,
  renderWorkerConfig,
  resolveWorkerStartBlocks,
  shouldRunConfigureOApp,
  shouldRunWorkerOnlyVerify,
  testOFTParameterFile,
  testOFTDeploymentAddressPath,
  type DeploymentProfile,
  workerDeploymentAddressPath,
} from "./deploy-profile.js";

test("normalizeProfile validates rehearsal mode and generic external DVNs", () => {
  const profile = normalizeProfile(baseProfile());

  assert.equal(profile.mode, "test-oft-rehearsal");
  assert.equal(profile.dvnMode, "active");
  assert.equal(profile.chains[0].eid, 40161);
  assert.equal(profile.chains[0].nativeAssetId, "eth");
  assert.equal(profile.chains[0].startBlockNumber, undefined);
  assert.equal(profile.chains[0].indexerPollIntervalSeconds, 5);
  assert.deepEqual(profile.chains[0].externalDVNs, [
    "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa",
  ]);
  assert.equal(profile.chains[0].includeLayerZeroLabsDVN, false);
  assert.equal(
    profile.chains[0].layerZero.endpointV2,
    "0x6EDCE65403992e310A62460808c4b910D972f10f",
  );
  assert.equal(profile.chains[1].eid, 40449);
  assert.equal(profile.chains[1].nativeAssetId, "eth");
  assert.equal(profile.chains[1].indexerPollIntervalSeconds, 5);
  assert.deepEqual(profile.chains[1].externalDVNs, [
    "0x9999999999999999999999999999999999999999",
  ]);
  assert.equal(profile.chains[1].includeLayerZeroLabsDVN, false);
});

test("normalizeProfile rejects non-positive or non-integer indexer poll intervals", () => {
  for (const value of [
    undefined,
    0,
    -1,
    1.5,
    "5",
    9_223_372_037,
    Number.MAX_SAFE_INTEGER + 1,
  ]) {
    const input = baseProfile();
    (input.chains[0] as Record<string, unknown>).indexerPollIntervalSeconds =
      value;

    assert.throws(
      () => normalizeProfile(input),
      /profile\.chains\[0\]\.indexerPollIntervalSeconds must be (?:an integer|a safe integer|>= 1|<= 9223372036)/,
    );
  }
});

test("normalizeProfile rejects runtime duration overflow", () => {
  const sourceTimeout = baseProfile();
  (sourceTimeout as Record<string, unknown>).pricing = {
    sourceRequestTimeoutSeconds: 9_223_372_037,
  };
  assert.throws(
    () => normalizeProfile(sourceTimeout),
    /profile\.pricing\.sourceRequestTimeoutSeconds must be <= 9223372036/,
  );

  const sourceMaxAge = baseProfile();
  (sourceMaxAge.chains[1] as Record<string, unknown>).nativeAssetId =
    "hoodi-eth";
  for (const [idx, chain] of sourceMaxAge.chains.entries()) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "coingecko",
      coinGecko: {
        id: "ethereum",
        maxAgeSeconds: idx === 0 ? 9_223_372_037 : 180,
      },
    };
  }
  assert.throws(
    () => normalizeProfile(sourceMaxAge),
    /profile\.chains\[0\]\.priceSources\.coinGecko\.maxAgeSeconds must be <= 9223372036/,
  );
});

test("normalizeProfile rejects legacy LayerZero Labs DVN profile fields", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).layerZero = {
    endpointV2: "0x6EDCE65403992e310A62460808c4b910D972f10f",
    sendUln302: "0xcc1ae8Cf5D3904Cef3360A9532B477529b177cCE",
    receiveUln302: "0xdAf00F5eE2158dD58E0d3857851c432E34A3A851",
    layerZeroLabsDVN: "0x8eebf8b423b73bfca51a1db4b7354aa0bfca9193",
  };

  assert.throws(
    () => normalizeProfile(input),
    /layerZero\.layerZeroLabsDVN is not supported/,
  );
});

test("normalizeProfile rejects Hardhat network and chain id mismatches", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).chainId = 560048;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.network sepolia uses chainId 11155111, but profile\.chains\[0\]\.chainId is 560048/,
  );
});

test("normalizeProfile rejects Hardhat network and EID mismatches with custom LayerZero", () => {
  const input = baseProfile();
  const chain = input.chains[0] as Record<string, unknown>;
  chain.eid = 40449;
  chain.layerZero = {
    endpointV2: "0x3aCAAf60502791D199a5a5F0B173D78229eBFe32",
    sendUln302: "0x45841dd1ca50265Da7614fC43A361e526c0e6160",
    receiveUln302: "0xd682ECF100f6F4284138AA925348633B0611Ae21",
  };

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.network sepolia uses eid 40161, but profile\.chains\[0\]\.eid is 40449/,
  );
});

test("normalizeProfile accepts opt-in LayerZero Labs DVN metadata", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).includeLayerZeroLabsDVN = true;
  (input.chains[0] as Record<string, unknown>).externalDVNs = [];

  const profile = normalizeProfile(input);

  assert.equal(profile.chains[0].includeLayerZeroLabsDVN, true);
  assert.deepEqual(profile.chains[0].externalDVNs, []);
});

test("normalizeProfile rejects opt-in LayerZero Labs DVN for unknown local chain metadata", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).includeLayerZeroLabsDVN = true;
  (input.chains[0] as Record<string, unknown>).layerZero = {
    endpointV2: "0x3333333333333333333333333333333333333333",
    sendUln302: "0x4444444444444444444444444444444444444444",
    receiveUln302: "0x5555555555555555555555555555555555555555",
  };

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.includeLayerZeroLabsDVN has no repo-known LayerZero Labs DVN metadata/,
  );
});

test("normalizeProfile requires external OApp addresses in external mode", () => {
  const input = externalProfile();
  delete (input.chains[0] as Record<string, unknown>).oapp;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.oapp is required for external-oapp mode/,
  );
});

test("normalizeProfile requires long-term price feed submitters", () => {
  const input = baseProfile();
  delete (input as Record<string, unknown>).priceFeedSubmitters;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.priceFeedSubmitters is required/,
  );
});

test("normalizeProfile rejects owner as a long-term price feed submitter", () => {
  const input = baseProfile();
  input.priceFeedSubmitters = ["0x1111111111111111111111111111111111111111"];

  assert.throws(
    () => normalizeProfile(input),
    /profile\.priceFeedSubmitters must not include profile\.owner/,
  );
});

test("normalizeProfile rejects uppercase native asset ids", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).nativeAssetId = "ETH";

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.nativeAssetId must be lowercase/,
  );
});

test("normalizeProfile requires source configuration for cross-asset pricing", () => {
  const input = baseProfile();
  (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";

  assert.throws(
    () => normalizeProfile(input),
    /sepolia\.priceSources is required for cross-asset pricing/,
  );
});

test("normalizeProfile accepts cross-asset pricing without Chainlink or Uniswap", () => {
  const input = baseProfile();
  (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";
  for (const chain of input.chains) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "coingecko",
      coinGecko: { id: "ethereum", maxAgeSeconds: 180 },
    };
  }

  const profile = normalizeProfile(input);

  assert.equal(profile.chains[0].priceSources?.primarySource, "coingecko");
  assert.deepEqual(profile.chains[0].priceSources?.sanitySources, []);
  assert.equal(profile.chains[0].priceSources?.chainlink, undefined);
  assert.equal(profile.chains[0].priceSources?.uniswap, undefined);
});

test("normalizeProfile accepts optional Chainlink primary and Uniswap sanity", () => {
  const input = baseProfile();
  (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";
  for (const chain of input.chains) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "chainlink",
      sanitySources: ["uniswap"],
      chainlink: {
        feedAddress: "0x1111111111111111111111111111111111111111",
        expectedDescription: "ETH / USD",
        maxAgeSeconds: 3600,
      },
      uniswap: {
        poolAddress: "0x2222222222222222222222222222222222222222",
        tokenIn: "0x3333333333333333333333333333333333333333",
        tokenOut: "0x4444444444444444444444444444444444444444",
        twapWindowSeconds: 1800,
        maxBlockAgeSeconds: 120,
        minHarmonicMeanLiquidity: "1000000",
      },
    };
  }

  const profile = normalizeProfile(input);

  assert.equal(profile.chains[0].priceSources?.primarySource, "chainlink");
  assert.deepEqual(profile.chains[0].priceSources?.sanitySources, ["uniswap"]);
});

test("normalizeProfile enforces the Uniswap TWAP window bounds", () => {
  const cases = [
    { name: "below minimum", window: 1_799, wantError: true },
    { name: "at minimum", window: 1_800, wantError: false },
    { name: "at uint32 maximum", window: 0xffff_ffff, wantError: false },
    { name: "above uint32 maximum", window: 0x1_0000_0000, wantError: true },
  ];

  for (const testCase of cases) {
    const input = baseProfile();
    (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";
    for (const chain of input.chains) {
      (chain as Record<string, unknown>).priceSources = {
        primarySource: "chainlink",
        sanitySources: ["uniswap"],
        chainlink: {
          feedAddress: "0x1111111111111111111111111111111111111111",
          expectedDescription: "ETH / USD",
          maxAgeSeconds: 3600,
        },
        uniswap: {
          poolAddress: "0x2222222222222222222222222222222222222222",
          tokenIn: "0x3333333333333333333333333333333333333333",
          tokenOut: "0x4444444444444444444444444444444444444444",
          twapWindowSeconds: testCase.window,
          maxBlockAgeSeconds: 120,
          minHarmonicMeanLiquidity: "1000000",
        },
      };
    }

    if (testCase.wantError) {
      assert.throws(
        () => normalizeProfile(input),
        /twapWindowSeconds must be between 1800 and 4294967295/,
        testCase.name,
      );
      continue;
    }
    assert.equal(
      normalizeProfile(input).chains[0].priceSources?.uniswap
        ?.twapWindowSeconds,
      testCase.window,
      testCase.name,
    );
  }
});

test("normalizeProfile accepts CoinMarketCap primary with environment key reference", () => {
  const input = baseProfile();
  (input as Record<string, unknown>).pricing = {
    coinMarketCapAPIKeyEnv: "COINMARKETCAP_API_KEY",
  };
  (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";
  for (const chain of input.chains) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "coinmarketcap",
      coinMarketCap: { id: 1027, maxAgeSeconds: 180 },
    };
  }

  const profile = normalizeProfile(input);

  assert.equal(profile.pricing.coinMarketCapAPIKeyEnv, "COINMARKETCAP_API_KEY");
  assert.equal(profile.chains[0].priceSources?.coinMarketCap?.id, 1027);
});

test("normalizeProfile rejects HTTP market-data BaseURLs without echoing them", () => {
  const cases = [
    {
      baseField: "coinMarketCapBaseURL",
      label: "profile.pricing.coinMarketCapBaseURL",
    },
    {
      baseField: "coinGeckoBaseURL",
      label: "profile.pricing.coinGeckoBaseURL",
    },
  ];
  const secretBaseURL =
    "http://pricing-user:pricing-password@pricing-secret.example/private-api-key";

  for (const testCase of cases) {
    const input = baseProfile();
    (input as Record<string, unknown>).pricing = {
      [testCase.baseField]: secretBaseURL,
    };
    let caught: unknown;
    try {
      normalizeProfile(input);
    } catch (error) {
      caught = error;
    }
    assert.ok(caught instanceof Error, `${testCase.baseField} should fail`);
    assert.match(
      caught.message,
      new RegExp(`${testCase.label} must be an absolute HTTPS URL without query or fragment`),
    );
    assert.doesNotMatch(caught.message, /pricing-user|pricing-password|pricing-secret|private-api-key/);
  }
});

test("normalizeProfile rejects malformed market-data BaseURLs without echoing them", () => {
  const input = baseProfile();
  (input as Record<string, unknown>).pricing = {
    coinGeckoBaseURL:
      "ftp://pricing-user:pricing-password@pricing-secret.example/private-api-key",
  };
  let caught: unknown;
  try {
    normalizeProfile(input);
  } catch (error) {
    caught = error;
  }
  assert.ok(caught instanceof Error);
  assert.match(
    caught.message,
    /profile\.pricing\.coinGeckoBaseURL must be an absolute HTTPS URL without query or fragment/,
  );
  assert.doesNotMatch(caught.message, /pricing-user|pricing-password|pricing-secret|private-api-key/);
});

test("normalizeProfile rejects base URLs that parse in JS but not in the Go worker", () => {
  // Forms the WHATWG URL parser accepts (or silently repairs) but the Go
  // worker's net/url validator rejects, so a rendered profile cannot pass here
  // yet be refused at worker startup.
  const rejected = [
    "https:example.com",
    "https:/example.com",
    "https:\\\\example.com",
    "https://exa\tmple.com/v1",
    "https://example.com/v\n1",
    "https://example.com\\foo",
    "https://example.com/%zz",
    "https://example.com/%",
    "https://%65xample.com",
    "https://example%2ecom",
    "https:///example.com",
    "https://user@/path",
  ];
  for (const baseURL of rejected) {
    const input = baseProfile();
    (input as Record<string, unknown>).pricing = {
      coinGeckoBaseURL: baseURL,
    };
    assert.throws(
      () => normalizeProfile(input),
      /profile\.pricing\.coinGeckoBaseURL must be an absolute HTTPS URL without query or fragment/,
      `expected ${JSON.stringify(baseURL)} to be rejected`,
    );
  }
});

test("normalizeProfile rejects enforced lzReceive gas outside [min, max]", () => {
  const belowMin = baseProfile();
  (belowMin.pathway as Record<string, unknown>).enforcedLzReceiveGas = "100000";
  assert.throws(
    () => normalizeProfile(belowMin),
    /pathway\.enforcedLzReceiveGas must be within/,
  );

  const aboveMax = baseProfile();
  (aboveMax.pathway as Record<string, unknown>).enforcedLzReceiveGas =
    "2000000";
  assert.throws(
    () => normalizeProfile(aboveMax),
    /pathway\.enforcedLzReceiveGas must be within/,
  );

  const zeroMax = baseProfile();
  (zeroMax.pathway as Record<string, unknown>).minLzReceiveGas = "0";
  (zeroMax.pathway as Record<string, unknown>).maxLzReceiveGas = "0";
  (zeroMax.pathway as Record<string, unknown>).enforcedLzReceiveGas = "0";
  assert.throws(
    () => normalizeProfile(zeroMax),
    /pathway\.maxLzReceiveGas must be positive/,
  );
});

test("normalizeProfile rejects missing and unreferenced price source blocks", () => {
  const missing = baseProfile();
  (missing.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";
  for (const chain of missing.chains) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "coingecko",
      sanitySources: ["uniswap"],
      coinGecko: { id: "ethereum", maxAgeSeconds: 180 },
    };
  }
  assert.throws(
    () => normalizeProfile(missing),
    /uniswap is required when source is referenced/,
  );

  const unreferenced = baseProfile();
  (unreferenced.chains[1] as Record<string, unknown>).nativeAssetId =
    "hoodi-eth";
  for (const chain of unreferenced.chains) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "coingecko",
      coinGecko: { id: "ethereum", maxAgeSeconds: 180 },
      chainlink: {
        feedAddress: "0x1111111111111111111111111111111111111111",
        expectedDescription: "ETH / USD",
        maxAgeSeconds: 3600,
      },
    };
  }
  assert.throws(
    () => normalizeProfile(unreferenced),
    /chainlink is configured but not referenced/,
  );
});

test("normalizeProfile rejects legacy top-level canary token balance", () => {
  const input = {
    ...baseProfile(),
    minCanaryTokenBalance: "1000000000000000",
  };

  assert.throws(
    () => normalizeProfile(input),
    /profile\.minCanaryTokenBalance is not supported/,
  );
});

test("normalizeProfile requires chain canary token balance in rehearsal mode", () => {
  const input = baseProfile();
  delete (input.chains[1] as Record<string, unknown>).minCanaryTokenBalance;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[1\]\.minCanaryTokenBalance must be a non-empty string/,
  );
});

test("normalizeProfile rejects tx roles that do not reference a configured signer", () => {
  const input = baseProfile();
  input.chains[0].txRoles.executor.signer =
    "0x9999999999999999999999999999999999999999";

  assert.throws(
    () => normalizeProfile(input),
    /txRoles\.executor\.signer must reference a configured signer/,
  );
});

test("normalizeProfile rejects zero tx role minimum native balance", () => {
  const input = baseProfile();
  input.chains[0].txRoles.executor.minNativeBalanceWei = "0";

  assert.throws(
    () => normalizeProfile(input),
    /txRoles\.executor\.minNativeBalanceWei must be positive/,
  );
});

test("normalizeProfile rejects invalid per-chain pricing transaction policies", () => {
  const cases: Array<{
    name: string;
    mutate: (input: ReturnType<typeof baseProfile>) => void;
    error: RegExp;
  }> = [
    {
      name: "missing policy",
      mutate: (input) => {
        delete (input.chains[0] as Record<string, unknown>).pricingTxPolicy;
      },
      error: /profile\.chains\[0\]\.pricingTxPolicy must be an object/,
    },
    {
      name: "zero max fee",
      mutate: (input) => {
        input.chains[0].pricingTxPolicy.maxFeePerGasWei = "0";
      },
      error:
        /profile\.chains\[0\]\.pricingTxPolicy\.maxFeePerGasWei must be positive/,
    },
    {
      name: "zero priority fee",
      mutate: (input) => {
        input.chains[0].pricingTxPolicy.maxPriorityFeePerGasWei = "0";
      },
      error:
        /profile\.chains\[0\]\.pricingTxPolicy\.maxPriorityFeePerGasWei must be positive/,
    },
    {
      name: "priority fee exceeds max fee",
      mutate: (input) => {
        input.chains[0].pricingTxPolicy.maxFeePerGasWei = "1";
        input.chains[0].pricingTxPolicy.maxPriorityFeePerGasWei = "2";
      },
      error:
        /profile\.chains\[0\]\.pricingTxPolicy\.maxPriorityFeePerGasWei must not exceed maxFeePerGasWei/,
    },
    {
      name: "zero minimum balance",
      mutate: (input) => {
        input.chains[1].pricingTxPolicy.minNativeBalanceWei = "0";
      },
      error:
        /profile\.chains\[1\]\.pricingTxPolicy\.minNativeBalanceWei must be positive/,
    },
  ];

  for (const testCase of cases) {
    const input = baseProfile();
    testCase.mutate(input);
    assert.throws(
      () => normalizeProfile(input),
      testCase.error,
      testCase.name,
    );
  }
});

test("normalizeProfile enforces the OpenPriceFeed staleAfter range", () => {
  const atLimit = baseProfile();
  atLimit.pathway.priceSnapshot.staleAfter = "86400";
  assert.equal(
    normalizeProfile(atLimit).pathway.priceSnapshot.staleAfter,
    "86400",
  );

  const cases = [
    {
      name: "zero",
      staleAfter: "0",
      error: /profile\.pathway\.priceSnapshot\.staleAfter must be positive/,
    },
    {
      name: "above contract maximum",
      staleAfter: "86401",
      error:
        /profile\.pathway\.priceSnapshot\.staleAfter must not exceed 86400/,
    },
  ];
  for (const testCase of cases) {
    const input = baseProfile();
    input.pathway.priceSnapshot.staleAfter = testCase.staleAfter;
    assert.throws(
      () => normalizeProfile(input),
      testCase.error,
      testCase.name,
    );
  }
});

test("normalizeProfile rejects Hardhat private key env injection", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).privateKeyEnv =
    "SEPOLIA_PRIVATE_KEY";

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.privateKeyEnv is not supported/,
  );
});

test("extractOpenWorkerContracts and buildDeploymentState require OpenWorkers price feed", () => {
  const profile = normalizeProfile(baseProfile());
  const workerDeployedAddresses = {
    sepolia: deployedWorkers(
      "0x1111111111111111111111111111111111111111",
      false,
    ),
    hoodi: deployedWorkers("0x2222222222222222222222222222222222222222", false),
  };
  const testOFTDeployedAddresses = {
    sepolia: deployedTestOFT("0x1111111111111111111111111111111111111111"),
    hoodi: deployedTestOFT("0x2222222222222222222222222222222222222222"),
  };

  assert.equal(
    extractOpenWorkerContracts(workerDeployedAddresses.sepolia, "sepolia")
      .openExecutor,
    "0x1111111111111111111111111111111111111112",
  );
  assert.throws(
    () =>
      buildDeploymentState({
        profile,
        workerDeployedAddresses,
        testOFTDeployedAddresses,
      }),
    /missing OpenWorkers#OpenPriceFeed/,
  );

  const state = buildDeploymentState({
    profile,
    workerDeployedAddresses,
    testOFTDeployedAddresses,
    priceFeedOverrides: {
      sepolia: "0x1111111111111111111111111111111111111114",
      hoodi: "0x2222222222222222222222222222222222222225",
    },
    generatedAt: "2026-07-05T00:00:00.000Z",
  });

  assert.equal(
    state.chains[0].workers.priceFeed,
    "0x1111111111111111111111111111111111111114",
  );
  assert.equal(
    state.chains[0].oapp,
    "0x1111111111111111111111111111111111111111",
  );
  assert.equal(
    state.directions[0].receiveLib,
    profile.chains[1].layerZero.receiveUln302,
  );
  assert.equal(
    state.directions[1].sourceWorkers.openDVN,
    state.chains[1].workers.openDVN,
  );
});

test("buildDeploymentState uses profile OApps in external mode without TestOFT state", () => {
  const profile = normalizeProfile(externalProfile());
  const state = buildDeploymentState({
    profile,
    workerDeployedAddresses: {
      sepolia: deployedWorkers("0x1111111111111111111111111111111111111111"),
      hoodi: deployedWorkers("0x2222222222222222222222222222222222222222"),
    },
    generatedAt: "2026-07-05T00:00:00.000Z",
  });

  assert.equal(state.mode, "external-oapp");
  assert.equal(
    state.chains[0].oapp,
    "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa",
  );
  assert.equal(
    state.chains[1].oapp,
    "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB",
  );
});

test("readPriceFeedFromWorkers hydrates only matching worker price feeds", async () => {
  const priceFeed = "0x3333333333333333333333333333333333333333";
  const publicClient = {
    readContract: async () => priceFeed,
  } as unknown as PublicClient;

  assert.equal(
    await readPriceFeedFromWorkers({
      publicClient,
      chainKey: "sepolia",
      openExecutor: "0x1111111111111111111111111111111111111112",
      openDVN: "0x1111111111111111111111111111111111111113",
      openExecutorAbi: [],
      openDVNAbi: [],
    }),
    priceFeed,
  );

  let count = 0;
  const mismatchedClient = {
    readContract: async () =>
      count++ === 0
        ? "0x3333333333333333333333333333333333333333"
        : "0x4444444444444444444444444444444444444444",
  } as unknown as PublicClient;

  await assert.rejects(
    readPriceFeedFromWorkers({
      publicClient: mismatchedClient,
      chainKey: "hoodi",
      openExecutor: "0x2222222222222222222222222222222222222222",
      openDVN: "0x2222222222222222222222222222222222222223",
      openExecutorAbi: [],
      openDVNAbi: [],
    }),
    /does not match/,
  );
});

test("oappEndpointParameterFile and openWorkersPathwayParameterFile split config surfaces", () => {
  const profile = normalizeProfile(baseProfile());
  const state = stateWithPriceFeeds(profile);
  const source = profile.chains[0];
  const destination = profile.chains[1];
  const oapp = oappEndpointParameterFile({
    profile,
    state,
    source,
    destination,
    priceSnapshotUpdatedAt: 1_800_000_000n,
  }).OAppEndpointConfig;
  const workers = openWorkersPathwayParameterFile({
    profile,
    state,
    source,
    destination,
    priceSnapshotUpdatedAt: 1_800_000_000n,
  }).OpenWorkersPathwayConfig;

  assert.equal(oapp.oapp, state.chains[0].oapp);
  assert.equal(oapp.endpoint, profile.chains[0].layerZero.endpointV2);
  assert.equal(oapp.remoteEid, 40449);
  assert.equal(oapp.sendConfig[0].configType, CONFIG_TYPE_EXECUTOR);
  assert.deepEqual(decodeExecutorConfig(oapp.sendConfig[0].config), {
    maxMessageSize: 10000,
    executor: state.chains[0].workers.openExecutor,
  });
  assert.equal(oapp.sendConfig[1].configType, CONFIG_TYPE_ULN);
  const uln = decodeUlnConfig(oapp.sendConfig[1].config);
  assert.equal(uln.confirmations, 12n);
  assert.equal(uln.requiredDVNCount, 2);
  assert.equal(uln.optionalDVNCount, NIL_DVN_COUNT);
  assert.deepEqual(
    uln.requiredDVNs.map((address) => address.toLowerCase()).sort(),
    [
      profile.chains[0].externalDVNs[0].toLowerCase(),
      state.chains[0].workers.openDVN.toLowerCase(),
    ].sort(),
  );
  assert.deepEqual(oapp.receiveConfig, [
    {
      eid: 40449,
      configType: CONFIG_TYPE_ULN,
      config: oapp.sendConfig[1].config,
    },
  ]);

  assert.equal(workers.oapp, state.chains[0].oapp);
  assert.equal(workers.remoteEid, 40449);
  assert.equal(workers.openExecutor, state.chains[0].workers.openExecutor);
  assert.equal(workers.openDVN, state.chains[0].workers.openDVN);
  assert.equal(workers.priceFeed, state.chains[0].workers.priceFeed);
  assert.equal(workers.bootstrapPriceSubmitter, profile.owner);
  assert.equal(workers.dvnVerifier, profile.chains[0].txRoles.dvn.signer);
  assert.deepEqual(workers.priceSnapshot, {
    dstGasPriceInSrcToken: "1",
    dstDataFeePerByteInSrcToken: "0",
    updatedAt: "1800000000",
    staleAfter: "1800",
  });
  assert.equal(Object.hasOwn(workers, "sendConfig"), false);
  assert.equal(Object.hasOwn(workers, "enforcedOptions"), false);
});

test("oappEndpointParameterFile appends opt-in LayerZero Labs DVN", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).includeLayerZeroLabsDVN = true;
  (input.chains[0] as Record<string, unknown>).externalDVNs = [];
  const profile = normalizeProfile(input);
  const state = stateWithPriceFeeds(profile);
  const oapp = oappEndpointParameterFile({
    profile,
    state,
    source: profile.chains[0],
    destination: profile.chains[1],
    priceSnapshotUpdatedAt: 1_800_000_000n,
  }).OAppEndpointConfig;

  const uln = decodeUlnConfig(oapp.sendConfig[1].config);

  assert.equal(uln.requiredDVNCount, 2);
  assert.deepEqual(
    uln.requiredDVNs.map((address) => address.toLowerCase()).sort(),
    [
      "0x8eebf8b423b73bfca51a1db4b7354aa0bfca9193",
      state.chains[0].workers.openDVN.toLowerCase(),
    ].sort(),
  );
});

test("oappEndpointParameterFile rejects duplicate explicit and opt-in DVNs", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).includeLayerZeroLabsDVN = true;
  (input.chains[0] as Record<string, unknown>).externalDVNs = [
    "0x8eebf8b423b73bfca51a1db4b7354aa0bfca9193",
  ];
  const profile = normalizeProfile(input);
  const state = stateWithPriceFeeds(profile);

  assert.throws(
    () =>
      oappEndpointParameterFile({
        profile,
        state,
        source: profile.chains[0],
        destination: profile.chains[1],
      }),
    /duplicate DVN address/,
  );
});

test("renderWorkerConfig emits external OApps, active DVN signer, and worker contracts", () => {
  const profile = normalizeProfile(externalProfile());
  const state = stateWithPriceFeeds(profile);
  const yaml = renderWorkerConfig({
    profile,
    state,
    rpcUrls: {
      sepolia: "https://sepolia.example.invalid/rpc?key=abc",
      hoodi: "https://hoodi.example.invalid/rpc?key=def",
    },
    workerStartBlocks: {
      sepolia: 123456,
      hoodi: 654321,
    },
  });

  assert.match(yaml, /database_url: "postgres:\/\/laz_worker/);
  assert.match(yaml, /src_oapp: "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"/);
  assert.match(yaml, /dst_oapp: "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB"/);
  assert.match(yaml, /mode: active/);
  assert.match(
    yaml,
    /source_workers:\n      open_executor: "0x1111111111111111111111111111111111111112"/,
  );
  assert.match(
    yaml,
    /destination_workers:\n      open_dvn: "0x2222222222222222222222222222222222222223"/,
  );
  assert.match(yaml, /signer: "0x2222222222222222222222222222222222222222"/);
  assert.match(yaml, /pricing:\n  enabled: true/);
  assert.match(
    yaml,
    /signer: "0x2222222222222222222222222222222222222222"\n  interval_seconds: 300/,
  );
  assert.match(yaml, /native_asset_id: eth/);
  assert.doesNotMatch(yaml, /primary_source:/);
  assert.match(yaml, /min_update_deviation_bps: 50/);
  assert.match(yaml, /heartbeat_seconds: 900/);
  assert.match(yaml, /source_request_timeout_seconds: 10/);
  assert.match(yaml, /start_block_number: 123456/);
  assert.match(yaml, /start_block_number: 654321/);
  assert.match(yaml, /indexer_poll_interval_seconds: 5/);
});

test("renderWorkerConfig emits only referenced optional price sources", () => {
  const input = externalProfile();
  (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";
  for (const chain of input.chains) {
    (chain as Record<string, unknown>).priceSources = {
      primarySource: "coingecko",
      coinGecko: { id: "ethereum", maxAgeSeconds: 180 },
    };
  }
  const profile = normalizeProfile(input);
  const yaml = renderWorkerConfig({
    profile,
    state: stateWithPriceFeeds(profile),
    rpcUrls: { sepolia: "https://sepolia.invalid", hoodi: "https://hoodi.invalid" },
    workerStartBlocks: { sepolia: 1, hoodi: 1 },
  });

  assert.match(yaml, /primary_source: coingecko/);
  assert.match(yaml, /coingecko:\n        id: ethereum\n        max_age_seconds: 180/);
  assert.doesNotMatch(yaml, /sanity_sources:/);
  assert.doesNotMatch(yaml, /chainlink:/);
  assert.doesNotMatch(yaml, /uniswap:/);
});

test("renderWorkerConfig emits pricing transaction policy per chain", () => {
  const input = externalProfile();
  input.chains[0].pricingTxPolicy = {
    maxFeePerGasWei: "111",
    maxPriorityFeePerGasWei: "11",
    minNativeBalanceWei: "1111",
  };
  input.chains[1].pricingTxPolicy = {
    maxFeePerGasWei: "222",
    maxPriorityFeePerGasWei: "22",
    minNativeBalanceWei: "2222",
  };
  const profile = normalizeProfile(input);
  const yaml = renderWorkerConfig({
    profile,
    state: stateWithPriceFeeds(profile),
    rpcUrls: {
      sepolia: "https://sepolia.invalid",
      hoodi: "https://hoodi.invalid",
    },
    workerStartBlocks: { sepolia: 1, hoodi: 1 },
  });

  assert.match(
    yaml,
    /- eid: 40161\n      tx_policy:\n        max_fee_per_gas_wei: "111"\n        max_priority_fee_per_gas_wei: "11"\n        min_native_balance_wei: "1111"/,
  );
  assert.match(
    yaml,
    /- eid: 40449\n      tx_policy:\n        max_fee_per_gas_wei: "222"\n        max_priority_fee_per_gas_wei: "22"\n        min_native_balance_wei: "2222"/,
  );
  assert.doesNotMatch(yaml, /^  max_fee_per_gas_wei:/m);
});

test("resolveWorkerStartBlocks queries latest height only for missing profile overrides", async () => {
  const input = baseProfile();
  (input.chains[1] as Record<string, unknown>).startBlockNumber = 0;
  const profile = normalizeProfile(input);
  const queried: string[] = [];

  assert.deepEqual(
    await resolveWorkerStartBlocks({
      profile,
      rpcUrls: {
        sepolia: "https://sepolia.example.invalid/rpc",
        hoodi: "https://hoodi.example.invalid/rpc",
      },
      latestBlockNumber: async (chain, rpcURL) => {
        queried.push(`${chain.key}:${rpcURL}`);
        return 123456n;
      },
    }),
    {
      sepolia: 123456,
      hoodi: 0,
    },
  );
  assert.deepEqual(queried, ["sepolia:https://sepolia.example.invalid/rpc"]);
});

test("resolveWorkerStartBlocks preserves explicit non-zero profile override", async () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).startBlockNumber = 111;
  (input.chains[1] as Record<string, unknown>).startBlockNumber = 222;
  const profile = normalizeProfile(input);

  assert.deepEqual(
    await resolveWorkerStartBlocks({
      profile,
      rpcUrls: {
        sepolia: "https://sepolia.example.invalid/rpc",
        hoodi: "https://hoodi.example.invalid/rpc",
      },
      latestBlockNumber: async () => {
        throw new Error("latest block should not be queried");
      },
    }),
    {
      sepolia: 111,
      hoodi: 222,
    },
  );
});

test("parameter files split TestOFT rehearsal from OpenWorkers deployment", () => {
  const input = baseProfile();
  input.priceFeedSubmitters = [
    "0x2222222222222222222222222222222222222222",
    "0x2222222222222222222222222222222222222222",
  ];
  const profile = normalizeProfile(input);

  assert.deepEqual(openWorkersParameterFile(profile, profile.chains[0]), {
    OpenWorkers: {
      owner: "0x1111111111111111111111111111111111111111",
      priceFeedSubmitters: [
        "0x2222222222222222222222222222222222222222",
        "0x1111111111111111111111111111111111111111",
      ],
    },
  });
  assert.deepEqual(testOFTParameterFile(profile, profile.chains[0]), {
    TestOFT: {
      tokenName: "Oh My Lazier Test OFT",
      tokenSymbol: "OMLTOFT",
      endpoint: "0x6EDCE65403992e310A62460808c4b910D972f10f",
      delegate: "0x1111111111111111111111111111111111111111",
      initialRecipient: "0x1111111111111111111111111111111111111111",
      initialSupply: "1000000000000000000000000n",
    },
  });
});

test("deployment state paths use profile deployment ids", () => {
  const profile = normalizeProfile(baseProfile());

  assert.equal(
    workerDeploymentAddressPath(profile.chains[0]),
    "ignition/deployments/sepolia-open-workers/deployed_addresses.json",
  );
  assert.equal(
    testOFTDeploymentAddressPath(profile.chains[0]),
    "ignition/deployments/sepolia-test-oft/deployed_addresses.json",
  );
});

test("deployment preflight args use chain canary token balances", () => {
  const profile = normalizeProfile(baseProfile());
  const state = stateWithPriceFeeds(profile);
  const sepoliaArgs = deploymentPreflightArgs({
    profile,
    chain: profile.chains[0],
    current: state.chains[0],
    rpcURL: "https://sepolia.example.invalid/rpc",
  });
  const hoodiArgs = deploymentPreflightArgs({
    profile,
    chain: profile.chains[1],
    current: state.chains[1],
    rpcURL: "https://hoodi.example.invalid/rpc",
  });

  assert.equal(
    sepoliaArgs[sepoliaArgs.indexOf("--min-canary-token-balance") + 1],
    "1000000000000000",
  );
  assert.equal(
    hoodiArgs[hoodiArgs.indexOf("--min-canary-token-balance") + 1],
    "0",
  );
});

test("command plan and phase gates keep external OApp config explicit", () => {
  const profile = normalizeProfile(externalProfile());
  const plan = buildCommandPlan({ profile, outDir: "tmp/deploy-profile" });
  const commandText = plan.commands
    .map((command) => command.command)
    .join("\n");

  assert.doesNotMatch(commandText, /deploy:test-oft/);
  assert.doesNotMatch(commandText, /PRIVATE_KEY/);
  assert.match(commandText, /npm run deploy:open-workers/);
  assert.match(commandText, /npm run configure:open-workers-pathway/);
  assert.match(commandText, /npm run configure:oapp-endpoint/);
  assert.doesNotMatch(commandText, /--build-profile/);
  assert.doesNotMatch(commandText, /--verify/);
  assert.doesNotMatch(commandText, /HARDHAT_IGNITION_CONFIRM/);
  assert.equal(shouldRunConfigureOApp(profile, "all", true), false);
  assert.equal(shouldRunConfigureOApp(profile, "configure-oapp", true), true);
  assert.equal(shouldRunWorkerOnlyVerify(profile, "all"), true);
  assert.equal(shouldRunWorkerOnlyVerify(profile, "verify"), false);
});

test("command plan forwards Ignition verify, build profile, and auto-confirm flags", () => {
  const profile = normalizeProfile(baseProfile());
  const plan = buildCommandPlan({
    profile,
    outDir: "tmp/deploy-profile",
    ignition: { verify: true, autoConfirm: true, buildProfile: "production" },
  });
  const mutatingCommands = plan.commands
    .filter((command) => command.mutates)
    .map((command) => command.command);
  const readOnlyCommands = plan.commands
    .filter((command) => !command.mutates)
    .map((command) => command.command)
    .join("\n");

  assert.equal(mutatingCommands.length, 8);
  assert.match(
    mutatingCommands[0],
    /^SEPOLIA_RPC_URL=\.\.\. HARDHAT_IGNITION_CONFIRM_DEPLOYMENT=true HARDHAT_IGNITION_CONFIRM_RESET=true npm run deploy:test-oft -- --build-profile production --network sepolia /,
  );
  for (const command of mutatingCommands) {
    assert.match(command, /HARDHAT_IGNITION_CONFIRM_DEPLOYMENT=true/);
    assert.match(command, /HARDHAT_IGNITION_CONFIRM_RESET=true/);
    assert.match(command, /--build-profile production/);
    assert.match(command, /--verify(?:\s|$)/);
  }
  assert.doesNotMatch(readOnlyCommands, /--build-profile/);
  assert.doesNotMatch(readOnlyCommands, /--verify/);
  assert.doesNotMatch(readOnlyCommands, /HARDHAT_IGNITION_CONFIRM/);
});

test("command plan rejects invalid Ignition build profile values", () => {
  const profile = normalizeProfile(baseProfile());

  assert.throws(
    () =>
      buildCommandPlan({
        profile,
        outDir: "tmp/deploy-profile",
        ignition: { buildProfile: "" },
      }),
    /--build-profile requires a value/,
  );
  assert.throws(
    () =>
      buildCommandPlan({
        profile,
        outDir: "tmp/deploy-profile",
        ignition: { buildProfile: "prod profile" },
      }),
    /--build-profile cannot contain whitespace/,
  );
});

test("pathway rendering rejects profiles without an external DVN", () => {
  const input = baseProfile();
  delete (input.chains[0] as Record<string, unknown>).externalDVNs;
  const profile = normalizeProfile(input);
  const state = stateWithPriceFeeds(profile);

  assert.deepEqual(profile.chains[0].externalDVNs, []);
  assert.throws(
    () =>
      oappEndpointParameterFile({
        profile,
        state,
        source: profile.chains[0],
        destination: profile.chains[1],
      }),
    /required DVNs must include at least two addresses/,
  );
});

test("isBootstrapStateUnavailable only allows missing new deployment state", () => {
  const missingDeploymentState = new Error(
    "ENOENT: no such file or directory, open 'ignition/deployments/sepolia-open-workers/deployed_addresses.json'",
  ) as NodeJS.ErrnoException;
  missingDeploymentState.code = "ENOENT";
  missingDeploymentState.path =
    "ignition/deployments/sepolia-open-workers/deployed_addresses.json";
  const missingArtifact = new Error(
    "ENOENT: no such file or directory, open 'contracts/artifacts/contracts/contracts/workers/OpenExecutor.sol/OpenExecutor.json'",
  ) as NodeJS.ErrnoException;
  missingArtifact.code = "ENOENT";
  missingArtifact.path =
    "contracts/artifacts/contracts/contracts/workers/OpenExecutor.sol/OpenExecutor.json";

  assert.equal(isBootstrapStateUnavailable(missingDeploymentState), true);
  assert.equal(isBootstrapStateUnavailable(missingArtifact), false);
  assert.equal(
    isBootstrapStateUnavailable(
      new Error(
        "sepolia deployed_addresses.json is missing OpenWorkers#OpenExecutor",
      ),
    ),
    true,
  );
  assert.equal(
    isBootstrapStateUnavailable(
      new Error("hoodi deployed_addresses.json is missing TestOFT#TestOFT"),
    ),
    true,
  );
  assert.equal(
    isBootstrapStateUnavailable(
      new Error(
        "hoodi deployed_addresses.json is missing OpenWorkers#OpenPriceFeed",
      ),
    ),
    false,
  );
});

function stateWithPriceFeeds(profile: DeploymentProfile) {
  return buildDeploymentState({
    profile,
    workerDeployedAddresses: {
      sepolia: deployedWorkers("0x1111111111111111111111111111111111111111"),
      hoodi: deployedWorkers("0x2222222222222222222222222222222222222222"),
    },
    testOFTDeployedAddresses:
      profile.mode === "test-oft-rehearsal"
        ? {
            sepolia: deployedTestOFT(
              "0x1111111111111111111111111111111111111111",
            ),
            hoodi: deployedTestOFT(
              "0x2222222222222222222222222222222222222222",
            ),
          }
        : undefined,
    generatedAt: "2026-07-05T00:00:00.000Z",
  });
}

function deployedWorkers(prefix: string, includePriceFeed = true) {
  const base = prefix.slice(0, -1);
  return {
    "OpenWorkers#OpenExecutor": `${base}2`,
    "OpenWorkers#OpenDVN": `${base}3`,
    ...(includePriceFeed ? { "OpenWorkers#OpenPriceFeed": `${base}4` } : {}),
  };
}
function deployedTestOFT(prefix: string) {
  const base = prefix.slice(0, -1);
  return {
    "TestOFT#TestOFT": `${base}1`,
  };
}

function externalProfile() {
  const profile = baseProfile();
  profile.mode = "external-oapp";
  const sepolia = profile.chains[0] as Record<string, unknown>;
  const hoodi = profile.chains[1] as Record<string, unknown>;
  sepolia.oapp = "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa";
  hoodi.oapp = "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB";
  delete sepolia.testOFTDeploymentId;
  delete hoodi.testOFTDeploymentId;
  return profile;
}

function baseProfile() {
  return {
    environment: "testnet",
    mode: "test-oft-rehearsal",
    databaseUrl:
      "postgres://laz_worker:laz_worker@localhost:5432/laz_worker?sslmode=disable",
    metricsListenAddress: ":9090",
    owner: "0x1111111111111111111111111111111111111111",
    priceFeedSubmitters: ["0x2222222222222222222222222222222222222222"],
    initialRecipient: "0x1111111111111111111111111111111111111111",
    canaryTreasury: "0x1111111111111111111111111111111111111111",
    minOwnerNativeBalanceWei: "10000000000000000",
    minCanaryNativeBalanceWei: "10000000000000000",
    dvnMode: "active",
    services: {
      executor: true,
      dvn: true,
    },
    signers: [
      {
        id: "0x2222222222222222222222222222222222222222",
        type: "keystore",
        keystore: {
          path: "/run/secrets/testnet-worker-keystore.json",
          passwordEnv: "KEYSTORE_PASSWORD",
        },
      },
    ],
    token: {
      name: "Oh My Lazier Test OFT",
      symbol: "OMLTOFT",
    },
    chains: [
      {
        key: "sepolia",
        network: "sepolia",
        name: "ethereum-sepolia",
        eid: 40161,
        chainId: 11155111,
        rpcUrlEnv: "SEPOLIA_RPC_URL",
        deploymentId: "sepolia-open-workers",
        testOFTDeploymentId: "sepolia-test-oft",
        initialSupply: "1000000000000000000000000",
        minCanaryTokenBalance: "1000000000000000",
        confirmations: 12,
        indexerQueryBlockRange: 500,
        indexerPollIntervalSeconds: 5,
        externalDVNs: ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        pricingTxPolicy: {
          maxFeePerGasWei: "100000000000",
          maxPriorityFeePerGasWei: "1000000000",
          minNativeBalanceWei: "100000000000000000",
        },
        txRoles: {
          executor: {
            signer: "0x2222222222222222222222222222222222222222",
            maxFeePerGasWei: "100000000000",
            maxPriorityFeePerGasWei: "1000000000",
            minNativeBalanceWei: "100000000000000000",
          },
          dvn: {
            signer: "0x2222222222222222222222222222222222222222",
            maxFeePerGasWei: "100000000000",
            maxPriorityFeePerGasWei: "1000000000",
            minNativeBalanceWei: "100000000000000000",
          },
        },
      },
      {
        key: "hoodi",
        network: "hoodi",
        name: "hoodi",
        eid: 40449,
        chainId: 560048,
        rpcUrlEnv: "HOODI_RPC_URL",
        deploymentId: "hoodi-open-workers",
        testOFTDeploymentId: "hoodi-test-oft",
        initialSupply: "0",
        minCanaryTokenBalance: "0",
        confirmations: 12,
        indexerQueryBlockRange: 500,
        indexerPollIntervalSeconds: 5,
        externalDVNs: ["0x9999999999999999999999999999999999999999"],
        pricingTxPolicy: {
          maxFeePerGasWei: "100000000000",
          maxPriorityFeePerGasWei: "1000000000",
          minNativeBalanceWei: "100000000000000000",
        },
        txRoles: {
          executor: {
            signer: "0x2222222222222222222222222222222222222222",
            maxFeePerGasWei: "100000000000",
            maxPriorityFeePerGasWei: "1000000000",
            minNativeBalanceWei: "100000000000000000",
          },
          dvn: {
            signer: "0x2222222222222222222222222222222222222222",
            maxFeePerGasWei: "100000000000",
            maxPriorityFeePerGasWei: "1000000000",
            minNativeBalanceWei: "100000000000000000",
          },
        },
      },
    ],
    pathway: {
      maxMessageSize: 10000,
      enforcedLzReceiveGas: "200000",
      minLzReceiveGas: "200000",
      maxLzReceiveGas: "1000000",
      priceSnapshot: {
        dstGasPriceInSrcToken: "1",
        dstDataFeePerByteInSrcToken: "0",
        staleAfter: "1800",
        maxAgeSeconds: "1800",
      },
      executorFee: {
        fixedFeeWei: "0",
        dstGasOverhead: "50000",
        dataSizeOverheadBytes: "0",
        marginBps: 1000,
      },
      dvnFee: {
        fixedFeeWei: "0",
        dstGasOverhead: "150000",
        dataSizeOverheadBytes: "0",
        marginBps: 1000,
      },
    },
  };
}
