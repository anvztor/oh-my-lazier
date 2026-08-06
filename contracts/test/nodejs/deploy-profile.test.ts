import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import {
  CONFIG_TYPE_EXECUTOR,
  CONFIG_TYPE_ULN,
  NIL_DVN_COUNT,
  decodeExecutorConfig,
  decodeUlnConfig,
} from "../../scripts/lz-config.js";
import {
  buildCommandPlan,
  buildDeploymentState,
  deployIgnitionModule,
  extractOpenWorkerContracts,
  isBootstrapStateUnavailable,
  loadDeploymentState,
  MissingDeploymentStateError,
  normalizeProfile,
  oappEndpointParameterFile,
  openWorkersParameterFile,
  openWorkersPathwayParameterFile,
  renderWorkerConfig,
  resolveProfileNetworks,
  resolveWorkerStartBlocks,
  runDeployProfile,
  runConfigureOApp,
  runConfigureWorkers,
  runDeployTestOFT,
  runDeployWorkers,
  shouldRunConfigureOApp,
  shouldRunWorkerOnlyVerify,
  testOFTParameterFile,
  type DeploymentProfile,
  type IgnitionContractDeployment,
  type ProgrammaticIgnitionDeployInput,
} from "../../scripts/deploy-profile.js";
import type { IgnitionDeploymentState } from "../../scripts/ignition-deployment-state.js";
import { parseDeployProfileInput } from "../../scripts/commands/deploy-profile-input.js";

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
    "0x6EDCE65403992e310A62460808c4b910D972f10f"
  );
  assert.equal(profile.chains[1].eid, 40449);
  assert.equal(profile.chains[1].nativeAssetId, "eth");
  assert.equal(profile.chains[1].indexerPollIntervalSeconds, 5);
  assert.deepEqual(profile.chains[1].externalDVNs, [
    "0x9999999999999999999999999999999999999999",
  ]);
  assert.equal(profile.chains[1].includeLayerZeroLabsDVN, false);
});

test("normalizeProfile rejects unknown and secret-bearing fields", () => {
  const cases: Array<{
    mutate: (input: ReturnType<typeof baseProfile>) => void;
    message: RegExp;
  }> = [
    {
      mutate: (input) => {
        (input as Record<string, unknown>).ownre = input.owner;
      },
      message: /profile contains unknown field: ownre/,
    },
    {
      mutate: (input) => {
        (input.services as Record<string, unknown>).executer = true;
      },
      message: /profile\.services contains unknown field: executer/,
    },
    {
      mutate: (input) => {
        (input.chains[0] as Record<string, unknown>).confirmaton = 1;
      },
      message: /profile\.chains\[0\] contains unknown field: confirmaton/,
    },
  ];

  for (const item of cases) {
    const input = baseProfile();
    item.mutate(input);
    assert.throws(() => normalizeProfile(input), item.message);
  }

  const secret = baseProfile();
  (secret as Record<string, unknown>).privateKey = "must-not-be-echoed";
  assert.throws(
    () => normalizeProfile(secret),
    (error: unknown) => {
      assert.ok(error instanceof Error);
      assert.match(error.message, /profile\.privateKey is not allowed/);
      assert.doesNotMatch(error.message, /must-not-be-echoed/);
      return true;
    }
  );
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
      /profile\.chains\[0\]\.indexerPollIntervalSeconds must be (?:an integer|a safe integer|>= 1|<= 9223372036)/
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
    /profile\.pricing\.sourceRequestTimeoutSeconds must be <= 9223372036/
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
    /profile\.chains\[0\]\.priceSources\.coinGecko\.maxAgeSeconds must be <= 9223372036/
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
    /layerZero\.layerZeroLabsDVN is not supported/
  );
});

test("normalizeProfile rejects Hardhat network and chain id mismatches", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).chainId = 560048;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.network sepolia uses chainId 11155111, but profile\.chains\[0\]\.chainId is 560048/
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
    /profile\.chains\[0\]\.network sepolia uses eid 40161, but profile\.chains\[0\]\.eid is 40449/
  );
});

test("normalizeProfile rejects unequal chain confirmations", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).confirmations = 6;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains confirmations must match for reciprocal pathways/
  );
});

test("normalizeProfile rejects Hardhat networks without a chain binding", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).network = "goatTestnet";

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.network goatTestnet has no chain_id\/eid binding in hardhatNetworks/
  );
});

test("normalizeProfile validates the bscTestnet chain binding", () => {
  const input = baseProfile();
  const chain = input.chains[0] as Record<string, unknown>;
  chain.network = "bscTestnet";

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.network bscTestnet uses chainId 97, but profile\.chains\[0\]\.chainId is 11155111/
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
    /profile\.chains\[0\]\.includeLayerZeroLabsDVN has no repo-known LayerZero Labs DVN metadata/
  );
});

test("normalizeProfile requires external OApp addresses in external mode", () => {
  const input = externalProfile();
  delete (input.chains[0] as Record<string, unknown>).oapp;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.oapp is required for external-oapp mode/
  );
});

test("normalizeProfile requires long-term price feed submitters", () => {
  const input = baseProfile();
  delete (input as Record<string, unknown>).priceFeedSubmitters;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.priceFeedSubmitters is required/
  );
});

test("normalizeProfile rejects owner as a long-term price feed submitter", () => {
  const input = baseProfile();
  input.priceFeedSubmitters = ["0x1111111111111111111111111111111111111111"];

  assert.throws(
    () => normalizeProfile(input),
    /profile\.priceFeedSubmitters must not include profile\.owner/
  );
});

test("normalizeProfile rejects uppercase native asset ids", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).nativeAssetId = "ETH";

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.nativeAssetId must be lowercase/
  );
});

test("normalizeProfile requires source configuration for cross-asset pricing", () => {
  const input = baseProfile();
  (input.chains[1] as Record<string, unknown>).nativeAssetId = "hoodi-eth";

  assert.throws(
    () => normalizeProfile(input),
    /sepolia\.priceSources is required for cross-asset pricing/
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
        testCase.name
      );
      continue;
    }
    assert.equal(
      normalizeProfile(input).chains[0].priceSources?.uniswap
        ?.twapWindowSeconds,
      testCase.window,
      testCase.name
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
      new RegExp(
        `${testCase.label} must be an absolute HTTPS URL without query or fragment`
      )
    );
    assert.doesNotMatch(
      caught.message,
      /pricing-user|pricing-password|pricing-secret|private-api-key/
    );
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
    /profile\.pricing\.coinGeckoBaseURL must be an absolute HTTPS URL without query or fragment/
  );
  assert.doesNotMatch(
    caught.message,
    /pricing-user|pricing-password|pricing-secret|private-api-key/
  );
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
    /uniswap is required when source is referenced/
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
    /chainlink is configured but not referenced/
  );
});

test("normalizeProfile rejects legacy top-level canary token balance", () => {
  const input = {
    ...baseProfile(),
    minCanaryTokenBalance: "1000000000000000",
  };

  assert.throws(
    () => normalizeProfile(input),
    /profile\.minCanaryTokenBalance is not supported/
  );
});

test("normalizeProfile requires chain canary token balance in rehearsal mode", () => {
  const input = baseProfile();
  delete (input.chains[1] as Record<string, unknown>).minCanaryTokenBalance;

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[1\]\.minCanaryTokenBalance must be a non-empty string/
  );
});

test("normalizeProfile rejects tx roles that do not reference a configured signer", () => {
  const input = baseProfile();
  input.chains[0].txRoles.executor.signer =
    "0x9999999999999999999999999999999999999999";

  assert.throws(
    () => normalizeProfile(input),
    /txRoles\.executor\.signer must reference a configured signer/
  );
});

test("normalizeProfile rejects zero tx role minimum native balance", () => {
  const input = baseProfile();
  input.chains[0].txRoles.executor.minNativeBalanceWei = "0";

  assert.throws(
    () => normalizeProfile(input),
    /txRoles\.executor\.minNativeBalanceWei must be positive/
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
    assert.throws(() => normalizeProfile(input), testCase.error, testCase.name);
  }
});

test("normalizeProfile enforces the OpenPriceFeed staleAfter range", () => {
  const atLimit = baseProfile();
  atLimit.pathway.priceSnapshot.staleAfter = "86400";
  assert.equal(
    normalizeProfile(atLimit).pathway.priceSnapshot.staleAfter,
    "86400"
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
    assert.throws(() => normalizeProfile(input), testCase.error, testCase.name);
  }
});

test("normalizeProfile rejects Hardhat private key env injection", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).privateKeyEnv =
    "SEPOLIA_PRIVATE_KEY";

  assert.throws(
    () => normalizeProfile(input),
    /profile\.chains\[0\]\.privateKeyEnv is not supported/
  );
});

test("normalizeProfile rejects legacy RPC URL environment indirection", () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).rpcUrlEnv = "SEPOLIA_RPC_URL";

  assert.throws(
    () => normalizeProfile(input),
    /chains\[0\]\.rpcUrlEnv is not supported/
  );
});

test("extractOpenWorkerContracts and buildDeploymentState require OpenWorkers price feed", () => {
  const profile = normalizeProfile(baseProfile());
  const incompleteWorkerDeployments = {
    sepolia: deployedWorkers(
      "0x1111111111111111111111111111111111111111",
      false
    ),
    hoodi: deployedWorkers("0x2222222222222222222222222222222222222222", false),
  };
  const testOFTDeployments = {
    sepolia: deployedTestOFT("0x1111111111111111111111111111111111111111"),
    hoodi: deployedTestOFT("0x2222222222222222222222222222222222222222"),
  };

  assert.equal(
    extractOpenWorkerContracts(
      deployedWorkers("0x1111111111111111111111111111111111111111"),
      "sepolia"
    ).openExecutor,
    "0x1111111111111111111111111111111111111112"
  );
  assert.throws(
    () =>
      extractOpenWorkerContracts(
        incompleteWorkerDeployments.sepolia,
        "sepolia"
      ),
    /missing required future OpenWorkers#OpenPriceFeed/
  );
  assert.throws(
    () =>
      buildDeploymentState({
        profile,
        workerDeployments: incompleteWorkerDeployments,
        testOFTDeployments,
      }),
    /missing required future OpenWorkers#OpenPriceFeed/
  );

  const state = buildDeploymentState({
    profile,
    workerDeployments: {
      sepolia: deployedWorkers("0x1111111111111111111111111111111111111111"),
      hoodi: deployedWorkers("0x2222222222222222222222222222222222222222"),
    },
    testOFTDeployments,
    generatedAt: "2026-07-05T00:00:00.000Z",
  });

  assert.equal(
    state.chains[0].workers.priceFeed,
    "0x1111111111111111111111111111111111111114"
  );
  assert.equal(
    state.chains[0].oapp,
    "0x1111111111111111111111111111111111111111"
  );
  assert.equal(
    state.directions[0].receiveLib,
    profile.chains[1].layerZero.receiveUln302
  );
  assert.equal(
    state.directions[1].sourceWorkers.openDVN,
    state.chains[1].workers.openDVN
  );
});

test("buildDeploymentState uses profile OApps in external mode without TestOFT state", () => {
  const profile = normalizeProfile(externalProfile());
  const state = buildDeploymentState({
    profile,
    workerDeployments: {
      sepolia: deployedWorkers("0x1111111111111111111111111111111111111111"),
      hoodi: deployedWorkers("0x2222222222222222222222222222222222222222"),
    },
    generatedAt: "2026-07-05T00:00:00.000Z",
  });

  assert.equal(state.mode, "external-oapp");
  assert.equal(
    state.chains[0].oapp,
    "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"
  );
  assert.equal(
    state.chains[1].oapp,
    "0xbBbBBBBbbBBBbbbBbbBbbbbBBbBbbbbBbBbbBBbB"
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
    ].sort()
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
    ].sort()
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
    /duplicate DVN address/
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
    /source_workers:\n      open_executor: "0x1111111111111111111111111111111111111112"/
  );
  assert.match(
    yaml,
    /destination_workers:\n      open_dvn: "0x2222222222222222222222222222222222222223"/
  );
  assert.match(
    yaml,
    /send_required_dvns:\n      - "0x1111111111111111111111111111111111111113"\n      - "0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa"/
  );
  assert.match(
    yaml,
    /receive_required_dvns:\n      - "0x2222222222222222222222222222222222222223"\n      - "0x9999999999999999999999999999999999999999"/
  );
  assert.match(yaml, /signer: "0x2222222222222222222222222222222222222222"/);
  assert.match(yaml, /pricing:\n  enabled: true/);
  assert.match(
    yaml,
    /signer: "0x2222222222222222222222222222222222222222"\n  interval_seconds: 300/
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
    rpcUrls: {
      sepolia: "https://sepolia.invalid",
      hoodi: "https://hoodi.invalid",
    },
    workerStartBlocks: { sepolia: 1, hoodi: 1 },
  });

  assert.match(yaml, /primary_source: coingecko/);
  assert.match(
    yaml,
    /coingecko:\n        id: ethereum\n        max_age_seconds: 180/
  );
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
    /- eid: 40161\n      tx_policy:\n        max_fee_per_gas_wei: "111"\n        max_priority_fee_per_gas_wei: "11"\n        min_native_balance_wei: "1111"/
  );
  assert.match(
    yaml,
    /- eid: 40449\n      tx_policy:\n        max_fee_per_gas_wei: "222"\n        max_priority_fee_per_gas_wei: "22"\n        min_native_balance_wei: "2222"/
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
    }
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
    }
  );
});

test("resolveProfileNetworks uses named read-only Hardhat connections and closes them", async () => {
  const input = baseProfile();
  (input.chains[0] as Record<string, unknown>).startBlockNumber = 50;
  const profile = normalizeProfile(input);
  const createCalls: unknown[] = [];
  const closed: string[] = [];
  const blockReads: string[] = [];
  const byNetwork = {
    sepolia: {
      chainId: 11155111,
      url: "https://sepolia.example.invalid/rpc",
      blockNumber: 100n,
    },
    hoodi: {
      chainId: 560048,
      url: "https://hoodi.example.invalid/rpc",
      blockNumber: 200n,
    },
  } as const;

  const resolved = await resolveProfileNetworks(
    {
      config: {
        networks: {
          sepolia: {
            type: "http",
            chainId: byNetwork.sepolia.chainId,
            url: byNetwork.sepolia.url,
          },
          hoodi: {
            type: "http",
            chainId: byNetwork.hoodi.chainId,
            url: byNetwork.hoodi.url,
          },
        },
      },
      network: {
        async create(selection: {
          network: keyof typeof byNetwork;
          override: { accounts: string };
        }) {
          createCalls.push(selection);
          const current = byNetwork[selection.network];
          return {
            networkName: selection.network,
            networkConfig: {
              type: "http",
              chainId: current.chainId,
              url: current.url,
            },
            viem: {
              async getPublicClient() {
                return {
                  chain: { id: current.chainId },
                  async getChainId() {
                    return current.chainId;
                  },
                  async getBlockNumber() {
                    blockReads.push(selection.network);
                    return current.blockNumber;
                  },
                };
              },
            },
            async close() {
              closed.push(selection.network);
            },
          };
        },
      },
    } as unknown as HardhatRuntimeEnvironment,
    profile
  );

  assert.deepEqual(createCalls, [
    { network: "sepolia", override: { accounts: "remote" } },
    { network: "hoodi", override: { accounts: "remote" } },
  ]);
  assert.deepEqual(resolved, {
    rpcUrls: {
      sepolia: "https://sepolia.example.invalid/rpc",
      hoodi: "https://hoodi.example.invalid/rpc",
    },
    latestBlockNumbers: { hoodi: 200n },
  });
  assert.deepEqual(blockReads, ["hoodi"]);
  assert.deepEqual(closed, ["sepolia", "hoodi"]);
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

test("loadDeploymentState reads required Futures through the Ignition state adapter", async () => {
  const profile = normalizeProfile(baseProfile());
  const requests: { deploymentId: string; futureIds: string[] }[] = [];
  const runtime = {
    config: { paths: { ignition: path.resolve("tmp/test-ignition") } },
  };

  const state = await loadDeploymentState(
    profile,
    runtime,
    async (receivedRuntime, request) => {
      assert.equal(receivedRuntime, runtime);
      requests.push({
        deploymentId: request.deploymentId,
        futureIds: request.requiredContracts.map(({ futureId }) => futureId),
      });
      const chain = profile.chains.find(
        ({ chainId }) => chainId === request.expectedChainId
      );
      assert.ok(chain);
      const contracts = request.deploymentId.endsWith("-test-oft")
        ? deployedTestOFT(
            chain.key === "sepolia"
              ? "0x1111111111111111111111111111111111111111"
              : "0x2222222222222222222222222222222222222222"
          )
        : deployedWorkers(
            chain.key === "sepolia"
              ? "0x1111111111111111111111111111111111111111"
              : "0x2222222222222222222222222222222222222222"
          );
      return readyDeployment(
        request.deploymentId,
        request.expectedChainId,
        contracts
      );
    }
  );

  assert.deepEqual(requests, [
    {
      deploymentId: "sepolia-open-workers",
      futureIds: [
        "OpenWorkers#OpenPriceFeed",
        "OpenWorkers#OpenDVN",
        "OpenWorkers#OpenExecutor",
      ],
    },
    {
      deploymentId: "sepolia-test-oft",
      futureIds: ["TestOFT#TestOFT"],
    },
    {
      deploymentId: "hoodi-open-workers",
      futureIds: [
        "OpenWorkers#OpenPriceFeed",
        "OpenWorkers#OpenDVN",
        "OpenWorkers#OpenExecutor",
      ],
    },
    {
      deploymentId: "hoodi-test-oft",
      futureIds: ["TestOFT#TestOFT"],
    },
  ]);
  assert.equal(
    state.chains[0].workers.openExecutor,
    "0x1111111111111111111111111111111111111112"
  );
  assert.equal(
    state.chains[1].oapp,
    "0x2222222222222222222222222222222222222221"
  );
});

test("loadDeploymentState treats only a missing Ignition deployment as bootstrap state", async () => {
  const profile = normalizeProfile(baseProfile());
  const missing = {
    kind: "missing",
    deploymentId: profile.chains[0].deploymentId,
    deploymentDir: path.resolve(
      "contracts/ignition/deployments",
      profile.chains[0].deploymentId
    ),
  } as const;

  await assert.rejects(
    loadDeploymentState(
      profile,
      {
        config: { paths: { ignition: path.resolve("contracts/ignition") } },
      },
      async () => missing
    ),
    (error: unknown) => {
      assert.ok(error instanceof MissingDeploymentStateError);
      assert.equal(error.deployment, missing);
      assert.equal(isBootstrapStateUnavailable(error), true);
      return true;
    }
  );
  assert.equal(isBootstrapStateUnavailable(new Error("corrupt state")), false);
});

test("loadDeploymentState does not hide later corrupt state behind a missing deployment", async () => {
  const profile = normalizeProfile(baseProfile());
  const missing = {
    kind: "missing",
    deploymentId: profile.chains[0].deploymentId,
    deploymentDir: path.resolve(
      "contracts/ignition/deployments",
      profile.chains[0].deploymentId
    ),
  } as const;
  const requests: string[] = [];

  await assert.rejects(
    loadDeploymentState(
      profile,
      {
        config: { paths: { ignition: path.resolve("contracts/ignition") } },
      },
      async (_runtime, request) => {
        requests.push(request.deploymentId);
        if (request.deploymentId === profile.chains[0].deploymentId) {
          return missing;
        }
        if (request.deploymentId === profile.chains[1].deploymentId) {
          throw new Error("later deployment is corrupt");
        }
        return readyDeployment(
          request.deploymentId,
          request.expectedChainId,
          deployedTestOFT("0x1111111111111111111111111111111111111111")
        );
      }
    ),
    /later deployment is corrupt/
  );
  assert.deepEqual(requests, [
    profile.chains[0].deploymentId,
    `${profile.chains[0].deploymentId.replace(/-open-workers$/, "")}-test-oft`,
    profile.chains[1].deploymentId,
  ]);
});

test("programmatic deployment stages use fixed modules, absolute parameters, and stable IDs", async () => {
  const profile = normalizeProfile(baseProfile());
  const deployments: ProgrammaticIgnitionDeployInput[] = [];
  const deploy = async (input: ProgrammaticIgnitionDeployInput) => {
    deployments.push(input);
  };
  const outDir = "tmp/deploy-profile";

  await runDeployTestOFT(profile, outDir, deploy);
  await runDeployWorkers(profile, outDir, deploy);
  await runConfigureWorkers(profile, outDir, deploy);
  await runConfigureOApp(profile, outDir, deploy);

  assert.deepEqual(
    deployments.map((deployment) => [
      deployment.network,
      deployment.module.id,
      deployment.deploymentId,
      path.basename(deployment.parametersPath),
    ]),
    [
      ["sepolia", "TestOFT", "sepolia-test-oft", "sepolia.test-oft.json"],
      ["hoodi", "TestOFT", "hoodi-test-oft", "hoodi.test-oft.json"],
      [
        "sepolia",
        "OpenWorkers",
        "sepolia-open-workers",
        "sepolia.open-workers.json",
      ],
      ["hoodi", "OpenWorkers", "hoodi-open-workers", "hoodi.open-workers.json"],
      [
        "sepolia",
        "OpenWorkersPathwayConfig",
        "sepolia-open-workers-sepolia-to-hoodi-open-workers-pathway",
        "sepolia-to-hoodi.open-workers-pathway.json",
      ],
      [
        "hoodi",
        "OpenWorkersPathwayConfig",
        "hoodi-open-workers-hoodi-to-sepolia-open-workers-pathway",
        "hoodi-to-sepolia.open-workers-pathway.json",
      ],
      [
        "sepolia",
        "OAppEndpointConfig",
        "sepolia-open-workers-sepolia-to-hoodi-oapp-endpoint",
        "sepolia-to-hoodi.oapp-endpoint.json",
      ],
      [
        "hoodi",
        "OAppEndpointConfig",
        "hoodi-open-workers-hoodi-to-sepolia-oapp-endpoint",
        "hoodi-to-sepolia.oapp-endpoint.json",
      ],
    ]
  );
  for (const deployment of deployments) {
    assert.equal(path.isAbsolute(deployment.parametersPath), true);
    assert.equal(deployment.expectedSigner, profile.owner);
  }
});

test("deployIgnitionModule passes reviewed parameters and closes its connection", async () => {
  const profile = normalizeProfile(baseProfile());
  let input: ProgrammaticIgnitionDeployInput | undefined;
  await runDeployWorkers(profile, "tmp/deploy-profile", async (deployment) => {
    input ??= deployment;
  });
  assert.ok(input);
  const deploymentInput = input;

  const calls: unknown[] = [];
  await deployIgnitionModule(deploymentInput, async (network) => ({
    chainId: deploymentInput.chainId,
    signerAddress: deploymentInput.expectedSigner,
    async deploy(module, options) {
      calls.push({ network, moduleId: module.id, options });
    },
    async close() {
      calls.push("close");
    },
  }));

  assert.deepEqual(calls, [
    {
      network: "sepolia",
      moduleId: "OpenWorkers",
      options: {
        parameters: path.resolve(
          "tmp/deploy-profile/ignition/parameters/sepolia.open-workers.json"
        ),
        deploymentId: "sepolia-open-workers",
        displayUi: true,
      },
    },
    "close",
  ]);

  let closed = false;
  await assert.rejects(
    deployIgnitionModule(deploymentInput, async () => ({
      chainId: deploymentInput.chainId,
      signerAddress: deploymentInput.expectedSigner,
      async deploy() {
        throw new Error("deploy failed");
      },
      async close() {
        closed = true;
      },
    })),
    /deploy failed/
  );
  assert.equal(closed, true);
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
  assert.match(commandText, /OML_SCRIPT_PARAMS=/);
  assert.match(commandText, /--build-profile 'production'/);
  assert.doesNotMatch(commandText, /--verify/);
  assert.doesNotMatch(commandText, /HARDHAT_IGNITION_CONFIRM/);
  assert.doesNotMatch(commandText, /RPC_URL/);
  assert.equal(shouldRunConfigureOApp(profile, "all", true), false);
  assert.equal(shouldRunConfigureOApp(profile, "configure-oapp", true), true);
  assert.equal(shouldRunWorkerOnlyVerify(profile, "all"), true);
  assert.equal(shouldRunWorkerOnlyVerify(profile, "verify"), false);
});

test("command plan uses generated JSON inputs and the selected Hardhat build profile", () => {
  const profile = normalizeProfile(baseProfile());
  const plan = buildCommandPlan({
    profile,
    outDir: "tmp/deploy-profile",
    buildProfile: "production",
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
    /^OML_SCRIPT_PARAMS='tmp\/deploy-profile\/commands\/sepolia\.deploy-test-oft\.json' npm run deploy:test-oft -- --network 'sepolia'$/
  );
  for (const command of mutatingCommands) {
    assert.match(command, /^OML_SCRIPT_PARAMS=/);
    assert.doesNotMatch(command, /--build-profile/);
    assert.doesNotMatch(command, /--parameters|--deployment-id|--verify/);
  }
  assert.match(readOnlyCommands, /--build-profile 'production'/);
  assert.doesNotMatch(readOnlyCommands, /--verify/);
  assert.doesNotMatch(readOnlyCommands, /HARDHAT_IGNITION_CONFIRM/);
});

test("deploy profile command input is camelCase and strict", () => {
  assert.deepEqual(
    parseDeployProfileInput(
      {
        profilePath: "config/deployments/testnet.json",
        outDir: "tmp/deploy-profile",
        phase: "verify",
        verifySource: true,
      },
      "input"
    ),
    {
      profilePath: "config/deployments/testnet.json",
      outDir: "tmp/deploy-profile",
      phase: "verify",
      verifySource: true,
    }
  );
  assert.throws(
    () =>
      parseDeployProfileInput(
        {
          profilePath: "profile.json",
          outDir: "tmp/deploy-profile",
          phase: "render",
          buildProfile: "production",
        },
        "input"
      ),
    /unknown field: buildProfile/
  );
  assert.throws(
    () =>
      parseDeployProfileInput(
        {
          profilePath: "profile.json",
          outDir: "tmp/deploy-profile",
          phase: "invalid",
        },
        "input"
      ),
    /input\.phase must be one of/
  );
});

test("runDeployProfile confirms once before programmatic deployment and writes command envelopes", async (t) => {
  const tempDir = await mkdtemp(path.join(tmpdir(), "oml-deploy-profile-"));
  t.after(() => rm(tempDir, { recursive: true, force: true }));
  const profilePath = path.join(tempDir, "profile.json");
  const outDir = path.join(tempDir, "out");
  await writeFile(profilePath, JSON.stringify(baseProfile()));

  const events: string[] = [];
  const summaries: unknown[] = [];
  const result = await runDeployProfile(
    { profilePath, outDir, phase: "deploy-workers" },
    {
      globalOptions: { buildProfile: "production" },
    } as unknown as HardhatRuntimeEnvironment,
    {
      shouldApply: true,
      async authorize(summary) {
        summaries.push(summary);
        events.push("authorize");
        return true;
      },
    },
    {
      async build(_hre, buildProfile) {
        assert.equal(buildProfile, "production");
        events.push("build");
      },
      async deploy(deployment) {
        events.push(`deploy:${deployment.network}:${deployment.deploymentId}`);
      },
    }
  );

  assert.equal(summaries.length, 1);
  assert.deepEqual(events, [
    "authorize",
    "build",
    "deploy:sepolia:sepolia-open-workers",
    "deploy:hoodi:hoodi-open-workers",
  ]);
  assert.equal(result.applied, true);
  assert.equal(result.outDir, outDir);

  const commandFile = JSON.parse(
    await readFile(
      path.join(outDir, "commands", "sepolia.deploy-open-workers.json"),
      "utf8"
    )
  ) as {
    input: {
      parameters: string;
      deploymentId: string;
      expectedSigner: string;
    };
    apply: boolean;
    confirmation: string;
  };
  assert.equal(path.isAbsolute(commandFile.input.parameters), true);
  assert.equal(commandFile.input.deploymentId, "sepolia-open-workers");
  assert.equal(commandFile.input.expectedSigner, baseProfile().owner);
  assert.equal(commandFile.apply, false);
  assert.equal(commandFile.confirmation, "interactive");
});

test("runDeployProfile dry-run never builds or deploys", async (t) => {
  const tempDir = await mkdtemp(path.join(tmpdir(), "oml-deploy-profile-"));
  t.after(() => rm(tempDir, { recursive: true, force: true }));
  const profilePath = path.join(tempDir, "profile.json");
  await writeFile(profilePath, JSON.stringify(baseProfile()));

  const result = await runDeployProfile(
    {
      profilePath,
      outDir: path.join(tempDir, "out"),
      phase: "deploy-workers",
    },
    {
      globalOptions: { buildProfile: "production" },
    } as unknown as HardhatRuntimeEnvironment,
    {
      shouldApply: false,
      async authorize() {
        throw new Error("dry-run must not authorize");
      },
    },
    {
      async build() {
        throw new Error("dry-run must not build");
      },
      async deploy() {
        throw new Error("dry-run must not deploy");
      },
    }
  );

  assert.equal(result.applied, false);
});

test("runDeployProfile builds before verification and runs source verification last", async (t) => {
  const tempDir = await mkdtemp(path.join(tmpdir(), "oml-deploy-profile-"));
  t.after(() => rm(tempDir, { recursive: true, force: true }));
  const profilePath = path.join(tempDir, "profile.json");
  await writeFile(profilePath, JSON.stringify(baseProfile()));
  const events: string[] = [];

  const result = await runDeployProfile(
    {
      profilePath,
      outDir: path.join(tempDir, "out"),
      phase: "verify",
      verifySource: true,
    },
    {
      globalOptions: { buildProfile: "production" },
    } as unknown as HardhatRuntimeEnvironment,
    {
      shouldApply: false,
      async authorize() {
        throw new Error("verification must not authorize writes");
      },
    },
    {
      async build() {
        events.push("build");
      },
      readState: readyProfileState,
      async resolveNetworks() {
        return {
          rpcUrls: {
            sepolia: "https://sepolia.example.invalid",
            hoodi: "https://hoodi.example.invalid",
          },
          latestBlockNumbers: { sepolia: 100n, hoodi: 200n },
        };
      },
      async verify() {
        events.push("verify");
      },
      async verifySource() {
        events.push("source");
      },
    }
  );

  assert.deepEqual(events, ["build", "verify", "source"]);
  assert.equal(result.applied, false);
  assert.equal(result.deploymentState, true);
});

test("runDeployProfile propagates verification failure before source verification", async (t) => {
  const tempDir = await mkdtemp(path.join(tmpdir(), "oml-deploy-profile-"));
  t.after(() => rm(tempDir, { recursive: true, force: true }));
  const profilePath = path.join(tempDir, "profile.json");
  await writeFile(profilePath, JSON.stringify(baseProfile()));
  let sourceVerificationRan = false;

  await assert.rejects(
    runDeployProfile(
      {
        profilePath,
        outDir: path.join(tempDir, "out"),
        phase: "verify",
        verifySource: true,
      },
      {
        globalOptions: { buildProfile: "production" },
      } as unknown as HardhatRuntimeEnvironment,
      {
        shouldApply: false,
        async authorize() {
          throw new Error("verification must not authorize writes");
        },
      },
      {
        async build() {},
        readState: readyProfileState,
        async resolveNetworks() {
          return {
            rpcUrls: {
              sepolia: "https://sepolia.example.invalid",
              hoodi: "https://hoodi.example.invalid",
            },
            latestBlockNumbers: { sepolia: 100n, hoodi: 200n },
          };
        },
        async verify() {
          throw new Error("runtime verification failed");
        },
        async verifySource() {
          sourceVerificationRan = true;
        },
      }
    ),
    /runtime verification failed/
  );
  assert.equal(sourceVerificationRan, false);
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
    /required DVNs must include at least two addresses/
  );
});

function stateWithPriceFeeds(profile: DeploymentProfile) {
  return buildDeploymentState({
    profile,
    workerDeployments: {
      sepolia: deployedWorkers("0x1111111111111111111111111111111111111111"),
      hoodi: deployedWorkers("0x2222222222222222222222222222222222222222"),
    },
    testOFTDeployments:
      profile.mode === "test-oft-rehearsal"
        ? {
            sepolia: deployedTestOFT(
              "0x1111111111111111111111111111111111111111"
            ),
            hoodi: deployedTestOFT(
              "0x2222222222222222222222222222222222222222"
            ),
          }
        : undefined,
    generatedAt: "2026-07-05T00:00:00.000Z",
  });
}

function deployedWorkers(
  prefix: string,
  includePriceFeed = true
): IgnitionContractDeployment {
  const base = prefix.slice(0, -1);
  return {
    contracts: {
      "OpenWorkers#OpenExecutor": {
        address: `${base}2` as `0x${string}`,
        contractName: "OpenExecutor",
      },
      "OpenWorkers#OpenDVN": {
        address: `${base}3` as `0x${string}`,
        contractName: "OpenDVN",
      },
      ...(includePriceFeed
        ? {
            "OpenWorkers#OpenPriceFeed": {
              address: `${base}4` as `0x${string}`,
              contractName: "OpenPriceFeed",
            },
          }
        : {}),
    },
  };
}
function deployedTestOFT(prefix: string): IgnitionContractDeployment {
  const base = prefix.slice(0, -1);
  return {
    contracts: {
      "TestOFT#TestOFT": {
        address: `${base}1` as `0x${string}`,
        contractName: "TestOFT",
      },
    },
  };
}

function readyDeployment(
  deploymentId: string,
  chainId: number,
  deployment: IgnitionContractDeployment
): IgnitionDeploymentState {
  return {
    kind: "ready",
    deploymentId,
    deploymentDir: path.resolve(
      "contracts/ignition/deployments",
      deploymentId
    ),
    chainId,
    contracts: Object.fromEntries(
      Object.entries(deployment.contracts).map(
        ([futureId, { address, contractName }]) => [
          futureId,
          {
            futureId,
            address,
            contractName,
            sourceName: `test/${contractName}.sol`,
          },
        ]
      )
    ),
  };
}

async function readyProfileState(
  _runtime: unknown,
  request: {
    deploymentId: string;
    expectedChainId: number;
    requiredContracts: readonly { futureId: string }[];
  }
): Promise<IgnitionDeploymentState> {
  const prefix =
    request.expectedChainId === 11155111
      ? "0x1111111111111111111111111111111111111111"
      : "0x2222222222222222222222222222222222222222";
  const deployment = request.requiredContracts.some(({ futureId }) =>
    futureId.startsWith("TestOFT#")
  )
    ? deployedTestOFT(prefix)
    : deployedWorkers(prefix);
  return readyDeployment(
    request.deploymentId,
    request.expectedChainId,
    deployment
  );
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
