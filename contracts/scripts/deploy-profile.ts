import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import type { IgnitionModule } from "@nomicfoundation/ignition-core";
import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import { getAddress, isAddress, isAddressEqual, type Address } from "viem";
import OAppEndpointConfigModule from "../ignition/modules/OAppEndpointConfig.js";
import OpenWorkersModule from "../ignition/modules/OpenWorkers.js";
import OpenWorkersPathwayConfigModule from "../ignition/modules/OpenWorkersPathwayConfig.js";
import TestOFTModule from "../ignition/modules/TestOFT.js";
import {
  readIgnitionDeploymentState,
  type IgnitionDeploymentRequest,
  type IgnitionDeploymentState,
  type IgnitionRuntime,
  type MissingIgnitionDeployment,
} from "./ignition-deployment-state.js";
import { verifyIgnitionDeploymentSources } from "./ignition-source-verification.js";
import { withIgnitionUiOnStderr } from "./ignition-ui-output.js";
import {
  type ApplyGate,
  assertExpectedSigner,
  assertNoSecretFields,
  expectOnlyKeys,
  sanitizeCommandErrorMessage,
  withReadOnlyConnection,
  withWriteConnection,
} from "./command-harness.js";
import {
  readDeploymentPreflight,
  validateDeploymentPreflight,
} from "./deployment-preflight.js";
import { inspectLzConfig } from "./inspect-lz-config.js";
import { jsonStringify, loadArtifact } from "./lib.js";
import {
  expectedLayerZeroChains,
  requireLayerZeroLabsDVNForLibraries,
  type ExpectedLayerZeroChain,
} from "./lz-addresses.js";
import {
  buildOAppEndpointConfigParameters,
  buildOpenWorkersParameters,
  buildOpenWorkersPathwayConfigParameters,
  buildTestOFTParameters,
  type PathwayConfigInput,
} from "./oft-pathway-ignition.js";
import {
  readPriceConfigReport,
  validatePriceConfigReport,
} from "./price-config-check.js";

const maxPriceSnapshotStaleAfter = 86_400n;
// Keep generated worker durations within Go's signed 64-bit nanosecond range.
const maxDurationSeconds = 9_223_372_036;
const minUniswapTWAPWindowSeconds = 1_800;
const maxUniswapTWAPWindowSeconds = 0xffff_ffff;

export type DeploymentMode = "test-oft-rehearsal" | "external-oapp";

export type DeploymentPhase =
  | "render"
  | "deploy-test-oft"
  | "deploy-workers"
  | "configure-workers"
  | "configure-oapp"
  | "verify"
  | "all";

export type SignerProfile =
  | {
      id: Address;
      type: "keystore";
      keystore: {
        path: string;
        passwordEnv?: string;
        passwordFile?: string;
      };
    }
  | {
      id: Address;
      type: "kms";
      kms: {
        keyId: string;
        region: string;
        address: Address;
        endpoint?: string;
      };
    };

export type LayerZeroAddresses = Pick<
  ExpectedLayerZeroChain,
  "endpointV2" | "sendUln302" | "receiveUln302"
> & {
  chainKey?: string;
  nativeChainId: number;
  eid: string;
};

export type ChainProfile = {
  key: string;
  network: string;
  name: string;
  nativeAssetId: string;
  pricingTxPolicy: TxPolicyProfile;
  priceSources?: PriceSourcesProfile;
  eid: number;
  chainId: number;
  deploymentId: string;
  testOFTDeploymentId?: string;
  oapp?: Address;
  initialSupply: string;
  minCanaryTokenBalance: string;
  confirmations: number;
  startBlockNumber?: number;
  indexerQueryBlockRange: number;
  indexerPollIntervalSeconds: number;
  externalDVNs: Address[];
  includeLayerZeroLabsDVN: boolean;
  txRoles: {
    executor: TxRoleProfile;
    dvn: TxRoleProfile;
  };
  layerZero: LayerZeroAddresses;
};

export type PriceSourceName =
  | "coinmarketcap"
  | "coingecko"
  | "chainlink"
  | "uniswap";

export type PriceSourcesProfile = {
  primarySource: Exclude<PriceSourceName, "uniswap">;
  sanitySources: PriceSourceName[];
  coinMarketCap?: { id: number; maxAgeSeconds: number };
  coinGecko?: { id: string; maxAgeSeconds: number };
  chainlink?: {
    feedAddress: Address;
    expectedDescription: string;
    maxAgeSeconds: number;
  };
  uniswap?: {
    poolAddress: Address;
    tokenIn: Address;
    tokenOut: Address;
    twapWindowSeconds: number;
    maxBlockAgeSeconds: number;
    minHarmonicMeanLiquidity: string;
  };
};

export type PricingProfile = {
  sourceRequestTimeoutSeconds: number;
  maxDeviationBps: number;
  coinMarketCapBaseURL?: string;
  coinMarketCapAPIKeyEnv?: string;
  coinGeckoBaseURL?: string;
  coinGeckoAPIKeyEnv?: string;
};

// Every Hardhat deploy network must be listed with its chain_id/eid binding;
// validateHardhatNetworkChain rejects networks missing from this map instead of
// silently skipping the binding check.
const hardhatNetworks = new Map<string, { chainId: number; eid: number }>([
  ["sepolia", { chainId: 11155111, eid: 40161 }],
  ["hoodi", { chainId: 560048, eid: 40449 }],
  ["bscTestnet", { chainId: 97, eid: 40102 }],
  ["local-anvil-a", { chainId: 31337, eid: 90101 }],
  ["local-anvil-b", { chainId: 31338, eid: 90102 }],
]);

export type TxRoleProfile = {
  signer: Address;
} & TxPolicyProfile;

export type TxPolicyProfile = {
  maxFeePerGasWei: string;
  maxPriorityFeePerGasWei: string;
  minNativeBalanceWei: string;
};

export type PathwayProfile = {
  maxMessageSize: number;
  enforcedLzReceiveGas: string;
  minLzReceiveGas: string;
  maxLzReceiveGas: string;
  priceSnapshot: {
    dstGasPriceInSrcToken: string;
    dstDataFeePerByteInSrcToken: string;
    staleAfter: string;
    maxAgeSeconds: string;
  };
  executorFee: WorkerFeeProfile;
  dvnFee: WorkerFeeProfile;
};

export type WorkerFeeProfile = {
  fixedFeeWei: string;
  dstGasOverhead: string;
  dataSizeOverheadBytes: string;
  marginBps: number;
};

export type DeploymentProfile = {
  environment: string;
  mode: DeploymentMode;
  databaseUrl: string;
  metricsListenAddress: string;
  owner: Address;
  priceFeedSubmitters: Address[];
  initialRecipient: Address;
  canaryTreasury?: Address;
  minOwnerNativeBalanceWei: string;
  minCanaryNativeBalanceWei: string;
  dvnMode: "active" | "shadow";
  services: {
    executor: boolean;
    dvn: boolean;
  };
  pricing: PricingProfile;
  signers: SignerProfile[];
  token: {
    name: string;
    symbol: string;
  };
  chains: ChainProfile[];
  pathway: PathwayProfile;
};

export type OpenWorkerContracts = {
  openExecutor: Address;
  openDVN: Address;
  priceFeed: Address;
};

export type IgnitionContractDeployment = {
  contracts: Readonly<
    Record<string, { address: Address; contractName: string }>
  >;
};

export type ChainDeploymentState = {
  key: string;
  network: string;
  name: string;
  eid: number;
  chainId: number;
  endpoint: Address;
  sendUln: Address;
  receiveUln: Address;
  externalDVNs: Address[];
  includeLayerZeroLabsDVN: boolean;
  oapp: Address;
  workers: {
    openExecutor: Address;
    openDVN: Address;
    priceFeed: Address;
  };
};

export type DeploymentDirectionState = {
  key: string;
  source: string;
  destination: string;
  srcEid: number;
  dstEid: number;
  srcOApp: Address;
  dstOApp: Address;
  sendLib: Address;
  receiveLib: Address;
  sourceWorkers: {
    openExecutor: Address;
    openDVN: Address;
    priceFeed: Address;
  };
  destinationWorkers: {
    openDVN: Address;
  };
};

export type DeploymentState = {
  environment: string;
  mode: DeploymentMode;
  generatedAt: string;
  chains: ChainDeploymentState[];
  directions: DeploymentDirectionState[];
};

export type PlannedCommand = {
  label: string;
  command: string;
  mutates: boolean;
  requiresApply: boolean;
  output?: string;
};

export type CommandPlan = {
  applyRequiredForMutations: true;
  commands: PlannedCommand[];
};

export type DeployProfileInput = {
  profilePath: string;
  outDir: string;
  phase: DeploymentPhase;
  verifySource?: boolean;
};

type RPCURLMap = Record<string, string>;
type WorkerStartBlockMap = Record<string, number>;
type LatestBlockNumberReader = (
  chain: ChainProfile,
  rpcURL: string
) => Promise<bigint>;

export type ResolvedProfileNetworks = {
  rpcUrls: RPCURLMap;
  latestBlockNumbers: Readonly<Record<string, bigint>>;
};

export type SourceVerificationInput = {
  hre: HardhatRuntimeEnvironment;
  profile: DeploymentProfile;
  state: DeploymentState;
  buildProfile: string;
};

export type DeployProfileDependencies = {
  build?: (
    hre: HardhatRuntimeEnvironment,
    buildProfile: string
  ) => Promise<void>;
  deploy?: ProgrammaticIgnitionDeployer;
  readState?: DeploymentStateReader;
  resolveNetworks?: (
    hre: HardhatRuntimeEnvironment,
    profile: DeploymentProfile
  ) => Promise<ResolvedProfileNetworks>;
  verify?: (
    hre: HardhatRuntimeEnvironment,
    profile: DeploymentProfile,
    state: DeploymentState,
    outDir: string,
    options: { workerOnly: boolean }
  ) => Promise<void>;
  verifySource?: (input: SourceVerificationInput) => Promise<void>;
};

export type DeployProfileResult = {
  ok: true;
  phase: DeploymentPhase;
  applied: boolean;
  mode: DeploymentMode;
  outDir: string;
  parameters: string;
  commands: string;
  workerConfig?: string;
  deploymentState?: boolean;
  status?: string;
};

export function normalizeProfile(value: unknown): DeploymentProfile {
  assertNoSecretFields(value, "profile", [
    "passwordEnv",
    "passwordFile",
    "coinMarketCapAPIKeyEnv",
    "coinGeckoAPIKeyEnv",
    "privateKeyEnv",
    "rpcUrlEnv",
  ]);
  const input = object(value, "profile", [
    "environment",
    "mode",
    "databaseUrl",
    "metricsListenAddress",
    "owner",
    "priceFeedSubmitters",
    "initialRecipient",
    "canaryTreasury",
    "minOwnerNativeBalanceWei",
    "minCanaryNativeBalanceWei",
    "minCanaryTokenBalance",
    "dvnMode",
    "services",
    "pricing",
    "signers",
    "token",
    "chains",
    "pathway",
  ]);
  const mode = normalizeMode(input.mode);
  if (Object.hasOwn(input, "minCanaryTokenBalance")) {
    throw new Error(
      "profile.minCanaryTokenBalance is not supported; configure chains[].minCanaryTokenBalance"
    );
  }
  const owner = addressField(input, "owner", "profile.owner");
  const priceFeedSubmitters = normalizeAddressArrayField(
    input,
    "priceFeedSubmitters",
    "profile.priceFeedSubmitters"
  );
  validateLongTermPriceSubmitters(priceFeedSubmitters, owner);
  const initialRecipient =
    optionalAddressField(
      input,
      "initialRecipient",
      "profile.initialRecipient"
    ) ?? owner;
  const canaryTreasury = optionalAddressField(
    input,
    "canaryTreasury",
    "profile.canaryTreasury"
  );
  const services = object(input.services ?? {}, "profile.services", [
    "executor",
    "dvn",
  ]);
  const signers = arrayField(input, "signers", "profile.signers").map(
    (signer, index) => normalizeSigner(signer, `profile.signers[${index}]`)
  );
  const signerIDs = new Set(signers.map((signer) => signer.id.toLowerCase()));
  const token = object(input.token ?? {}, "profile.token", ["name", "symbol"]);
  const pricing = normalizePricingProfile(input.pricing);
  const chains = arrayField(input, "chains", "profile.chains").map(
    (chain, index) =>
      normalizeChain(chain, `profile.chains[${index}]`, signerIDs, mode)
  );
  validateTwoChainPair(chains);
  validateProfilePriceSources(chains, pricing);
  return {
    environment: stringField(input, "environment", "profile.environment"),
    mode,
    databaseUrl: stringField(input, "databaseUrl", "profile.databaseUrl"),
    metricsListenAddress: stringField(
      input,
      "metricsListenAddress",
      "profile.metricsListenAddress"
    ),
    owner,
    priceFeedSubmitters,
    initialRecipient,
    canaryTreasury,
    minOwnerNativeBalanceWei: optionalDecimalField(
      input,
      "minOwnerNativeBalanceWei",
      "profile.minOwnerNativeBalanceWei",
      "0"
    ),
    minCanaryNativeBalanceWei: optionalDecimalField(
      input,
      "minCanaryNativeBalanceWei",
      "profile.minCanaryNativeBalanceWei",
      "0"
    ),
    dvnMode: normalizeDVNMode(input.dvnMode),
    services: {
      executor: optionalBoolean(
        services.executor,
        true,
        "profile.services.executor"
      ),
      dvn: optionalBoolean(services.dvn, true, "profile.services.dvn"),
    },
    pricing,
    signers,
    token: {
      name: optionalString(
        token.name,
        "Oh My Lazier Test OFT",
        "profile.token.name"
      ),
      symbol: optionalString(token.symbol, "OMLTOFT", "profile.token.symbol"),
    },
    chains,
    pathway: normalizePathway(input.pathway, "profile.pathway"),
  };
}

export function extractOpenWorkerContracts(
  deployment: IgnitionContractDeployment,
  chainKey: string
): OpenWorkerContracts {
  const openExecutor = deploymentContractAddress(
    deployment,
    "OpenWorkers#OpenExecutor",
    "OpenExecutor",
    chainKey
  );
  const openDVN = deploymentContractAddress(
    deployment,
    "OpenWorkers#OpenDVN",
    "OpenDVN",
    chainKey
  );
  const priceFeed = deploymentContractAddress(
    deployment,
    "OpenWorkers#OpenPriceFeed",
    "OpenPriceFeed",
    chainKey
  );
  return { openExecutor, openDVN, priceFeed };
}

export function extractTestOFTAddress(
  deployment: IgnitionContractDeployment,
  chainKey: string
): Address {
  return deploymentContractAddress(
    deployment,
    "TestOFT#TestOFT",
    "TestOFT",
    chainKey
  );
}

export function buildDeploymentState(input: {
  profile: DeploymentProfile;
  workerDeployments: Record<string, IgnitionContractDeployment>;
  testOFTDeployments?: Record<string, IgnitionContractDeployment>;
  generatedAt?: string;
}): DeploymentState {
  const chains = input.profile.chains.map((chain) => {
    const workers = extractOpenWorkerContracts(
      input.workerDeployments[chain.key],
      chain.key
    );
    const oapp =
      input.profile.mode === "external-oapp"
        ? requiredOApp(chain)
        : extractTestOFTAddress(
            input.testOFTDeployments?.[chain.key] ??
              missingTestOFTDeployment(chain.key),
            chain.key
          );
    return {
      key: chain.key,
      network: chain.network,
      name: chain.name,
      eid: chain.eid,
      chainId: chain.chainId,
      endpoint: chain.layerZero.endpointV2,
      sendUln: chain.layerZero.sendUln302,
      receiveUln: chain.layerZero.receiveUln302,
      externalDVNs: chain.externalDVNs,
      includeLayerZeroLabsDVN: chain.includeLayerZeroLabsDVN,
      oapp,
      workers: {
        openExecutor: workers.openExecutor,
        openDVN: workers.openDVN,
        priceFeed: workers.priceFeed,
      },
    };
  });
  const directions = deploymentDirections(chains);
  return {
    environment: input.profile.environment,
    mode: input.profile.mode,
    generatedAt: input.generatedAt ?? new Date().toISOString(),
    chains,
    directions,
  };
}

export function openWorkersParameterFile(
  profile: DeploymentProfile,
  _chain: ChainProfile
) {
  return buildOpenWorkersParameters({
    owner: profile.owner,
    priceFeedSubmitters: priceFeedDeploymentSubmitters(profile),
  });
}

export function testOFTParameterFile(
  profile: DeploymentProfile,
  chain: ChainProfile
) {
  return buildTestOFTParameters({
    tokenName: profile.token.name,
    tokenSymbol: profile.token.symbol,
    endpoint: chain.layerZero.endpointV2,
    delegate: profile.owner,
    initialRecipient: profile.initialRecipient,
    initialSupply: chain.initialSupply,
  });
}

function requiredDVNsForPathway(
  source: ChainProfile,
  sourceState: ChainDeploymentState
): Address[] {
  return requiredDVNSetForChain(source, sourceState.workers.openDVN);
}

// The chain's ULN required set: its own OpenDVN plus every approved external DVN.
// The same set is applied to the chain's send and receive ULN configs and must
// match the worker config's send/receive_required_dvns pins exactly.
function requiredDVNSetForChain(chain: ChainProfile, openDVN: Address): Address[] {
  const dvns = [openDVN, ...chain.externalDVNs];
  if (chain.includeLayerZeroLabsDVN) {
    dvns.push(
      requireLayerZeroLabsDVNForLibraries(
        chain.layerZero,
        `${chain.key}.includeLayerZeroLabsDVN`
      )
    );
  }
  return dvns;
}

function renderAddressList(field: string, addresses: readonly Address[]): string {
  const lines = addresses.map((address) => `      - "${address}"`);
  return [`    ${field}:`, ...lines].join("\n");
}

function profileChainByKey(
  profile: DeploymentProfile,
  key: string
): ChainProfile {
  const chain = profile.chains.find((candidate) => candidate.key === key);
  if (chain === undefined) {
    throw new Error(`profile is missing chain ${key}`);
  }
  return chain;
}

export function pathwayInput(input: {
  profile: DeploymentProfile;
  state: DeploymentState;
  source: ChainProfile;
  destination: ChainProfile;
  priceSnapshotUpdatedAt?: bigint;
}): PathwayConfigInput {
  const sourceState = chainState(input.state, input.source.key);
  const destinationState = chainState(input.state, input.destination.key);
  return {
    oapp: sourceState.oapp,
    endpoint: sourceState.endpoint,
    delegate: input.profile.owner,
    remoteEid: destinationState.eid,
    remoteOApp: destinationState.oapp,
    sendUln: sourceState.sendUln,
    receiveUln: sourceState.receiveUln,
    openExecutor: sourceState.workers.openExecutor,
    openDVN: sourceState.workers.openDVN,
    priceFeed: sourceState.workers.priceFeed,
    bootstrapPriceSubmitter: input.profile.owner,
    requiredDVNs: requiredDVNsForPathway(input.source, sourceState),
    confirmations: BigInt(input.source.confirmations),
    maxMessageSize: input.profile.pathway.maxMessageSize,
    minLzReceiveGas: BigInt(input.profile.pathway.minLzReceiveGas),
    maxLzReceiveGas: BigInt(input.profile.pathway.maxLzReceiveGas),
    priceSnapshot: {
      dstGasPriceInSrcToken: BigInt(
        input.profile.pathway.priceSnapshot.dstGasPriceInSrcToken
      ),
      dstDataFeePerByteInSrcToken: BigInt(
        input.profile.pathway.priceSnapshot.dstDataFeePerByteInSrcToken
      ),
      updatedAt:
        input.priceSnapshotUpdatedAt ?? BigInt(Math.floor(Date.now() / 1000)),
      staleAfter: BigInt(input.profile.pathway.priceSnapshot.staleAfter),
    },
    executorFeeModel: feeModelInput(input.profile.pathway.executorFee),
    dvnFeeModel: feeModelInput(input.profile.pathway.dvnFee),
    dvnVerifier: input.source.txRoles.dvn.signer,
    enforcedLzReceiveGas: BigInt(input.profile.pathway.enforcedLzReceiveGas),
  };
}

export function oappEndpointParameterFile(input: {
  profile: DeploymentProfile;
  state: DeploymentState;
  source: ChainProfile;
  destination: ChainProfile;
  priceSnapshotUpdatedAt?: bigint;
}) {
  return buildOAppEndpointConfigParameters(pathwayInput(input));
}

export function openWorkersPathwayParameterFile(input: {
  profile: DeploymentProfile;
  state: DeploymentState;
  source: ChainProfile;
  destination: ChainProfile;
  priceSnapshotUpdatedAt?: bigint;
}) {
  return buildOpenWorkersPathwayConfigParameters(pathwayInput(input));
}

export function renderWorkerConfig(input: {
  profile: DeploymentProfile;
  state: DeploymentState;
  rpcUrls: RPCURLMap;
  workerStartBlocks: WorkerStartBlockMap;
}): string {
  const signers = input.profile.signers.map(renderSigner).join("\n");
  const chainBlocks = input.profile.chains
    .map((chain) =>
      renderWorkerChain(
        chain,
        input.rpcUrls[chain.key],
        workerStartBlock(input.workerStartBlocks, chain)
      )
    )
    .join("\n");
  const pathwayBlocks = input.state.directions
    .map((direction) => renderWorkerPathway(input.profile, direction))
    .join("\n");
  return `database_url: ${yamlString(input.profile.databaseUrl)}
metrics:
  listen_address: ${yamlString(input.profile.metricsListenAddress)}
services:
  executor:
    enabled: ${input.profile.services.executor}
  dvn:
    enabled: ${input.profile.services.dvn}
signers:
${signers}
${renderPricingConfig(input.profile)}
chains:
${chainBlocks}
pathways:
${pathwayBlocks}
`;
}

function renderPricingConfig(profile: DeploymentProfile): string {
  const signer = pricingSigner(profile);
  const pricingChains = profile.chains.map(renderPricingChain).join("\n");
  return `pricing:
  enabled: true
  signer: "${signer}"
  interval_seconds: 300
  stale_after_seconds: 1800
  max_deviation_bps: ${profile.pricing.maxDeviationBps}
  min_update_deviation_bps: 50
  heartbeat_seconds: 900
  source_request_timeout_seconds: ${profile.pricing.sourceRequestTimeoutSeconds}
  gas_spike_bps: 1000
${renderOptionalPricingGlobal(profile.pricing)}  chains:
${pricingChains}`;
}

function renderOptionalPricingGlobal(pricing: PricingProfile): string {
  const lines: string[] = [];
  if (pricing.coinMarketCapBaseURL !== undefined) {
    lines.push(
      `  coinmarketcap_base_url: ${yamlString(pricing.coinMarketCapBaseURL)}`
    );
  }
  if (pricing.coinMarketCapAPIKeyEnv !== undefined) {
    lines.push(
      `  coinmarketcap_api_key_env: ${pricing.coinMarketCapAPIKeyEnv}`
    );
  }
  if (pricing.coinGeckoBaseURL !== undefined) {
    lines.push(`  coingecko_base_url: ${yamlString(pricing.coinGeckoBaseURL)}`);
  }
  if (pricing.coinGeckoAPIKeyEnv !== undefined) {
    lines.push(`  coingecko_api_key_env: ${pricing.coinGeckoAPIKeyEnv}`);
  }
  return lines.length === 0 ? "" : `${lines.join("\n")}\n`;
}

function renderPricingChain(chain: ChainProfile): string {
  const lines = [
    `    - eid: ${chain.eid}`,
    "      tx_policy:",
    `        max_fee_per_gas_wei: "${chain.pricingTxPolicy.maxFeePerGasWei}"`,
    `        max_priority_fee_per_gas_wei: "${chain.pricingTxPolicy.maxPriorityFeePerGasWei}"`,
    `        min_native_balance_wei: "${chain.pricingTxPolicy.minNativeBalanceWei}"`,
    `      native_asset_id: ${chain.nativeAssetId}`,
    `      data_fee_per_byte_wei: "0"`,
  ];
  const sources = chain.priceSources;
  if (sources === undefined) {
    return lines.join("\n");
  }
  lines.push(`      primary_source: ${sources.primarySource}`);
  if (sources.sanitySources.length > 0) {
    lines.push("      sanity_sources:");
    for (const source of sources.sanitySources) {
      lines.push(`        - ${source}`);
    }
  }
  if (sources.coinMarketCap !== undefined) {
    lines.push(
      "      coinmarketcap:",
      `        id: ${sources.coinMarketCap.id}`,
      `        max_age_seconds: ${sources.coinMarketCap.maxAgeSeconds}`
    );
  }
  if (sources.coinGecko !== undefined) {
    lines.push(
      "      coingecko:",
      `        id: ${sources.coinGecko.id}`,
      `        max_age_seconds: ${sources.coinGecko.maxAgeSeconds}`
    );
  }
  if (sources.chainlink !== undefined) {
    lines.push(
      "      chainlink:",
      `        feed_address: "${sources.chainlink.feedAddress}"`,
      `        expected_description: ${yamlString(
        sources.chainlink.expectedDescription
      )}`,
      `        max_age_seconds: ${sources.chainlink.maxAgeSeconds}`
    );
  }
  if (sources.uniswap !== undefined) {
    lines.push(
      "      uniswap:",
      `        pool_address: "${sources.uniswap.poolAddress}"`,
      `        token_in: "${sources.uniswap.tokenIn}"`,
      `        token_out: "${sources.uniswap.tokenOut}"`,
      `        twap_window_seconds: ${sources.uniswap.twapWindowSeconds}`,
      `        max_block_age_seconds: ${sources.uniswap.maxBlockAgeSeconds}`,
      `        min_harmonic_mean_liquidity: "${sources.uniswap.minHarmonicMeanLiquidity}"`
    );
  }
  return lines.join("\n");
}

function pricingSigner(profile: DeploymentProfile): Address {
  const localSigners = new Set(
    profile.signers.map((signer) => signer.id.toLowerCase())
  );
  const submitter = profile.priceFeedSubmitters.find((candidate) =>
    localSigners.has(candidate.toLowerCase())
  );
  if (submitter === undefined) {
    throw new Error(
      "profile.priceFeedSubmitters must include a configured signer for worker pricing"
    );
  }
  return submitter;
}

export function buildCommandPlan(input: {
  profile: DeploymentProfile;
  outDir: string;
  buildProfile?: string;
}): CommandPlan {
  const commands: PlannedCommand[] = [];
  const buildProfile = input.buildProfile ?? "production";
  if (input.profile.mode === "test-oft-rehearsal") {
    for (const chain of input.profile.chains) {
      commands.push({
        label: `Deploy ${chain.key} TestOFT rehearsal OApp`,
        command: hardhatCommand(
          chain,
          "deploy:test-oft",
          testOFTCommandInputPath(input.outDir, chain),
          buildProfile
        ),
        mutates: true,
        requiresApply: true,
      });
    }
  }
  for (const chain of input.profile.chains) {
    commands.push({
      label: `Deploy ${chain.key} OpenWorkers`,
      command: hardhatCommand(
        chain,
        "deploy:open-workers",
        openWorkersCommandInputPath(input.outDir, chain),
        buildProfile
      ),
      mutates: true,
      requiresApply: true,
    });
  }
  for (const [source, destination] of profileDirections(input.profile)) {
    commands.push({
      label: `Configure ${source.key} OpenWorkers for ${destination.key}`,
      command: hardhatCommand(
        source,
        "configure:open-workers-pathway",
        openWorkersPathwayCommandInputPath(input.outDir, source, destination),
        buildProfile
      ),
      mutates: true,
      requiresApply: true,
    });
    commands.push({
      label: `Configure ${source.key} OApp/Endpoint for ${destination.key}`,
      command: hardhatCommand(
        source,
        "configure:oapp-endpoint",
        oappEndpointCommandInputPath(input.outDir, source, destination),
        buildProfile
      ),
      mutates: true,
      requiresApply: true,
    });
  }
  commands.push({
    label: "Validate generated worker config against live chains",
    command: `go run ./go/cmd/configcheck -config ${path.join(
      input.outDir,
      "worker.yaml"
    )} -format json`,
    mutates: false,
    requiresApply: false,
    output: path.join(input.outDir, "artifacts", "configcheck.json"),
  });
  if (input.profile.mode === "test-oft-rehearsal") {
    for (const chain of input.profile.chains) {
      commands.push({
        label: `Run deployment preflight for ${chain.key}`,
        command: hardhatCommand(
          chain,
          "check:deployment-preflight",
          deploymentPreflightCommandInputPath(input.outDir, chain),
          buildProfile
        ),
        mutates: false,
        requiresApply: false,
        output: path.join(
          input.outDir,
          "artifacts",
          `deployment-preflight-${chain.key}.json`
        ),
      });
    }
  }
  for (const [source, destination] of profileDirections(input.profile)) {
    const direction = directionKey(source, destination);
    commands.push({
      label: `Inspect LayerZero config for ${direction}`,
      command: hardhatCommand(
        source,
        "inspect:lz-config",
        inspectLzCommandInputPath(input.outDir, source, destination),
        buildProfile
      ),
      mutates: false,
      requiresApply: false,
      output: path.join(
        input.outDir,
        "artifacts",
        `lz-config-${direction}.json`
      ),
    });
    commands.push({
      label: `Check worker price config for ${direction}`,
      command: hardhatCommand(
        source,
        "check:price-config",
        priceConfigCommandInputPath(input.outDir, source, destination),
        buildProfile
      ),
      mutates: false,
      requiresApply: false,
      output: path.join(
        input.outDir,
        "artifacts",
        `price-config-${direction}.json`
      ),
    });
  }
  return { applyRequiredForMutations: true, commands };
}

export async function writeRenderedDeployment(input: {
  profile: DeploymentProfile;
  state: DeploymentState;
  outDir: string;
  rpcUrls: RPCURLMap;
  buildProfile: string;
  priceSnapshotUpdatedAt?: bigint;
  latestBlockNumber: LatestBlockNumberReader;
}): Promise<void> {
  await writeInitialParameterFiles(input.profile, input.outDir);
  await writeInitialCommandFiles(input.profile, input.outDir);
  for (const [source, destination] of profileDirections(input.profile)) {
    await writeJSON(
      openWorkersPathwayParameterPath(input.outDir, source, destination),
      openWorkersPathwayParameterFile({
        profile: input.profile,
        state: input.state,
        source,
        destination,
        priceSnapshotUpdatedAt: input.priceSnapshotUpdatedAt,
      })
    );
    await writeJSON(
      oappEndpointParameterPath(input.outDir, source, destination),
      oappEndpointParameterFile({
        profile: input.profile,
        state: input.state,
        source,
        destination,
        priceSnapshotUpdatedAt: input.priceSnapshotUpdatedAt,
      })
    );
  }
  await writeRenderedCommandFiles(input.profile, input.state, input.outDir);
  await writeJSON(
    path.join(input.outDir, "deployment-state.json"),
    input.state
  );
  const workerStartBlocks = await resolveWorkerStartBlocks({
    profile: input.profile,
    rpcUrls: input.rpcUrls,
    latestBlockNumber: input.latestBlockNumber,
  });
  await writeFile(
    path.join(input.outDir, "worker.yaml"),
    renderWorkerConfig({
      profile: input.profile,
      state: input.state,
      rpcUrls: input.rpcUrls,
      workerStartBlocks,
    })
  );
  const commands = buildCommandPlan({
    profile: input.profile,
    outDir: input.outDir,
    buildProfile: input.buildProfile,
  });
  await writeJSON(path.join(input.outDir, "commands.json"), commands);
  await writeFile(
    path.join(input.outDir, "commands.md"),
    renderCommands(commands)
  );
}

export async function resolveWorkerStartBlocks(input: {
  profile: DeploymentProfile;
  rpcUrls: RPCURLMap;
  latestBlockNumber: LatestBlockNumberReader;
}): Promise<WorkerStartBlockMap> {
  const workerStartBlocks: WorkerStartBlockMap = {};
  for (const chain of input.profile.chains) {
    if (chain.startBlockNumber !== undefined) {
      workerStartBlocks[chain.key] = chain.startBlockNumber;
      continue;
    }
    const rpcURL = input.rpcUrls[chain.key];
    if (rpcURL === undefined) {
      throw new Error(
        `${chain.key} RPC URL is required to resolve start_block_number`
      );
    }
    const latest = await input.latestBlockNumber(chain, rpcURL);
    workerStartBlocks[chain.key] = safeBlockNumber(
      latest,
      `${chain.key} latest block number`
    );
  }
  return workerStartBlocks;
}

function safeBlockNumber(value: bigint, label: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${label} exceeds JavaScript safe integer range`);
  }
  return Number(value);
}

export async function writeInitialParameterFiles(
  profile: DeploymentProfile,
  outDir: string
): Promise<void> {
  await mkdir(path.join(outDir, "ignition", "parameters"), { recursive: true });
  for (const chain of profile.chains) {
    await writeJSON(
      openWorkersParameterPath(outDir, chain),
      openWorkersParameterFile(profile, chain)
    );
    if (profile.mode === "test-oft-rehearsal") {
      await writeJSON(
        testOFTParameterPath(outDir, chain),
        testOFTParameterFile(profile, chain)
      );
    }
  }
}

export async function writeInitialCommandFiles(
  profile: DeploymentProfile,
  outDir: string
): Promise<void> {
  for (const chain of profile.chains) {
    await writeJSON(openWorkersCommandInputPath(outDir, chain), {
      input: {
        parameters: path.resolve(openWorkersParameterPath(outDir, chain)),
        deploymentId: chain.deploymentId,
        expectedSigner: profile.owner,
      },
      apply: false,
      confirmation: "interactive",
    });
    if (profile.mode === "test-oft-rehearsal") {
      await writeJSON(testOFTCommandInputPath(outDir, chain), {
        input: {
          parameters: path.resolve(testOFTParameterPath(outDir, chain)),
          deploymentId: testOFTDeploymentId(chain),
          expectedSigner: profile.owner,
        },
        apply: false,
        confirmation: "interactive",
      });
    }
  }
}

export async function writeRenderedCommandFiles(
  profile: DeploymentProfile,
  state: DeploymentState,
  outDir: string
): Promise<void> {
  for (const [source, destination] of profileDirections(profile)) {
    const sourceState = chainState(state, source.key);
    await writeJSON(
      openWorkersPathwayCommandInputPath(outDir, source, destination),
      {
        input: {
          parameters: path.resolve(
            openWorkersPathwayParameterPath(outDir, source, destination)
          ),
          deploymentId: openWorkersPathwayDeploymentId(source, destination),
          expectedSigner: profile.owner,
        },
        apply: false,
        confirmation: "interactive",
      }
    );
    await writeJSON(oappEndpointCommandInputPath(outDir, source, destination), {
      input: {
        parameters: path.resolve(
          oappEndpointParameterPath(outDir, source, destination)
        ),
        deploymentId: oappEndpointDeploymentId(source, destination),
        expectedSigner: profile.owner,
      },
      apply: false,
      confirmation: "interactive",
    });
    await writeJSON(inspectLzCommandInputPath(outDir, source, destination), {
      input: {
        endpoint: sourceState.endpoint,
        oapp: sourceState.oapp,
        remoteEid: String(destination.eid),
        sendUln: sourceState.sendUln,
        receiveUln: sourceState.receiveUln,
      },
    });
    await writeJSON(priceConfigCommandInputPath(outDir, source, destination), {
      input: {
        dstEid: String(destination.eid),
        maxPriceAgeSeconds: profile.pathway.priceSnapshot.maxAgeSeconds,
        expectedStaleAfter: profile.pathway.priceSnapshot.staleAfter,
        priceFeed: sourceState.workers.priceFeed,
        openExecutor: sourceState.workers.openExecutor,
        openDVN: sourceState.workers.openDVN,
      },
    });
  }

  if (profile.mode === "test-oft-rehearsal") {
    for (const chain of profile.chains) {
      const current = chainState(state, chain.key);
      await writeJSON(deploymentPreflightCommandInputPath(outDir, chain), {
        input: {
          testOFT: current.oapp,
          openExecutor: current.workers.openExecutor,
          openDVN: current.workers.openDVN,
          expectedOwner: profile.owner,
          minOwnerNativeBalance: profile.minOwnerNativeBalanceWei,
          ...(profile.canaryTreasury === undefined
            ? {}
            : {
                canaryTreasury: profile.canaryTreasury,
                minCanaryNativeBalance: profile.minCanaryNativeBalanceWei,
                minCanaryTokenBalance: chain.minCanaryTokenBalance,
              }),
          expectedTestOFTTotalSupply: chain.initialSupply,
        },
      });
    }
  }
}

export async function runDeployProfile(
  input: DeployProfileInput,
  hre: HardhatRuntimeEnvironment,
  gate: Pick<ApplyGate, "shouldApply" | "authorize">,
  dependencies: DeployProfileDependencies = {}
): Promise<DeployProfileResult> {
  const phase = normalizePhase(input.phase);
  if (input.verifySource === true && phase !== "verify" && phase !== "all") {
    throw new Error("input.verifySource requires phase verify or all");
  }
  const profilePath = path.resolve(input.profilePath);
  const outDir = path.resolve(input.outDir);
  const buildProfile = hre.globalOptions.buildProfile ?? "production";
  let profileInput: unknown;
  try {
    profileInput = JSON.parse(await readFile(profilePath, "utf8")) as unknown;
  } catch {
    throw new Error(`deployment profile contains invalid JSON: ${profilePath}`);
  }
  const profile = normalizeProfile(profileInput);

  await writeInitialParameterFiles(profile, outDir);
  await writeInitialCommandFiles(profile, outDir);
  const commands = buildCommandPlan({ profile, outDir, buildProfile });
  await writeJSON(path.join(outDir, "commands.json"), commands);
  await writeFile(path.join(outDir, "commands.md"), renderCommands(commands));

  const mutates = phaseMutates(phase);
  const applied = gate.shouldApply && mutates;
  if (applied) {
    await gate.authorize(deployProfileApplySummary(profile, phase));
  }
  if (applied || phase === "verify") {
    await (dependencies.build ?? buildDeployProfile)(hre, buildProfile);
  }
  const deploy = dependencies.deploy ?? createProgrammaticIgnitionDeployer(hre);

  if (phase === "deploy-test-oft") {
    requireRehearsalMode(profile, phase);
    if (applied) {
      await runDeployTestOFT(profile, outDir, deploy);
    }
    return deploymentSummary(phase, applied, outDir, profile);
  }

  if (phase === "deploy-workers") {
    if (applied) {
      await runDeployWorkers(profile, outDir, deploy);
    }
    return deploymentSummary(phase, applied, outDir, profile);
  }

  if (phase === "all" && applied) {
    if (profile.mode === "test-oft-rehearsal") {
      await runDeployTestOFT(profile, outDir, deploy);
    }
    await runDeployWorkers(profile, outDir, deploy);
  }

  let state: DeploymentState;
  try {
    state = await loadDeploymentState(
      profile,
      hre,
      dependencies.readState ?? readIgnitionDeploymentState
    );
  } catch (error) {
    if (phase === "render" && isBootstrapStateUnavailable(error)) {
      await writeBootstrapRenderStatus(outDir, error);
      return deploymentSummary(phase, false, outDir, profile, {
        deploymentState: false,
      });
    }
    throw error;
  }
  const networks = await (
    dependencies.resolveNetworks ?? resolveProfileNetworks
  )(hre, profile);
  await writeRenderedDeployment({
    profile,
    state,
    outDir,
    rpcUrls: networks.rpcUrls,
    buildProfile,
    latestBlockNumber: async (chain) => {
      const latest = networks.latestBlockNumbers[chain.key];
      if (latest === undefined) {
        throw new Error(`${chain.key} latest block number was not resolved`);
      }
      return latest;
    },
  });

  if ((phase === "configure-workers" || phase === "all") && applied) {
    await runConfigureWorkers(profile, outDir, deploy);
  }
  if (shouldRunConfigureOApp(profile, phase, applied)) {
    await runConfigureOApp(profile, outDir, deploy);
  }
  if (phase === "verify" || (phase === "all" && applied)) {
    await (dependencies.verify ?? verifyDeploymentProfile)(
      hre,
      profile,
      state,
      outDir,
      {
        workerOnly: shouldRunWorkerOnlyVerify(profile, phase),
      }
    );
    if (input.verifySource === true) {
      await (dependencies.verifySource ?? verifyProfileSources)({
        hre,
        profile,
        state,
        buildProfile,
      });
    }
  }
  return deploymentSummary(phase, applied, outDir, profile, {
    deploymentState: true,
  });
}

export async function verifyProfileSources(
  input: SourceVerificationInput
): Promise<void> {
  await verifyIgnitionDeploymentSources({
    hre: input.hre,
    buildProfile: input.buildProfile,
    targets: input.profile.chains.flatMap((chain) => [
      {
        network: chain.network,
        deploymentId: chain.deploymentId,
      },
      ...(input.profile.mode === "test-oft-rehearsal"
        ? [
            {
              network: chain.network,
              deploymentId: testOFTDeploymentId(chain),
            },
          ]
        : []),
    ]),
  });
}

export function shouldRunConfigureOApp(
  profile: DeploymentProfile,
  phase: DeploymentPhase,
  apply: boolean
): boolean {
  return (
    apply &&
    (phase === "configure-oapp" ||
      (phase === "all" && profile.mode === "test-oft-rehearsal"))
  );
}

export function shouldRunWorkerOnlyVerify(
  profile: DeploymentProfile,
  phase: DeploymentPhase
): boolean {
  return phase === "all" && profile.mode === "external-oapp";
}

export function isBootstrapStateUnavailable(error: unknown): boolean {
  return error instanceof MissingDeploymentStateError;
}

export type DeploymentStateReader = (
  hre: IgnitionRuntime,
  request: IgnitionDeploymentRequest
) => Promise<IgnitionDeploymentState>;

export class MissingDeploymentStateError extends Error {
  constructor(
    readonly chainKey: string,
    readonly deployment: MissingIgnitionDeployment
  ) {
    super(
      `${chainKey} Ignition deployment "${deployment.deploymentId}" is missing at ${deployment.deploymentDir}`
    );
    this.name = "MissingDeploymentStateError";
  }
}

export async function loadDeploymentState(
  profile: DeploymentProfile,
  hre: IgnitionRuntime,
  readState: DeploymentStateReader = readIgnitionDeploymentState
): Promise<DeploymentState> {
  const workerDeployments: Record<string, IgnitionContractDeployment> = {};
  const testOFTDeployments: Record<string, IgnitionContractDeployment> = {};
  const missingDeployments: Array<{
    chainKey: string;
    deployment: MissingIgnitionDeployment;
  }> = [];
  for (const chain of profile.chains) {
    const workerDeployment = await readState(hre, {
      deploymentId: chain.deploymentId,
      expectedChainId: chain.chainId,
      requiredContracts: [
        {
          futureId: "OpenWorkers#OpenPriceFeed",
          contractName: "OpenPriceFeed",
        },
        { futureId: "OpenWorkers#OpenDVN", contractName: "OpenDVN" },
        {
          futureId: "OpenWorkers#OpenExecutor",
          contractName: "OpenExecutor",
        },
      ],
    });
    if (workerDeployment.kind === "missing") {
      missingDeployments.push({
        chainKey: chain.key,
        deployment: workerDeployment,
      });
    } else {
      workerDeployments[chain.key] = workerDeployment;
    }
    if (profile.mode === "test-oft-rehearsal") {
      const testOFTDeployment = await readState(hre, {
        deploymentId: testOFTDeploymentId(chain),
        expectedChainId: chain.chainId,
        requiredContracts: [
          { futureId: "TestOFT#TestOFT", contractName: "TestOFT" },
        ],
      });
      if (testOFTDeployment.kind === "missing") {
        missingDeployments.push({
          chainKey: chain.key,
          deployment: testOFTDeployment,
        });
      } else {
        testOFTDeployments[chain.key] = testOFTDeployment;
      }
    }
  }
  const firstMissing = missingDeployments[0];
  if (firstMissing !== undefined) {
    throw new MissingDeploymentStateError(
      firstMissing.chainKey,
      firstMissing.deployment
    );
  }
  return buildDeploymentState({
    profile,
    workerDeployments,
    testOFTDeployments:
      profile.mode === "test-oft-rehearsal" ? testOFTDeployments : undefined,
  });
}

export type ProgrammaticIgnitionDeployInput = {
  network: string;
  chainId: number;
  module: IgnitionModule;
  parametersPath: string;
  deploymentId: string;
  expectedSigner: Address;
};

export type ProgrammaticIgnitionDeployOptions = {
  parameters: string;
  deploymentId: string;
  displayUi: true;
};

export type ProgrammaticIgnitionConnection = {
  chainId: number;
  signerAddress: Address;
  deploy(
    module: IgnitionModule,
    options: ProgrammaticIgnitionDeployOptions
  ): Promise<unknown>;
  close(): Promise<void>;
};

export type ProgrammaticIgnitionConnectionFactory = (
  network: string
) => Promise<ProgrammaticIgnitionConnection>;

export type ProgrammaticIgnitionDeployer = (
  input: ProgrammaticIgnitionDeployInput
) => Promise<void>;

export async function deployIgnitionModule(
  input: ProgrammaticIgnitionDeployInput,
  createConnection: ProgrammaticIgnitionConnectionFactory
): Promise<void> {
  const connection = await createConnection(input.network);
  try {
    if (connection.chainId !== input.chainId) {
      throw new Error(
        `${input.network} connection chain id ${connection.chainId} does not match ${input.chainId}`
      );
    }
    assertExpectedSigner(
      connection.signerAddress,
      input.expectedSigner,
      `${input.network} deployment signer`
    );
    await withIgnitionUiOnStderr(() =>
      connection.deploy(input.module, {
        parameters: path.resolve(input.parametersPath),
        deploymentId: input.deploymentId,
        displayUi: true,
      })
    );
  } finally {
    await connection.close();
  }
}

export function createProgrammaticIgnitionDeployer(
  hre: HardhatRuntimeEnvironment
): ProgrammaticIgnitionDeployer {
  return async (input) =>
    withWriteConnection(
      hre,
      { network: input.network, expectedChainId: input.chainId },
      async (context) => {
        assertExpectedSigner(
          context.signerAddress,
          input.expectedSigner,
          `${input.network} deployment signer`
        );
        await withIgnitionUiOnStderr(() =>
          context.connection.ignition.deploy(input.module, {
            parameters: path.resolve(input.parametersPath),
            deploymentId: input.deploymentId,
            displayUi: true,
          })
        );
      }
    );
}

export async function runDeployTestOFT(
  profile: DeploymentProfile,
  outDir: string,
  deploy: ProgrammaticIgnitionDeployer
): Promise<void> {
  requireRehearsalMode(profile, "deploy-test-oft");
  for (const chain of profile.chains) {
    await deploy({
      network: chain.network,
      chainId: chain.chainId,
      module: TestOFTModule,
      parametersPath: path.resolve(testOFTParameterPath(outDir, chain)),
      deploymentId: testOFTDeploymentId(chain),
      expectedSigner: profile.owner,
    });
  }
}

export async function runDeployWorkers(
  profile: DeploymentProfile,
  outDir: string,
  deploy: ProgrammaticIgnitionDeployer
): Promise<void> {
  for (const chain of profile.chains) {
    await deploy({
      network: chain.network,
      chainId: chain.chainId,
      module: OpenWorkersModule,
      parametersPath: path.resolve(openWorkersParameterPath(outDir, chain)),
      deploymentId: chain.deploymentId,
      expectedSigner: profile.owner,
    });
  }
}

export async function runConfigureWorkers(
  profile: DeploymentProfile,
  outDir: string,
  deploy: ProgrammaticIgnitionDeployer
): Promise<void> {
  for (const [source, destination] of profileDirections(profile)) {
    await deploy({
      network: source.network,
      chainId: source.chainId,
      module: OpenWorkersPathwayConfigModule,
      parametersPath: path.resolve(
        openWorkersPathwayParameterPath(outDir, source, destination)
      ),
      deploymentId: openWorkersPathwayDeploymentId(source, destination),
      expectedSigner: profile.owner,
    });
  }
}

export async function runConfigureOApp(
  profile: DeploymentProfile,
  outDir: string,
  deploy: ProgrammaticIgnitionDeployer
): Promise<void> {
  for (const [source, destination] of profileDirections(profile)) {
    await deploy({
      network: source.network,
      chainId: source.chainId,
      module: OAppEndpointConfigModule,
      parametersPath: path.resolve(
        oappEndpointParameterPath(outDir, source, destination)
      ),
      deploymentId: oappEndpointDeploymentId(source, destination),
      expectedSigner: profile.owner,
    });
  }
}

export async function verifyDeploymentProfile(
  hre: HardhatRuntimeEnvironment,
  profile: DeploymentProfile,
  state: DeploymentState,
  outDir: string,
  options: { workerOnly: boolean }
): Promise<void> {
  const artifactDir = path.join(outDir, "artifacts");
  const testOFTArtifact = loadArtifact(
    "contracts/artifacts/contracts/contracts/oft/TestOFT.sol/TestOFT.json"
  );
  const openExecutorArtifact = loadArtifact(
    "contracts/artifacts/contracts/contracts/workers/OpenExecutor.sol/OpenExecutor.json"
  );
  const openDVNArtifact = loadArtifact(
    "contracts/artifacts/contracts/contracts/workers/OpenDVN.sol/OpenDVN.json"
  );
  const priceFeedArtifact = loadArtifact(
    "contracts/artifacts/contracts/contracts/workers/OpenPriceFeed.sol/OpenPriceFeed.json"
  );

  for (const [source, destination] of profileDirections(profile)) {
    const direction = directionKey(source, destination);
    const sourceState = chainState(state, source.key);
    await withReadOnlyConnection(
      hre,
      { network: source.network, expectedChainId: source.chainId },
      async ({ publicClient }) => {
        if (profile.mode === "test-oft-rehearsal") {
          const report = await readDeploymentPreflight({
            publicClient,
            testOFT: sourceState.oapp,
            openExecutor: sourceState.workers.openExecutor,
            openDVN: sourceState.workers.openDVN,
            expectedOwner: profile.owner,
            minOwnerNativeBalance: BigInt(profile.minOwnerNativeBalanceWei),
            canaryTreasury: profile.canaryTreasury,
            minCanaryNativeBalance: BigInt(profile.minCanaryNativeBalanceWei),
            minCanaryTokenBalance: BigInt(source.minCanaryTokenBalance),
            expectedTestOFTTotalSupply: BigInt(source.initialSupply),
            testOFTAbi: testOFTArtifact.abi,
            openExecutorAbi: openExecutorArtifact.abi,
            openDVNAbi: openDVNArtifact.abi,
          });
          const errors = validateDeploymentPreflight(report);
          await writeJSON(
            path.join(artifactDir, `deployment-preflight-${source.key}.json`),
            { ok: errors.length === 0, ...report, errors }
          );
          if (errors.length > 0) {
            throw new Error(
              `deployment preflight failed with ${errors.length} error(s)`
            );
          }
        }

        if (!options.workerOnly) {
          const report = await inspectLzConfig(
            {
              endpoint: sourceState.endpoint,
              oapp: sourceState.oapp,
              remoteEid: destination.eid,
              sendUln: sourceState.sendUln,
              receiveUln: sourceState.receiveUln,
            },
            publicClient
          );
          await writeJSON(
            path.join(artifactDir, `lz-config-${direction}.json`),
            report
          );
        }

        const priceReport = await readPriceConfigReport({
          publicClient,
          dstEid: destination.eid,
          checkedAt: BigInt(Math.floor(Date.now() / 1_000)),
          maxAgeSeconds: BigInt(profile.pathway.priceSnapshot.maxAgeSeconds),
          expectedStaleAfter: BigInt(profile.pathway.priceSnapshot.staleAfter),
          priceFeed: sourceState.workers.priceFeed,
          openExecutor: sourceState.workers.openExecutor,
          openDVN: sourceState.workers.openDVN,
          priceFeedAbi: priceFeedArtifact.abi,
          openExecutorAbi: openExecutorArtifact.abi,
          openDVNAbi: openDVNArtifact.abi,
        });
        const priceErrors = validatePriceConfigReport(priceReport);
        await writeJSON(
          path.join(artifactDir, `price-config-${direction}.json`),
          { ok: priceErrors.length === 0, ...priceReport, errors: priceErrors }
        );
        if (priceErrors.length > 0) {
          throw new Error(
            `price config check failed with ${priceErrors.length} error(s)`
          );
        }
      }
    );
  }
  if (!options.workerOnly) {
    runCommand({
      label: "configcheck",
      command: "go",
      args: [
        "run",
        "./go/cmd/configcheck",
        "-config",
        path.join(outDir, "worker.yaml"),
        "-format",
        "json",
      ],
      outputPath: path.join(artifactDir, "configcheck.json"),
    });
  }
}

type RunCommandStdio = "pipe" | "inherit";

function runCommand(input: {
  label: string;
  command: string;
  args: string[];
  env?: NodeJS.ProcessEnv;
  outputPath?: string;
  stdio?: RunCommandStdio;
}) {
  const stdio = input.stdio ?? "pipe";
  if (input.outputPath !== undefined && stdio !== "pipe") {
    throw new Error(
      `${input.label} cannot capture output with inherited stdio`
    );
  }
  const result = spawnSync(input.command, input.args, {
    env: input.env ?? process.env,
    encoding: "utf8",
    stdio,
  });
  if (stdio === "inherit") {
    if (result.status !== 0) {
      throw new Error(`${input.label} failed with exit ${result.status}`);
    }
    return;
  }
  const stdout = result.stdout ?? "";
  const stderr = result.stderr ?? "";
  if (input.outputPath !== undefined) {
    const dir = path.dirname(input.outputPath);
    const output = stdout === "" ? stderr : stdout;
    mkdirSync(dir, { recursive: true });
    writeFileSync(input.outputPath, output);
  } else if (stdout !== "") {
    process.stdout.write(stdout);
  }
  if (stderr !== "" && input.outputPath === undefined) {
    process.stderr.write(stderr);
  }
  if (result.status !== 0) {
    throw new Error(`${input.label} failed with exit ${result.status}`);
  }
}

export async function resolveProfileNetworks(
  hre: HardhatRuntimeEnvironment,
  profile: DeploymentProfile
): Promise<ResolvedProfileNetworks> {
  const rpcUrls: RPCURLMap = {};
  const latestBlockNumbers: Record<string, bigint> = {};
  for (const chain of profile.chains) {
    const networkConfig = hre.config.networks[chain.network];
    if (networkConfig === undefined) {
      throw new Error(`Hardhat network ${chain.network} is not configured`);
    }
    await withReadOnlyConnection(
      hre,
      { network: chain.network, expectedChainId: chain.chainId },
      async ({ publicClient }) => {
        rpcUrls[chain.key] = configuredHTTPRPCURL(networkConfig, chain.network);
        if (chain.startBlockNumber === undefined) {
          latestBlockNumbers[chain.key] = await publicClient.getBlockNumber();
        }
      }
    );
  }
  return { rpcUrls, latestBlockNumbers };
}

function configuredHTTPRPCURL(config: unknown, network: string): string {
  if (
    config === null ||
    typeof config !== "object" ||
    !("type" in config) ||
    config.type !== "http" ||
    !("url" in config) ||
    typeof config.url !== "string" ||
    config.url === ""
  ) {
    throw new Error(`Hardhat network ${network} must be an HTTP network`);
  }
  return config.url;
}

async function buildDeployProfile(
  hre: HardhatRuntimeEnvironment,
  buildProfile: string
): Promise<void> {
  await hre.tasks.getTask(["build"]).run({
    force: false,
    files: [],
    quiet: true,
    defaultBuildProfile: buildProfile,
    noTests: true,
    noContracts: false,
  });
}

function normalizeSigner(value: unknown, pathLabel: string): SignerProfile {
  const input = object(value, pathLabel);
  const id = addressField(input, "id", `${pathLabel}.id`);
  const type = stringField(input, "type", `${pathLabel}.type`);
  if (type === "keystore") {
    expectOnlyKeys(input, ["id", "type", "keystore"], pathLabel);
    const keystore = object(input.keystore, `${pathLabel}.keystore`, [
      "path",
      "passwordEnv",
      "passwordFile",
    ]);
    const passwordEnv = optionalEnvVarName(
      keystore.passwordEnv,
      `${pathLabel}.keystore.passwordEnv`
    );
    const passwordFile = optionalStringValue(
      keystore.passwordFile,
      `${pathLabel}.keystore.passwordFile`
    );
    if ((passwordEnv === undefined) === (passwordFile === undefined)) {
      throw new Error(
        `${pathLabel}.keystore must configure exactly one passwordEnv or passwordFile`
      );
    }
    return {
      id,
      type,
      keystore: {
        path: stringField(keystore, "path", `${pathLabel}.keystore.path`),
        passwordEnv,
        passwordFile,
      },
    };
  }
  if (type === "kms") {
    expectOnlyKeys(input, ["id", "type", "kms"], pathLabel);
    const kms = object(input.kms, `${pathLabel}.kms`, [
      "keyId",
      "region",
      "address",
      "endpoint",
    ]);
    const address = addressField(kms, "address", `${pathLabel}.kms.address`);
    if (!isAddressEqual(id, address)) {
      throw new Error(`${pathLabel}.kms.address must match signer id`);
    }
    return {
      id,
      type,
      kms: {
        keyId: stringField(kms, "keyId", `${pathLabel}.kms.keyId`),
        region: stringField(kms, "region", `${pathLabel}.kms.region`),
        address,
        endpoint: optionalStringValue(
          kms.endpoint,
          `${pathLabel}.kms.endpoint`
        ),
      },
    };
  }
  throw new Error(`${pathLabel}.type must be keystore or kms`);
}

function normalizeChain(
  value: unknown,
  pathLabel: string,
  signerIDs: ReadonlySet<string>,
  mode: DeploymentMode
): ChainProfile {
  const input = object(value, pathLabel, [
    "key",
    "network",
    "name",
    "nativeAssetId",
    "priceSources",
    "eid",
    "chainId",
    "deploymentId",
    "testOFTDeploymentId",
    "oapp",
    "initialSupply",
    "minCanaryTokenBalance",
    "confirmations",
    "startBlockNumber",
    "indexerQueryBlockRange",
    "indexerPollIntervalSeconds",
    "externalDVNs",
    "includeLayerZeroLabsDVN",
    "pricingTxPolicy",
    "txRoles",
    "layerZero",
    "privateKeyEnv",
    "rpcUrlEnv",
  ]);
  const eid = integerField(input, "eid", `${pathLabel}.eid`);
  const chainID = integerField(input, "chainId", `${pathLabel}.chainId`);
  const key = stringField(input, "key", `${pathLabel}.key`);
  const network = stringField(input, "network", `${pathLabel}.network`);
  const oapp = optionalAddressField(input, "oapp", `${pathLabel}.oapp`);
  if (mode === "external-oapp" && oapp === undefined) {
    throw new Error(`${pathLabel}.oapp is required for external-oapp mode`);
  }
  if (Object.hasOwn(input, "privateKeyEnv")) {
    throw new Error(
      `${pathLabel}.privateKeyEnv is not supported; store Hardhat private key config variables with hardhat keystore`
    );
  }
  if (Object.hasOwn(input, "rpcUrlEnv")) {
    throw new Error(
      `${pathLabel}.rpcUrlEnv is not supported; configure the RPC URL on the named Hardhat network`
    );
  }
  validateHardhatNetworkChain(pathLabel, network, chainID, eid);
  const layerZero = normalizeLayerZero(
    input.layerZero,
    pathLabel,
    eid,
    chainID
  );
  validateLayerZeroChain(pathLabel, eid, chainID, layerZero);
  const includeLayerZeroLabsDVN = optionalBoolean(
    input.includeLayerZeroLabsDVN,
    false,
    `${pathLabel}.includeLayerZeroLabsDVN`
  );
  if (includeLayerZeroLabsDVN) {
    requireLayerZeroLabsDVNForLibraries(
      layerZero,
      `${pathLabel}.includeLayerZeroLabsDVN`
    );
  }
  return {
    key,
    network,
    name: stringField(input, "name", `${pathLabel}.name`),
    nativeAssetId: normalizeNativeAssetID(
      input.nativeAssetId,
      `${pathLabel}.nativeAssetId`
    ),
    priceSources: normalizePriceSources(
      input.priceSources,
      `${pathLabel}.priceSources`
    ),
    eid,
    chainId: chainID,
    deploymentId: stringField(
      input,
      "deploymentId",
      `${pathLabel}.deploymentId`
    ),
    testOFTDeploymentId: optionalStringValue(
      input.testOFTDeploymentId,
      `${pathLabel}.testOFTDeploymentId`
    ),
    oapp,
    initialSupply: optionalDecimalField(
      input,
      "initialSupply",
      `${pathLabel}.initialSupply`,
      "0"
    ),
    minCanaryTokenBalance:
      mode === "test-oft-rehearsal"
        ? decimalField(
            input,
            "minCanaryTokenBalance",
            `${pathLabel}.minCanaryTokenBalance`
          )
        : optionalDecimalField(
            input,
            "minCanaryTokenBalance",
            `${pathLabel}.minCanaryTokenBalance`,
            "0"
          ),
    confirmations: integerField(
      input,
      "confirmations",
      `${pathLabel}.confirmations`
    ),
    startBlockNumber: optionalIntegerField(
      input,
      "startBlockNumber",
      `${pathLabel}.startBlockNumber`,
      { allowZero: true }
    ),
    indexerQueryBlockRange: integerField(
      input,
      "indexerQueryBlockRange",
      `${pathLabel}.indexerQueryBlockRange`
    ),
    indexerPollIntervalSeconds: integerField(
      input,
      "indexerPollIntervalSeconds",
      `${pathLabel}.indexerPollIntervalSeconds`,
      { max: maxDurationSeconds }
    ),
    externalDVNs: optionalAddressArrayField(
      input,
      "externalDVNs",
      `${pathLabel}.externalDVNs`
    ),
    includeLayerZeroLabsDVN,
    pricingTxPolicy: normalizeTxPolicy(
      input.pricingTxPolicy,
      `${pathLabel}.pricingTxPolicy`
    ),
    txRoles: normalizeTxRoles(input.txRoles, `${pathLabel}.txRoles`, signerIDs),
    layerZero,
  };
}

function normalizePricingProfile(value: unknown): PricingProfile {
  const input = object(value ?? {}, "profile.pricing", [
    "sourceRequestTimeoutSeconds",
    "maxDeviationBps",
    "coinMarketCapBaseURL",
    "coinMarketCapAPIKeyEnv",
    "coinGeckoBaseURL",
    "coinGeckoAPIKeyEnv",
  ]);
  const coinMarketCapAPIKeyEnv = optionalEnvVarName(
    input.coinMarketCapAPIKeyEnv,
    "profile.pricing.coinMarketCapAPIKeyEnv"
  );
  const coinGeckoAPIKeyEnv = optionalEnvVarName(
    input.coinGeckoAPIKeyEnv,
    "profile.pricing.coinGeckoAPIKeyEnv"
  );
  return {
    sourceRequestTimeoutSeconds:
      optionalIntegerField(
        input,
        "sourceRequestTimeoutSeconds",
        "profile.pricing.sourceRequestTimeoutSeconds",
        { max: maxDurationSeconds }
      ) ?? 10,
    maxDeviationBps:
      optionalIntegerField(
        input,
        "maxDeviationBps",
        "profile.pricing.maxDeviationBps"
      ) ?? 500,
    coinMarketCapBaseURL: optionalMarketDataBaseURL(
      input.coinMarketCapBaseURL,
      "profile.pricing.coinMarketCapBaseURL"
    ),
    coinMarketCapAPIKeyEnv,
    coinGeckoBaseURL: optionalMarketDataBaseURL(
      input.coinGeckoBaseURL,
      "profile.pricing.coinGeckoBaseURL"
    ),
    coinGeckoAPIKeyEnv,
  };
}

function normalizePriceSources(
  value: unknown,
  label: string
): PriceSourcesProfile | undefined {
  if (value === undefined) {
    return undefined;
  }
  const input = object(value, label, [
    "primarySource",
    "sanitySources",
    "coinMarketCap",
    "coinGecko",
    "chainlink",
    "uniswap",
  ]);
  const primarySource = normalizePrimaryPriceSource(
    input.primarySource,
    `${label}.primarySource`
  );
  const sanitySources = normalizeSanityPriceSources(
    input.sanitySources,
    `${label}.sanitySources`
  );
  if (sanitySources.includes(primarySource)) {
    throw new Error(`${label}.sanitySources must not contain primarySource`);
  }
  const result: PriceSourcesProfile = {
    primarySource,
    sanitySources,
    coinMarketCap: normalizeOptionalCoinMarketCapSource(
      input.coinMarketCap,
      `${label}.coinMarketCap`
    ),
    coinGecko: normalizeOptionalCoinGeckoSource(
      input.coinGecko,
      `${label}.coinGecko`
    ),
    chainlink: normalizeOptionalChainlinkSource(
      input.chainlink,
      `${label}.chainlink`
    ),
    uniswap: normalizeOptionalUniswapSource(input.uniswap, `${label}.uniswap`),
  };
  const referenced = new Set<PriceSourceName>([
    primarySource,
    ...sanitySources,
  ]);
  for (const source of [
    "coinmarketcap",
    "coingecko",
    "chainlink",
    "uniswap",
  ] as const) {
    const configured = sourceProfile(result, source) !== undefined;
    if (referenced.has(source) && !configured) {
      throw new Error(
        `${label}.${source} is required when source is referenced`
      );
    }
    if (!referenced.has(source) && configured) {
      throw new Error(`${label}.${source} is configured but not referenced`);
    }
  }
  return result;
}

function normalizePrimaryPriceSource(
  value: unknown,
  label: string
): Exclude<PriceSourceName, "uniswap"> {
  if (
    value === "coinmarketcap" ||
    value === "coingecko" ||
    value === "chainlink"
  ) {
    return value;
  }
  throw new Error(`${label} must be coinmarketcap, coingecko, or chainlink`);
}

function normalizeSanityPriceSources(
  value: unknown,
  label: string
): PriceSourceName[] {
  if (value === undefined) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  const result = value.map((source, index) => {
    if (
      source !== "coinmarketcap" &&
      source !== "coingecko" &&
      source !== "chainlink" &&
      source !== "uniswap"
    ) {
      throw new Error(`${label}[${index}] has unsupported source`);
    }
    return source;
  });
  if (new Set(result).size !== result.length) {
    throw new Error(`${label} must not contain duplicates`);
  }
  return result;
}

function normalizeOptionalCoinMarketCapSource(
  value: unknown,
  label: string
): PriceSourcesProfile["coinMarketCap"] {
  if (value === undefined) return undefined;
  const input = object(value, label, ["id", "maxAgeSeconds"]);
  return {
    id: integerField(input, "id", `${label}.id`),
    maxAgeSeconds: integerField(
      input,
      "maxAgeSeconds",
      `${label}.maxAgeSeconds`,
      { max: maxDurationSeconds }
    ),
  };
}

function normalizeOptionalCoinGeckoSource(
  value: unknown,
  label: string
): PriceSourcesProfile["coinGecko"] {
  if (value === undefined) return undefined;
  const input = object(value, label, ["id", "maxAgeSeconds"]);
  return {
    id: stringField(input, "id", `${label}.id`),
    maxAgeSeconds: integerField(
      input,
      "maxAgeSeconds",
      `${label}.maxAgeSeconds`,
      { max: maxDurationSeconds }
    ),
  };
}

function normalizeOptionalChainlinkSource(
  value: unknown,
  label: string
): PriceSourcesProfile["chainlink"] {
  if (value === undefined) return undefined;
  const input = object(value, label, [
    "feedAddress",
    "expectedDescription",
    "maxAgeSeconds",
  ]);
  return {
    feedAddress: addressField(input, "feedAddress", `${label}.feedAddress`),
    expectedDescription: stringField(
      input,
      "expectedDescription",
      `${label}.expectedDescription`
    ),
    maxAgeSeconds: integerField(
      input,
      "maxAgeSeconds",
      `${label}.maxAgeSeconds`,
      { max: maxDurationSeconds }
    ),
  };
}

function normalizeOptionalUniswapSource(
  value: unknown,
  label: string
): PriceSourcesProfile["uniswap"] {
  if (value === undefined) return undefined;
  const input = object(value, label, [
    "poolAddress",
    "tokenIn",
    "tokenOut",
    "twapWindowSeconds",
    "maxBlockAgeSeconds",
    "minHarmonicMeanLiquidity",
  ]);
  const tokenIn = addressField(input, "tokenIn", `${label}.tokenIn`);
  const tokenOut = addressField(input, "tokenOut", `${label}.tokenOut`);
  if (isAddressEqual(tokenIn, tokenOut)) {
    throw new Error(`${label}.tokenIn and tokenOut must differ`);
  }
  const twapWindowSeconds = integerField(
    input,
    "twapWindowSeconds",
    `${label}.twapWindowSeconds`
  );
  if (
    twapWindowSeconds < minUniswapTWAPWindowSeconds ||
    twapWindowSeconds > maxUniswapTWAPWindowSeconds
  ) {
    throw new Error(
      `${label}.twapWindowSeconds must be between ${minUniswapTWAPWindowSeconds} and ${maxUniswapTWAPWindowSeconds}`
    );
  }
  return {
    poolAddress: addressField(input, "poolAddress", `${label}.poolAddress`),
    tokenIn,
    tokenOut,
    twapWindowSeconds,
    maxBlockAgeSeconds: integerField(
      input,
      "maxBlockAgeSeconds",
      `${label}.maxBlockAgeSeconds`,
      { max: maxDurationSeconds }
    ),
    minHarmonicMeanLiquidity: positiveDecimalField(
      input,
      "minHarmonicMeanLiquidity",
      `${label}.minHarmonicMeanLiquidity`
    ),
  };
}

function sourceProfile(sources: PriceSourcesProfile, source: PriceSourceName) {
  switch (source) {
    case "coinmarketcap":
      return sources.coinMarketCap;
    case "coingecko":
      return sources.coinGecko;
    case "chainlink":
      return sources.chainlink;
    case "uniswap":
      return sources.uniswap;
  }
}

function validateProfilePriceSources(
  chains: readonly ChainProfile[],
  pricing: PricingProfile
) {
  const crossAsset = chains[0].nativeAssetId !== chains[1].nativeAssetId;
  for (const chain of chains) {
    if (crossAsset && chain.priceSources === undefined) {
      throw new Error(
        `${chain.key}.priceSources is required for cross-asset pricing`
      );
    }
    if (!crossAsset && chain.priceSources !== undefined) {
      throw new Error(
        `${chain.key}.priceSources must be omitted for same-native pricing`
      );
    }
    if (
      chain.priceSources?.coinMarketCap !== undefined &&
      pricing.coinMarketCapAPIKeyEnv === undefined
    ) {
      throw new Error(
        "profile.pricing.coinMarketCapAPIKeyEnv is required when CoinMarketCap is referenced"
      );
    }
  }
}

function validateHardhatNetworkChain(
  pathLabel: string,
  network: string,
  chainID: number,
  eid: number
): void {
  const expected = hardhatNetworks.get(network);
  if (expected === undefined) {
    throw new Error(
      `${pathLabel}.network ${network} has no chain_id/eid binding in hardhatNetworks; add the network binding before deploying to it`
    );
  }
  if (expected.chainId !== chainID) {
    throw new Error(
      `${pathLabel}.network ${network} uses chainId ${expected.chainId}, but ${pathLabel}.chainId is ${chainID}`
    );
  }
  if (expected.eid !== eid) {
    throw new Error(
      `${pathLabel}.network ${network} uses eid ${expected.eid}, but ${pathLabel}.eid is ${eid}`
    );
  }
}

function validateLayerZeroChain(
  pathLabel: string,
  eid: number,
  chainID: number,
  layerZero: LayerZeroAddresses
): void {
  if (Number(layerZero.eid) !== eid) {
    throw new Error(
      `${pathLabel}.layerZero.eid ${layerZero.eid} does not match ${pathLabel}.eid ${eid}`
    );
  }
  if (layerZero.nativeChainId !== chainID) {
    throw new Error(
      `${pathLabel}.layerZero.nativeChainId ${layerZero.nativeChainId} does not match ${pathLabel}.chainId ${chainID}`
    );
  }
}

function normalizeLayerZero(
  value: unknown,
  pathLabel: string,
  eid: number,
  chainID: number
): LayerZeroAddresses {
  if (value !== undefined) {
    const input = object(value, `${pathLabel}.layerZero`, [
      "chainKey",
      "endpointV2",
      "sendUln302",
      "receiveUln302",
      "layerZeroLabsDVN",
    ]);
    if (Object.hasOwn(input, "layerZeroLabsDVN")) {
      throw new Error(
        `${pathLabel}.layerZero.layerZeroLabsDVN is not supported; configure ${pathLabel}.externalDVNs or ${pathLabel}.includeLayerZeroLabsDVN instead`
      );
    }
    return {
      chainKey: optionalStringValue(
        input.chainKey,
        `${pathLabel}.layerZero.chainKey`
      ),
      nativeChainId: chainID,
      eid: String(eid),
      endpointV2: addressField(
        input,
        "endpointV2",
        `${pathLabel}.layerZero.endpointV2`
      ),
      sendUln302: addressField(
        input,
        "sendUln302",
        `${pathLabel}.layerZero.sendUln302`
      ),
      receiveUln302: addressField(
        input,
        "receiveUln302",
        `${pathLabel}.layerZero.receiveUln302`
      ),
    };
  }
  const layerZero = expectedLayerZeroChains.find(
    (chain) => Number(chain.eid) === eid && chain.nativeChainId === chainID
  );
  if (layerZero === undefined) {
    throw new Error(
      `${pathLabel}.layerZero is required when EID ${eid} and chainId ${chainID} are not in repo metadata`
    );
  }
  return {
    chainKey: layerZero.chainKey,
    nativeChainId: layerZero.nativeChainId,
    eid: layerZero.eid,
    endpointV2: layerZero.endpointV2,
    sendUln302: layerZero.sendUln302,
    receiveUln302: layerZero.receiveUln302,
  };
}

function normalizeTxRoles(
  value: unknown,
  pathLabel: string,
  signerIDs: ReadonlySet<string>
) {
  const roles = object(value, pathLabel, ["executor", "dvn"]);
  return {
    executor: normalizeTxRole(
      roles.executor,
      `${pathLabel}.executor`,
      signerIDs
    ),
    dvn: normalizeTxRole(roles.dvn, `${pathLabel}.dvn`, signerIDs),
  };
}

function normalizeTxRole(
  value: unknown,
  pathLabel: string,
  signerIDs: ReadonlySet<string>
): TxRoleProfile {
  const role = object(value, pathLabel, [
    "signer",
    "maxFeePerGasWei",
    "maxPriorityFeePerGasWei",
    "minNativeBalanceWei",
  ]);
  const signer = addressField(role, "signer", `${pathLabel}.signer`);
  if (!signerIDs.has(signer.toLowerCase())) {
    throw new Error(`${pathLabel}.signer must reference a configured signer`);
  }
  return { signer, ...normalizeTxPolicy(role, pathLabel, true) };
}

function normalizeTxPolicy(
  value: unknown,
  pathLabel: string,
  allowSigner = false
): TxPolicyProfile {
  const policy = object(value, pathLabel);
  expectOnlyKeys(
    policy,
    [
      ...(allowSigner ? ["signer"] : []),
      "maxFeePerGasWei",
      "maxPriorityFeePerGasWei",
      "minNativeBalanceWei",
    ],
    pathLabel
  );
  const maxFeePerGasWei = positiveDecimalField(
    policy,
    "maxFeePerGasWei",
    `${pathLabel}.maxFeePerGasWei`
  );
  const maxPriorityFeePerGasWei = positiveDecimalField(
    policy,
    "maxPriorityFeePerGasWei",
    `${pathLabel}.maxPriorityFeePerGasWei`
  );
  if (BigInt(maxPriorityFeePerGasWei) > BigInt(maxFeePerGasWei)) {
    throw new Error(
      `${pathLabel}.maxPriorityFeePerGasWei must not exceed maxFeePerGasWei`
    );
  }
  const minNativeBalanceWei = positiveDecimalField(
    policy,
    "minNativeBalanceWei",
    `${pathLabel}.minNativeBalanceWei`
  );
  return {
    maxFeePerGasWei,
    maxPriorityFeePerGasWei,
    minNativeBalanceWei,
  };
}

function normalizePathway(value: unknown, pathLabel: string): PathwayProfile {
  const input = object(value, pathLabel, [
    "maxMessageSize",
    "enforcedLzReceiveGas",
    "minLzReceiveGas",
    "maxLzReceiveGas",
    "priceSnapshot",
    "executorFee",
    "dvnFee",
  ]);
  const priceSnapshot = object(
    input.priceSnapshot,
    `${pathLabel}.priceSnapshot`,
    [
      "dstGasPriceInSrcToken",
      "dstDataFeePerByteInSrcToken",
      "staleAfter",
      "maxAgeSeconds",
    ]
  );
  const staleAfter = positiveDecimalField(
    priceSnapshot,
    "staleAfter",
    `${pathLabel}.priceSnapshot.staleAfter`
  );
  if (BigInt(staleAfter) > maxPriceSnapshotStaleAfter) {
    throw new Error(
      `${pathLabel}.priceSnapshot.staleAfter must not exceed ${maxPriceSnapshotStaleAfter}`
    );
  }
  const enforcedLzReceiveGas = decimalField(
    input,
    "enforcedLzReceiveGas",
    `${pathLabel}.enforcedLzReceiveGas`
  );
  const minLzReceiveGas = decimalField(
    input,
    "minLzReceiveGas",
    `${pathLabel}.minLzReceiveGas`
  );
  const maxLzReceiveGas = decimalField(
    input,
    "maxLzReceiveGas",
    `${pathLabel}.maxLzReceiveGas`
  );
  if (BigInt(maxLzReceiveGas) === 0n) {
    throw new Error(`${pathLabel}.maxLzReceiveGas must be positive`);
  }
  if (BigInt(minLzReceiveGas) > BigInt(maxLzReceiveGas)) {
    throw new Error(
      `${pathLabel}.minLzReceiveGas must not exceed ${pathLabel}.maxLzReceiveGas`
    );
  }
  // The enforced lzReceive gas is what the OApp actually sends; if it falls
  // outside [min, max] the deployment still verifies green but OpenExecutor
  // reverts InvalidGas on every quote/send, bricking the pathway.
  if (
    BigInt(enforcedLzReceiveGas) < BigInt(minLzReceiveGas) ||
    BigInt(enforcedLzReceiveGas) > BigInt(maxLzReceiveGas)
  ) {
    throw new Error(
      `${pathLabel}.enforcedLzReceiveGas must be within [${pathLabel}.minLzReceiveGas, ${pathLabel}.maxLzReceiveGas]`
    );
  }
  return {
    maxMessageSize: integerField(
      input,
      "maxMessageSize",
      `${pathLabel}.maxMessageSize`
    ),
    enforcedLzReceiveGas,
    minLzReceiveGas,
    maxLzReceiveGas,
    priceSnapshot: {
      dstGasPriceInSrcToken: decimalField(
        priceSnapshot,
        "dstGasPriceInSrcToken",
        `${pathLabel}.priceSnapshot.dstGasPriceInSrcToken`
      ),
      dstDataFeePerByteInSrcToken: decimalField(
        priceSnapshot,
        "dstDataFeePerByteInSrcToken",
        `${pathLabel}.priceSnapshot.dstDataFeePerByteInSrcToken`
      ),
      staleAfter,
      maxAgeSeconds: decimalField(
        priceSnapshot,
        "maxAgeSeconds",
        `${pathLabel}.priceSnapshot.maxAgeSeconds`
      ),
    },
    executorFee: normalizeWorkerFee(
      input.executorFee,
      `${pathLabel}.executorFee`
    ),
    dvnFee: normalizeWorkerFee(input.dvnFee, `${pathLabel}.dvnFee`),
  };
}

function normalizeWorkerFee(
  value: unknown,
  pathLabel: string
): WorkerFeeProfile {
  const input = object(value, pathLabel, [
    "fixedFeeWei",
    "dstGasOverhead",
    "dataSizeOverheadBytes",
    "marginBps",
  ]);
  const marginBps = integerField(input, "marginBps", `${pathLabel}.marginBps`, {
    allowZero: true,
  });
  if (marginBps > 10_000) {
    throw new Error(`${pathLabel}.marginBps must be at most 10000`);
  }
  return {
    fixedFeeWei: decimalField(input, "fixedFeeWei", `${pathLabel}.fixedFeeWei`),
    dstGasOverhead: decimalField(
      input,
      "dstGasOverhead",
      `${pathLabel}.dstGasOverhead`
    ),
    dataSizeOverheadBytes: decimalField(
      input,
      "dataSizeOverheadBytes",
      `${pathLabel}.dataSizeOverheadBytes`
    ),
    marginBps,
  };
}

function deploymentContractAddress(
  deployment: IgnitionContractDeployment,
  futureId: string,
  expectedContractName: string,
  chainKey: string
): Address {
  const contract = deployment.contracts[futureId];
  if (contract === undefined) {
    throw new Error(
      `${chainKey} Ignition deployment is missing required future ${futureId}`
    );
  }
  if (contract.contractName !== expectedContractName) {
    throw new Error(
      `${chainKey} Ignition future ${futureId} has contract name ${contract.contractName}, expected ${expectedContractName}`
    );
  }
  return normalizeAddress(contract.address, `${chainKey}.${futureId}`);
}

function deploymentDirections(
  chains: readonly ChainDeploymentState[]
): DeploymentDirectionState[] {
  if (chains.length !== 2) {
    throw new Error("deployment state requires exactly two chains");
  }
  return [
    directionState(chains[0], chains[1]),
    directionState(chains[1], chains[0]),
  ];
}

function directionState(
  source: ChainDeploymentState,
  destination: ChainDeploymentState
): DeploymentDirectionState {
  return {
    key: `${source.key}-to-${destination.key}`,
    source: source.key,
    destination: destination.key,
    srcEid: source.eid,
    dstEid: destination.eid,
    srcOApp: source.oapp,
    dstOApp: destination.oapp,
    sendLib: source.sendUln,
    receiveLib: destination.receiveUln,
    sourceWorkers: source.workers,
    destinationWorkers: {
      openDVN: destination.workers.openDVN,
    },
  };
}

function renderSigner(signer: SignerProfile): string {
  if (signer.type === "keystore") {
    const passwordLine =
      signer.keystore.passwordEnv !== undefined
        ? `      password_env: ${yamlString(signer.keystore.passwordEnv)}`
        : `      password_file: ${yamlString(
            signer.keystore.passwordFile ?? ""
          )}`;
    return `  - id: "${signer.id}"
    type: keystore
    keystore:
      path: ${yamlString(signer.keystore.path)}
${passwordLine}`;
  }
  const endpointLine =
    signer.kms.endpoint === undefined
      ? ""
      : `\n      endpoint: ${yamlString(signer.kms.endpoint)}`;
  return `  - id: "${signer.id}"
    type: kms
    kms:
      key_id: "${signer.kms.keyId}"
      region: "${signer.kms.region}"
      address: "${signer.kms.address}"${endpointLine}`;
}

function renderWorkerChain(
  chain: ChainProfile,
  rpcURL: string,
  startBlockNumber: number
): string {
  return `  - eid: ${chain.eid}
    name: ${yamlString(chain.name)}
    family: evm
    chain_id: ${chain.chainId}
    endpoint_address: "${chain.layerZero.endpointV2}"
    confirmations: ${chain.confirmations}
    start_block_number: ${startBlockNumber}
    indexer_query_block_range: ${chain.indexerQueryBlockRange}
    indexer_poll_interval_seconds: ${chain.indexerPollIntervalSeconds}
    rpc_urls:
      - ${yamlString(rpcURL)}
    tx_roles:
      executor:
        signer: "${chain.txRoles.executor.signer}"
        max_fee_per_gas_wei: "${chain.txRoles.executor.maxFeePerGasWei}"
        max_priority_fee_per_gas_wei: "${
          chain.txRoles.executor.maxPriorityFeePerGasWei
        }"
        min_native_balance_wei: "${chain.txRoles.executor.minNativeBalanceWei}"
      dvn:
        signer: "${chain.txRoles.dvn.signer}"
        max_fee_per_gas_wei: "${chain.txRoles.dvn.maxFeePerGasWei}"
        max_priority_fee_per_gas_wei: "${
          chain.txRoles.dvn.maxPriorityFeePerGasWei
        }"
        min_native_balance_wei: "${chain.txRoles.dvn.minNativeBalanceWei}"`;
}

function workerStartBlock(
  workerStartBlocks: WorkerStartBlockMap,
  chain: ChainProfile
): number {
  const startBlockNumber = workerStartBlocks[chain.key];
  if (!Number.isInteger(startBlockNumber) || startBlockNumber < 0) {
    throw new Error(
      `${chain.key} start_block_number must be a non-negative integer`
    );
  }
  return startBlockNumber;
}

function renderWorkerPathway(
  profile: DeploymentProfile,
  direction: DeploymentDirectionState
): string {
  const sendRequiredDVNs = requiredDVNSetForChain(
    profileChainByKey(profile, direction.source),
    direction.sourceWorkers.openDVN
  );
  const receiveRequiredDVNs = requiredDVNSetForChain(
    profileChainByKey(profile, direction.destination),
    direction.destinationWorkers.openDVN
  );
  return `  - src_eid: ${direction.srcEid}
    dst_eid: ${direction.dstEid}
    src_oapp: "${direction.srcOApp}"
    dst_oapp: "${direction.dstOApp}"
    send_lib: "${direction.sendLib}"
    receive_lib: "${direction.receiveLib}"
    source_workers:
      open_executor: "${direction.sourceWorkers.openExecutor}"
      open_dvn: "${direction.sourceWorkers.openDVN}"
      price_feed: "${direction.sourceWorkers.priceFeed}"
    destination_workers:
      open_dvn: "${direction.destinationWorkers.openDVN}"
${renderAddressList("send_required_dvns", sendRequiredDVNs)}
${renderAddressList("receive_required_dvns", receiveRequiredDVNs)}
    dvn:
      mode: ${profile.dvnMode}
    pricing:
      executor_fee:
        fixed_fee_wei: "${profile.pathway.executorFee.fixedFeeWei}"
        dst_gas_overhead: ${profile.pathway.executorFee.dstGasOverhead}
        data_size_overhead_bytes: ${profile.pathway.executorFee.dataSizeOverheadBytes}
        margin_bps: ${profile.pathway.executorFee.marginBps}
      dvn_fee:
        fixed_fee_wei: "${profile.pathway.dvnFee.fixedFeeWei}"
        dst_gas_overhead: ${profile.pathway.dvnFee.dstGasOverhead}
        data_size_overhead_bytes: ${profile.pathway.dvnFee.dataSizeOverheadBytes}
        margin_bps: ${profile.pathway.dvnFee.marginBps}
    enabled: true
    max_message_size: ${profile.pathway.maxMessageSize}
    min_lz_receive_gas: ${profile.pathway.minLzReceiveGas}
    max_lz_receive_gas: ${profile.pathway.maxLzReceiveGas}`;
}

function renderCommands(plan: CommandPlan): string {
  const lines = [
    "# Generated Deployment Commands",
    "",
    "Each command reads its input envelope from `OML_SCRIPT_PARAMS`. Set `apply: true` only after reviewing the generated file.",
    "",
  ];
  for (const command of plan.commands) {
    lines.push(
      `## ${command.label}`,
      "",
      "```bash",
      command.command,
      "```",
      ""
    );
    if (command.output !== undefined) {
      lines.push(`Output: \`${command.output}\``, "");
    }
  }
  return `${lines.join("\n")}\n`;
}

function profileDirections(
  profile: DeploymentProfile
): [ChainProfile, ChainProfile][] {
  if (profile.chains.length !== 2) {
    throw new Error("profile requires exactly two chains");
  }
  return [
    [profile.chains[0], profile.chains[1]],
    [profile.chains[1], profile.chains[0]],
  ];
}

function chainState(state: DeploymentState, key: string): ChainDeploymentState {
  const chain = state.chains.find((candidate) => candidate.key === key);
  if (chain === undefined) {
    throw new Error(`deployment state is missing ${key}`);
  }
  return chain;
}

function feeModelInput(fee: WorkerFeeProfile) {
  return {
    baseFee: BigInt(fee.fixedFeeWei),
    dstGasOverhead: BigInt(fee.dstGasOverhead),
    dataSizeOverheadBytes: BigInt(fee.dataSizeOverheadBytes),
    marginBps: fee.marginBps,
  };
}

function priceFeedDeploymentSubmitters(profile: DeploymentProfile): Address[] {
  return uniqueAddresses([...profile.priceFeedSubmitters, profile.owner]);
}

function uniqueAddresses(addresses: readonly Address[]): Address[] {
  const seen = new Set<string>();
  const unique: Address[] = [];
  for (const address of addresses) {
    const normalized = getAddress(address);
    const key = normalized.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(normalized);
  }
  return unique;
}

function openWorkersParameterPath(outDir: string, chain: ChainProfile): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${chain.key}.open-workers.json`
  );
}

function testOFTParameterPath(outDir: string, chain: ChainProfile): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${chain.key}.test-oft.json`
  );
}

function openWorkersPathwayParameterPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile
): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${directionKey(source, destination)}.open-workers-pathway.json`
  );
}

function oappEndpointParameterPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile
): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${directionKey(source, destination)}.oapp-endpoint.json`
  );
}

function commandInputPath(outDir: string, name: string): string {
  return path.join(outDir, "commands", `${name}.json`);
}

function testOFTCommandInputPath(outDir: string, chain: ChainProfile): string {
  return commandInputPath(outDir, `${chain.key}.deploy-test-oft`);
}

function openWorkersCommandInputPath(
  outDir: string,
  chain: ChainProfile
): string {
  return commandInputPath(outDir, `${chain.key}.deploy-open-workers`);
}

function openWorkersPathwayCommandInputPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile
): string {
  return commandInputPath(
    outDir,
    `${directionKey(source, destination)}.configure-open-workers-pathway`
  );
}

function oappEndpointCommandInputPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile
): string {
  return commandInputPath(
    outDir,
    `${directionKey(source, destination)}.configure-oapp-endpoint`
  );
}

function deploymentPreflightCommandInputPath(
  outDir: string,
  chain: ChainProfile
): string {
  return commandInputPath(outDir, `${chain.key}.deployment-preflight`);
}

function inspectLzCommandInputPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile
): string {
  return commandInputPath(
    outDir,
    `${directionKey(source, destination)}.inspect-lz-config`
  );
}

function priceConfigCommandInputPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile
): string {
  return commandInputPath(
    outDir,
    `${directionKey(source, destination)}.price-config`
  );
}

async function writeJSON(filePath: string, value: unknown): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${jsonStringify(value)}\n`);
}

async function writeBootstrapRenderStatus(
  outDir: string,
  error: unknown
): Promise<void> {
  await writeJSON(path.join(outDir, "render-status.json"), {
    ok: true,
    deploymentState: false,
    message:
      "Bootstrap render wrote initial Ignition parameters and command plan only. Run deploy-test-oft/deploy-workers, then run render again to produce pathway parameters, deployment-state.json, worker.yaml, and verification artifacts.",
    detail: sanitizeCommandErrorMessage(
      error instanceof Error ? error.message : String(error)
    ),
  });
}

function deploymentSummary(
  phase: DeploymentPhase,
  applied: boolean,
  outDir: string,
  profile: DeploymentProfile,
  options?: { deploymentState?: boolean }
): DeployProfileResult {
  const summary: DeployProfileResult = {
    ok: true,
    phase,
    applied,
    mode: profile.mode,
    outDir,
    parameters: path.join(outDir, "ignition", "parameters"),
    ...(phase === "deploy-test-oft" ||
    phase === "deploy-workers" ||
    options?.deploymentState === false
      ? {}
      : { workerConfig: path.join(outDir, "worker.yaml") }),
    commands: path.join(outDir, "commands.md"),
  };
  if (options?.deploymentState !== undefined) {
    Object.assign(summary, { deploymentState: options.deploymentState });
    if (!options.deploymentState) {
      Object.assign(summary, {
        status: path.join(outDir, "render-status.json"),
      });
    }
  }
  return summary;
}

function phaseMutates(phase: DeploymentPhase): boolean {
  return (
    phase === "deploy-test-oft" ||
    phase === "deploy-workers" ||
    phase === "configure-workers" ||
    phase === "configure-oapp" ||
    phase === "all"
  );
}

function deployProfileApplySummary(
  profile: DeploymentProfile,
  phase: DeploymentPhase
) {
  return {
    command: "deploy:profile",
    targets: profile.chains.map((chain) => ({
      network: chain.network,
      chainId: chain.chainId,
      deploymentIds: deploymentIDsForPhase(profile, chain, phase),
    })),
    actions: actionsForPhase(profile, phase),
  };
}

function deploymentIDsForPhase(
  profile: DeploymentProfile,
  chain: ChainProfile,
  phase: DeploymentPhase
): string[] {
  const destination = profile.chains.find(
    (candidate) => candidate.key !== chain.key
  );
  if (destination === undefined) {
    throw new Error(`profile is missing a destination chain for ${chain.key}`);
  }
  switch (phase) {
    case "deploy-test-oft":
      return [testOFTDeploymentId(chain)];
    case "deploy-workers":
      return [chain.deploymentId];
    case "configure-workers":
      return [openWorkersPathwayDeploymentId(chain, destination)];
    case "configure-oapp":
      return [oappEndpointDeploymentId(chain, destination)];
    case "all":
      return [
        ...(profile.mode === "test-oft-rehearsal"
          ? [testOFTDeploymentId(chain)]
          : []),
        chain.deploymentId,
        openWorkersPathwayDeploymentId(chain, destination),
        ...(profile.mode === "test-oft-rehearsal"
          ? [oappEndpointDeploymentId(chain, destination)]
          : []),
      ];
    case "render":
    case "verify":
      return [];
  }
}

function actionsForPhase(
  profile: DeploymentProfile,
  phase: DeploymentPhase
): string[] {
  switch (phase) {
    case "deploy-test-oft":
      return ["reconcile TestOFT Ignition deployments"];
    case "deploy-workers":
      return ["reconcile OpenWorkers Ignition deployments"];
    case "configure-workers":
      return ["reconcile OpenWorkers pathway configuration"];
    case "configure-oapp":
      return ["reconcile OApp Endpoint configuration"];
    case "all":
      return [
        ...(profile.mode === "test-oft-rehearsal"
          ? ["reconcile TestOFT Ignition deployments"]
          : []),
        "reconcile OpenWorkers Ignition deployments",
        "reconcile OpenWorkers pathway configuration",
        ...(profile.mode === "test-oft-rehearsal"
          ? ["reconcile OApp Endpoint configuration"]
          : []),
      ];
    case "render":
    case "verify":
      return [];
  }
}

function normalizePhase(value: string): DeploymentPhase {
  switch (value) {
    case "render":
    case "deploy-test-oft":
    case "deploy-workers":
    case "configure-workers":
    case "configure-oapp":
    case "verify":
    case "all":
      return value;
    default:
      throw new Error(
        "input.phase must be render, deploy-test-oft, deploy-workers, configure-workers, configure-oapp, verify, or all"
      );
  }
}

function validateLongTermPriceSubmitters(
  submitters: readonly Address[],
  owner: Address
) {
  for (const submitter of submitters) {
    if (isAddressEqual(submitter, owner)) {
      throw new Error(
        "profile.priceFeedSubmitters must not include profile.owner; owner is added only as a temporary deployment submitter"
      );
    }
  }
}

function normalizeMode(value: unknown): DeploymentMode {
  if (value === "test-oft-rehearsal" || value === "external-oapp") {
    return value;
  }
  throw new Error("profile.mode must be test-oft-rehearsal or external-oapp");
}

function validateTwoChainPair(chains: readonly ChainProfile[]) {
  if (chains.length !== 2) {
    throw new Error("profile.chains must contain exactly two chains");
  }
  const keys = new Set(chains.map((chain) => chain.key));
  if (keys.size !== 2) {
    throw new Error("profile.chains keys must be unique");
  }
  const eids = new Set(chains.map((chain) => chain.eid));
  if (eids.size !== 2) {
    throw new Error("profile.chains eids must be unique");
  }
  // The profile deploys both reciprocal directions, and each direction's send
  // ULN carries the source chain's confirmations while the receive ULN enforces
  // the destination chain's value. Unequal values would deploy one direction
  // whose verifications the receive ULN always rejects, and the worker config
  // generated from this profile would fail the same validation at startup --
  // after the chain mutations already ran. Reject before any deployment phase.
  if (chains[0].confirmations !== chains[1].confirmations) {
    throw new Error(
      `profile.chains confirmations must match for reciprocal pathways: ${chains[0].key} uses ${chains[0].confirmations}, ${chains[1].key} uses ${chains[1].confirmations}`
    );
  }
}

function normalizeDVNMode(value: unknown): "active" | "shadow" {
  if (value === undefined || value === "") {
    return "active";
  }
  if (value === "active" || value === "shadow") {
    return value;
  }
  throw new Error("profile.dvnMode must be active or shadow");
}

function requiredOApp(chain: ChainProfile): Address {
  if (chain.oapp === undefined) {
    throw new Error(`${chain.key}.oapp is required`);
  }
  return chain.oapp;
}

function missingTestOFTDeployment(chainKey: string): never {
  throw new Error(`${chainKey} deployment state is missing TestOFT`);
}

function requireRehearsalMode(profile: DeploymentProfile, phase: string) {
  if (profile.mode !== "test-oft-rehearsal") {
    throw new Error(`${phase} requires profile.mode test-oft-rehearsal`);
  }
}

function testOFTDeploymentId(chain: ChainProfile): string {
  return chain.testOFTDeploymentId ?? `${chain.deploymentId}-test-oft`;
}

function openWorkersPathwayDeploymentId(
  source: ChainProfile,
  destination: ChainProfile
): string {
  return `${source.deploymentId}-${directionKey(
    source,
    destination
  )}-open-workers-pathway`;
}

function oappEndpointDeploymentId(
  source: ChainProfile,
  destination: ChainProfile
): string {
  return `${source.deploymentId}-${directionKey(
    source,
    destination
  )}-oapp-endpoint`;
}

function directionKey(source: ChainProfile, destination: ChainProfile): string {
  return `${source.key}-to-${destination.key}`;
}

function hardhatCommand(
  chain: ChainProfile,
  script: string,
  parametersPath: string,
  buildProfile: string
): string {
  const command = `OML_SCRIPT_PARAMS=${shellWord(
    parametersPath
  )} npm run ${script} -- --network ${shellWord(
    chain.network
  )}`;
  return fixedProductionBuildProfileScripts.has(script)
    ? command
    : `${command} --build-profile ${shellWord(buildProfile)}`;
}

const fixedProductionBuildProfileScripts = new Set([
  "deploy:open-workers",
  "deploy:open-dvn-worker",
  "deploy:test-oft",
  "configure:oapp-endpoint",
  "configure:open-workers-pathway",
  "configure:open-dvn-pathway",
]);

function shellWord(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

function object(
  value: unknown,
  label: string,
  allowedKeys?: readonly string[]
): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  const result = value as Record<string, unknown>;
  if (allowedKeys !== undefined) {
    expectOnlyKeys(result, allowedKeys, label);
  }
  return result;
}

function arrayField(
  input: Record<string, unknown>,
  field: string,
  label: string
): unknown[] {
  const value = input[field];
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value;
}

function normalizeAddressArrayField(
  input: Record<string, unknown>,
  field: string,
  label: string
): Address[] {
  const value = input[field];
  if (value === undefined) {
    throw new Error(`${label} is required`);
  }
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  if (value.length === 0) {
    throw new Error(`${label} must not be empty`);
  }
  return value.map((entry, index) => {
    if (typeof entry !== "string") {
      throw new Error(`${label}[${index}] must be an address`);
    }
    return normalizeAddress(entry, `${label}[${index}]`);
  });
}

function optionalAddressArrayField(
  input: Record<string, unknown>,
  field: string,
  label: string
): Address[] {
  const value = input[field];
  if (value === undefined) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value.map((entry, index) => {
    if (typeof entry !== "string") {
      throw new Error(`${label}[${index}] must be an address`);
    }
    return normalizeAddress(entry, `${label}[${index}]`);
  });
}

function stringField(
  input: Record<string, unknown>,
  field: string,
  label: string
): string {
  const value = input[field];
  if (typeof value !== "string" || value === "") {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function optionalString(
  value: unknown,
  fallback: string,
  label: string
): string {
  if (value === undefined || value === "") {
    return fallback;
  }
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

function optionalStringValue(
  value: unknown,
  label: string
): string | undefined {
  if (value === undefined || value === "") {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

function optionalMarketDataBaseURL(
  value: unknown,
  label: string
): string | undefined {
  const baseURL = optionalStringValue(value, label);
  if (baseURL === undefined) {
    return undefined;
  }
  let parsed: URL;
  try {
    parsed = new URL(baseURL);
  } catch {
    throw new Error(
      `${label} must be an absolute HTTPS URL without query or fragment`
    );
  }
  if (
    baseURL.trim() !== baseURL ||
    !/^https:\/\//i.test(baseURL) ||
    parsed.protocol !== "https:" ||
    parsed.hostname === "" ||
    baseURL.includes("?") ||
    baseURL.includes("#") ||
    // WHATWG URL repairs several forms that Go's net/url rejects, and this
    // function persists the raw string, so anything accepted here must also be
    // accepted at worker startup. Reject the known divergences: control
    // characters (silently stripped by WHATWG),
    // eslint-disable-next-line no-control-regex
    /[\u0000-\u001f\u007f]/.test(baseURL) ||
    // backslashes (WHATWG maps them to "/"; Go rejects them in host/path),
    baseURL.includes("\\") ||
    // and invalid percent-escapes (a "%" not followed by two hex digits).
    /%(?![0-9a-fA-F]{2})/.test(baseURL)
  ) {
    throw new Error(
      `${label} must be an absolute HTTPS URL without query or fragment`
    );
  }
  // Even a well-formed percent-escape is rejected by Go's net/url when it sits
  // in the host/port (WHATWG decodes it, e.g. "%65" -> "e"). Userinfo may keep
  // valid escapes, so only inspect the authority after the last "@".
  const authority = baseURL.slice("https://".length).split(/[/?#]/)[0];
  const hostPort = authority.slice(authority.lastIndexOf("@") + 1);
  // WHATWG "repairs" an empty raw authority (https:///example.com parses with
  // hostname example.com), but Go's net/url sees an empty host and rejects it
  // at worker startup — the profile must fail here, not there.
  if (!hostPort || hostPort.includes("%")) {
    throw new Error(
      `${label} must be an absolute HTTPS URL without query or fragment`
    );
  }
  return baseURL;
}

function normalizeNativeAssetID(value: unknown, label: string): string {
  const assetID = optionalStringValue(value, label) ?? "eth";
  if (assetID.trim() !== assetID) {
    throw new Error(`${label} must not contain leading or trailing whitespace`);
  }
  if (assetID.toLowerCase() !== assetID) {
    throw new Error(`${label} must be lowercase`);
  }
  return assetID;
}

function optionalEnvVarName(value: unknown, label: string): string | undefined {
  if (value === undefined || value === "") {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`${label} must be an environment variable name`);
  }
  return envVarName(value, label);
}

function envVarName(value: string, label: string): string {
  if (!/^[A-Z_][A-Z0-9_]*$/.test(value)) {
    throw new Error(`${label} must be an uppercase environment variable name`);
  }
  return value;
}

function integerField(
  input: Record<string, unknown>,
  field: string,
  label: string,
  options?: { allowZero?: boolean; max?: number }
): number {
  const value = input[field];
  if (!Number.isInteger(value)) {
    throw new Error(`${label} must be an integer`);
  }
  if (!Number.isSafeInteger(value)) {
    throw new Error(`${label} must be a safe integer`);
  }
  const min = options?.allowZero === true ? 0 : 1;
  if ((value as number) < min) {
    throw new Error(`${label} must be >= ${min}`);
  }
  if (options?.max !== undefined && (value as number) > options.max) {
    throw new Error(`${label} must be <= ${options.max}`);
  }
  return value as number;
}

function optionalIntegerField(
  input: Record<string, unknown>,
  field: string,
  label: string,
  options?: { allowZero?: boolean; max?: number }
): number | undefined {
  const value = input[field];
  if (value === undefined) {
    return undefined;
  }
  return integerField(input, field, label, options);
}

function decimalField(
  input: Record<string, unknown>,
  field: string,
  label: string
): string {
  const value = stringField(input, field, label);
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(`${label} must be a base-10 integer string`);
  }
  return value;
}

function positiveDecimalField(
  input: Record<string, unknown>,
  field: string,
  label: string
): string {
  const value = decimalField(input, field, label);
  if (value === "0") {
    throw new Error(`${label} must be positive`);
  }
  return value;
}

function optionalDecimalField(
  input: Record<string, unknown>,
  field: string,
  label: string,
  fallback: string
): string {
  if (input[field] === undefined || input[field] === "") {
    return fallback;
  }
  return decimalField(input, field, label);
}

function addressField(
  input: Record<string, unknown>,
  field: string,
  label: string
): Address {
  const value = stringField(input, field, label);
  return normalizeAddress(value, label);
}

function optionalAddressField(
  input: Record<string, unknown>,
  field: string,
  label: string
): Address | undefined {
  const value = input[field];
  if (value === undefined || value === "") {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`${label} must be an address`);
  }
  return normalizeAddress(value, label);
}

function normalizeAddress(value: string, label: string): Address {
  if (!isAddress(value)) {
    throw new Error(`${label} must be an EVM address`);
  }
  return getAddress(value);
}

function optionalBoolean(
  value: unknown,
  fallback: boolean,
  label: string
): boolean {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== "boolean") {
    throw new Error(`${label} must be boolean`);
  }
  return value;
}

function yamlString(value: string): string {
  return JSON.stringify(value);
}
