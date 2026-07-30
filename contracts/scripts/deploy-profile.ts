import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  createPublicClient,
  defineChain,
  getAddress,
  http,
  isAddress,
  isAddressEqual,
  type Abi,
  type Address,
  type PublicClient,
} from "viem";
import { jsonStringify, loadArtifact, parseCLIParams } from "./lib.js";
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
  rpcUrlEnv: string;
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

export type PriceSourceName = "coinmarketcap" | "coingecko" | "chainlink" | "uniswap";

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

const hardhatNetworks = new Map<string, { chainId: number; eid: number }>([
  ["sepolia", { chainId: 11155111, eid: 40161 }],
  ["hoodi", { chainId: 560048, eid: 40449 }],
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
  priceFeed?: Address;
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

export type IgnitionCommandOptions = {
  verify: boolean;
  autoConfirm: boolean;
  buildProfile?: string;
};

type PriceFeedOverrides = Record<string, Address>;
type RPCURLMap = Record<string, string>;
type WorkerStartBlockMap = Record<string, number>;
type LatestBlockNumberReader = (
  chain: ChainProfile,
  rpcURL: string,
) => Promise<bigint>;

export function normalizeProfile(value: unknown): DeploymentProfile {
  const input = object(value, "profile");
  const mode = normalizeMode(input.mode);
  if (Object.hasOwn(input, "minCanaryTokenBalance")) {
    throw new Error(
      "profile.minCanaryTokenBalance is not supported; configure chains[].minCanaryTokenBalance",
    );
  }
  const owner = addressField(input, "owner", "profile.owner");
  const priceFeedSubmitters = normalizeAddressArrayField(
    input,
    "priceFeedSubmitters",
    "profile.priceFeedSubmitters",
  );
  validateLongTermPriceSubmitters(priceFeedSubmitters, owner);
  const initialRecipient =
    optionalAddressField(
      input,
      "initialRecipient",
      "profile.initialRecipient",
    ) ?? owner;
  const canaryTreasury = optionalAddressField(
    input,
    "canaryTreasury",
    "profile.canaryTreasury",
  );
  const services = object(input.services ?? {}, "profile.services");
  const signers = arrayField(input, "signers", "profile.signers").map(
    (signer, index) => normalizeSigner(signer, `profile.signers[${index}]`),
  );
  const signerIDs = new Set(signers.map((signer) => signer.id.toLowerCase()));
  const token = object(input.token ?? {}, "profile.token");
  const pricing = normalizePricingProfile(input.pricing);
  const chains = arrayField(input, "chains", "profile.chains").map(
    (chain, index) =>
      normalizeChain(chain, `profile.chains[${index}]`, signerIDs, mode),
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
      "profile.metricsListenAddress",
    ),
    owner,
    priceFeedSubmitters,
    initialRecipient,
    canaryTreasury,
    minOwnerNativeBalanceWei: optionalDecimalField(
      input,
      "minOwnerNativeBalanceWei",
      "profile.minOwnerNativeBalanceWei",
      "0",
    ),
    minCanaryNativeBalanceWei: optionalDecimalField(
      input,
      "minCanaryNativeBalanceWei",
      "profile.minCanaryNativeBalanceWei",
      "0",
    ),
    dvnMode: normalizeDVNMode(input.dvnMode),
    services: {
      executor: optionalBoolean(
        services.executor,
        true,
        "profile.services.executor",
      ),
      dvn: optionalBoolean(services.dvn, true, "profile.services.dvn"),
    },
    pricing,
    signers,
    token: {
      name: optionalString(
        token.name,
        "Oh My Lazier Test OFT",
        "profile.token.name",
      ),
      symbol: optionalString(token.symbol, "OMLTOFT", "profile.token.symbol"),
    },
    chains,
    pathway: normalizePathway(input.pathway, "profile.pathway"),
  };
}

export function extractOpenWorkerContracts(
  deployedAddresses: unknown,
  chainKey: string,
): OpenWorkerContracts {
  const deployed = object(deployedAddresses, `${chainKey}.deployed_addresses`);
  const openExecutor = moduleAddress(
    deployed,
    "OpenWorkers",
    "OpenExecutor",
    chainKey,
  );
  const openDVN = moduleAddress(deployed, "OpenWorkers", "OpenDVN", chainKey);
  const priceFeed = optionalModuleAddress(
    deployed,
    "OpenWorkers",
    "OpenPriceFeed",
    chainKey,
  );
  return { openExecutor, openDVN, priceFeed };
}

export function extractTestOFTAddress(
  deployedAddresses: unknown,
  chainKey: string,
): Address {
  const deployed = object(deployedAddresses, `${chainKey}.deployed_addresses`);
  return moduleAddress(deployed, "TestOFT", "TestOFT", chainKey);
}

export function buildDeploymentState(input: {
  profile: DeploymentProfile;
  workerDeployedAddresses: Record<string, unknown>;
  testOFTDeployedAddresses?: Record<string, unknown>;
  priceFeedOverrides?: PriceFeedOverrides;
  generatedAt?: string;
}): DeploymentState {
  const chains = input.profile.chains.map((chain) => {
    const workers = extractOpenWorkerContracts(
      input.workerDeployedAddresses[chain.key],
      chain.key,
    );
    const priceFeed =
      workers.priceFeed ?? input.priceFeedOverrides?.[chain.key];
    if (priceFeed === undefined) {
      throw new Error(
        `${chain.key} deployed_addresses.json is missing OpenWorkers#OpenPriceFeed; rerun with ${chain.rpcUrlEnv} set so OpenExecutor.priceFeed() and OpenDVN.priceFeed() can be hydrated`,
      );
    }
    const oapp =
      input.profile.mode === "external-oapp"
        ? requiredOApp(chain)
        : extractTestOFTAddress(
            input.testOFTDeployedAddresses?.[chain.key] ??
              input.workerDeployedAddresses[chain.key],
            chain.key,
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
        priceFeed,
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

export async function readPriceFeedFromWorkers(input: {
  publicClient: PublicClient;
  chainKey: string;
  openExecutor: Address;
  openDVN: Address;
  openExecutorAbi: Abi;
  openDVNAbi: Abi;
}): Promise<Address> {
  const [executorPriceFeed, dvnPriceFeed] = await Promise.all([
    input.publicClient.readContract({
      address: input.openExecutor,
      abi: input.openExecutorAbi,
      functionName: "priceFeed",
    }) as Promise<Address>,
    input.publicClient.readContract({
      address: input.openDVN,
      abi: input.openDVNAbi,
      functionName: "priceFeed",
    }) as Promise<Address>,
  ]);
  if (!isAddressEqual(executorPriceFeed, dvnPriceFeed)) {
    throw new Error(
      `${input.chainKey} OpenExecutor.priceFeed ${executorPriceFeed} does not match OpenDVN.priceFeed ${dvnPriceFeed}`,
    );
  }
  return getAddress(executorPriceFeed);
}

export function openWorkersParameterFile(
  profile: DeploymentProfile,
  _chain: ChainProfile,
) {
  return buildOpenWorkersParameters({
    owner: profile.owner,
    priceFeedSubmitters: priceFeedDeploymentSubmitters(profile),
  });
}

export function testOFTParameterFile(
  profile: DeploymentProfile,
  chain: ChainProfile,
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
  sourceState: ChainDeploymentState,
): Address[] {
  const dvns = [sourceState.workers.openDVN, ...source.externalDVNs];
  if (source.includeLayerZeroLabsDVN) {
    dvns.push(
      requireLayerZeroLabsDVNForLibraries(
        source.layerZero,
        `${source.key}.includeLayerZeroLabsDVN`,
      ),
    );
  }
  return dvns;
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
        input.profile.pathway.priceSnapshot.dstGasPriceInSrcToken,
      ),
      dstDataFeePerByteInSrcToken: BigInt(
        input.profile.pathway.priceSnapshot.dstDataFeePerByteInSrcToken,
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
        workerStartBlock(input.workerStartBlocks, chain),
      ),
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
    lines.push(`  coinmarketcap_base_url: ${yamlString(pricing.coinMarketCapBaseURL)}`);
  }
  if (pricing.coinMarketCapAPIKeyEnv !== undefined) {
    lines.push(`  coinmarketcap_api_key_env: ${pricing.coinMarketCapAPIKeyEnv}`);
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
      `        max_age_seconds: ${sources.coinMarketCap.maxAgeSeconds}`,
    );
  }
  if (sources.coinGecko !== undefined) {
    lines.push(
      "      coingecko:",
      `        id: ${sources.coinGecko.id}`,
      `        max_age_seconds: ${sources.coinGecko.maxAgeSeconds}`,
    );
  }
  if (sources.chainlink !== undefined) {
    lines.push(
      "      chainlink:",
      `        feed_address: "${sources.chainlink.feedAddress}"`,
      `        expected_description: ${yamlString(sources.chainlink.expectedDescription)}`,
      `        max_age_seconds: ${sources.chainlink.maxAgeSeconds}`,
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
      `        min_harmonic_mean_liquidity: "${sources.uniswap.minHarmonicMeanLiquidity}"`,
    );
  }
  return lines.join("\n");
}

function pricingSigner(profile: DeploymentProfile): Address {
  const localSigners = new Set(
    profile.signers.map((signer) => signer.id.toLowerCase()),
  );
  const submitter = profile.priceFeedSubmitters.find((candidate) =>
    localSigners.has(candidate.toLowerCase()),
  );
  if (submitter === undefined) {
    throw new Error(
      "profile.priceFeedSubmitters must include a configured signer for worker pricing",
    );
  }
  return submitter;
}

export function buildCommandPlan(input: {
  profile: DeploymentProfile;
  outDir: string;
  ignition?: Partial<IgnitionCommandOptions>;
}): CommandPlan {
  const commands: PlannedCommand[] = [];
  const ignition = normalizeIgnitionCommandOptions(input.ignition);
  if (input.profile.mode === "test-oft-rehearsal") {
    for (const chain of input.profile.chains) {
      commands.push({
        label: `Deploy ${chain.key} TestOFT rehearsal OApp`,
        command: hardhatCommand(
          chain,
          "npm run deploy:test-oft",
          testOFTParameterPath(input.outDir, chain),
          testOFTDeploymentId(chain),
          ignition,
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
        "npm run deploy:open-workers",
        openWorkersParameterPath(input.outDir, chain),
        chain.deploymentId,
        ignition,
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
        "npm run configure:open-workers-pathway",
        openWorkersPathwayParameterPath(input.outDir, source, destination),
        openWorkersPathwayDeploymentId(source, destination),
        ignition,
      ),
      mutates: true,
      requiresApply: true,
    });
    commands.push({
      label: `Configure ${source.key} OApp/Endpoint for ${destination.key}`,
      command: hardhatCommand(
        source,
        "npm run configure:oapp-endpoint",
        oappEndpointParameterPath(input.outDir, source, destination),
        oappEndpointDeploymentId(source, destination),
        ignition,
      ),
      mutates: true,
      requiresApply: true,
    });
  }
  commands.push({
    label: "Validate generated worker config against live chains",
    command: `go run ./go/cmd/configcheck -config ${path.join(input.outDir, "worker.yaml")} -format json`,
    mutates: false,
    requiresApply: false,
    output: path.join(input.outDir, "artifacts", "configcheck.json"),
  });
  for (const [source, destination] of profileDirections(input.profile)) {
    const direction = directionKey(source, destination);
    commands.push({
      label: `Inspect LayerZero config for ${direction}`,
      command: `${source.rpcUrlEnv}=... npm run inspect:lz-config -- --chain-id ${source.chainId} --endpoint ${source.layerZero.endpointV2} --remote-eid ${destination.eid} --send-uln ${source.layerZero.sendUln302} --receive-uln ${source.layerZero.receiveUln302} --oapp <${source.key} OApp>`,
      mutates: false,
      requiresApply: false,
      output: path.join(
        input.outDir,
        "artifacts",
        `lz-config-${direction}.json`,
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
  ignition?: Partial<IgnitionCommandOptions>;
  priceSnapshotUpdatedAt?: bigint;
  latestBlockNumber?: LatestBlockNumberReader;
}): Promise<void> {
  await writeInitialParameterFiles(input.profile, input.outDir);
  for (const [source, destination] of profileDirections(input.profile)) {
    await writeJSON(
      openWorkersPathwayParameterPath(input.outDir, source, destination),
      openWorkersPathwayParameterFile({
        profile: input.profile,
        state: input.state,
        source,
        destination,
        priceSnapshotUpdatedAt: input.priceSnapshotUpdatedAt,
      }),
    );
    await writeJSON(
      oappEndpointParameterPath(input.outDir, source, destination),
      oappEndpointParameterFile({
        profile: input.profile,
        state: input.state,
        source,
        destination,
        priceSnapshotUpdatedAt: input.priceSnapshotUpdatedAt,
      }),
    );
  }
  await writeJSON(
    path.join(input.outDir, "deployment-state.json"),
    input.state,
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
    }),
  );
  const commands = buildCommandPlan({
    profile: input.profile,
    outDir: input.outDir,
    ignition: input.ignition,
  });
  await writeJSON(path.join(input.outDir, "commands.json"), commands);
  await writeFile(
    path.join(input.outDir, "commands.md"),
    renderCommands(commands),
  );
}

export async function resolveWorkerStartBlocks(input: {
  profile: DeploymentProfile;
  rpcUrls: RPCURLMap;
  latestBlockNumber?: LatestBlockNumberReader;
}): Promise<WorkerStartBlockMap> {
  const latestBlockNumber = input.latestBlockNumber ?? readLatestBlockNumber;
  const workerStartBlocks: WorkerStartBlockMap = {};
  for (const chain of input.profile.chains) {
    if (chain.startBlockNumber !== undefined) {
      workerStartBlocks[chain.key] = chain.startBlockNumber;
      continue;
    }
    const rpcURL = input.rpcUrls[chain.key];
    if (rpcURL === undefined) {
      throw new Error(
        `${chain.key} RPC URL is required to resolve start_block_number`,
      );
    }
    const latest = await latestBlockNumber(chain, rpcURL);
    workerStartBlocks[chain.key] = safeBlockNumber(
      latest,
      `${chain.key} latest block number`,
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
  outDir: string,
): Promise<void> {
  await mkdir(path.join(outDir, "ignition", "parameters"), { recursive: true });
  for (const chain of profile.chains) {
    await writeJSON(
      openWorkersParameterPath(outDir, chain),
      openWorkersParameterFile(profile, chain),
    );
    if (profile.mode === "test-oft-rehearsal") {
      await writeJSON(
        testOFTParameterPath(outDir, chain),
        testOFTParameterFile(profile, chain),
      );
    }
  }
}

async function main() {
  const flags = parseCLIParams(process.argv.slice(2));
  const profilePath =
    flags.get("profile") ?? "config/deployments/template.json";
  const outDir = flags.get("out") ?? "tmp/deploy-profile";
  const phase = normalizePhase(flags.get("phase") ?? "render");
  const apply = flagEnabled(flags.get("apply"));
  const ignition = parseIgnitionCommandOptions(flags);
  const profile = normalizeProfile(
    JSON.parse(await readFile(profilePath, "utf8")),
  );

  await writeInitialParameterFiles(profile, outDir);
  const commands = buildCommandPlan({ profile, outDir, ignition });
  await writeJSON(path.join(outDir, "commands.json"), commands);
  await writeFile(path.join(outDir, "commands.md"), renderCommands(commands));

  if (phase === "deploy-test-oft") {
    requireRehearsalMode(profile, phase);
    if (apply) {
      runDeployTestOFT(profile, outDir, ignition);
    }
    printSummary(phase, apply, outDir, profile);
    return;
  }

  if (phase === "deploy-workers") {
    if (apply) {
      runDeployWorkers(profile, outDir, ignition);
    }
    printSummary(phase, apply, outDir, profile);
    return;
  }

  if (phase === "all" && apply) {
    if (profile.mode === "test-oft-rehearsal") {
      runDeployTestOFT(profile, outDir, ignition);
    }
    runDeployWorkers(profile, outDir, ignition);
  }

  let state: DeploymentState;
  try {
    state = await loadDeploymentState(profile);
  } catch (error) {
    if (phase === "render" && isBootstrapStateUnavailable(error)) {
      await writeBootstrapRenderStatus(outDir, error);
      printSummary(phase, apply, outDir, profile, { deploymentState: false });
      return;
    }
    throw error;
  }
  const rpcUrls = resolveRPCURLs(profile);
  await writeRenderedDeployment({ profile, state, outDir, rpcUrls, ignition });

  if ((phase === "configure-workers" || phase === "all") && apply) {
    runConfigureWorkers(profile, outDir, ignition);
  }
  if (shouldRunConfigureOApp(profile, phase, apply)) {
    runConfigureOApp(profile, outDir, ignition);
  }
  if (phase === "verify" || (phase === "all" && apply)) {
    runVerify(profile, state, outDir, rpcUrls, {
      workerOnly: shouldRunWorkerOnlyVerify(profile, phase),
    });
  }
  printSummary(phase, apply, outDir, profile);
}

export function shouldRunConfigureOApp(
  profile: DeploymentProfile,
  phase: DeploymentPhase,
  apply: boolean,
): boolean {
  return (
    apply &&
    (phase === "configure-oapp" ||
      (phase === "all" && profile.mode === "test-oft-rehearsal"))
  );
}

export function shouldRunWorkerOnlyVerify(
  profile: DeploymentProfile,
  phase: DeploymentPhase,
): boolean {
  return phase === "all" && profile.mode === "external-oapp";
}

export function isBootstrapStateUnavailable(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  return (
    isMissingDeployedAddressesFile(error) ||
    /deployed_addresses\.json is missing (OpenWorkers#OpenExecutor|OpenWorkers#OpenDVN|TestOFT#TestOFT)/.test(
      error.message,
    )
  );
}

async function loadDeploymentState(
  profile: DeploymentProfile,
): Promise<DeploymentState> {
  const workerDeployedAddresses: Record<string, unknown> = {};
  const testOFTDeployedAddresses: Record<string, unknown> = {};
  const overrides: PriceFeedOverrides = {};
  let workerArtifacts:
    | {
        openExecutorAbi: Abi;
        openDVNAbi: Abi;
      }
    | undefined;
  for (const chain of profile.chains) {
    const workerRaw = JSON.parse(
      await readFile(workerDeploymentAddressPath(chain), "utf8"),
    );
    workerDeployedAddresses[chain.key] = workerRaw;
    if (profile.mode === "test-oft-rehearsal") {
      testOFTDeployedAddresses[chain.key] = JSON.parse(
        await readFile(testOFTDeploymentAddressPath(chain), "utf8"),
      );
    }
    const contracts = extractOpenWorkerContracts(workerRaw, chain.key);
    if (contracts.priceFeed === undefined) {
      workerArtifacts ??= {
        openExecutorAbi: loadArtifact(
          "contracts/artifacts/contracts/contracts/workers/OpenExecutor.sol/OpenExecutor.json",
        ).abi,
        openDVNAbi: loadArtifact(
          "contracts/artifacts/contracts/contracts/workers/OpenDVN.sol/OpenDVN.json",
        ).abi,
      };
      const rpcURL = requiredProcessEnv(chain.rpcUrlEnv);
      overrides[chain.key] = await readPriceFeedFromWorkers({
        publicClient: publicClientFor(chain, rpcURL),
        chainKey: chain.key,
        openExecutor: contracts.openExecutor,
        openDVN: contracts.openDVN,
        openExecutorAbi: workerArtifacts.openExecutorAbi,
        openDVNAbi: workerArtifacts.openDVNAbi,
      });
    }
  }
  return buildDeploymentState({
    profile,
    workerDeployedAddresses,
    testOFTDeployedAddresses,
    priceFeedOverrides: overrides,
  });
}

function runDeployTestOFT(
  profile: DeploymentProfile,
  outDir: string,
  ignition: IgnitionCommandOptions,
) {
  requireRehearsalMode(profile, "deploy-test-oft");
  for (const chain of profile.chains) {
    runHardhatIgnition({
      label: `deploy ${chain.key} TestOFT`,
      script: "deploy:test-oft",
      chain,
      parametersPath: testOFTParameterPath(outDir, chain),
      deploymentId: testOFTDeploymentId(chain),
      ignition,
    });
  }
}

function runDeployWorkers(
  profile: DeploymentProfile,
  outDir: string,
  ignition: IgnitionCommandOptions,
) {
  for (const chain of profile.chains) {
    runHardhatIgnition({
      label: `deploy ${chain.key} OpenWorkers`,
      script: "deploy:open-workers",
      chain,
      parametersPath: openWorkersParameterPath(outDir, chain),
      deploymentId: chain.deploymentId,
      ignition,
    });
  }
}

function runConfigureWorkers(
  profile: DeploymentProfile,
  outDir: string,
  ignition: IgnitionCommandOptions,
) {
  for (const [source, destination] of profileDirections(profile)) {
    runHardhatIgnition({
      label: `configure ${source.key} OpenWorkers for ${destination.key}`,
      script: "configure:open-workers-pathway",
      chain: source,
      parametersPath: openWorkersPathwayParameterPath(
        outDir,
        source,
        destination,
      ),
      deploymentId: openWorkersPathwayDeploymentId(source, destination),
      ignition,
    });
  }
}

function runConfigureOApp(
  profile: DeploymentProfile,
  outDir: string,
  ignition: IgnitionCommandOptions,
) {
  for (const [source, destination] of profileDirections(profile)) {
    runHardhatIgnition({
      label: `configure ${source.key} OApp/Endpoint for ${destination.key}`,
      script: "configure:oapp-endpoint",
      chain: source,
      parametersPath: oappEndpointParameterPath(outDir, source, destination),
      deploymentId: oappEndpointDeploymentId(source, destination),
      ignition,
    });
  }
}

function runVerify(
  profile: DeploymentProfile,
  state: DeploymentState,
  outDir: string,
  rpcUrls: RPCURLMap,
  options: { workerOnly: boolean },
) {
  const artifactDir = path.join(outDir, "artifacts");
  if (profile.mode === "test-oft-rehearsal") {
    for (const chain of profile.chains) {
      const current = chainState(state, chain.key);
      runCommand({
        label: `deployment preflight ${chain.key}`,
        command: "npm",
        args: deploymentPreflightArgs({
          profile,
          chain,
          current,
          rpcURL: rpcUrls[chain.key],
        }),
        outputPath: path.join(
          artifactDir,
          `deployment-preflight-${chain.key}.json`,
        ),
      });
    }
  }
  for (const [source, destination] of profileDirections(profile)) {
    const direction = directionKey(source, destination);
    const sourceState = chainState(state, source.key);
    if (!options.workerOnly) {
      runCommand({
        label: `inspect lz config ${direction}`,
        command: "npm",
        args: [
          "run",
          "--silent",
          "inspect:lz-config",
          "--",
          "--rpc-url",
          rpcUrls[source.key],
          "--chain-id",
          String(source.chainId),
          "--endpoint",
          sourceState.endpoint,
          "--oapp",
          sourceState.oapp,
          "--remote-eid",
          String(destination.eid),
          "--send-uln",
          sourceState.sendUln,
          "--receive-uln",
          sourceState.receiveUln,
        ],
        outputPath: path.join(artifactDir, `lz-config-${direction}.json`),
      });
    }
    runCommand({
      label: `price config ${direction}`,
      command: "npm",
      args: [
        "run",
        "--silent",
        "check:price-config",
        "--",
        "--rpc-url",
        rpcUrls[source.key],
        "--chain-id",
        String(source.chainId),
        "--dst-eid",
        String(destination.eid),
        "--max-price-age-seconds",
        profile.pathway.priceSnapshot.maxAgeSeconds,
        "--expected-stale-after",
        profile.pathway.priceSnapshot.staleAfter,
        "--price-feed",
        sourceState.workers.priceFeed,
        "--open-executor",
        sourceState.workers.openExecutor,
        "--open-dvn",
        sourceState.workers.openDVN,
      ],
      outputPath: path.join(artifactDir, `price-config-${direction}.json`),
    });
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

export function deploymentPreflightArgs(input: {
  profile: DeploymentProfile;
  chain: ChainProfile;
  current: ChainDeploymentState;
  rpcURL: string;
}): string[] {
  return [
    "run",
    "--silent",
    "check:deployment-preflight",
    "--",
    "--rpc-url",
    input.rpcURL,
    "--chain-id",
    String(input.chain.chainId),
    "--test-oft",
    input.current.oapp,
    "--open-executor",
    input.current.workers.openExecutor,
    "--open-dvn",
    input.current.workers.openDVN,
    "--expected-owner",
    input.profile.owner,
    "--min-owner-native-balance",
    input.profile.minOwnerNativeBalanceWei,
    ...(input.profile.canaryTreasury === undefined
      ? []
      : [
          "--canary-treasury",
          input.profile.canaryTreasury,
          "--min-canary-native-balance",
          input.profile.minCanaryNativeBalanceWei,
          "--min-canary-token-balance",
          input.chain.minCanaryTokenBalance,
        ]),
    "--expected-total-supply",
    input.chain.initialSupply,
  ];
}

function runHardhatIgnition(input: {
  label: string;
  script: string;
  chain: ChainProfile;
  parametersPath: string;
  deploymentId: string;
  ignition: IgnitionCommandOptions;
}) {
  runCommand({
    label: input.label,
    command: "npm",
    args: [
      "run",
      "--silent",
      input.script,
      "--",
      ...hardhatIgnitionArgs(input),
    ],
    env: hardhatEnv(input.chain, input.ignition),
    stdio: "inherit",
  });
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
      `${input.label} cannot capture output with inherited stdio`,
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

function hardhatEnv(
  chain: ChainProfile,
  ignition?: IgnitionCommandOptions,
): NodeJS.ProcessEnv {
  const env = { ...process.env };
  const rpcURL = requiredProcessEnv(chain.rpcUrlEnv);
  env[chain.rpcUrlEnv] = rpcURL;
  const prefix = chain.network.toUpperCase().replace(/[^A-Z0-9]/g, "_");
  env[`${prefix}_RPC_URL`] = rpcURL;
  if (ignition?.autoConfirm) {
    env.HARDHAT_IGNITION_CONFIRM_DEPLOYMENT = "true";
    env.HARDHAT_IGNITION_CONFIRM_RESET = "true";
  }
  return env;
}

function resolveRPCURLs(profile: DeploymentProfile): RPCURLMap {
  return Object.fromEntries(
    profile.chains.map((chain) => [
      chain.key,
      requiredProcessEnv(chain.rpcUrlEnv),
    ]),
  );
}

function publicClientFor(chain: ChainProfile, rpcURL: string): PublicClient {
  const viemChain = defineChain({
    id: chain.chainId,
    name: chain.name,
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: { default: { http: [rpcURL] } },
  });
  return createPublicClient({ chain: viemChain, transport: http(rpcURL) });
}

async function readLatestBlockNumber(
  chain: ChainProfile,
  rpcURL: string,
): Promise<bigint> {
  return publicClientFor(chain, rpcURL).getBlockNumber();
}

function normalizeSigner(value: unknown, pathLabel: string): SignerProfile {
  const input = object(value, pathLabel);
  const id = addressField(input, "id", `${pathLabel}.id`);
  const type = stringField(input, "type", `${pathLabel}.type`);
  if (type === "keystore") {
    const keystore = object(input.keystore, `${pathLabel}.keystore`);
    const passwordEnv = optionalEnvVarName(
      keystore.passwordEnv,
      `${pathLabel}.keystore.passwordEnv`,
    );
    const passwordFile = optionalStringValue(
      keystore.passwordFile,
      `${pathLabel}.keystore.passwordFile`,
    );
    if ((passwordEnv === undefined) === (passwordFile === undefined)) {
      throw new Error(
        `${pathLabel}.keystore must configure exactly one passwordEnv or passwordFile`,
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
    const kms = object(input.kms, `${pathLabel}.kms`);
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
          `${pathLabel}.kms.endpoint`,
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
  mode: DeploymentMode,
): ChainProfile {
  const input = object(value, pathLabel);
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
      `${pathLabel}.privateKeyEnv is not supported; store Hardhat private key config variables with hardhat keystore`,
    );
  }
  validateHardhatNetworkChain(pathLabel, network, chainID, eid);
  const layerZero = normalizeLayerZero(input.layerZero, pathLabel, eid, chainID);
  validateLayerZeroChain(pathLabel, eid, chainID, layerZero);
  const includeLayerZeroLabsDVN = optionalBoolean(
    input.includeLayerZeroLabsDVN,
    false,
    `${pathLabel}.includeLayerZeroLabsDVN`,
  );
  if (includeLayerZeroLabsDVN) {
    requireLayerZeroLabsDVNForLibraries(
      layerZero,
      `${pathLabel}.includeLayerZeroLabsDVN`,
    );
  }
  return {
    key,
    network,
    name: stringField(input, "name", `${pathLabel}.name`),
    nativeAssetId: normalizeNativeAssetID(
      input.nativeAssetId,
      `${pathLabel}.nativeAssetId`,
    ),
    priceSources: normalizePriceSources(
      input.priceSources,
      `${pathLabel}.priceSources`,
    ),
    eid,
    chainId: chainID,
    rpcUrlEnv: envVarNameField(input, "rpcUrlEnv", `${pathLabel}.rpcUrlEnv`),
    deploymentId: stringField(
      input,
      "deploymentId",
      `${pathLabel}.deploymentId`,
    ),
    testOFTDeploymentId: optionalStringValue(
      input.testOFTDeploymentId,
      `${pathLabel}.testOFTDeploymentId`,
    ),
    oapp,
    initialSupply: optionalDecimalField(
      input,
      "initialSupply",
      `${pathLabel}.initialSupply`,
      "0",
    ),
    minCanaryTokenBalance:
      mode === "test-oft-rehearsal"
        ? decimalField(
            input,
            "minCanaryTokenBalance",
            `${pathLabel}.minCanaryTokenBalance`,
          )
        : optionalDecimalField(
            input,
            "minCanaryTokenBalance",
            `${pathLabel}.minCanaryTokenBalance`,
            "0",
          ),
    confirmations: integerField(
      input,
      "confirmations",
      `${pathLabel}.confirmations`,
    ),
    startBlockNumber: optionalIntegerField(
      input,
      "startBlockNumber",
      `${pathLabel}.startBlockNumber`,
      { allowZero: true },
    ),
    indexerQueryBlockRange: integerField(
      input,
      "indexerQueryBlockRange",
      `${pathLabel}.indexerQueryBlockRange`,
    ),
    indexerPollIntervalSeconds: integerField(
      input,
      "indexerPollIntervalSeconds",
      `${pathLabel}.indexerPollIntervalSeconds`,
      { max: maxDurationSeconds },
    ),
    externalDVNs: optionalAddressArrayField(
      input,
      "externalDVNs",
      `${pathLabel}.externalDVNs`,
    ),
    includeLayerZeroLabsDVN,
    pricingTxPolicy: normalizeTxPolicy(
      input.pricingTxPolicy,
      `${pathLabel}.pricingTxPolicy`,
    ),
    txRoles: normalizeTxRoles(input.txRoles, `${pathLabel}.txRoles`, signerIDs),
    layerZero,
  };
}

function normalizePricingProfile(value: unknown): PricingProfile {
  const input = object(value ?? {}, "profile.pricing");
  const coinMarketCapAPIKeyEnv = optionalEnvVarName(
    input.coinMarketCapAPIKeyEnv,
    "profile.pricing.coinMarketCapAPIKeyEnv",
  );
  const coinGeckoAPIKeyEnv = optionalEnvVarName(
    input.coinGeckoAPIKeyEnv,
    "profile.pricing.coinGeckoAPIKeyEnv",
  );
  return {
    sourceRequestTimeoutSeconds:
      optionalIntegerField(
        input,
        "sourceRequestTimeoutSeconds",
        "profile.pricing.sourceRequestTimeoutSeconds",
        { max: maxDurationSeconds },
      ) ?? 10,
    maxDeviationBps:
      optionalIntegerField(
        input,
        "maxDeviationBps",
        "profile.pricing.maxDeviationBps",
      ) ?? 500,
    coinMarketCapBaseURL: optionalMarketDataBaseURL(
      input.coinMarketCapBaseURL,
      "profile.pricing.coinMarketCapBaseURL",
    ),
    coinMarketCapAPIKeyEnv,
    coinGeckoBaseURL: optionalMarketDataBaseURL(
      input.coinGeckoBaseURL,
      "profile.pricing.coinGeckoBaseURL",
    ),
    coinGeckoAPIKeyEnv,
  };
}

function normalizePriceSources(
  value: unknown,
  label: string,
): PriceSourcesProfile | undefined {
  if (value === undefined) {
    return undefined;
  }
  const input = object(value, label);
  const primarySource = normalizePrimaryPriceSource(
    input.primarySource,
    `${label}.primarySource`,
  );
  const sanitySources = normalizeSanityPriceSources(
    input.sanitySources,
    `${label}.sanitySources`,
  );
  if (sanitySources.includes(primarySource)) {
    throw new Error(`${label}.sanitySources must not contain primarySource`);
  }
  const result: PriceSourcesProfile = {
    primarySource,
    sanitySources,
    coinMarketCap: normalizeOptionalCoinMarketCapSource(
      input.coinMarketCap,
      `${label}.coinMarketCap`,
    ),
    coinGecko: normalizeOptionalCoinGeckoSource(
      input.coinGecko,
      `${label}.coinGecko`,
    ),
    chainlink: normalizeOptionalChainlinkSource(
      input.chainlink,
      `${label}.chainlink`,
    ),
    uniswap: normalizeOptionalUniswapSource(
      input.uniswap,
      `${label}.uniswap`,
    ),
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
      throw new Error(`${label}.${source} is required when source is referenced`);
    }
    if (!referenced.has(source) && configured) {
      throw new Error(`${label}.${source} is configured but not referenced`);
    }
  }
  return result;
}

function normalizePrimaryPriceSource(
  value: unknown,
  label: string,
): Exclude<PriceSourceName, "uniswap"> {
  if (value === "coinmarketcap" || value === "coingecko" || value === "chainlink") {
    return value;
  }
  throw new Error(`${label} must be coinmarketcap, coingecko, or chainlink`);
}

function normalizeSanityPriceSources(
  value: unknown,
  label: string,
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
  label: string,
): PriceSourcesProfile["coinMarketCap"] {
  if (value === undefined) return undefined;
  const input = object(value, label);
  return {
    id: integerField(input, "id", `${label}.id`),
    maxAgeSeconds: integerField(
      input,
      "maxAgeSeconds",
      `${label}.maxAgeSeconds`,
      { max: maxDurationSeconds },
    ),
  };
}

function normalizeOptionalCoinGeckoSource(
  value: unknown,
  label: string,
): PriceSourcesProfile["coinGecko"] {
  if (value === undefined) return undefined;
  const input = object(value, label);
  return {
    id: stringField(input, "id", `${label}.id`),
    maxAgeSeconds: integerField(
      input,
      "maxAgeSeconds",
      `${label}.maxAgeSeconds`,
      { max: maxDurationSeconds },
    ),
  };
}

function normalizeOptionalChainlinkSource(
  value: unknown,
  label: string,
): PriceSourcesProfile["chainlink"] {
  if (value === undefined) return undefined;
  const input = object(value, label);
  return {
    feedAddress: addressField(input, "feedAddress", `${label}.feedAddress`),
    expectedDescription: stringField(
      input,
      "expectedDescription",
      `${label}.expectedDescription`,
    ),
    maxAgeSeconds: integerField(
      input,
      "maxAgeSeconds",
      `${label}.maxAgeSeconds`,
      { max: maxDurationSeconds },
    ),
  };
}

function normalizeOptionalUniswapSource(
  value: unknown,
  label: string,
): PriceSourcesProfile["uniswap"] {
  if (value === undefined) return undefined;
  const input = object(value, label);
  const tokenIn = addressField(input, "tokenIn", `${label}.tokenIn`);
  const tokenOut = addressField(input, "tokenOut", `${label}.tokenOut`);
  if (isAddressEqual(tokenIn, tokenOut)) {
    throw new Error(`${label}.tokenIn and tokenOut must differ`);
  }
  const twapWindowSeconds = integerField(
    input,
    "twapWindowSeconds",
    `${label}.twapWindowSeconds`,
  );
  if (
    twapWindowSeconds < minUniswapTWAPWindowSeconds ||
    twapWindowSeconds > maxUniswapTWAPWindowSeconds
  ) {
    throw new Error(
      `${label}.twapWindowSeconds must be between ${minUniswapTWAPWindowSeconds} and ${maxUniswapTWAPWindowSeconds}`,
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
      { max: maxDurationSeconds },
    ),
    minHarmonicMeanLiquidity: positiveDecimalField(
      input,
      "minHarmonicMeanLiquidity",
      `${label}.minHarmonicMeanLiquidity`,
    ),
  };
}

function sourceProfile(
  sources: PriceSourcesProfile,
  source: PriceSourceName,
) {
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
  pricing: PricingProfile,
) {
  const crossAsset = chains[0].nativeAssetId !== chains[1].nativeAssetId;
  for (const chain of chains) {
    if (crossAsset && chain.priceSources === undefined) {
      throw new Error(`${chain.key}.priceSources is required for cross-asset pricing`);
    }
    if (!crossAsset && chain.priceSources !== undefined) {
      throw new Error(`${chain.key}.priceSources must be omitted for same-native pricing`);
    }
    if (
      chain.priceSources?.coinMarketCap !== undefined &&
      pricing.coinMarketCapAPIKeyEnv === undefined
    ) {
      throw new Error(
        "profile.pricing.coinMarketCapAPIKeyEnv is required when CoinMarketCap is referenced",
      );
    }
  }
}

function validateHardhatNetworkChain(
  pathLabel: string,
  network: string,
  chainID: number,
  eid: number,
): void {
  const expected = hardhatNetworks.get(network);
  if (expected === undefined) {
    return;
  }
  if (expected.chainId !== chainID) {
    throw new Error(
      `${pathLabel}.network ${network} uses chainId ${expected.chainId}, but ${pathLabel}.chainId is ${chainID}`,
    );
  }
  if (expected.eid !== eid) {
    throw new Error(
      `${pathLabel}.network ${network} uses eid ${expected.eid}, but ${pathLabel}.eid is ${eid}`,
    );
  }
}

function validateLayerZeroChain(
  pathLabel: string,
  eid: number,
  chainID: number,
  layerZero: LayerZeroAddresses,
): void {
  if (Number(layerZero.eid) !== eid) {
    throw new Error(
      `${pathLabel}.layerZero.eid ${layerZero.eid} does not match ${pathLabel}.eid ${eid}`,
    );
  }
  if (layerZero.nativeChainId !== chainID) {
    throw new Error(
      `${pathLabel}.layerZero.nativeChainId ${layerZero.nativeChainId} does not match ${pathLabel}.chainId ${chainID}`,
    );
  }
}

function normalizeLayerZero(
  value: unknown,
  pathLabel: string,
  eid: number,
  chainID: number,
): LayerZeroAddresses {
  if (value !== undefined) {
    const input = object(value, `${pathLabel}.layerZero`);
    if (Object.hasOwn(input, "layerZeroLabsDVN")) {
      throw new Error(
        `${pathLabel}.layerZero.layerZeroLabsDVN is not supported; configure ${pathLabel}.externalDVNs or ${pathLabel}.includeLayerZeroLabsDVN instead`,
      );
    }
    return {
      chainKey: optionalStringValue(
        input.chainKey,
        `${pathLabel}.layerZero.chainKey`,
      ),
      nativeChainId: chainID,
      eid: String(eid),
      endpointV2: addressField(
        input,
        "endpointV2",
        `${pathLabel}.layerZero.endpointV2`,
      ),
      sendUln302: addressField(
        input,
        "sendUln302",
        `${pathLabel}.layerZero.sendUln302`,
      ),
      receiveUln302: addressField(
        input,
        "receiveUln302",
        `${pathLabel}.layerZero.receiveUln302`,
      ),
    };
  }
  const layerZero = expectedLayerZeroChains.find(
    (chain) => Number(chain.eid) === eid && chain.nativeChainId === chainID,
  );
  if (layerZero === undefined) {
    throw new Error(
      `${pathLabel}.layerZero is required when EID ${eid} and chainId ${chainID} are not in repo metadata`,
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
  signerIDs: ReadonlySet<string>,
) {
  const roles = object(value, pathLabel);
  return {
    executor: normalizeTxRole(
      roles.executor,
      `${pathLabel}.executor`,
      signerIDs,
    ),
    dvn: normalizeTxRole(roles.dvn, `${pathLabel}.dvn`, signerIDs),
  };
}

function normalizeTxRole(
  value: unknown,
  pathLabel: string,
  signerIDs: ReadonlySet<string>,
): TxRoleProfile {
  const role = object(value, pathLabel);
  const signer = addressField(role, "signer", `${pathLabel}.signer`);
  if (!signerIDs.has(signer.toLowerCase())) {
    throw new Error(`${pathLabel}.signer must reference a configured signer`);
  }
  return { signer, ...normalizeTxPolicy(role, pathLabel) };
}

function normalizeTxPolicy(value: unknown, pathLabel: string): TxPolicyProfile {
  const policy = object(value, pathLabel);
  const maxFeePerGasWei = positiveDecimalField(
    policy,
    "maxFeePerGasWei",
    `${pathLabel}.maxFeePerGasWei`,
  );
  const maxPriorityFeePerGasWei = positiveDecimalField(
    policy,
    "maxPriorityFeePerGasWei",
    `${pathLabel}.maxPriorityFeePerGasWei`,
  );
  if (BigInt(maxPriorityFeePerGasWei) > BigInt(maxFeePerGasWei)) {
    throw new Error(
      `${pathLabel}.maxPriorityFeePerGasWei must not exceed maxFeePerGasWei`,
    );
  }
  const minNativeBalanceWei = positiveDecimalField(
    policy,
    "minNativeBalanceWei",
    `${pathLabel}.minNativeBalanceWei`,
  );
  return {
    maxFeePerGasWei,
    maxPriorityFeePerGasWei,
    minNativeBalanceWei,
  };
}

function normalizePathway(value: unknown, pathLabel: string): PathwayProfile {
  const input = object(value, pathLabel);
  const priceSnapshot = object(
    input.priceSnapshot,
    `${pathLabel}.priceSnapshot`,
  );
  const staleAfter = positiveDecimalField(
    priceSnapshot,
    "staleAfter",
    `${pathLabel}.priceSnapshot.staleAfter`,
  );
  if (BigInt(staleAfter) > maxPriceSnapshotStaleAfter) {
    throw new Error(
      `${pathLabel}.priceSnapshot.staleAfter must not exceed ${maxPriceSnapshotStaleAfter}`,
    );
  }
  const enforcedLzReceiveGas = decimalField(
    input,
    "enforcedLzReceiveGas",
    `${pathLabel}.enforcedLzReceiveGas`,
  );
  const minLzReceiveGas = decimalField(
    input,
    "minLzReceiveGas",
    `${pathLabel}.minLzReceiveGas`,
  );
  const maxLzReceiveGas = decimalField(
    input,
    "maxLzReceiveGas",
    `${pathLabel}.maxLzReceiveGas`,
  );
  if (BigInt(maxLzReceiveGas) === 0n) {
    throw new Error(`${pathLabel}.maxLzReceiveGas must be positive`);
  }
  if (BigInt(minLzReceiveGas) > BigInt(maxLzReceiveGas)) {
    throw new Error(
      `${pathLabel}.minLzReceiveGas must not exceed ${pathLabel}.maxLzReceiveGas`,
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
      `${pathLabel}.enforcedLzReceiveGas must be within [${pathLabel}.minLzReceiveGas, ${pathLabel}.maxLzReceiveGas]`,
    );
  }
  return {
    maxMessageSize: integerField(
      input,
      "maxMessageSize",
      `${pathLabel}.maxMessageSize`,
    ),
    enforcedLzReceiveGas,
    minLzReceiveGas,
    maxLzReceiveGas,
    priceSnapshot: {
      dstGasPriceInSrcToken: decimalField(
        priceSnapshot,
        "dstGasPriceInSrcToken",
        `${pathLabel}.priceSnapshot.dstGasPriceInSrcToken`,
      ),
      dstDataFeePerByteInSrcToken: decimalField(
        priceSnapshot,
        "dstDataFeePerByteInSrcToken",
        `${pathLabel}.priceSnapshot.dstDataFeePerByteInSrcToken`,
      ),
      staleAfter,
      maxAgeSeconds: decimalField(
        priceSnapshot,
        "maxAgeSeconds",
        `${pathLabel}.priceSnapshot.maxAgeSeconds`,
      ),
    },
    executorFee: normalizeWorkerFee(
      input.executorFee,
      `${pathLabel}.executorFee`,
    ),
    dvnFee: normalizeWorkerFee(input.dvnFee, `${pathLabel}.dvnFee`),
  };
}

function normalizeWorkerFee(
  value: unknown,
  pathLabel: string,
): WorkerFeeProfile {
  const input = object(value, pathLabel);
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
      `${pathLabel}.dstGasOverhead`,
    ),
    dataSizeOverheadBytes: decimalField(
      input,
      "dataSizeOverheadBytes",
      `${pathLabel}.dataSizeOverheadBytes`,
    ),
    marginBps,
  };
}

function moduleAddress(
  deployed: Record<string, unknown>,
  moduleID: string,
  contract: string,
  chainKey: string,
): Address {
  const value = optionalModuleAddress(deployed, moduleID, contract, chainKey);
  if (value === undefined) {
    throw new Error(
      `${chainKey} deployed_addresses.json is missing ${moduleID}#${contract}`,
    );
  }
  return value;
}

function optionalModuleAddress(
  deployed: Record<string, unknown>,
  moduleID: string,
  contract: string,
  chainKey: string,
): Address | undefined {
  const key = `${moduleID}#${contract}`;
  const value = deployed[key];
  if (value === undefined) {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`${chainKey} deployed address ${key} must be a string`);
  }
  return normalizeAddress(value, `${chainKey}.${key}`);
}

function deploymentDirections(
  chains: readonly ChainDeploymentState[],
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
  destination: ChainDeploymentState,
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
        : `      password_file: ${yamlString(signer.keystore.passwordFile ?? "")}`;
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
  startBlockNumber: number,
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
        max_priority_fee_per_gas_wei: "${chain.txRoles.executor.maxPriorityFeePerGasWei}"
        min_native_balance_wei: "${chain.txRoles.executor.minNativeBalanceWei}"
      dvn:
        signer: "${chain.txRoles.dvn.signer}"
        max_fee_per_gas_wei: "${chain.txRoles.dvn.maxFeePerGasWei}"
        max_priority_fee_per_gas_wei: "${chain.txRoles.dvn.maxPriorityFeePerGasWei}"
        min_native_balance_wei: "${chain.txRoles.dvn.minNativeBalanceWei}"`;
}

function workerStartBlock(
  workerStartBlocks: WorkerStartBlockMap,
  chain: ChainProfile,
): number {
  const startBlockNumber = workerStartBlocks[chain.key];
  if (!Number.isInteger(startBlockNumber) || startBlockNumber < 0) {
    throw new Error(
      `${chain.key} start_block_number must be a non-negative integer`,
    );
  }
  return startBlockNumber;
}

function renderWorkerPathway(
  profile: DeploymentProfile,
  direction: DeploymentDirectionState,
): string {
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
    "State-changing commands are not executed unless `--apply` is supplied to `npm run deploy:profile`.",
    "",
  ];
  for (const command of plan.commands) {
    lines.push(
      `## ${command.label}`,
      "",
      "```bash",
      command.command,
      "```",
      "",
    );
    if (command.output !== undefined) {
      lines.push(`Output: \`${command.output}\``, "");
    }
  }
  return `${lines.join("\n")}\n`;
}

function profileDirections(
  profile: DeploymentProfile,
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
    `${chain.key}.open-workers.json`,
  );
}

function testOFTParameterPath(outDir: string, chain: ChainProfile): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${chain.key}.test-oft.json`,
  );
}

function openWorkersPathwayParameterPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile,
): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${directionKey(source, destination)}.open-workers-pathway.json`,
  );
}

function oappEndpointParameterPath(
  outDir: string,
  source: ChainProfile,
  destination: ChainProfile,
): string {
  return path.join(
    outDir,
    "ignition",
    "parameters",
    `${directionKey(source, destination)}.oapp-endpoint.json`,
  );
}

export function workerDeploymentAddressPath(chain: ChainProfile): string {
  return ignitionDeploymentAddressPath(chain.deploymentId);
}

export function testOFTDeploymentAddressPath(chain: ChainProfile): string {
  return ignitionDeploymentAddressPath(testOFTDeploymentId(chain));
}

function ignitionDeploymentAddressPath(deploymentId: string): string {
  return path.join(
    "ignition",
    "deployments",
    deploymentId,
    "deployed_addresses.json",
  );
}

function isMissingDeployedAddressesFile(error: Error): boolean {
  const code = (error as NodeJS.ErrnoException).code;
  const missingPath = (error as NodeJS.ErrnoException).path;
  if (code !== "ENOENT" || typeof missingPath !== "string") {
    return false;
  }
  const normalized = missingPath.split(path.sep).join("/");
  return (
    normalized.endsWith("/deployed_addresses.json") &&
    normalized.includes("ignition/deployments/")
  );
}

async function writeJSON(filePath: string, value: unknown): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${jsonStringify(value)}\n`);
}

async function writeBootstrapRenderStatus(
  outDir: string,
  error: unknown,
): Promise<void> {
  await writeJSON(path.join(outDir, "render-status.json"), {
    ok: true,
    deploymentState: false,
    message:
      "Bootstrap render wrote initial Ignition parameters and command plan only. Run deploy-test-oft/deploy-workers, then run render again to produce pathway parameters, deployment-state.json, worker.yaml, and verification artifacts.",
    detail: error instanceof Error ? error.message : String(error),
  });
}

function printSummary(
  phase: DeploymentPhase,
  apply: boolean,
  outDir: string,
  profile: DeploymentProfile,
  options?: { deploymentState?: boolean },
) {
  const summary = {
    ok: true,
    phase,
    apply,
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
  console.log(jsonStringify(summary));
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
        "--phase must be render, deploy-test-oft, deploy-workers, configure-workers, configure-oapp, verify, or all",
      );
  }
}

function parseIgnitionCommandOptions(
  flags: ReadonlyMap<string, string>,
): IgnitionCommandOptions {
  return {
    verify: flagEnabled(flags.get("verify")),
    autoConfirm:
      flagEnabled(flags.get("auto-confirm")) || flagEnabled(flags.get("yes")),
    buildProfile: parseBuildProfileFlag(flags.get("build-profile")),
  };
}

function normalizeIgnitionCommandOptions(
  options: Partial<IgnitionCommandOptions> | undefined,
): IgnitionCommandOptions {
  return {
    verify: options?.verify ?? false,
    autoConfirm: options?.autoConfirm ?? false,
    buildProfile:
      options?.buildProfile === undefined
        ? undefined
        : normalizeBuildProfileValue(options.buildProfile),
  };
}

function parseBuildProfileFlag(value: string | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (value === "" || value === "true") {
    throw new Error("--build-profile requires a value");
  }
  return normalizeBuildProfileValue(value);
}

function normalizeBuildProfileValue(value: string): string {
  if (value.trim() === "") {
    throw new Error("--build-profile requires a value");
  }
  if (/\s/.test(value)) {
    throw new Error("--build-profile cannot contain whitespace");
  }
  return value;
}

function validateLongTermPriceSubmitters(
  submitters: readonly Address[],
  owner: Address,
) {
  for (const submitter of submitters) {
    if (isAddressEqual(submitter, owner)) {
      throw new Error(
        "profile.priceFeedSubmitters must not include profile.owner; owner is added only as a temporary deployment submitter",
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

function flagEnabled(value: string | undefined): boolean {
  if (value === undefined || value === "") {
    return false;
  }
  return ["1", "true", "yes"].includes(value.toLowerCase());
}

function requiredProcessEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`${name} is required`);
  }
  return value;
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
  destination: ChainProfile,
): string {
  return `${source.deploymentId}-${directionKey(source, destination)}-open-workers-pathway`;
}

function oappEndpointDeploymentId(
  source: ChainProfile,
  destination: ChainProfile,
): string {
  return `${source.deploymentId}-${directionKey(source, destination)}-oapp-endpoint`;
}

function directionKey(source: ChainProfile, destination: ChainProfile): string {
  return `${source.key}-to-${destination.key}`;
}

function hardhatCommand(
  chain: ChainProfile,
  script: string,
  parametersPath: string,
  deploymentId: string,
  ignition: IgnitionCommandOptions,
): string {
  return `${hardhatEnvPrefix(chain, ignition).join(" ")} ${script} -- ${hardhatIgnitionArgs(
    {
      chain,
      parametersPath,
      deploymentId,
      ignition,
    },
  ).join(" ")}`;
}

function hardhatIgnitionArgs(input: {
  chain: ChainProfile;
  parametersPath: string;
  deploymentId: string;
  ignition: IgnitionCommandOptions;
}): string[] {
  const args = [
    ...(input.ignition.buildProfile === undefined
      ? []
      : ["--build-profile", input.ignition.buildProfile]),
    "--network",
    input.chain.network,
    "--parameters",
    input.parametersPath,
    "--deployment-id",
    input.deploymentId,
  ];
  if (input.ignition.verify) {
    args.push("--verify");
  }
  return args;
}

function hardhatEnvPrefix(
  chain: ChainProfile,
  ignition: IgnitionCommandOptions,
): string[] {
  return [
    `${chain.rpcUrlEnv}=...`,
    ...(ignition.autoConfirm
      ? [
          "HARDHAT_IGNITION_CONFIRM_DEPLOYMENT=true",
          "HARDHAT_IGNITION_CONFIRM_RESET=true",
        ]
      : []),
  ];
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function arrayField(
  input: Record<string, unknown>,
  field: string,
  label: string,
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
  label: string,
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
  label: string,
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
  label: string,
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
  label: string,
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
  label: string,
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
  label: string,
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
      `${label} must be an absolute HTTPS URL without query or fragment`,
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
      `${label} must be an absolute HTTPS URL without query or fragment`,
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
      `${label} must be an absolute HTTPS URL without query or fragment`,
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

function envVarNameField(
  input: Record<string, unknown>,
  field: string,
  label: string,
): string {
  return envVarName(stringField(input, field, label), label);
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
  options?: { allowZero?: boolean; max?: number },
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
  options?: { allowZero?: boolean; max?: number },
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
  label: string,
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
  label: string,
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
  fallback: string,
): string {
  if (input[field] === undefined || input[field] === "") {
    return fallback;
  }
  return decimalField(input, field, label);
}

function addressField(
  input: Record<string, unknown>,
  field: string,
  label: string,
): Address {
  const value = stringField(input, field, label);
  return normalizeAddress(value, label);
}

function optionalAddressField(
  input: Record<string, unknown>,
  field: string,
  label: string,
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
  label: string,
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

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  await main();
}
