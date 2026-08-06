// Deploys the GOATED LayerZero bridge on a LOCAL regtest across two EVM chains
// (chain A anvil, chain B e.g. goat-geth) and emits self-contained
// oh-my-lazier worker configs for an AUTONOMOUS two-worker relay:
//
//   worker 1: OpenExecutor + primary OpenDVN   (signer = worker-1 keystore)
//   worker 2: secondary OpenDVN only           (signer = worker-2 keystore)
//
// Each worker uses its own database and verifies exactly one DVN; the executor
// waits for both on-chain DVN verifications, so the workers coordinate purely
// through on-chain state.
//
// SINGLE TRUST DOMAIN: one deployer owns everything and both DVNs share one
// OpenPriceFeed and one local infrastructure. This layout only exercises the
// 2-of-2 required-DVN protocol flow; it proves nothing about operational,
// key, price-source, or infrastructure independence and must not be copied as
// a production topology.
//
// Emits into tmpDir: deployments.json plus worker-1/worker-2 host and
// container YAML configs. Worker keystores (worker-<n>-keystore.json) must
// exist in tmpDir before deploying; generate them with go/cmd/e2ekeystore.

import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import { getAddress, type Address } from "viem";
import LocalE2EChainModule from "../ignition/modules/LocalE2EChain.js";
import LocalE2EPathwayModule from "../ignition/modules/LocalE2EPathway.js";
import {
  type ApplyGate,
  type WriteNetworkContext,
  withWriteConnection,
} from "./command-harness.js";
import {
  CONFIG_TYPE_EXECUTOR,
  CONFIG_TYPE_ULN,
  encodeExecutorConfig,
  encodeUlnConfig,
  requiredDVNsConfig,
} from "./lz-config.js";
import { addressToBytes32, jsonStringify } from "./lib.js";
import { buildLzReceiveOption } from "./oft-canary.js";
import { withIgnitionUiOnStderr } from "./ignition-ui-output.js";
import {
  deployedAddress,
  readKeystoreAddress,
  writeLocalE2EIgnitionParameters,
} from "./e2e-local-deploy.js";
import {
  regtestChains,
  regtestKeystorePasswordEnv,
  regtestWorkerInfrastructure,
  type RegtestChainSpec,
  type RegtestWorkerInfrastructure,
} from "./regtest-config.js";

const maxMessageSize = 10_000;
const minLzReceiveGas = 100_000n;
const lzReceiveGas = 250_000n;
const maxLzReceiveGas = 1_000_000n;
const priceStaleAfter = 86_400n;
const defaultInitialSupply = 1_000_000n * 10n ** 18n;

export const REGTEST_IGNITION_DEPLOYMENT_IDS = {
  chainA: "regtest-chain-a",
  chainB: "regtest-chain-b",
  pathwayAToB: "regtest-pathway-a-to-b",
  pathwayBToA: "regtest-pathway-b-to-a",
} as const;

export type RegtestDVNMode = "active" | "shadow";

export type RegtestWorkerSpec = RegtestWorkerInfrastructure & {
  address: Address;
};

export type RegtestChainDeployment = RegtestChainSpec & {
  endpoint: Address;
  sendUln: Address;
  receiveUln: Address;
  oft: Address;
  priceFeed: Address;
  openExecutor: Address;
  primaryOpenDVN: Address;
  secondaryOpenDVN: Address;
  primaryDVNSigner: Address;
  secondaryDVNSigner: Address;
  executorSigner: Address;
};

export type RegtestDeployment = {
  generatedAt: string;
  deployer: Address;
  workers: { worker1: Address; worker2: Address };
  parameters: {
    confirmations: string;
    maxMessageSize: number;
    minLzReceiveGas: string;
    lzReceiveGas: string;
    maxLzReceiveGas: string;
    initialSupply: string;
    dvnMode: RegtestDVNMode;
    requiredDVNCount: number;
  };
  chains: { a: RegtestChainDeployment; b: RegtestChainDeployment };
};

type RegtestChainPair = readonly [RegtestChainSpec, RegtestChainSpec];
type Environment = Readonly<Record<string, string | undefined>>;

export type RegtestDeployBusinessInput = {
  tmpDir: string;
  /** Source-chain confirmations per pathway; defaults to 1. */
  confirmations?: bigint;
  /** GOATED supply minted on chain A; defaults to 1,000,000e18. */
  initialSupply?: bigint;
  /** DVN operating mode for both workers; defaults to "active". */
  dvnMode?: RegtestDVNMode;
};

export type RegtestDeployInput = RegtestDeployBusinessInput & {
  chains: RegtestChainPair;
  confirmations: bigint;
  initialSupply: bigint;
  dvnMode: RegtestDVNMode;
  keystorePasswordEnv: string;
  workers: readonly [RegtestWorkerSpec, RegtestWorkerSpec];
};

export type RegtestDeployContext = {
  hre: HardhatRuntimeEnvironment;
  gate: Pick<ApplyGate, "authorize">;
  displayUi?: boolean;
  now?: () => Date;
};

export type RegtestDeployResult =
  | {
      applied: false;
      deploymentIds: typeof REGTEST_IGNITION_DEPLOYMENT_IDS;
    }
  | {
      applied: true;
      deploymentIds: typeof REGTEST_IGNITION_DEPLOYMENT_IDS;
      deployment: RegtestDeployment;
    };

/**
 * Resolve the deployment input: business settings (confirmations, supply,
 * DVN mode) come only from the reviewed OML_SCRIPT_PARAMS envelope with
 * explicit defaults, while RPC endpoints and other infrastructure resolve
 * from the environment. Worker signer addresses come from the pre-generated
 * keystore files in tmpDir.
 */
export function resolveRegtestDeployInput(
  input: RegtestDeployBusinessInput,
  environment: Environment = process.env
): RegtestDeployInput {
  if (input.tmpDir.trim() === "") {
    throw new Error("tmpDir must not be empty");
  }
  const resolve = (name: string, fallback: string) =>
    environment[name] ?? fallback;
  const chains = regtestChains(resolve);
  const chainA = chains[0];
  const chainB = chains[1];
  if (chainA === undefined || chainB === undefined || chains.length !== 2) {
    throw new Error("regtest requires exactly two chains");
  }
  const confirmations = input.confirmations ?? 1n;
  if (confirmations < 1n) {
    throw new Error("confirmations must be at least 1");
  }
  const initialSupply = input.initialSupply ?? defaultInitialSupply;
  if (initialSupply < 0n) {
    throw new Error("initialSupply must not be negative");
  }
  const dvnMode = input.dvnMode ?? "active";
  const workers = [1, 2].map((index) => {
    const infrastructure = regtestWorkerInfrastructure(
      index as 1 | 2,
      input.tmpDir,
      resolve
    );
    return {
      ...infrastructure,
      address: readKeystoreAddress(infrastructure.keystorePaths.host),
    };
  }) as [RegtestWorkerSpec, RegtestWorkerSpec];
  if (workers[0].address.toLowerCase() === workers[1].address.toLowerCase()) {
    throw new Error(
      "regtest worker keystores must hold two distinct signer addresses"
    );
  }
  return {
    tmpDir: input.tmpDir,
    chains: [chainA, chainB],
    confirmations,
    initialSupply,
    dvnMode,
    keystorePasswordEnv: regtestKeystorePasswordEnv(resolve),
    workers,
  };
}

/** Require the Hardhat Ignition path to be isolated inside the regtest tmpDir. */
export function assertRegtestIgnitionDirectory(
  configuredIgnitionDir: string,
  tmpDir: string
): void {
  const actual = path.resolve(configuredIgnitionDir);
  const expected = path.resolve(tmpDir, "ignition");
  if (actual !== expected) {
    throw new Error(
      `regtest requires Hardhat Ignition path ${expected}; configured ${actual}`
    );
  }
}

/** Deploy and configure both sides of the regtest topology with Ignition. */
export async function runRegtestDeploy(
  input: RegtestDeployInput,
  context: RegtestDeployContext
): Promise<RegtestDeployResult> {
  assertRegtestIgnitionDirectory(
    context.hre.config.paths.ignition,
    input.tmpDir
  );
  const [chainASpec, chainBSpec] = input.chains;
  const applied = await context.gate.authorize({
    command: "regtest:deploy",
    targets: [
      {
        network: chainASpec.name,
        chainId: chainASpec.chainId,
        deploymentIds: [
          REGTEST_IGNITION_DEPLOYMENT_IDS.chainA,
          REGTEST_IGNITION_DEPLOYMENT_IDS.pathwayAToB,
        ],
      },
      {
        network: chainBSpec.name,
        chainId: chainBSpec.chainId,
        deploymentIds: [
          REGTEST_IGNITION_DEPLOYMENT_IDS.chainB,
          REGTEST_IGNITION_DEPLOYMENT_IDS.pathwayBToA,
        ],
      },
    ],
    actions: [
      "deploy regtest Endpoint, ULN, GOATED OFT, price feed, executor, and two DVNs on both chains",
      "configure reciprocal LayerZero pathways and worker fee models",
      "authorize the worker-1 and worker-2 DVN verifier roles",
      "write deployments.json and worker-1/worker-2 host and container configs",
    ],
  });
  if (!applied) {
    return {
      applied: false,
      deploymentIds: REGTEST_IGNITION_DEPLOYMENT_IDS,
    };
  }
  return await withWriteConnection(
    context.hre,
    { network: chainASpec.name, expectedChainId: chainASpec.chainId },
    async (chainAContext) =>
      await withWriteConnection(
        context.hre,
        { network: chainBSpec.name, expectedChainId: chainBSpec.chainId },
        async (chainBContext) =>
          await applyRegtestDeploy(input, context, chainAContext, chainBContext)
      )
  );
}

async function applyRegtestDeploy(
  input: RegtestDeployInput,
  context: RegtestDeployContext,
  chainAContext: WriteNetworkContext,
  chainBContext: WriteNetworkContext
): Promise<RegtestDeployResult> {
  const [chainASpec, chainBSpec] = input.chains;
  const deployerAddress = getAddress(chainAContext.signerAddress);
  if (
    deployerAddress.toLowerCase() !== chainBContext.signerAddress.toLowerCase()
  ) {
    throw new Error(
      `regtest networks use different deployers: ${deployerAddress} and ${chainBContext.signerAddress}`
    );
  }
  const displayUi = context.displayUi ?? true;
  forceLegacyFees(chainAContext, chainASpec);
  forceLegacyFees(chainBContext, chainBSpec);
  await mkdir(input.tmpDir, { recursive: true });

  const [chainA, chainB] = await Promise.all([
    deployChain(
      chainASpec,
      chainAContext,
      input,
      deployerAddress,
      REGTEST_IGNITION_DEPLOYMENT_IDS.chainA,
      input.initialSupply,
      displayUi
    ),
    deployChain(
      chainBSpec,
      chainBContext,
      input,
      deployerAddress,
      REGTEST_IGNITION_DEPLOYMENT_IDS.chainB,
      0n,
      displayUi
    ),
  ]);

  // Anchor the initial price snapshots to each source chain's CURRENT tip (a
  // long-running goat-geth regtest has an old genesis, and a genesis-anchored
  // snapshot would already be stale) — but reuse a previously persisted
  // timestamp when this tmpDir already deployed the pathway: Ignition
  // reconciliation rejects changed arguments for an executed Future, so a
  // rerun or a resume after a partial failure must supply the original
  // updatedAt. A stale-again snapshot on a much-later rerun means the
  // environment should be recreated from a clean tmpDir instead.
  const [chainATimestamp, chainBTimestamp] = await Promise.all([
    pathwayTimestamp(
      input.tmpDir,
      REGTEST_IGNITION_DEPLOYMENT_IDS.pathwayAToB,
      chainAContext
    ),
    pathwayTimestamp(
      input.tmpDir,
      REGTEST_IGNITION_DEPLOYMENT_IDS.pathwayBToA,
      chainBContext
    ),
  ]);
  await Promise.all([
    deployPathway(
      chainA,
      chainB,
      chainATimestamp,
      input,
      chainAContext,
      REGTEST_IGNITION_DEPLOYMENT_IDS.pathwayAToB,
      displayUi
    ),
    deployPathway(
      chainB,
      chainA,
      chainBTimestamp,
      input,
      chainBContext,
      REGTEST_IGNITION_DEPLOYMENT_IDS.pathwayBToA,
      displayUi
    ),
  ]);

  const [worker1, worker2] = input.workers;
  const deployment: RegtestDeployment = {
    generatedAt: (context.now ?? (() => new Date()))().toISOString(),
    deployer: deployerAddress,
    workers: { worker1: worker1.address, worker2: worker2.address },
    parameters: {
      confirmations: input.confirmations.toString(),
      maxMessageSize,
      minLzReceiveGas: minLzReceiveGas.toString(),
      lzReceiveGas: lzReceiveGas.toString(),
      maxLzReceiveGas: maxLzReceiveGas.toString(),
      initialSupply: input.initialSupply.toString(),
      dvnMode: input.dvnMode,
      requiredDVNCount: 2,
    },
    chains: { a: chainA, b: chainB },
  };

  await Promise.all([
    writeFile(
      path.join(input.tmpDir, "deployments.json"),
      `${jsonStringify(deployment)}\n`
    ),
    ...input.workers.flatMap((worker) =>
      (["host", "container"] as const).map((mode) =>
        writeFile(
          path.join(
            input.tmpDir,
            mode === "host"
              ? `worker-${worker.index}.yaml`
              : `worker-${worker.index}.container.yaml`
          ),
          regtestWorkerConfig(deployment, input, worker, mode)
        )
      )
    ),
  ]);
  console.error(
    `[regtest:deploy] wrote deployments.json + worker-1/2.yaml (+ .container.yaml) into ${input.tmpDir}`
  );
  return {
    applied: true,
    deploymentIds: REGTEST_IGNITION_DEPLOYMENT_IDS,
    deployment,
  };
}

async function deployChain(
  spec: RegtestChainSpec,
  context: WriteNetworkContext,
  input: RegtestDeployInput,
  deployerAddress: Address,
  deploymentId: string,
  supply: bigint,
  displayUi: boolean
): Promise<RegtestChainDeployment> {
  const parameters = await writeLocalE2EIgnitionParameters(
    input.tmpDir,
    deploymentId,
    buildRegtestChainParameters(spec, input, deployerAddress, supply)
  );
  const deployed = await withIgnitionUiOnStderr(() =>
    context.connection.ignition.deploy(LocalE2EChainModule, {
      deploymentId,
      displayUi,
      parameters,
    })
  );
  const [worker1, worker2] = input.workers;
  return {
    ...spec,
    endpoint: deployedAddress(deployed, "endpoint"),
    sendUln: deployedAddress(deployed, "sendUln"),
    receiveUln: deployedAddress(deployed, "receiveUln"),
    oft: deployedAddress(deployed, "oft"),
    priceFeed: deployedAddress(deployed, "priceFeed"),
    openExecutor: deployedAddress(deployed, "openExecutor"),
    primaryOpenDVN: deployedAddress(deployed, "primaryOpenDVN"),
    secondaryOpenDVN: deployedAddress(deployed, "secondaryOpenDVN"),
    primaryDVNSigner: worker1.address,
    secondaryDVNSigner: worker2.address,
    executorSigner: worker1.address,
  };
}

async function deployPathway(
  source: RegtestChainDeployment,
  destination: RegtestChainDeployment,
  updatedAt: bigint,
  input: RegtestDeployInput,
  context: WriteNetworkContext,
  deploymentId: string,
  displayUi: boolean
): Promise<void> {
  const parameters = await writeLocalE2EIgnitionParameters(
    input.tmpDir,
    deploymentId,
    buildRegtestPathwayParameters(
      source,
      destination,
      updatedAt,
      input.confirmations
    )
  );
  await withIgnitionUiOnStderr(() =>
    context.connection.ignition.deploy(LocalE2EPathwayModule, {
      deploymentId,
      displayUi,
      parameters,
    })
  );
}

export function buildRegtestChainParameters(
  spec: RegtestChainSpec,
  input: Pick<RegtestDeployInput, "workers">,
  deployerAddress: Address,
  supply: bigint
) {
  const [worker1, worker2] = input.workers;
  return {
    LocalE2EChain: {
      eid: spec.eid,
      owner: deployerAddress,
      tokenName: "Goat Network",
      tokenSymbol: "GOATED",
      delegate: deployerAddress,
      initialRecipient: deployerAddress,
      initialSupply: supply,
      priceFeedSubmitters: [deployerAddress, worker1.address, worker2.address],
    },
  };
}

export function buildRegtestPathwayParameters(
  source: RegtestChainDeployment,
  destination: RegtestChainDeployment,
  updatedAt: bigint,
  confirmations: bigint
) {
  const ulnConfig = requiredDVNsConfig(confirmations, [
    source.primaryOpenDVN,
    source.secondaryOpenDVN,
  ]);
  const defaultUlnConfig = { ...ulnConfig, optionalDVNCount: 0 };
  const executorConfig = {
    maxMessageSize,
    executor: source.openExecutor,
  };
  const encodedUlnConfig = encodeUlnConfig(ulnConfig);
  return {
    LocalE2EPathway: {
      endpoint: source.endpoint,
      sendUln: source.sendUln,
      receiveUln: source.receiveUln,
      oft: source.oft,
      priceFeed: source.priceFeed,
      openExecutor: source.openExecutor,
      primaryOpenDVN: source.primaryOpenDVN,
      secondaryOpenDVN: source.secondaryOpenDVN,
      remoteEid: destination.eid,
      remotePeer: addressToBytes32(destination.oft),
      receiveLibraryGracePeriod: 0n,
      defaultUlnConfig,
      defaultExecutorConfig: executorConfig,
      sendConfig: [
        {
          eid: destination.eid,
          configType: CONFIG_TYPE_EXECUTOR,
          config: encodeExecutorConfig(executorConfig),
        },
        {
          eid: destination.eid,
          configType: CONFIG_TYPE_ULN,
          config: encodedUlnConfig,
        },
      ],
      receiveConfig: [
        {
          eid: destination.eid,
          configType: CONFIG_TYPE_ULN,
          config: encodedUlnConfig,
        },
      ],
      enforcedOptions: [
        {
          eid: destination.eid,
          msgType: 1,
          options: buildLzReceiveOption(lzReceiveGas),
        },
      ],
      workerPathwayConfig: {
        enabled: true,
        maxMessageSize: BigInt(maxMessageSize),
        minLzReceiveGas,
        maxLzReceiveGas,
      },
      priceSnapshot: {
        dstGasPriceInSrcToken: 1n,
        dstDataFeePerByteInSrcToken: 0n,
        updatedAt,
        staleAfter: priceStaleAfter,
      },
      executorFeeModel: regtestFeeModel(),
      dvnFeeModel: regtestFeeModel(),
      primaryDVNVerifier: source.primaryDVNSigner,
      secondaryDVNVerifier: source.secondaryDVNSigner,
    },
  };
}

// forceLegacyFees interposes on the connection's EIP-1193 provider when the
// chain pins a legacy gas price: goat-geth's mempool can drop EIP-1559
// transactions, and Ignition offers no supported switch to type-0 fees on a
// chain that reports baseFeePerGas. Hiding baseFeePerGas from
// eth_getBlockByNumber steers both Ignition and viem onto their legacy fee
// paths, and the pinned eth_gasPrice answer fixes the price — mirroring the
// pre-Ignition behavior where every transaction on that chain went out as
// type 0 with REGTEST_<X>_GAS_PRICE.
export function forceLegacyFees(
  context: WriteNetworkContext,
  spec: Pick<RegtestChainSpec, "gasPrice">
): void {
  const gasPrice = spec.gasPrice;
  if (gasPrice === undefined) {
    return;
  }
  const provider = context.connection.provider;
  const originalRequest = provider.request.bind(provider);
  provider.request = async (args) => {
    if (args.method === "eth_gasPrice") {
      return `0x${gasPrice.toString(16)}`;
    }
    const result = await originalRequest(args);
    if (
      args.method === "eth_getBlockByNumber" &&
      typeof result === "object" &&
      result !== null &&
      "baseFeePerGas" in result
    ) {
      const { baseFeePerGas: _dropped, ...legacyBlock } = result as Record<
        string,
        unknown
      >;
      return legacyBlock;
    }
    return result;
  };
}

function regtestFeeModel() {
  return {
    baseFee: 1n,
    dstGasOverhead: 0n,
    dataSizeOverheadBytes: 0n,
    marginBps: 0,
  };
}

async function latestTimestamp(context: WriteNetworkContext): Promise<bigint> {
  return (await context.publicClient.getBlock()).timestamp;
}

async function pathwayTimestamp(
  tmpDir: string,
  deploymentId: string,
  context: WriteNetworkContext
): Promise<bigint> {
  const persisted = await persistedPathwayUpdatedAt(tmpDir, deploymentId);
  if (persisted !== undefined) {
    return persisted;
  }
  return latestTimestamp(context);
}

/**
 * Read the price snapshot updatedAt from a previously written Ignition
 * parameters file, so reruns replay the exact Future arguments. Returns
 * undefined when no prior run persisted parameters for this deployment.
 */
export async function persistedPathwayUpdatedAt(
  tmpDir: string,
  deploymentId: string
): Promise<bigint | undefined> {
  const parametersPath = path.resolve(
    tmpDir,
    "ignition-parameters",
    `${deploymentId}.json`
  );
  let raw: string;
  try {
    raw = await readFile(parametersPath, "utf8");
  } catch {
    return undefined;
  }
  let value: unknown;
  try {
    value = JSON.parse(raw) as unknown;
  } catch {
    throw new Error(
      `regtest ignition parameters are not valid JSON: ${parametersPath}`
    );
  }
  const updatedAt = (
    (value as { LocalE2EPathway?: { priceSnapshot?: { updatedAt?: unknown } } })
      .LocalE2EPathway?.priceSnapshot?.updatedAt
  );
  if (typeof updatedAt !== "string" || !/^[0-9]+n$/.test(updatedAt)) {
    throw new Error(
      `regtest ignition parameters have no priceSnapshot.updatedAt bigint: ${parametersPath}`
    );
  }
  return BigInt(updatedAt.slice(0, -1));
}

/** Render one worker's YAML config (worker 1 = executor + primary DVN). */
export function regtestWorkerConfig(
  deployment: RegtestDeployment,
  input: Pick<RegtestDeployInput, "keystorePasswordEnv" | "dvnMode">,
  worker: RegtestWorkerSpec,
  mode: "host" | "container"
): string {
  const primary = worker.index === 1;
  const executorEnabled = primary;
  const rpcURL = (chain: RegtestChainDeployment) =>
    mode === "host" ? chain.hostRpcUrl : chain.containerRpcUrl;
  const workerDVN = (chain: RegtestChainDeployment) =>
    primary ? chain.primaryOpenDVN : chain.secondaryOpenDVN;
  const chainList = [deployment.chains.a, deployment.chains.b];
  const pathways = [
    [deployment.chains.a, deployment.chains.b],
    [deployment.chains.b, deployment.chains.a],
  ] as const;
  return `database_url: ${worker.databaseURLs[mode]}
metrics:
  listen_address: ${worker.metricsAddress}
services:
  executor:
    enabled: ${executorEnabled}
  dvn:
    enabled: true
tx_manager:
  stale_broadcast_replacement_after_seconds: 2
signers:
  - id: "${worker.address}"
    type: keystore
    keystore:
      path: ${worker.keystorePaths[mode]}
      password_env: ${input.keystorePasswordEnv}
pricing:
  enabled: false
chains:
${chainList
  .map(
    (chain) => `  - eid: ${chain.eid}
    name: ${chain.name}
    family: evm
    chain_id: ${chain.chainId}
    endpoint_address: "${chain.endpoint}"
    confirmations: ${deployment.parameters.confirmations}
    start_block_number: 0
    indexer_query_block_range: 500
    indexer_poll_interval_seconds: 5
    rpc_urls:
      - ${rpcURL(chain)}${
      chain.gasPrice === undefined
        ? ""
        : `
    legacy_transactions: true`
    }
    tx_roles:${
      executorEnabled
        ? `
      executor:
        signer: "${worker.address}"
        max_fee_per_gas_wei: "100000000000"
        max_priority_fee_per_gas_wei: "2000000000"
        min_native_balance_wei: "1000000000000000000"`
        : ""
    }
      dvn:
        signer: "${worker.address}"
        max_fee_per_gas_wei: "100000000000"
        max_priority_fee_per_gas_wei: "2000000000"
        min_native_balance_wei: "1000000000000000000"`
  )
  .join("\n")}
pathways:
${pathways
  .map(
    ([source, destination]) => `  - src_eid: ${source.eid}
    dst_eid: ${destination.eid}
    src_oapp: "${source.oft}"
    dst_oapp: "${destination.oft}"
    send_lib: "${source.sendUln}"
    receive_lib: "${destination.receiveUln}"
    source_workers:
      open_executor: "${source.openExecutor}"
      open_dvn: "${workerDVN(source)}"
      price_feed: "${source.priceFeed}"
    destination_workers:
      open_dvn: "${workerDVN(destination)}"
    send_required_dvns:
      - "${source.primaryOpenDVN}"
      - "${source.secondaryOpenDVN}"
    receive_required_dvns:
      - "${destination.primaryOpenDVN}"
      - "${destination.secondaryOpenDVN}"
    dvn:
      mode: ${input.dvnMode}
    enabled: true
    max_message_size: ${deployment.parameters.maxMessageSize}
    min_lz_receive_gas: ${deployment.parameters.minLzReceiveGas}
    max_lz_receive_gas: ${deployment.parameters.maxLzReceiveGas}`
  )
  .join("\n")}
`;
}

/** Load and validate a regtest deployments.json produced by regtest:deploy. */
export async function loadRegtestDeployment(
  tmpDir: string
): Promise<RegtestDeployment> {
  const deploymentPath = path.join(tmpDir, "deployments.json");
  let value: unknown;
  try {
    value = JSON.parse(await readFile(deploymentPath, "utf8")) as unknown;
  } catch {
    throw new Error(
      `regtest deployment could not be read: ${deploymentPath} (run regtest:deploy first)`
    );
  }
  return validateRegtestDeployment(value);
}

export function validateRegtestDeployment(value: unknown): RegtestDeployment {
  const record = expectRecord(value, "deployment");
  const chains = expectRecord(record.chains, "deployment.chains");
  const parameters = expectRecord(record.parameters, "deployment.parameters");
  const workers = expectRecord(record.workers, "deployment.workers");
  const dvnMode = parameters.dvnMode;
  if (dvnMode !== "active" && dvnMode !== "shadow") {
    throw new Error("deployment.parameters.dvnMode must be active or shadow");
  }
  return {
    generatedAt: expectString(record.generatedAt, "deployment.generatedAt"),
    deployer: expectAddress(record.deployer, "deployment.deployer"),
    workers: {
      worker1: expectAddress(workers.worker1, "deployment.workers.worker1"),
      worker2: expectAddress(workers.worker2, "deployment.workers.worker2"),
    },
    parameters: {
      confirmations: expectString(
        parameters.confirmations,
        "deployment.parameters.confirmations"
      ),
      maxMessageSize: expectNumber(
        parameters.maxMessageSize,
        "deployment.parameters.maxMessageSize"
      ),
      minLzReceiveGas: expectString(
        parameters.minLzReceiveGas,
        "deployment.parameters.minLzReceiveGas"
      ),
      lzReceiveGas: expectString(
        parameters.lzReceiveGas,
        "deployment.parameters.lzReceiveGas"
      ),
      maxLzReceiveGas: expectString(
        parameters.maxLzReceiveGas,
        "deployment.parameters.maxLzReceiveGas"
      ),
      initialSupply: expectString(
        parameters.initialSupply,
        "deployment.parameters.initialSupply"
      ),
      dvnMode,
      requiredDVNCount: expectNumber(
        parameters.requiredDVNCount,
        "deployment.parameters.requiredDVNCount"
      ),
    },
    chains: {
      a: validateChainDeployment(chains.a, "deployment.chains.a", "a"),
      b: validateChainDeployment(chains.b, "deployment.chains.b", "b"),
    },
  };
}

function validateChainDeployment(
  value: unknown,
  label: string,
  key: "a" | "b"
): RegtestChainDeployment {
  const record = expectRecord(value, label);
  if (record.key !== key) {
    throw new Error(`${label}.key must be ${key}`);
  }
  const gasPrice = record.gasPrice;
  if (
    gasPrice !== undefined &&
    (typeof gasPrice !== "string" || !/^[0-9]+$/.test(gasPrice))
  ) {
    throw new Error(`${label}.gasPrice must be an unsigned wei integer string`);
  }
  return {
    key,
    name: expectString(record.name, `${label}.name`),
    eid: expectNumber(record.eid, `${label}.eid`),
    chainId: expectNumber(record.chainId, `${label}.chainId`),
    ...(gasPrice === undefined ? {} : { gasPrice: BigInt(gasPrice) }),
    hostRpcUrl: expectString(record.hostRpcUrl, `${label}.hostRpcUrl`),
    containerRpcUrl: expectString(
      record.containerRpcUrl,
      `${label}.containerRpcUrl`
    ),
    endpoint: expectAddress(record.endpoint, `${label}.endpoint`),
    sendUln: expectAddress(record.sendUln, `${label}.sendUln`),
    receiveUln: expectAddress(record.receiveUln, `${label}.receiveUln`),
    oft: expectAddress(record.oft, `${label}.oft`),
    priceFeed: expectAddress(record.priceFeed, `${label}.priceFeed`),
    openExecutor: expectAddress(record.openExecutor, `${label}.openExecutor`),
    primaryOpenDVN: expectAddress(
      record.primaryOpenDVN,
      `${label}.primaryOpenDVN`
    ),
    secondaryOpenDVN: expectAddress(
      record.secondaryOpenDVN,
      `${label}.secondaryOpenDVN`
    ),
    primaryDVNSigner: expectAddress(
      record.primaryDVNSigner,
      `${label}.primaryDVNSigner`
    ),
    secondaryDVNSigner: expectAddress(
      record.secondaryDVNSigner,
      `${label}.secondaryDVNSigner`
    ),
    executorSigner: expectAddress(
      record.executorSigner,
      `${label}.executorSigner`
    ),
  };
}

function expectRecord(
  value: unknown,
  label: string
): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function expectString(value: unknown, label: string): string {
  if (typeof value !== "string" || value === "") {
    throw new Error(`${label} must be a non-empty string`);
  }
  return value;
}

function expectNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new Error(`${label} must be a safe integer`);
  }
  return value;
}

function expectAddress(value: unknown, label: string): Address {
  return getAddress(expectString(value, label));
}
