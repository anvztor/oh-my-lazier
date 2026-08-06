import { readFileSync } from "node:fs";
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
import {
  validateLocalE2EGeneratedKMSKey,
  type LocalChainDeployment as ChainDeployment,
  type LocalE2EDeployment,
} from "./e2e-local-artifacts.js";
import {
  localE2EChains,
  localE2EDatabaseURL,
  localE2EKMS,
  type LocalE2EChainSpec,
  type LocalE2EKMSConfig,
} from "./e2e-local-config.js";
import { buildLzReceiveOption } from "./oft-canary.js";
import { withIgnitionUiOnStderr } from "./ignition-ui-output.js";

const maxMessageSize = 10_000;
const minLzReceiveGas = 100_000n;
const lzReceiveGas = 250_000n;
const maxLzReceiveGas = 1_000_000n;
const confirmations = 1n;
const initialSupply = 1_000_000n * 10n ** 18n;
const signerFunding = 100n * 10n ** 18n;
const priceStaleAfter = 86_400n;

export const LOCAL_E2E_IGNITION_DEPLOYMENT_IDS = {
  chainA: "local-e2e-chain-a",
  chainB: "local-e2e-chain-b",
  pathwayAToB: "local-e2e-pathway-a-to-b",
  pathwayBToA: "local-e2e-pathway-b-to-a",
} as const;

type LocalE2EChainPair = readonly [LocalE2EChainSpec, LocalE2EChainSpec];
type UnsignedChainDeployment = Omit<
  ChainDeployment,
  "executorSigner" | "dvnSigner"
>;
type ChainSignerRoles = Pick<ChainDeployment, "executorSigner" | "dvnSigner">;
type Environment = Readonly<Record<string, string | undefined>>;

export type LocalE2EDeployBusinessInput = {
  tmpDir: string;
};

export type LocalE2EDeployInput = LocalE2EDeployBusinessInput & {
  chains: LocalE2EChainPair;
  databaseURLs: {
    host: string;
    container: string;
  };
  kms: LocalE2EKMSConfig;
  workerAddress: Address;
};

export type LocalE2EDeployContext = {
  hre: HardhatRuntimeEnvironment;
  gate: Pick<ApplyGate, "authorize">;
  displayUi?: boolean;
  now?: () => Date;
};

export type LocalE2EDeployResult =
  | {
      applied: false;
      deploymentIds: typeof LOCAL_E2E_IGNITION_DEPLOYMENT_IDS;
    }
  | {
      applied: true;
      deploymentIds: typeof LOCAL_E2E_IGNITION_DEPLOYMENT_IDS;
      deployment: LocalE2EDeployment;
    };

/**
 * Resolve infrastructure-only local E2E settings without putting RPC URLs or
 * private keys in the command's JSON input.
 */
export function resolveLocalE2EDeployInput(
  input: LocalE2EDeployBusinessInput,
  environment: Environment = process.env
): LocalE2EDeployInput {
  if (input.tmpDir.trim() === "") {
    throw new Error("tmpDir must not be empty");
  }
  const resolve = (name: string, fallback: string) =>
    environment[name] ?? fallback;
  const chains = localE2EChains(resolve);
  const chainA = chains[0];
  const chainB = chains[1];
  if (chainA === undefined || chainB === undefined || chains.length !== 2) {
    throw new Error("local e2e requires exactly two chains");
  }
  const workerAddress = readKeystoreAddress(
    path.join(input.tmpDir, "worker-keystore.json")
  );
  return {
    tmpDir: input.tmpDir,
    chains: [chainA, chainB],
    databaseURLs: {
      host: localE2EDatabaseURL("host", resolve),
      container: localE2EDatabaseURL("container", resolve),
    },
    kms: localE2EKMS(resolve),
    workerAddress,
  };
}

/** Deploy and configure both sides of the local E2E topology with Ignition. */
export async function runLocalE2EDeploy(
  input: LocalE2EDeployInput,
  context: LocalE2EDeployContext
): Promise<LocalE2EDeployResult> {
  assertLocalE2EIgnitionDirectory(
    context.hre.config.paths.ignition,
    input.tmpDir
  );
  const [chainASpec, chainBSpec] = validateDeployInput(input);
  const kmsKey = validateLocalE2EGeneratedKMSKey(
    JSON.parse(
      await readFile(path.join(input.tmpDir, "kms.json"), "utf8")
    ) as unknown
  );
  if (kmsKey.region !== input.kms.region) {
    throw new Error(
      `kms.json region ${kmsKey.region} does not match configured ${input.kms.region}`
    );
  }

  const applied = await context.gate.authorize({
    command: "e2e:deploy-local",
    targets: [
      {
        network: chainASpec.name,
        chainId: chainASpec.chainId,
        deploymentIds: [
          LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.chainA,
          LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.pathwayAToB,
        ],
      },
      {
        network: chainBSpec.name,
        chainId: chainBSpec.chainId,
        deploymentIds: [
          LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.chainB,
          LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.pathwayBToA,
        ],
      },
    ],
    actions: [
      "deploy local Endpoint, ULN, OFT, price feed, executor, and two DVNs",
      "configure reciprocal LayerZero pathways and worker fee models",
      "authorize primary and independent secondary DVN verifier roles",
      "fund the chain A KMS signer",
    ],
  });
  if (!applied) {
    return {
      applied: false,
      deploymentIds: LOCAL_E2E_IGNITION_DEPLOYMENT_IDS,
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
          await applyLocalE2EDeploy(
            input,
            context,
            kmsKey,
            chainAContext,
            chainBContext
          )
      )
  );
}

export function assertLocalE2EIgnitionDirectory(
  configuredIgnitionDir: string,
  tmpDir: string
): void {
  const actual = path.resolve(configuredIgnitionDir);
  const expected = path.resolve(tmpDir, "ignition");
  if (actual !== expected) {
    throw new Error(
      `local E2E requires Hardhat Ignition path ${expected}; configured ${actual}`
    );
  }
}

async function applyLocalE2EDeploy(
  input: LocalE2EDeployInput,
  context: LocalE2EDeployContext,
  kmsKey: ReturnType<typeof validateLocalE2EGeneratedKMSKey>,
  chainAContext: WriteNetworkContext,
  chainBContext: WriteNetworkContext
): Promise<LocalE2EDeployResult> {
  const [chainASpec, chainBSpec] = input.chains;
  const deployerAddress = getAddress(chainAContext.signerAddress);
  if (
    deployerAddress.toLowerCase() !== chainBContext.signerAddress.toLowerCase()
  ) {
    throw new Error(
      `local E2E networks use different deployers: ${deployerAddress} and ${chainBContext.signerAddress}`
    );
  }
  const kmsAddress = getAddress(kmsKey.address);
  const workerAddress = getAddress(input.workerAddress);
  const displayUi = context.displayUi ?? true;

  const [unsignedChainA, unsignedChainB] = await Promise.all([
    deployChain(
      chainASpec,
      chainAContext,
      deployerAddress,
      workerAddress,
      LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.chainA,
      initialSupply,
      displayUi,
      input.tmpDir
    ),
    deployChain(
      chainBSpec,
      chainBContext,
      deployerAddress,
      workerAddress,
      LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.chainB,
      0n,
      displayUi,
      input.tmpDir
    ),
  ]);
  const chainA = withSignerRoles(unsignedChainA, {
    executorSigner: kmsAddress,
    dvnSigner: kmsAddress,
  });
  const chainB = withSignerRoles(unsignedChainB, {
    executorSigner: workerAddress,
    dvnSigner: workerAddress,
  });

  const [chainATimestamp, chainBTimestamp] = await Promise.all([
    stableLocalTimestamp(chainAContext),
    stableLocalTimestamp(chainBContext),
  ]);
  await Promise.all([
    deployPathway(
      chainA,
      chainB,
      chainATimestamp,
      deployerAddress,
      chainAContext,
      LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.pathwayAToB,
      displayUi,
      input.tmpDir
    ),
    deployPathway(
      chainB,
      chainA,
      chainBTimestamp,
      deployerAddress,
      chainBContext,
      LOCAL_E2E_IGNITION_DEPLOYMENT_IDS.pathwayBToA,
      displayUi,
      input.tmpDir
    ),
  ]);
  await fundAddress(chainAContext, kmsAddress, signerFunding);

  const output: LocalE2EDeployment = {
    generatedAt: (context.now ?? (() => new Date()))().toISOString(),
    deployer: deployerAddress,
    worker: workerAddress,
    signers: {
      kms: {
        ...kmsKey,
        address: kmsAddress,
        hostEndpoint: input.kms.hostEndpoint,
        containerEndpoint: input.kms.containerEndpoint,
      },
      keystore: { address: workerAddress },
    },
    parameters: {
      confirmations: confirmations.toString(),
      maxMessageSize,
      minLzReceiveGas: minLzReceiveGas.toString(),
      lzReceiveGas: lzReceiveGas.toString(),
      maxLzReceiveGas: maxLzReceiveGas.toString(),
    },
    chains: { a: chainA, b: chainB },
  };

  await Promise.all([
    writeFile(
      path.join(input.tmpDir, "deployments.json"),
      `${jsonStringify(output)}\n`
    ),
    writeFile(
      path.join(input.tmpDir, "worker.host.yaml"),
      workerConfig(output, "host", input.tmpDir, input.databaseURLs)
    ),
    writeFile(
      path.join(input.tmpDir, "worker.container.yaml"),
      workerConfig(output, "container", input.tmpDir, input.databaseURLs)
    ),
  ]);

  return {
    applied: true,
    deploymentIds: LOCAL_E2E_IGNITION_DEPLOYMENT_IDS,
    deployment: output,
  };
}

async function deployChain(
  spec: LocalE2EChainSpec,
  context: WriteNetworkContext,
  deployerAddress: Address,
  workerAddress: Address,
  deploymentId: string,
  supply: bigint,
  displayUi: boolean,
  tmpDir: string
): Promise<UnsignedChainDeployment> {
  const parameters = await writeLocalE2EIgnitionParameters(
    tmpDir,
    deploymentId,
    buildLocalE2EChainParameters(spec, deployerAddress, workerAddress, supply)
  );
  const deployed = await withIgnitionUiOnStderr(() =>
    context.connection.ignition.deploy(LocalE2EChainModule, {
      deploymentId,
      displayUi,
      parameters,
    })
  );
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
  };
}

async function deployPathway(
  source: ChainDeployment,
  destination: ChainDeployment,
  updatedAt: bigint,
  deployerAddress: Address,
  context: WriteNetworkContext,
  deploymentId: string,
  displayUi: boolean,
  tmpDir: string
): Promise<void> {
  const parameters = await writeLocalE2EIgnitionParameters(
    tmpDir,
    deploymentId,
    buildLocalE2EPathwayParameters(
      source,
      destination,
      updatedAt,
      deployerAddress
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

export async function writeLocalE2EIgnitionParameters(
  tmpDir: string,
  deploymentId: string,
  parameters: unknown
): Promise<string> {
  const parametersDir = path.resolve(tmpDir, "ignition-parameters");
  await mkdir(parametersDir, { recursive: true });
  const parametersPath = path.join(parametersDir, `${deploymentId}.json`);
  await writeFile(
    parametersPath,
    `${JSON.stringify(
      parameters,
      (_key, value) =>
        typeof value === "bigint" ? `${value.toString()}n` : value,
      2
    )}\n`
  );
  return parametersPath;
}

export function buildLocalE2EChainParameters(
  spec: LocalE2EChainSpec,
  deployerAddress: Address,
  workerAddress: Address,
  supply: bigint
) {
  return {
    LocalE2EChain: {
      eid: spec.eid,
      owner: deployerAddress,
      tokenName: `Local OFT ${spec.key.toUpperCase()}`,
      tokenSymbol: `LOFT${spec.key.toUpperCase()}`,
      delegate: deployerAddress,
      initialRecipient: deployerAddress,
      initialSupply: supply,
      priceFeedSubmitters: [deployerAddress, workerAddress],
    },
  };
}

export function buildLocalE2EPathwayParameters(
  source: ChainDeployment,
  destination: ChainDeployment,
  updatedAt: bigint,
  deployerAddress: Address
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
      executorFeeModel: localFeeModel(),
      dvnFeeModel: localFeeModel(),
      primaryDVNVerifier: source.dvnSigner,
      secondaryDVNVerifier: deployerAddress,
    },
  };
}

function withSignerRoles(
  chain: UnsignedChainDeployment,
  roles: ChainSignerRoles
): ChainDeployment {
  return { ...chain, ...roles };
}

async function stableLocalTimestamp(
  context: WriteNetworkContext
): Promise<bigint> {
  return (await context.publicClient.getBlock({ blockNumber: 0n })).timestamp;
}

async function fundAddress(
  context: WriteNetworkContext,
  recipient: Address,
  value: bigint
): Promise<void> {
  const account = context.walletClient.account;
  if (account === undefined) {
    throw new Error(`Hardhat network ${context.networkName} has no account`);
  }
  const hash = await context.walletClient.sendTransaction({
    account,
    chain: context.walletClient.chain,
    to: recipient,
    value,
  });
  const receipt = await context.publicClient.waitForTransactionReceipt({
    hash,
  });
  if (receipt.status !== "success") {
    throw new Error(`${context.networkName} signer funding ${hash} failed`);
  }
}

function validateDeployInput(input: LocalE2EDeployInput): LocalE2EChainPair {
  if (input.tmpDir.trim() === "") {
    throw new Error("tmpDir must not be empty");
  }
  const [chainA, chainB] = input.chains;
  if (chainA.key !== "a" || chainB.key !== "b") {
    throw new Error("local e2e chains must be ordered as a then b");
  }
  if (chainA.name === chainB.name) {
    throw new Error("local e2e chains must use distinct Hardhat networks");
  }
  return [chainA, chainB];
}

function localFeeModel() {
  return {
    baseFee: 1n,
    dstGasOverhead: 0n,
    dataSizeOverheadBytes: 0n,
    marginBps: 0,
  };
}

/** Read the signer address recorded in a local geth-style keystore file. */
export function readKeystoreAddress(keystorePath: string): Address {
  let value: unknown;
  try {
    value = JSON.parse(readFileSync(keystorePath, "utf8")) as unknown;
  } catch {
    throw new Error(`local worker keystore could not be read: ${keystorePath}`);
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("local worker keystore must be a JSON object");
  }
  const rawAddress = (value as Record<string, unknown>).address;
  if (typeof rawAddress !== "string") {
    throw new Error("local worker keystore is missing address");
  }
  return getAddress(
    rawAddress.startsWith("0x") ? rawAddress : `0x${rawAddress}`
  );
}

/** Extract one named contract address from an Ignition deploy() result. */
export function deployedAddress(value: unknown, key: string): Address {
  if (typeof value !== "object" || value === null) {
    throw new Error("Ignition deployment did not return contract results");
  }
  const contract = (value as Record<string, unknown>)[key];
  if (typeof contract !== "object" || contract === null) {
    throw new Error(`Ignition deployment result ${key} is missing`);
  }
  const address = (contract as Record<string, unknown>).address;
  if (typeof address !== "string") {
    throw new Error(`Ignition deployment result ${key} has no address`);
  }
  return getAddress(address);
}

export function workerConfig(
  output: LocalE2EDeployment,
  mode: "host" | "container",
  tmpDir: string,
  databaseURLs: LocalE2EDeployInput["databaseURLs"]
): string {
  const rpcURL = (chain: ChainDeployment) =>
    mode === "host" ? chain.hostRpcUrl : chain.containerRpcUrl;
  const keystorePath =
    mode === "host"
      ? path.join(tmpDir, "worker-keystore.json")
      : "/app/tmp/e2e/worker-keystore.json";
  const chainList = [output.chains.a, output.chains.b];
  const pathways = [
    [output.chains.a, output.chains.b],
    [output.chains.b, output.chains.a],
  ] as const;
  return `database_url: ${databaseURLs[mode]}
metrics:
  listen_address: :9090
services:
  executor:
    enabled: true
  dvn:
    enabled: true
tx_manager:
  stale_broadcast_replacement_after_seconds: 2
signers:
  - id: "${output.signers.kms.address}"
    type: kms
    kms:
      key_id: "${output.signers.kms.keyId}"
      region: "${output.signers.kms.region}"
      address: "${output.signers.kms.address}"
      endpoint: "${
        mode === "host"
          ? output.signers.kms.hostEndpoint
          : output.signers.kms.containerEndpoint
      }"
  - id: "${output.signers.keystore.address}"
    type: keystore
    keystore:
      path: ${keystorePath}
      password_env: E2E_KEYSTORE_PASSWORD
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
    confirmations: ${output.parameters.confirmations}
    start_block_number: 0
    indexer_query_block_range: 500
    indexer_poll_interval_seconds: 5
    rpc_urls:
      - ${rpcURL(chain)}
    tx_roles:
      executor:
        signer: "${chain.executorSigner}"
        max_fee_per_gas_wei: "100000000000"
        max_priority_fee_per_gas_wei: "2000000000"
        min_native_balance_wei: "1000000000000000000"
      dvn:
        signer: "${chain.dvnSigner}"
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
      open_dvn: "${source.primaryOpenDVN}"
      price_feed: "${source.priceFeed}"
    destination_workers:
      open_dvn: "${destination.primaryOpenDVN}"
    send_required_dvns:
      - "${source.primaryOpenDVN}"
      - "${source.secondaryOpenDVN}"
    receive_required_dvns:
      - "${destination.primaryOpenDVN}"
      - "${destination.secondaryOpenDVN}"
    dvn:
      mode: active
    enabled: true
    max_message_size: ${output.parameters.maxMessageSize}
    min_lz_receive_gas: ${output.parameters.minLzReceiveGas}
    max_lz_receive_gas: ${output.parameters.maxLzReceiveGas}`
  )
  .join("\n")}
`;
}
