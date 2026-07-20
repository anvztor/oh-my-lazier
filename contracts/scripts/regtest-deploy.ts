// regtest-deploy.ts
//
// Deploys the GOATED LayerZero bridge on a LOCAL regtest across two EVM chains
// and emits self-contained oh-my-lazier worker configs. Chain A is the source
// (holds the OFT initial supply); chain B is the destination (e.g. goat-geth).
//
// Topology (mirrors the tested e2e-local-deploy, adapted for goat-geth + an
// AUTONOMOUS worker-only relay):
//   * OApp = oh-my-lazier's TestOFT used as "GOATED" (mirrors testnet3): premint
//     on A, 0 on B, transfer A->B.
//   * TWO required DVNs per pathway (primary + secondary OpenDVN). The
//     oh-my-lazier worker HARD-REQUIRES >=2 required DVNs at runtime
//     (go/internal/dvn/dvn.go) and each worker process verifies exactly ONE DVN,
//     so an autonomous relay runs TWO worker instances:
//       worker 1: OpenExecutor + primary OpenDVN   (signer = worker1 key)
//       worker 2: secondary OpenDVN only           (signer = worker2 key)
//     Each worker uses its OWN database. The executor waits for BOTH on-chain
//     DVN verifications (read from chain state) before delivering, so the two
//     workers coordinate purely through on-chain state.
//   * keystore-only signers (no localstack/KMS). Fully env-driven so chain B can
//     point at goat-geth's RPC/chainId.
//
// Emits into $REGTEST_TMP_DIR (default tmp/regtest):
//   deployments.json          -- all addresses + params
//   worker-1.yaml / worker-1.container.yaml   (executor + primary DVN)
//   worker-2.yaml / worker-2.container.yaml   (secondary DVN)
//
// Env (all optional; defaults shown):
//   REGTEST_TMP_DIR=tmp/regtest
//   REGTEST_DEPLOYER_PRIVATE_KEY=0xac0974...   (anvil[0]; MUST be funded on BOTH chains)
//   REGTEST_WORKER1_PRIVATE_KEY=0x59c6995e...  (anvil[1]; executor + primary DVN; funded on BOTH chains)
//   REGTEST_WORKER2_PRIVATE_KEY=0x5de4111a...  (anvil[2]; secondary DVN; funded on BOTH chains)
//   REGTEST_CONFIRMATIONS=1
//   REGTEST_INITIAL_SUPPLY=1000000000000000000000000  (1_000_000e18)
//   REGTEST_DVN_MODE=active
//   REGTEST_METRICS_ADDR_1=:9090   REGTEST_METRICS_ADDR_2=:9091
//   REGTEST_KEYSTORE_PASSWORD_ENV=REGTEST_KEYSTORE_PASSWORD
//   REGTEST_KEYSTORE1_PATH / REGTEST_KEYSTORE2_PATH (host keystore JSON paths)
//   REGTEST_KEYSTORE1_CONTAINER_PATH / REGTEST_KEYSTORE2_CONTAINER_PATH
//   REGTEST_HOST_DATABASE_URL_1 / REGTEST_HOST_DATABASE_URL_2
//   REGTEST_CONTAINER_DATABASE_URL_1 / REGTEST_CONTAINER_DATABASE_URL_2
//   Chain A: REGTEST_A_EID=90101 REGTEST_A_CHAIN_ID=31337 REGTEST_A_NAME=evm-regtest-a
//            REGTEST_A_HOST_RPC_URL=http://127.0.0.1:18545 REGTEST_A_CONTAINER_RPC_URL=http://anvil-a:8545
//   Chain B: REGTEST_B_EID=90102 REGTEST_B_CHAIN_ID=31338 REGTEST_B_NAME=goat-regtest-b
//            REGTEST_B_HOST_RPC_URL=http://127.0.0.1:18546 REGTEST_B_CONTAINER_RPC_URL=http://goat-geth:8545

import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import {
  createPublicClient,
  createWalletClient,
  defineChain,
  getAddress,
  http,
  type Abi,
  type Address,
  type Hex,
  type PublicClient,
  type WalletClient,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import {
  CONFIG_TYPE_EXECUTOR,
  CONFIG_TYPE_ULN,
  encodeExecutorConfig,
  encodeUlnConfig,
  requiredDVNsConfig,
  type UlnConfig,
} from "./lz-config.js";
import {
  addressToBytes32,
  jsonStringify,
  loadArtifact,
  optionalEnv,
  waitForContract,
} from "./lib.js";

type ChainSpec = {
  key: "a" | "b";
  eid: number;
  chainId: number;
  name: string;
  hostRpcUrl: string;
  containerRpcUrl: string;
  // When set, all txs on this chain are sent as LEGACY with this gasPrice (wei).
  // goat-geth's own tooling deploys with --legacy; EIP-1559 txs can be dropped
  // from its mempool. Set REGTEST_B_GAS_PRICE for chain B.
  gasPrice?: bigint;
};

type ChainDeployment = ChainSpec & {
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

type Clients = {
  publicClient: PublicClient;
  walletClient: WalletClient;
  gasPrice?: bigint;
};

// goat-geth blocks are ~2s; give receipts generous headroom + polling.
const receiptOpts = { timeout: 180_000, pollingInterval: 1_000 } as const;

const tmpDir = optionalEnv("REGTEST_TMP_DIR", "tmp/regtest");
const deployerPrivateKey = normalizePrivateKey(
  optionalEnv(
    "REGTEST_DEPLOYER_PRIVATE_KEY",
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
  ),
);
const worker1PrivateKey = normalizePrivateKey(
  optionalEnv(
    "REGTEST_WORKER1_PRIVATE_KEY",
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
  ),
);
const worker2PrivateKey = normalizePrivateKey(
  optionalEnv(
    "REGTEST_WORKER2_PRIVATE_KEY",
    "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a",
  ),
);
const deployer = privateKeyToAccount(deployerPrivateKey);
const worker1 = privateKeyToAccount(worker1PrivateKey);
const worker2 = privateKeyToAccount(worker2PrivateKey);

const confirmations = BigInt(optionalEnv("REGTEST_CONFIRMATIONS", "1"));
const initialSupply = BigInt(
  optionalEnv("REGTEST_INITIAL_SUPPLY", (1_000_000n * 10n ** 18n).toString()),
);
const dvnMode = optionalEnv("REGTEST_DVN_MODE", "active");
const maxMessageSize = 10_000;
const minLzReceiveGas = 100_000n;
const lzReceiveGas = 250_000n;
const maxLzReceiveGas = 1_000_000n;

const endpointArtifact = loadArtifact(
  "node_modules/@layerzerolabs/lz-evm-protocol-v2/artifacts/contracts/EndpointV2.sol/EndpointV2.json",
);
const sendUlnArtifact = loadArtifact(
  "node_modules/@layerzerolabs/lz-evm-messagelib-v2/artifacts/contracts/uln/uln302/SendUln302.sol/SendUln302.json",
);
const receiveUlnArtifact = loadArtifact(
  "node_modules/@layerzerolabs/lz-evm-messagelib-v2/artifacts/contracts/uln/uln302/ReceiveUln302.sol/ReceiveUln302.json",
);
const oftArtifact = loadArtifact(
  "contracts/artifacts/contracts/contracts/oft/TestOFT.sol/TestOFT.json",
);
const openPriceFeedArtifact = loadArtifact(
  "contracts/artifacts/contracts/contracts/workers/OpenPriceFeed.sol/OpenPriceFeed.json",
);
const openExecutorArtifact = loadArtifact(
  "contracts/artifacts/contracts/contracts/workers/OpenExecutor.sol/OpenExecutor.json",
);
const openDVNArtifact = loadArtifact(
  "contracts/artifacts/contracts/contracts/workers/OpenDVN.sol/OpenDVN.json",
);

const chainSpecs: readonly ChainSpec[] = [
  {
    key: "a",
    eid: Number(optionalEnv("REGTEST_A_EID", "90101")),
    chainId: Number(optionalEnv("REGTEST_A_CHAIN_ID", "31337")),
    name: optionalEnv("REGTEST_A_NAME", "evm-regtest-a"),
    hostRpcUrl: optionalEnv("REGTEST_A_HOST_RPC_URL", "http://127.0.0.1:18545"),
    containerRpcUrl: optionalEnv("REGTEST_A_CONTAINER_RPC_URL", "http://anvil-a:8545"),
    gasPrice: optionalGasPrice("REGTEST_A_GAS_PRICE"),
  },
  {
    key: "b",
    eid: Number(optionalEnv("REGTEST_B_EID", "90102")),
    chainId: Number(optionalEnv("REGTEST_B_CHAIN_ID", "31338")),
    name: optionalEnv("REGTEST_B_NAME", "goat-regtest-b"),
    hostRpcUrl: optionalEnv("REGTEST_B_HOST_RPC_URL", "http://127.0.0.1:18546"),
    containerRpcUrl: optionalEnv("REGTEST_B_CONTAINER_RPC_URL", "http://goat-geth:8545"),
    gasPrice: optionalGasPrice("REGTEST_B_GAS_PRICE"),
  },
];

function optionalGasPrice(name: string): bigint | undefined {
  const v = optionalEnv(name, "");
  return v === "" ? undefined : BigInt(v);
}

await mkdir(tmpDir, { recursive: true });

const deployments = await Promise.all(chainSpecs.map((spec) => deployChain(spec)));
const [chainA, chainB] = deployments;
if (chainA === undefined || chainB === undefined) {
  throw new Error("regtest requires exactly two chains");
}

await configureDirection(chainA, chainB);
await configureDirection(chainB, chainA);

const output = {
  generatedAt: new Date().toISOString(),
  deployer: getAddress(deployer.address),
  workers: {
    worker1: getAddress(worker1.address),
    worker2: getAddress(worker2.address),
  },
  parameters: {
    confirmations: confirmations.toString(),
    maxMessageSize,
    minLzReceiveGas: minLzReceiveGas.toString(),
    lzReceiveGas: lzReceiveGas.toString(),
    maxLzReceiveGas: maxLzReceiveGas.toString(),
    initialSupply: initialSupply.toString(),
    dvnMode,
    requiredDVNCount: 2,
  },
  chains: { a: chainA, b: chainB },
};

await writeFile(path.join(tmpDir, "deployments.json"), `${jsonStringify(output)}\n`);
await writeFile(path.join(tmpDir, "worker-1.yaml"), workerConfig(1, "host"));
await writeFile(path.join(tmpDir, "worker-1.container.yaml"), workerConfig(1, "container"));
await writeFile(path.join(tmpDir, "worker-2.yaml"), workerConfig(2, "host"));
await writeFile(path.join(tmpDir, "worker-2.container.yaml"), workerConfig(2, "container"));

console.log(jsonStringify(output));
console.error(
  `[regtest-deploy] wrote deployments.json + worker-1/2.yaml (+ .container.yaml) into ${tmpDir}`,
);

async function deployChain(spec: ChainSpec): Promise<ChainDeployment> {
  const clients = clientsFor(spec);
  await assertChainId(clients.publicClient, spec);
  const label = spec.name;

  const endpoint = await deploy(clients, `${label} EndpointV2`, endpointArtifact, [spec.eid, deployer.address]);
  const sendUln = await deploy(clients, `${label} SendUln302`, sendUlnArtifact, [endpoint, 0n, 0n]);
  const receiveUln = await deploy(clients, `${label} ReceiveUln302`, receiveUlnArtifact, [endpoint]);
  const oft = await deploy(clients, `${label} TestOFT (GOATED)`, oftArtifact, [
    "Goat Network",
    "GOATED",
    endpoint,
    deployer.address,
    deployer.address,
    spec.key === "a" ? initialSupply : 0n,
  ]);
  const priceFeed = await deploy(clients, `${label} OpenPriceFeed`, openPriceFeedArtifact, [
    deployer.address,
    [deployer.address, worker1.address, worker2.address],
  ]);
  const openExecutor = await deploy(clients, `${label} OpenExecutor`, openExecutorArtifact, [deployer.address, priceFeed]);
  const primaryOpenDVN = await deploy(clients, `${label} OpenDVN primary`, openDVNArtifact, [deployer.address, priceFeed]);
  const secondaryOpenDVN = await deploy(clients, `${label} OpenDVN secondary`, openDVNArtifact, [deployer.address, priceFeed]);

  return {
    ...spec,
    endpoint,
    sendUln,
    receiveUln,
    oft,
    priceFeed,
    openExecutor,
    primaryOpenDVN,
    secondaryOpenDVN,
    primaryDVNSigner: getAddress(worker1.address),
    secondaryDVNSigner: getAddress(worker2.address),
    executorSigner: getAddress(worker1.address),
  };
}

async function configureDirection(source: ChainDeployment, destination: ChainDeployment) {
  const src = clientsFor(source);
  const dst = clientsFor(destination);
  // Two required DVNs (sorted) -- matches the tested e2e flow and satisfies the
  // worker's runtime >=2 required-DVN rule.
  const sourceDVNs = [source.primaryOpenDVN, source.secondaryOpenDVN];
  const ulnOApp = requiredDVNsConfig(confirmations, sourceDVNs); // optionalDVNCount = NIL (255)
  const ulnDefault = defaultUlnConfig(ulnOApp); // optionalDVNCount = 0
  const executorConfig = { maxMessageSize, executor: source.openExecutor };

  await tx(src, `${source.name} Endpoint.registerLibrary SendUln302`, source.endpoint, endpointArtifact.abi, "registerLibrary", [source.sendUln]);
  await tx(src, `${source.name} Endpoint.registerLibrary ReceiveUln302`, source.endpoint, endpointArtifact.abi, "registerLibrary", [source.receiveUln]);
  await tx(src, `${source.name} SendUln302.setDefaultUlnConfigs`, source.sendUln, sendUlnArtifact.abi, "setDefaultUlnConfigs", [[{ eid: destination.eid, config: ulnDefault }]]);
  await tx(src, `${source.name} ReceiveUln302.setDefaultUlnConfigs`, source.receiveUln, receiveUlnArtifact.abi, "setDefaultUlnConfigs", [[{ eid: destination.eid, config: ulnDefault }]]);
  await tx(src, `${source.name} SendUln302.setDefaultExecutorConfigs`, source.sendUln, sendUlnArtifact.abi, "setDefaultExecutorConfigs", [[{ eid: destination.eid, config: executorConfig }]]);
  await tx(src, `${source.name} Endpoint.setDefaultSendLibrary`, source.endpoint, endpointArtifact.abi, "setDefaultSendLibrary", [destination.eid, source.sendUln]);
  await tx(src, `${source.name} Endpoint.setDefaultReceiveLibrary`, source.endpoint, endpointArtifact.abi, "setDefaultReceiveLibrary", [destination.eid, source.receiveUln, 0n]);
  await tx(src, `${source.name} TestOFT.setPeer`, source.oft, oftArtifact.abi, "setPeer", [destination.eid, addressToBytes32(destination.oft)]);
  await tx(src, `${source.name} TestOFT.setEnforcedOptions`, source.oft, oftArtifact.abi, "setEnforcedOptions", [[{ eid: destination.eid, msgType: 1, options: enforcedLzReceiveOption(lzReceiveGas) }]]);
  await tx(src, `${source.name} Endpoint.setConfig SendUln302`, source.endpoint, endpointArtifact.abi, "setConfig", [
    source.oft,
    source.sendUln,
    [
      { eid: destination.eid, configType: CONFIG_TYPE_EXECUTOR, config: encodeExecutorConfig(executorConfig) },
      { eid: destination.eid, configType: CONFIG_TYPE_ULN, config: encodeUlnConfig(ulnOApp) },
    ],
  ]);
  await tx(src, `${source.name} Endpoint.setConfig ReceiveUln302`, source.endpoint, endpointArtifact.abi, "setConfig", [
    source.oft,
    source.receiveUln,
    [{ eid: destination.eid, configType: CONFIG_TYPE_ULN, config: encodeUlnConfig(ulnOApp) }],
  ]);

  await configureSourceWorkers(src, source, destination);

  // Each DVN verifies on the DESTINATION ReceiveUln, so authorize each worker's
  // signer on the DESTINATION OpenDVN it drives.
  await tx(dst, `${destination.name} primary OpenDVN.setVerifier ${destination.primaryDVNSigner}`, destination.primaryOpenDVN, openDVNArtifact.abi, "setVerifier", [destination.primaryDVNSigner, true]);
  await tx(dst, `${destination.name} secondary OpenDVN.setVerifier ${destination.secondaryDVNSigner}`, destination.secondaryOpenDVN, openDVNArtifact.abi, "setVerifier", [destination.secondaryDVNSigner, true]);
}

async function configureSourceWorkers(clients: Clients, source: ChainDeployment, destination: ChainDeployment) {
  const pathwayConfig = { enabled: true, maxMessageSize: BigInt(maxMessageSize), minLzReceiveGas, maxLzReceiveGas };
  const timestamp = (await clients.publicClient.getBlock()).timestamp;
  const priceSnapshot = { dstGasPriceInSrcToken: 1n, dstDataFeePerByteInSrcToken: 0n, updatedAt: timestamp, staleAfter: 86_400n };
  const feeModel = { baseFee: 1n, dstGasOverhead: 0n, dataSizeOverheadBytes: 0n, marginBps: 0 };
  await tx(clients, `${source.name} OpenPriceFeed.setPriceSnapshot`, source.priceFeed, openPriceFeedArtifact.abi, "setPriceSnapshot", [[{ dstEid: destination.eid, snapshot: priceSnapshot }]]);
  for (const workerAddress of [source.openExecutor, source.primaryOpenDVN, source.secondaryOpenDVN]) {
    const abi = workerAddress === source.openExecutor ? openExecutorArtifact.abi : openDVNArtifact.abi;
    await tx(clients, `${source.name} worker.setAllowedSendLib ${workerAddress}`, workerAddress, abi, "setAllowedSendLib", [source.sendUln, true]);
    await tx(clients, `${source.name} worker.setPathwayConfig ${workerAddress}`, workerAddress, abi, "setPathwayConfig", [destination.eid, source.oft, pathwayConfig]);
    await tx(clients, `${source.name} worker.setFeeModel ${workerAddress}`, workerAddress, abi, "setFeeModel", [destination.eid, feeModel]);
  }
}

function defaultUlnConfig(config: UlnConfig): UlnConfig {
  return { ...config, optionalDVNCount: 0 };
}

function enforcedLzReceiveOption(gas: bigint): Hex {
  // OptionsBuilder.newOptions().addExecutorLzReceiveOption(gas, 0):
  //   0x0003  TYPE_3 header | 01 WORKER_ID(executor) | 0011 len(17) | 01 optType(LZRECEIVE) | <16-byte gas>
  const gasHex = gas.toString(16).padStart(32, "0");
  return `0x000301001101${gasHex}` as Hex;
}

async function deploy(clients: Clients, label: string, artifact: { abi: Abi; bytecode: Hex }, args: readonly unknown[]): Promise<Address> {
  const hash = await clients.walletClient.deployContract({
    abi: artifact.abi,
    bytecode: artifact.bytecode,
    args: [...args],
    account: deployer,
    chain: clients.walletClient.chain,
    ...(clients.gasPrice === undefined ? {} : { gasPrice: clients.gasPrice }),
  });
  const receipt = await clients.publicClient.waitForTransactionReceipt({ hash, ...receiptOpts });
  if (receipt.status !== "success" || receipt.contractAddress == null) {
    throw new Error(`${label} deploy ${hash} failed`);
  }
  await waitForContract(clients.publicClient, hash);
  console.error(`[regtest-deploy] ${label}: ${receipt.contractAddress}`);
  return getAddress(receipt.contractAddress);
}

async function tx(clients: Clients, label: string, address: Address, abi: Abi, functionName: string, args: readonly unknown[]): Promise<void> {
  const hash = await clients.walletClient.writeContract({
    address,
    abi,
    functionName,
    args: [...args],
    account: deployer,
    chain: clients.walletClient.chain,
    ...(clients.gasPrice === undefined ? {} : { gasPrice: clients.gasPrice }),
  });
  const receipt = await clients.publicClient.waitForTransactionReceipt({ hash, ...receiptOpts });
  if (receipt.status !== "success") {
    throw new Error(`${label} transaction ${hash} failed`);
  }
  console.error(`[regtest-deploy] ${label}: ${hash}`);
}

function clientsFor(spec: ChainSpec): Clients {
  const chain = defineChain({
    id: spec.chainId,
    name: spec.name,
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: { default: { http: [spec.hostRpcUrl] } },
  });
  const transport = http(spec.hostRpcUrl);
  return {
    publicClient: createPublicClient({ chain, transport }),
    walletClient: createWalletClient({ account: deployer, chain, transport }),
    gasPrice: spec.gasPrice,
  };
}

async function assertChainId(publicClient: PublicClient, spec: ChainSpec) {
  const chainId = await publicClient.getChainId();
  if (chainId !== spec.chainId) {
    throw new Error(`${spec.name} chain_id ${chainId} does not match expected ${spec.chainId} (set REGTEST_${spec.key.toUpperCase()}_CHAIN_ID)`);
  }
}

// worker=1 -> executor + primary DVN; worker=2 -> secondary DVN only.
function workerConfig(workerIndex: 1 | 2, mode: "host" | "container"): string {
  const primary = workerIndex === 1;
  const signer = primary ? getAddress(worker1.address) : getAddress(worker2.address);
  const executorEnabled = primary;
  const metricsAddr = optionalEnv(`REGTEST_METRICS_ADDR_${workerIndex}`, primary ? ":9090" : ":9091");
  const rpc = (c: ChainDeployment) => (mode === "host" ? c.hostRpcUrl : c.containerRpcUrl);
  const keystorePath =
    mode === "host"
      ? optionalEnv(`REGTEST_KEYSTORE${workerIndex}_PATH`, path.join(tmpDir, `worker-${workerIndex}-keystore.json`))
      : optionalEnv(`REGTEST_KEYSTORE${workerIndex}_CONTAINER_PATH`, `/app/tmp/regtest/worker-${workerIndex}-keystore.json`);
  const databaseUrl =
    mode === "host"
      ? optionalEnv(`REGTEST_HOST_DATABASE_URL_${workerIndex}`, `postgres://laz_worker:laz_worker@127.0.0.1:55433/laz_worker_${workerIndex}?sslmode=disable`)
      : optionalEnv(`REGTEST_CONTAINER_DATABASE_URL_${workerIndex}`, `postgres://laz_worker:laz_worker@postgres:5432/laz_worker_${workerIndex}?sslmode=disable`);
  const passwordEnv = optionalEnv("REGTEST_KEYSTORE_PASSWORD_ENV", "REGTEST_KEYSTORE_PASSWORD");
  const srcDVN = (c: ChainDeployment) => (primary ? c.primaryOpenDVN : c.secondaryOpenDVN);
  const chains = [chainA, chainB] as const;
  const pathways = [
    [chainA, chainB],
    [chainB, chainA],
  ] as const;
  return `database_url: ${databaseUrl}
metrics:
  listen_address: ${metricsAddr}
services:
  executor:
    enabled: ${executorEnabled}
  dvn:
    enabled: true
tx_manager:
  stale_broadcast_replacement_after_seconds: 2
signers:
  - id: "${signer}"
    type: keystore
    keystore:
      path: ${keystorePath}
      password_env: ${passwordEnv}
pricing:
  enabled: false
chains:
${chains
  .map(
    (c) => `  - eid: ${c.eid}
    name: ${c.name}
    family: evm
    chain_id: ${c.chainId}
    endpoint_address: "${c.endpoint}"
    confirmations: ${confirmations}
    start_block_number: 0
    indexer_query_block_range: 500
    indexer_poll_interval_seconds: 5
    rpc_urls:
      - ${rpc(c)}
    tx_roles:${
      executorEnabled
        ? `
      executor:
        signer: "${signer}"
        max_fee_per_gas_wei: "100000000000"
        max_priority_fee_per_gas_wei: "2000000000"
        min_native_balance_wei: "1000000000000000000"`
        : ""
    }
      dvn:
        signer: "${signer}"
        max_fee_per_gas_wei: "100000000000"
        max_priority_fee_per_gas_wei: "2000000000"
        min_native_balance_wei: "1000000000000000000"`,
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
      open_dvn: "${srcDVN(source)}"
      price_feed: "${source.priceFeed}"
    destination_workers:
      open_dvn: "${srcDVN(destination)}"
    dvn:
      mode: ${dvnMode}
    enabled: true
    max_message_size: ${maxMessageSize}
    min_lz_receive_gas: ${minLzReceiveGas}
    max_lz_receive_gas: ${maxLzReceiveGas}`,
  )
  .join("\n")}
`;
}

function normalizePrivateKey(value: string): Hex {
  const normalized = value.startsWith("0x") ? value : `0x${value}`;
  if (!/^0x[0-9a-fA-F]{64}$/.test(normalized)) {
    throw new Error("private key must be a 32-byte hex value");
  }
  return normalized as Hex;
}
