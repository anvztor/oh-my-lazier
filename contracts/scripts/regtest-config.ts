// Infrastructure settings for the two-chain GOATED regtest topology. Values
// resolve from the environment with local defaults; business input arrives
// only through the OML_SCRIPT_PARAMS command envelope.
//
// The regtest is a single trust domain (one deployer, shared OpenPriceFeed,
// shared local infrastructure). It exercises the 2-of-2 required-DVN protocol
// flow only and proves nothing about operational, key, price-source, or
// infrastructure independence; production must not copy its shared
// owner/feed/infra layout.

export type RegtestChainKey = "a" | "b";

export type RegtestChainSpec = {
  key: RegtestChainKey;
  /** Hardhat network name; also used as the worker config chain name. */
  name: string;
  eid: number;
  chainId: number;
  hostRpcUrl: string;
  containerRpcUrl: string;
  /**
   * When set, every transaction on this chain is sent as LEGACY (type 0) with
   * this fixed gas price in wei. goat-geth's own tooling deploys with
   * --legacy and its mempool can drop EIP-1559 transactions; set
   * REGTEST_B_GAS_PRICE for chain B in that setup.
   */
  gasPrice?: bigint;
};

export type RegtestWorkerInfrastructure = {
  index: 1 | 2;
  metricsAddress: string;
  keystorePaths: { host: string; container: string };
  databaseURLs: { host: string; container: string };
};

type ResolveValue = (name: string, fallback: string) => string;

const environmentValue: ResolveValue = (name, fallback) =>
  process.env[name] ?? fallback;

// Chain B defaults to a goat-geth regtest node; run it with network id 31338
// so it matches the static regtest-b Hardhat network chain id.
const defaultChains = [
  {
    key: "a",
    name: "regtest-a",
    eid: 90101,
    chainId: 31337,
    hostRpcUrl: "http://127.0.0.1:18545",
    containerRpcUrl: "http://anvil-a:8545",
  },
  {
    key: "b",
    name: "regtest-b",
    eid: 90102,
    chainId: 31338,
    hostRpcUrl: "http://127.0.0.1:18546",
    containerRpcUrl: "http://goat-geth:8545",
  },
] as const satisfies readonly RegtestChainSpec[];

export function regtestChains(
  resolve: ResolveValue = environmentValue
): readonly RegtestChainSpec[] {
  return defaultChains.map((chain) => {
    const gasPrice = resolve(
      `REGTEST_${chain.key.toUpperCase()}_GAS_PRICE`,
      ""
    );
    if (gasPrice !== "" && !/^[0-9]+$/.test(gasPrice)) {
      throw new Error(
        `REGTEST_${chain.key.toUpperCase()}_GAS_PRICE must be an unsigned wei integer`
      );
    }
    return {
      ...chain,
      hostRpcUrl: resolve(
        `REGTEST_${chain.key.toUpperCase()}_HOST_RPC_URL`,
        chain.hostRpcUrl
      ),
      containerRpcUrl: resolve(
        `REGTEST_${chain.key.toUpperCase()}_CONTAINER_RPC_URL`,
        chain.containerRpcUrl
      ),
      ...(gasPrice === "" ? {} : { gasPrice: BigInt(gasPrice) }),
    };
  });
}

export function regtestWorkerInfrastructure(
  index: 1 | 2,
  tmpDir: string,
  resolve: ResolveValue = environmentValue
): RegtestWorkerInfrastructure {
  return {
    index,
    metricsAddress: resolve(
      `REGTEST_METRICS_ADDR_${index}`,
      index === 1 ? ":9090" : ":9091"
    ),
    keystorePaths: {
      host: resolve(
        `REGTEST_KEYSTORE${index}_PATH`,
        `${tmpDir}/worker-${index}-keystore.json`
      ),
      container: resolve(
        `REGTEST_KEYSTORE${index}_CONTAINER_PATH`,
        `/app/tmp/regtest/worker-${index}-keystore.json`
      ),
    },
    databaseURLs: {
      host: resolve(
        `REGTEST_HOST_DATABASE_URL_${index}`,
        `postgres://laz_worker:laz_worker@127.0.0.1:55433/laz_worker_${index}?sslmode=disable`
      ),
      container: resolve(
        `REGTEST_CONTAINER_DATABASE_URL_${index}`,
        `postgres://laz_worker:laz_worker@postgres:5432/laz_worker_${index}?sslmode=disable`
      ),
    },
  };
}

export function regtestKeystorePasswordEnv(
  resolve: ResolveValue = environmentValue
): string {
  return resolve("REGTEST_KEYSTORE_PASSWORD_ENV", "REGTEST_KEYSTORE_PASSWORD");
}
