// regtest-send.ts
//
// Smoke test / demo: send GOATED (TestOFT) from chain A -> chain B (or reverse)
// and wait for the oh-my-lazier worker to relay + deliver. Reads addresses from
// $REGTEST_TMP_DIR/deployments.json (produced by regtest-deploy.ts).
//
// Env:
//   REGTEST_TMP_DIR=tmp/regtest
//   REGTEST_DEPLOYER_PRIVATE_KEY=0xac0974...   (sender + recipient)
//   REGTEST_SEND_DIRECTION=ab | ba              (default ab)
//   REGTEST_SEND_AMOUNT=1000000000000000000     (1e18 default)
//   REGTEST_SEND_TIMEOUT_MS=180000
//   REGTEST_A_HOST_RPC_URL / REGTEST_B_HOST_RPC_URL (override RPCs if needed)

import { readFile } from "node:fs/promises";
import path from "node:path";
import {
  createPublicClient,
  createWalletClient,
  defineChain,
  http,
  type Address,
  type Hex,
  type PublicClient,
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { addressToBytes32, jsonStringify, loadArtifact, optionalEnv } from "./lib.js";

const tmpDir = optionalEnv("REGTEST_TMP_DIR", "tmp/regtest");
const deployerPrivateKey = normalizePrivateKey(
  optionalEnv(
    "REGTEST_DEPLOYER_PRIVATE_KEY",
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
  ),
);
const deployer = privateKeyToAccount(deployerPrivateKey);
const direction = optionalEnv("REGTEST_SEND_DIRECTION", "ab");
const amountLD = BigInt(optionalEnv("REGTEST_SEND_AMOUNT", (10n ** 18n).toString()));
const timeoutMs = Number(optionalEnv("REGTEST_SEND_TIMEOUT_MS", "180000"));

const oftArtifact = loadArtifact(
  "contracts/artifacts/contracts/contracts/oft/TestOFT.sol/TestOFT.json",
);

const deployment = JSON.parse(
  await readFile(path.join(tmpDir, "deployments.json"), "utf8"),
);

const source = direction === "ba" ? deployment.chains.b : deployment.chains.a;
const destination = direction === "ba" ? deployment.chains.a : deployment.chains.b;
source.hostRpcUrl = overrideRpc(source);
destination.hostRpcUrl = overrideRpc(destination);
const lzReceiveGas = BigInt(deployment.parameters.lzReceiveGas);

const src = clientsFor(source);
const dst = clientsFor(destination);

const before = await balanceOf(dst.publicClient, destination.oft, deployer.address);
log("sending", {
  direction: `${source.name}->${destination.name}`,
  src_eid: source.eid,
  dst_eid: destination.eid,
  amount_ld: amountLD.toString(),
  dst_balance_before: before.toString(),
});

// extraOptions is EMPTY: the OApp's enforced options (set at deploy) already
// supply the lzReceive gas. Passing a lzReceive option here too would produce a
// duplicate and revert.
void lzReceiveGas;
const sendParam = {
  dstEid: destination.eid,
  to: addressToBytes32(deployer.address),
  amountLD,
  minAmountLD: amountLD,
  extraOptions: "0x" as Hex,
  composeMsg: "0x" as Hex,
  oftCmd: "0x" as Hex,
};
const quote = await src.publicClient.readContract({
  address: source.oft,
  abi: oftArtifact.abi,
  functionName: "quoteSend",
  args: [sendParam, false],
});
const nativeFee = nativeFeeFromQuote(quote);
log("quoted", { native_fee: nativeFee.toString() });

const hash = await src.walletClient.writeContract({
  address: source.oft,
  abi: oftArtifact.abi,
  functionName: "send",
  args: [sendParam, { nativeFee, lzTokenFee: 0n }, deployer.address],
  account: deployer,
  chain: src.walletClient.chain,
  value: nativeFee,
});
const receipt = await src.publicClient.waitForTransactionReceipt({ hash });
if (receipt.status !== "success") {
  throw new Error(`source send ${hash} failed`);
}
log("source send confirmed", { tx: hash, block: receipt.blockNumber.toString() });

const want = before + amountLD;
const started = Date.now();
let last = 0n;
while (Date.now() - started < timeoutMs) {
  const balance = await balanceOf(dst.publicClient, destination.oft, deployer.address);
  if (balance !== last) {
    log("destination balance", { balance: balance.toString(), want: want.toString() });
    last = balance;
  }
  if (balance >= want) {
    console.log(
      jsonStringify({
        ok: true,
        direction: `${source.name}->${destination.name}`,
        tx: hash,
        delivered_amount: amountLD.toString(),
        dst_balance: balance.toString(),
        elapsed_ms: Date.now() - started,
      }),
    );
    process.exit(0);
  }
  await sleep(2000);
}
throw new Error(
  `timed out after ${timeoutMs}ms waiting for ${source.name}->${destination.name} delivery (worker relaying?)`,
);

function clientsFor(chain: { chainId: number; name: string; hostRpcUrl: string }) {
  const viemChain = defineChain({
    id: chain.chainId,
    name: chain.name,
    nativeCurrency: { name: "Ether", symbol: "ETH", decimals: 18 },
    rpcUrls: { default: { http: [chain.hostRpcUrl] } },
  });
  const transport = http(chain.hostRpcUrl);
  return {
    publicClient: createPublicClient({ chain: viemChain, transport }),
    walletClient: createWalletClient({ account: deployer, chain: viemChain, transport }),
  };
}

function overrideRpc(chain: { key: string; hostRpcUrl: string }): string {
  return optionalEnv(`REGTEST_${chain.key.toUpperCase()}_HOST_RPC_URL`, chain.hostRpcUrl);
}

async function balanceOf(
  publicClient: PublicClient,
  token: Address,
  account: Address,
): Promise<bigint> {
  return (await publicClient.readContract({
    address: token,
    abi: oftArtifact.abi,
    functionName: "balanceOf",
    args: [account],
  })) as bigint;
}

function nativeFeeFromQuote(value: unknown): bigint {
  if (Array.isArray(value)) {
    return BigInt(value[0] as string | number | bigint);
  }
  const record = value as { nativeFee?: bigint; 0?: bigint };
  if (record.nativeFee !== undefined) return record.nativeFee;
  if (record[0] !== undefined) return record[0];
  throw new Error(`unexpected quoteSend return: ${jsonStringify(value)}`);
}

function normalizePrivateKey(value: string): Hex {
  const normalized = value.startsWith("0x") ? value : `0x${value}`;
  if (!/^0x[0-9a-fA-F]{64}$/.test(normalized)) {
    throw new Error("private key must be a 32-byte hex value");
  }
  return normalized as Hex;
}

function log(message: string, fields: Record<string, unknown> = {}) {
  const suffix = Object.entries(fields)
    .map(([k, v]) => `${k}=${String(v)}`)
    .join(" ");
  console.error(`[regtest-send] ${message}${suffix ? ` ${suffix}` : ""}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
