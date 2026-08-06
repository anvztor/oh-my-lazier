// Regtest smoke send: transfer GOATED (TestOFT) across the regtest pathway
// and wait for the autonomous two-worker relay to verify and deliver it.
// Reads addresses from tmpDir/deployments.json produced by regtest:deploy.

import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import { getAddress, type Address, type Hex } from "viem";
import {
  type ApplyGate,
  withReadOnlyConnection,
  withWriteConnection,
} from "./command-harness.js";
import {
  addressToBytes32,
  jsonStringify,
  loadArtifact,
  type ChainClients,
} from "./lib.js";
import {
  forceLegacyFees,
  type RegtestChainDeployment,
  type RegtestDeployment,
} from "./regtest-deploy.js";

const oftArtifact = loadArtifact(
  "contracts/artifacts/contracts/contracts/oft/TestOFT.sol/TestOFT.json"
);

// goat-geth blocks are ~2s; give receipts generous headroom plus polling.
const receiptOptions = { timeout: 180_000, pollingInterval: 1_000 } as const;
const deliveryPollIntervalMs = 2_000;

export type RegtestSendDirection = "ab" | "ba";

export type RegtestSendInput = {
  tmpDir: string;
  direction: RegtestSendDirection;
  amountLD: bigint;
  timeoutMs: number;
};

export type RegtestSendContext = {
  hre: HardhatRuntimeEnvironment;
  gate: Pick<ApplyGate, "authorize">;
};

export type RegtestSendResult = {
  ok: true;
  direction: string;
  tx: Hex;
  deliveredAmount: string;
  destinationBalance: string;
  elapsedMs: number;
};

export function regtestSendChains(
  deployment: RegtestDeployment,
  direction: RegtestSendDirection
): { source: RegtestChainDeployment; destination: RegtestChainDeployment } {
  if (direction === "ab") {
    return { source: deployment.chains.a, destination: deployment.chains.b };
  }
  return { source: deployment.chains.b, destination: deployment.chains.a };
}

export function buildRegtestSendPlan(
  input: RegtestSendInput,
  deployment: RegtestDeployment
) {
  if (input.amountLD <= 0n) {
    throw new Error("amountLD must be positive");
  }
  if (input.timeoutMs <= 0) {
    throw new Error("timeoutMs must be positive");
  }
  const { source, destination } = regtestSendChains(deployment, input.direction);
  return {
    direction: `${source.name}->${destination.name}`,
    source: {
      network: source.name,
      chainId: source.chainId,
      eid: source.eid,
      oft: source.oft,
    },
    destination: {
      network: destination.name,
      chainId: destination.chainId,
      eid: destination.eid,
      oft: destination.oft,
    },
    amountLD: input.amountLD.toString(),
    timeoutMs: input.timeoutMs,
  };
}

/** Send one OFT transfer and wait for worker-relayed destination delivery. */
export async function runRegtestSend(
  input: RegtestSendInput,
  deployment: RegtestDeployment,
  context: RegtestSendContext
): Promise<RegtestSendResult> {
  const plan = buildRegtestSendPlan(input, deployment);
  const { source, destination } = regtestSendChains(deployment, input.direction);
  const applied = await context.gate.authorize({
    command: "regtest:send",
    targets: [{ network: source.name, chainId: source.chainId }],
    actions: [
      `send ${plan.amountLD} GOATED (LD) ${plan.direction} and wait for worker delivery`,
    ],
  });
  if (!applied) {
    throw new Error("regtest:send requires an authorized apply");
  }
  return await withWriteConnection(
    context.hre,
    { network: source.name, expectedChainId: source.chainId },
    async (sourceContext) =>
      await withReadOnlyConnection(
        context.hre,
        { network: destination.name, expectedChainId: destination.chainId },
        async (destinationContext) => {
          // The send must honor the chain's legacy pin recorded at deploy time
          // (goat-geth can drop EIP-1559 transactions from its mempool).
          forceLegacyFees(sourceContext, source);
          const account = sourceContext.walletClient.account;
          if (account === undefined) {
            throw new Error(
              `Hardhat network ${source.name} wallet has no account`
            );
          }
          const sender: ChainClients = {
            account,
            publicClient: sourceContext.publicClient,
            walletClient: sourceContext.walletClient,
          };
          const recipient = getAddress(sourceContext.signerAddress);
          return await sendAndAwaitDelivery(
            input,
            plan.direction,
            source,
            destination,
            sender,
            destinationContext.publicClient,
            recipient
          );
        }
      )
  );
}

async function sendAndAwaitDelivery(
  input: RegtestSendInput,
  direction: string,
  source: RegtestChainDeployment,
  destination: RegtestChainDeployment,
  sender: ChainClients,
  destinationClient: {
    readContract: ChainClients["publicClient"]["readContract"];
  },
  recipient: Address
): Promise<RegtestSendResult> {
  const balanceBefore = await balanceOf(
    destinationClient,
    destination.oft,
    recipient
  );
  log("sending", {
    direction,
    src_eid: source.eid,
    dst_eid: destination.eid,
    amount_ld: input.amountLD,
    dst_balance_before: balanceBefore,
  });
  // extraOptions stays EMPTY: the OApp's enforced options (set at deploy)
  // already supply the lzReceive gas; adding it here again would duplicate the
  // option and revert.
  const sendParam = {
    dstEid: destination.eid,
    to: addressToBytes32(recipient),
    amountLD: input.amountLD,
    minAmountLD: input.amountLD,
    extraOptions: "0x" as Hex,
    composeMsg: "0x" as Hex,
    oftCmd: "0x" as Hex,
  };
  const quote = await sender.publicClient.readContract({
    address: source.oft,
    abi: oftArtifact.abi,
    functionName: "quoteSend",
    args: [sendParam, false],
  });
  const nativeFee = nativeFeeFromQuote(quote);
  log("quoted", { native_fee: nativeFee });

  const hash = await sender.walletClient.writeContract({
    address: source.oft,
    abi: oftArtifact.abi,
    functionName: "send",
    args: [sendParam, { nativeFee, lzTokenFee: 0n }, recipient],
    account: sender.account,
    chain: sender.walletClient.chain,
    value: nativeFee,
  });
  const receipt = await sender.publicClient.waitForTransactionReceipt({
    hash,
    ...receiptOptions,
  });
  if (receipt.status !== "success") {
    throw new Error(`source send ${hash} failed`);
  }
  log("source send confirmed", { tx: hash, block: receipt.blockNumber });

  const want = balanceBefore + input.amountLD;
  const started = Date.now();
  let lastReported = balanceBefore;
  for (;;) {
    const balance = await balanceOf(destinationClient, destination.oft, recipient);
    if (balance !== lastReported) {
      log("destination balance", { balance, want });
      lastReported = balance;
    }
    if (balance >= want) {
      return {
        ok: true,
        direction,
        tx: hash,
        deliveredAmount: input.amountLD.toString(),
        destinationBalance: balance.toString(),
        elapsedMs: Date.now() - started,
      };
    }
    if (Date.now() - started >= input.timeoutMs) {
      throw new Error(
        `timed out after ${input.timeoutMs}ms waiting for ${direction} delivery (are both regtest workers running?)`
      );
    }
    await sleep(deliveryPollIntervalMs);
  }
}

async function balanceOf(
  client: { readContract: ChainClients["publicClient"]["readContract"] },
  token: Address,
  account: Address
): Promise<bigint> {
  return (await client.readContract({
    address: token,
    abi: oftArtifact.abi,
    functionName: "balanceOf",
    args: [account],
  })) as bigint;
}

export function nativeFeeFromQuote(value: unknown): bigint {
  if (Array.isArray(value)) {
    return BigInt(value[0] as string | number | bigint);
  }
  const record = value as { nativeFee?: bigint; 0?: bigint };
  if (record.nativeFee !== undefined) {
    return record.nativeFee;
  }
  if (record[0] !== undefined) {
    return record[0];
  }
  throw new Error(`unexpected quoteSend return: ${jsonStringify(value)}`);
}

function log(message: string, fields: Record<string, unknown> = {}) {
  const suffix = Object.entries(fields)
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(" ");
  console.error(`[regtest:send] ${message}${suffix ? ` ${suffix}` : ""}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
