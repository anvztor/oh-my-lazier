import {
  CONFIG_TYPE_ULN,
  encodeUlnConfig,
  requiredDVNsConfig,
} from "./lz-config.js";
import {
  jsonStringify,
  loadABIArtifact,
  waitForTx,
  type ChainClients,
} from "./lib.js";
import type { Address } from "viem";

const endpointArtifact = loadABIArtifact(
  "node_modules/@layerzerolabs/lz-evm-protocol-v2/artifacts/contracts/interfaces/ILayerZeroEndpointV2.sol/ILayerZeroEndpointV2.json"
);

export type ConfigureLzDVNInput = {
  endpoint: Address;
  oapp: Address;
  remoteEid: number;
  sendUln: Address;
  receiveUln: Address;
  requiredDVNs: Address[];
  sendConfirmations: bigint;
  receiveConfirmations: bigint;
};

export function buildConfigureLzDVNPlan(input: ConfigureLzDVNInput) {
  if (input.sendConfirmations < 1n || input.receiveConfirmations < 1n) {
    throw new Error("ULN confirmations must be at least 1 on both libraries");
  }
  // The worker startup validation enforces send >= receive per pathway; reject
  // the inconsistent write here instead of producing a pathway the worker
  // refuses to run.
  if (input.sendConfirmations < input.receiveConfirmations) {
    throw new Error(
      `send confirmations ${input.sendConfirmations} must be at least receive confirmations ${input.receiveConfirmations}`
    );
  }
  const sendUlnConfig = requiredDVNsConfig(
    input.sendConfirmations,
    input.requiredDVNs
  );
  const receiveUlnConfig = requiredDVNsConfig(
    input.receiveConfirmations,
    input.requiredDVNs
  );
  return {
    endpoint: input.endpoint,
    oapp: input.oapp,
    remoteEid: input.remoteEid,
    sendUln: input.sendUln,
    receiveUln: input.receiveUln,
    sendUlnConfig,
    receiveUlnConfig,
    encodedSendConfig: encodeUlnConfig(sendUlnConfig),
    encodedReceiveConfig: encodeUlnConfig(receiveUlnConfig),
  };
}

export async function configureLzDVN(
  input: ConfigureLzDVNInput,
  clients: ChainClients
): Promise<void> {
  const plan = buildConfigureLzDVNPlan(input);

  for (const [label, library, encodedConfig] of [
    ["SendUln302", input.sendUln, plan.encodedSendConfig],
    ["ReceiveUln302", input.receiveUln, plan.encodedReceiveConfig],
  ] as const) {
    await waitForTx(
      clients.publicClient,
      `Endpoint.setConfig ${label} UlnConfig`,
      await clients.walletClient.writeContract({
        address: input.endpoint,
        abi: endpointArtifact.abi,
        functionName: "setConfig",
        args: [
          input.oapp,
          library,
          [
            {
              eid: input.remoteEid,
              configType: CONFIG_TYPE_ULN,
              config: encodedConfig,
            },
          ],
        ],
        account: clients.account,
        chain: clients.walletClient.chain,
      })
    );
  }

  console.log(
    jsonStringify({
      endpoint: plan.endpoint,
      oapp: plan.oapp,
      remoteEid: plan.remoteEid,
      sendUln: plan.sendUln,
      receiveUln: plan.receiveUln,
      sendUlnConfig: plan.sendUlnConfig,
      receiveUlnConfig: plan.receiveUlnConfig,
    })
  );
}
