// Temporary ops script (k8s-deploy rehearsal): set the ULN config on ONE
// library (send or receive) so send/receive confirmations can differ.
// Env: RPC_URL CHAIN_ID PRIVATE_KEY ENDPOINT OAPP REMOTE_EID TARGET_LIB
//      REQUIRED_DVNS CONFIRMATIONS
import {
  CONFIG_TYPE_ULN,
  encodeUlnConfig,
  requiredDVNsConfig,
} from "./lz-config.js";
import {
  assertConfiguredChain,
  createClients,
  envAddress,
  envAddressList,
  envBigInt,
  envUint32,
  jsonStringify,
  loadABIArtifact,
  waitForTx,
} from "./lib.js";

const endpointArtifact = loadABIArtifact(
  "node_modules/@layerzerolabs/lz-evm-protocol-v2/artifacts/contracts/interfaces/ILayerZeroEndpointV2.sol/ILayerZeroEndpointV2.json",
);

const { account, publicClient, walletClient } = createClients();
await assertConfiguredChain(publicClient);
const endpoint = envAddress("ENDPOINT");
const oapp = envAddress("OAPP");
const remoteEid = envUint32("REMOTE_EID");
const targetLib = envAddress("TARGET_LIB");
const requiredDVNs = envAddressList("REQUIRED_DVNS");
const confirmations = envBigInt("CONFIRMATIONS");

const ulnConfig = requiredDVNsConfig(confirmations, requiredDVNs);
const encodedUlnConfig = encodeUlnConfig(ulnConfig);

await waitForTx(
  publicClient,
  `Endpoint.setConfig ULN lib=${targetLib} confirmations=${confirmations}`,
  await walletClient.writeContract({
    address: endpoint,
    abi: endpointArtifact.abi,
    functionName: "setConfig",
    args: [
      oapp,
      targetLib,
      [
        {
          eid: remoteEid,
          configType: CONFIG_TYPE_ULN,
          config: encodedUlnConfig,
        },
      ],
    ],
    account,
    chain: walletClient.chain,
  }),
);

console.log(
  jsonStringify({ endpoint, oapp, remoteEid, targetLib, ulnConfig }),
);
