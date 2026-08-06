import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import type { Address } from "viem";
import {
  buildConfigureLzDVNPlan,
  configureLzDVN,
  type ConfigureLzDVNInput,
} from "../configure-lz-dvn.js";
import {
  assertExpectedSigner,
  createApplyGate,
  loadScriptRunFile,
  withWriteConnection,
} from "../command-harness.js";
import { jsonStringify } from "../lib.js";
import {
  addressArrayField,
  addressField,
  bigintField,
  parseInputObject,
  uint32Field,
} from "../commands/input-parsers.js";
import {
  chainClients,
  requireApplyFlag,
  requiredNetwork,
} from "../commands/runtime.js";

export type ConfigureLzDVNCommandInput = ConfigureLzDVNInput & {
  expectedSigner: Address;
};

export function parseConfigureLzDVNCommandInput(
  value: unknown,
  label: string
): ConfigureLzDVNCommandInput {
  const input = parseInputObject(value, label, [
    "endpoint",
    "oapp",
    "remoteEid",
    "sendUln",
    "receiveUln",
    "requiredDVNs",
    "sendConfirmations",
    "receiveConfirmations",
    "expectedSigner",
  ]);
  return {
    endpoint: addressField(input, "endpoint", label),
    oapp: addressField(input, "oapp", label),
    remoteEid: uint32Field(input, "remoteEid", label),
    sendUln: addressField(input, "sendUln", label),
    receiveUln: addressField(input, "receiveUln", label),
    requiredDVNs: addressArrayField(input, "requiredDVNs", label),
    sendConfirmations: bigintField(input, "sendConfirmations", label),
    receiveConfirmations: bigintField(input, "receiveConfirmations", label),
    expectedSigner: addressField(input, "expectedSigner", label),
  };
}

export async function runConfigureLzDVNCommand(
  hre: HardhatRuntimeEnvironment
): Promise<void> {
  const runFile = loadScriptRunFile(parseConfigureLzDVNCommandInput);
  const apply = requireApplyFlag(runFile);
  const network = requiredNetwork(hre);
  const plan = buildConfigureLzDVNPlan(runFile.input);
  if (!apply) {
    console.log(jsonStringify({ applied: false, network, plan }));
    return;
  }
  const gate = createApplyGate(runFile);
  await withWriteConnection(hre, { network }, async (context) => {
    assertExpectedSigner(context.signerAddress, runFile.input.expectedSigner);
    await gate.authorize({
      command: "configure:lz-dvn",
      targets: [{ network: context.networkName, chainId: context.chainId }],
      actions: [
        "set LayerZero send and receive ULN configurations (per-library confirmations)",
      ],
    });
    await configureLzDVN(runFile.input, chainClients(context));
  });
}
