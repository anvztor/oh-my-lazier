import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import { loadRegtestDeployment } from "../regtest-deploy.js";
import {
  buildRegtestSendPlan,
  runRegtestSend,
  type RegtestSendInput,
} from "../regtest-send.js";
import { createApplyGate, loadScriptRunFile } from "../command-harness.js";
import { jsonStringify } from "../lib.js";
import {
  bigintField,
  enumField,
  parseInputObject,
  stringField,
  uint32Field,
} from "../commands/input-parsers.js";
import { requireApplyFlag } from "../commands/runtime.js";

export function parseRegtestSendCommandInput(
  value: unknown,
  label: string
): RegtestSendInput {
  const input = parseInputObject(value, label, [
    "tmpDir",
    "direction",
    "amountLD",
    "timeoutMs",
  ]);
  return {
    tmpDir: stringField(input, "tmpDir", label),
    direction: enumField(input, "direction", ["ab", "ba"] as const, label),
    amountLD: bigintField(input, "amountLD", label),
    timeoutMs: uint32Field(input, "timeoutMs", label),
  };
}

export async function runRegtestSendCommand(
  hre: HardhatRuntimeEnvironment
): Promise<void> {
  const runFile = loadScriptRunFile(parseRegtestSendCommandInput);
  const apply = requireApplyFlag(runFile);
  const deployment = await loadRegtestDeployment(runFile.input.tmpDir);
  const plan = buildRegtestSendPlan(runFile.input, deployment);
  if (!apply) {
    console.log(jsonStringify({ applied: false, plan }));
    return;
  }
  const result = await runRegtestSend(runFile.input, deployment, {
    hre,
    gate: createApplyGate(runFile),
  });
  console.log(jsonStringify({ applied: true, ...result }));
}
