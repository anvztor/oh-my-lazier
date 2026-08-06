import type { HardhatRuntimeEnvironment } from "hardhat/types/hre";
import {
  resolveRegtestDeployInput,
  runRegtestDeploy,
  type RegtestDeployBusinessInput,
} from "../regtest-deploy.js";
import { createApplyGate, loadScriptRunFile } from "../command-harness.js";
import { jsonStringify } from "../lib.js";
import {
  enumField,
  optionalBigintField,
  parseInputObject,
  stringField,
} from "../commands/input-parsers.js";
import { requireApplyFlag } from "../commands/runtime.js";

export function parseRegtestDeployCommandInput(
  value: unknown,
  label: string
): RegtestDeployBusinessInput {
  const input = parseInputObject(value, label, [
    "tmpDir",
    "confirmations",
    "initialSupply",
    "dvnMode",
  ]);
  return {
    tmpDir: stringField(input, "tmpDir", label),
    confirmations: optionalBigintField(input, "confirmations", label),
    initialSupply: optionalBigintField(input, "initialSupply", label),
    dvnMode:
      input.dvnMode === undefined
        ? undefined
        : enumField(input, "dvnMode", ["active", "shadow"] as const, label),
  };
}

export async function runRegtestDeployCommand(
  hre: HardhatRuntimeEnvironment
): Promise<void> {
  const runFile = loadScriptRunFile(parseRegtestDeployCommandInput);
  requireApplyFlag(runFile);
  await hre.tasks.getTask(["build"]).run({
    force: false,
    files: [],
    quiet: true,
    defaultBuildProfile: "production",
    noTests: true,
    noContracts: false,
  });
  const result = await runRegtestDeploy(
    resolveRegtestDeployInput(runFile.input),
    {
      hre,
      gate: createApplyGate(runFile),
      displayUi: true,
    }
  );
  console.log(jsonStringify(result));
}
