import hre from "hardhat";
import { runRegtestDeployCommand } from "../command-cores/regtest-deploy.js";
import { runCommand } from "../command-harness.js";

await runCommand(() => runRegtestDeployCommand(hre));
