import hre from "hardhat";
import { runRegtestSendCommand } from "../command-cores/regtest-send.js";
import { runCommand } from "../command-harness.js";

await runCommand(() => runRegtestSendCommand(hre));
