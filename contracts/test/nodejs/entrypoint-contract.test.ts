import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

type PackageJSON = {
  scripts: Record<string, string>;
  dependencies: Record<string, string>;
};

const hardhatScriptNames = [
  "deploy:open-workers",
  "deploy:open-dvn-worker",
  "deploy:test-oft",
  "configure:oapp-endpoint",
  "configure:open-workers-pathway",
  "configure:open-dvn-pathway",
  "deploy:profile",
  "render:oft-pathway-params",
  "configure:workers",
  "generate:lzabi",
  "check:lzabi",
  "generate:pricing-abi",
  "check:pricing-abi",
  "inspect:lz-config",
  "configure:lz-executor",
  "configure:lz-dvn",
  "configure:lz-rollback",
  "send:oft",
  "send:oft-canary",
  "check:oft-canary",
  "check:dvn-verification",
  "check:lz-addresses",
  "check:deployment-preflight",
  "oft:pathway",
  "check:price-config",
  "e2e:deploy-local",
  "e2e:run-local",
  "regtest:deploy",
  "regtest:send",
  "check:migration-evidence",
  "check:npm-audit-disposition",
  "check:security-review",
  "check:runbooks",
] as const;

const thinOrchestrationWrappers = [
  "oft-pathway.ts",
  "configure-lz-executor.ts",
  "configure-lz-dvn.ts",
  "check-dvn-verification.ts",
  "inspect-lz-config.ts",
  "configure-lz-rollback.ts",
  "check-deployment-preflight.ts",
  "e2e-local-deploy.ts",
  "regtest-deploy.ts",
  "regtest-send.ts",
  "check-migration-evidence.ts",
  "check-oft-canary.ts",
  "check-price-config.ts",
  "e2e-local-run.ts",
  "configure-workers.ts",
  "deploy-profile.ts",
] as const;

const fixedIgnitionCommands = {
  "deploy:open-workers": ["deploy-open-workers.ts", "OpenWorkers"],
  "deploy:open-dvn-worker": ["deploy-open-dvn-worker.ts", "OpenDVNWorker"],
  "deploy:test-oft": ["deploy-test-oft.ts", "TestOFT"],
  "configure:oapp-endpoint": [
    "configure-oapp-endpoint.ts",
    "OAppEndpointConfig",
  ],
  "configure:open-workers-pathway": [
    "configure-open-workers-pathway.ts",
    "OpenWorkersPathwayConfig",
  ],
  "configure:open-dvn-pathway": [
    "configure-open-dvn-pathway.ts",
    "OpenDVNPathwayConfig",
  ],
} as const;

test("every contract-script entrypoint is a Hardhat run wrapper", async () => {
  const packageJSON = JSON.parse(
    await readFile("package.json", "utf8")
  ) as PackageJSON;

  for (const name of hardhatScriptNames) {
    const command = packageJSON.scripts[name];
    assert.ok(command, `missing npm script ${name}`);
    assert.match(command, /^hardhat run --no-compile\b/, name);
    assert.doesNotMatch(command, /\btsx\b|hardhat ignition deploy/, name);
    assert.match(command, /contracts\/scripts\/commands\/.+\.ts$/, name);
  }
  for (const [name, [entrypoint, moduleName]] of Object.entries(
    fixedIgnitionCommands
  )) {
    const command = packageJSON.scripts[name];
    assert.match(command, /--build-profile production\b/, name);
    assert.match(command, new RegExp(`${entrypoint}$`), name);
    const source = await readFile(
      path.join("contracts/scripts/commands", entrypoint),
      "utf8"
    );
    assert.match(
      source,
      new RegExp(
        `from "\\.\\./\\.\\./ignition/modules/${moduleName}\\.js";`
      ),
      name
    );
    assert.match(source, new RegExp(`command: "${name}"`), name);
    if (name.startsWith("deploy:")) {
      assert.match(source, /supportsSourceVerification: true/, name);
    } else {
      assert.doesNotMatch(source, /supportsSourceVerification/, name);
    }
  }
  assert.equal(packageJSON.dependencies.tsx, undefined);
  assert.equal(
    packageJSON.scripts["test:scripts"],
    "hardhat test nodejs --no-compile"
  );
});

test("business modules contain no legacy direct-entrypoint or CLI fallback", async () => {
  const files = await typescriptFiles("contracts/scripts");
  const sources = await Promise.all(
    files
      .filter((file) => !file.includes(`${path.sep}commands${path.sep}`))
      .map(async (file) => `${file}\n${await readFile(file, "utf8")}`)
  );
  const combined = sources.join("\n");

  assert.doesNotMatch(combined, /\bisMainModule\b/);
  assert.doesNotMatch(combined, /\bparseCLIParams\b/);
  assert.doesNotMatch(combined, /process\.argv/);
  assert.doesNotMatch(
    combined,
    /readFile(?:Sync)?\([^)]*deployed_addresses\.json/
  );
});

test("orchestration entrypoints remain thin command-core delegates", async () => {
  for (const wrapper of thinOrchestrationWrappers) {
    const source = await readFile(
      path.join("contracts/scripts/commands", wrapper),
      "utf8"
    );
    const lines = source.trim().split(/\r?\n/);
    assert.ok(lines.length <= 5, `${wrapper} has ${lines.length} lines`);
    assert.match(source, /from "\.\.\/command-cores\/.+\.js";/, wrapper);
    assert.match(source, /await runCommand\(/, wrapper);
    assert.doesNotMatch(
      source,
      /loadScriptRunFile|withReadOnlyConnection|withWriteConnection/,
      wrapper
    );
  }
});

async function typescriptFiles(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const resolved = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await typescriptFiles(resolved)));
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      files.push(resolved);
    }
  }
  return files;
}
