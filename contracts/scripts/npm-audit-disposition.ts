import { execFileSync } from "node:child_process";

type AuditVulnerability = {
  name: string;
  severity: "info" | "low" | "moderate" | "high" | "critical";
  isDirect?: boolean;
};

type AuditReport = {
  vulnerabilities?: Record<string, AuditVulnerability>;
  metadata?: {
    vulnerabilities?: Partial<
      Record<AuditVulnerability["severity"] | "total", number>
    >;
  };
  message?: string;
};

const allowedOpenFindings = new Map<string, AuditVulnerability["severity"]>([
  ["@chainlink/contracts-ccip", "high"],
  ["@layerzerolabs/lz-evm-messagelib-v2", "high"],
  ["@layerzerolabs/lz-evm-oapp-v2", "high"],
  ["@nomicfoundation/hardhat-ignition", "high"],
  ["@nomicfoundation/hardhat-ignition-viem", "high"],
  ["@nomicfoundation/hardhat-keystore", "high"],
  ["@nomicfoundation/hardhat-network-helpers", "high"],
  ["@nomicfoundation/hardhat-node-test-runner", "high"],
  ["@nomicfoundation/hardhat-toolbox-viem", "high"],
  ["@nomicfoundation/hardhat-verify", "high"],
  ["@nomicfoundation/hardhat-viem", "high"],
  ["@nomicfoundation/hardhat-viem-assertions", "high"],
  ["@openzeppelin/contracts", "high"],
  ["@openzeppelin/contracts-upgradeable", "high"],
  ["adm-zip", "high"],
  ["hardhat", "high"],
  ["lodash-es", "high"],
  ["tmp", "high"],
  ["@arbitrum/nitro-contracts", "moderate"],
  ["@chainlink/contracts", "moderate"],
  ["@nomicfoundation/ignition-core", "moderate"],
  ["@offchainlabs/upgrade-executor", "moderate"],
  ["@nomicfoundation/hardhat-utils", "moderate"],
  ["@actions/http-client", "moderate"]
]);

function runAudit(): AuditReport {
  try {
    return JSON.parse(
      execFileSync("npm", ["audit", "--audit-level=moderate", "--json"], {
        encoding: "utf8",
      })
    );
  } catch (error) {
    const output = (error as { stdout?: Buffer | string }).stdout;
    if (!output) {
      throw error;
    }
    return JSON.parse(output.toString());
  }
}

export function checkNPMAuditDisposition(): void {
  const report = runAudit();
  const counts = report.metadata?.vulnerabilities;
  if (!counts || !report.vulnerabilities) {
    throw new Error(
      `npm audit did not return a vulnerability report: ${report.message ?? "missing metadata"
      }`
    );
  }

  const critical = counts.critical ?? 0;
  if (critical !== 0) {
    throw new Error(`npm audit critical vulnerabilities: ${critical}`);
  }

  const errors: string[] = [];
  for (const vulnerability of Object.values(report.vulnerabilities)) {
    if (
      vulnerability.severity !== "high" &&
      vulnerability.severity !== "moderate"
    ) {
      continue;
    }
    const expectedSeverity = allowedOpenFindings.get(vulnerability.name);
    if (!expectedSeverity) {
      errors.push(
        `${vulnerability.name}: undisposed ${vulnerability.severity} finding`
      );
      continue;
    }
    if (expectedSeverity !== vulnerability.severity) {
      errors.push(
        `${vulnerability.name}: severity changed from ${expectedSeverity} to ${vulnerability.severity}`
      );
    }
  }

  if (errors.length > 0) {
    throw new Error(
      `npm audit disposition check failed:\n${errors.join("\n")}`
    );
  }

  const openModerateOrHigh = Object.values(report.vulnerabilities).filter(
    (vulnerability) =>
      vulnerability.severity === "high" || vulnerability.severity === "moderate"
  );

  console.log(
    `npm audit disposition ok: critical=0, disposed high/moderate findings=${openModerateOrHigh.length
    }, total=${counts.total ?? 0}`
  );
}
