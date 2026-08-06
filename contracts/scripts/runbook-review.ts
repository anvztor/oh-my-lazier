import { readFileSync } from "node:fs";

type RequiredDoc = {
  path: string;
  anchors: string[];
};

type RequiredAlertRule = {
  alert: string;
  anchors: string[];
};

const requiredDocs: RequiredDoc[] = [
  {
    path: "docs/runbooks/mainnet-readiness.md",
    anchors: [
      "No `composeMsg`.",
      "No `lzCompose`.",
      "No native drop.",
      "No ordered execution.",
      "No non-EVM chain support.",
      "No self-only DVN.",
      "Required DVNs must include OpenDVN and at least one independent external DVN.",
      "Confirmations must be explicitly configured per chain and match the approved LayerZero ULN configuration.",
      "OML_SCRIPT_PARAMS=<record-params.json> npm run check:migration-evidence",
      "OML_SCRIPT_PARAMS=<inspect-params.json>",
      "--network <network>",
      "apply: false",
      "npm run check:runbooks",
      "make security-check",
      "go run ./go/cmd/readinesscheck -config <worker.yaml>",
      "npm run inspect:lz-config",
      "Reject mainnet readiness if:",
    ],
  },
  {
    path: "docs/runbooks/monitoring.md",
    anchors: [
      "/healthz",
      "/readyz",
      "/metrics",
      "docs/monitoring/prometheus-alerts.yml",
      "LazWorkerReadinessFailed",
      "LazChainPaused",
      "LazPathwayPaused",
      "LazDVNQuorumConflict",
      "LazDVNReorgDetected",
      "LazPacketManualReview",
      "LazExecutorReceiveFailed",
      "LazWorkerManualReview",
      "LazTxOutboxFailed",
      "LazWorkerFeeNegativeMargin",
      "LazWorkerFeeReconciliationPending",
      "LazSignerLowNativeBalance",
      "LazIndexerPollFailing",
      "LazIndexerPollStalled",
      "LazRPCProviderConflict",
      "LazRPCQuorumUnavailable",
      "LazPricingSnapshotNearStale",
      "LazPricingPendingStalled",
      "laz_chain_paused == 1",
      "laz_pathway_paused == 1",
      "laz_indexer_poll_success",
      "laz_indexer_poll_interval_seconds",
      "laz_indexer_start_timestamp_seconds",
      "laz_indexer_last_poll_timestamp_seconds",
      "laz_indexer_last_success_timestamp_seconds",
      "laz_indexer_failure_since_timestamp_seconds",
      "laz_indexer_cursor_last_block",
      "laz_rpc_provider_status",
      "laz_rpc_provider_log_conflict",
      "laz_rpc_provider_state_conflict",
      "laz_signer_native_balance_wei",
      "laz_worker_fee_negative_margin_jobs",
      "laz_worker_fee_unpriced_receipts",
      "go run ./go/cmd/readinesscheck -config <worker.yaml> -format json",
    ],
  },
  {
    path: "docs/runbooks/config-diff.md",
    anchors: [
      "go run ./go/cmd/configdiff",
      "-fail-on-diff",
      "Pricing market-data BaseURLs and signer KMS endpoints",
      "Confirm `tx_manager.stale_broadcast_replacement_after_seconds` changes are intentional.",
      "Confirm signer changes are expected and do not point to unapproved keys.",
      "For DVN migration, confirm each proposed pathway still uses `pathways[].dvn.mode: shadow` until the explicit active-mode change is approved.",
    ],
  },
  {
    path: "docs/runbooks/key-management.md",
    anchors: [
      "AWS KMS `ECC_SECG_P256K1`",
      "local geth keystore JSON",
      "Never infer approval from a successful transaction alone.",
      "Run `make test-integration` when Docker is available",
      "Never log private key material, decrypted keystore content, KMS signatures, or raw secrets.",
      "rollback signer",
    ],
  },
  {
    path: "docs/runbooks/price-bot.md",
    anchors: [
      "go run ./go/cmd/pricebot-once -config <worker.yaml>",
      "npm run check:price-config",
      "OML_SCRIPT_PARAMS=tmp/check-price-config.json",
      "--network sepolia",
      "For each unique source/source price-feed key",
      "`updatedAt` is recent",
      "`staleAfter` matches the approved config",
      "If the newly submitted price snapshot is wrong:",
    ],
  },
  {
    path: "docs/runbooks/rate-limit.md",
    anchors: [
      "pauseSend(uint32 dstEid, bool paused)",
      "pauseReceive(uint32 srcEid, bool paused)",
      "setOutboundRateLimit(uint32 dstEid, RateLimitConfig config)",
      "capacity = 0",
      "OML_SCRIPT_PARAMS=tmp/oft-pathway-drain.json",
      'input.action: "set-rate-limit"',
      "top-level `apply` flag",
      "go run ./go/cmd/draincheck -config <worker.yaml>",
      "Do not approve mainnet readiness if:",
    ],
  },
  {
    path: "contracts/scripts/README.md",
    anchors: [
      "Every executable TypeScript entrypoint in this repository is a thin Hardhat",
      "OML_SCRIPT_PARAMS",
      'accounts: "remote"',
      'confirmation: "approved"',
      "connection.ignition.deploy",
      "listDeployments()",
      "status()",
      "hardhat.verify.config.ts",
      "tmp/e2e/ignition",
    ],
  },
  {
    path: "docs/deployments/test-oft-policy.md",
    anchors: [
      "OML_SCRIPT_PARAMS=tmp/deploy-profile-render.json",
      "NetworkConnection.ignition.deploy()",
      "listDeployments()",
      "status()",
      "OpenWorkers#OpenPriceFeed",
      'confirmation: "approved"',
    ],
  },
  {
    path: "docs/deployments/layerzero-testnet-addresses.md",
    anchors: [
      "input.requiredDVNs",
      "config/scripts/examples",
      "--network sepolia",
      "--network hoodi",
    ],
  },
  {
    path: "docs/deployments/sepolia-hoodi/migration-evidence.json",
    anchors: [
      '"evidenceType": "deployment"',
      '"layerZeroAddressCheck"',
      '"readinessCheck"',
      '"runbookReview"',
      '"ownerAccount"',
      '"signerAccount"',
      '"sourceWorkers"',
      '"destinationWorkers"',
      '"lzConfigAfter"',
      '"canary"',
      '"dvnJoin"',
      '"dvnVerificationReceipt"',
    ],
  },
];

const requiredAlertRules: RequiredAlertRule[] = [
  {
    alert: "LazWorkerReadinessFailed",
    anchors: ['probe_success{job="laz-worker-readyz"} == 0', "severity: page"],
  },
  {
    alert: "LazChainPaused",
    anchors: ["laz_chain_paused == 1", "severity: page"],
  },
  {
    alert: "LazPathwayPaused",
    anchors: ["laz_pathway_paused == 1", "severity: page"],
  },
  {
    alert: "LazDVNQuorumConflict",
    anchors: [
      'laz_dvn_jobs_total{status="QUORUM_CONFLICT"} > 0',
      "severity: page",
    ],
  },
  {
    alert: "LazDVNReorgDetected",
    anchors: [
      'laz_dvn_jobs_total{status="REORG_DETECTED"} > 0',
      "severity: page",
    ],
  },
  {
    alert: "LazPacketManualReview",
    anchors: [
      'laz_packets_total{status="MANUAL_REVIEW"} > 0',
      "severity: ticket",
    ],
  },
  {
    alert: "LazExecutorReceiveFailed",
    anchors: [
      'laz_executor_jobs_total{status="LZ_RECEIVE_FAILED"} > 0',
      "severity: ticket",
    ],
  },
  {
    alert: "LazWorkerManualReview",
    anchors: [
      'laz_executor_jobs_total{status="MANUAL_REVIEW"} > 0 or laz_dvn_jobs_total{status="MANUAL_REVIEW"} > 0',
      "severity: ticket",
    ],
  },
  {
    alert: "LazTxOutboxFailed",
    anchors: [
      'laz_tx_outbox_total{status="failed",retry_state="exhausted"} > 0',
      "severity: ticket",
    ],
  },
  {
    alert: "LazWorkerFeeNegativeMargin",
    anchors: ["laz_worker_fee_negative_margin_jobs > 0", "severity: page"],
  },
  {
    alert: "LazWorkerFeeReconciliationPending",
    anchors: ["laz_worker_fee_unpriced_receipts > 0", "severity: ticket"],
  },
  {
    alert: "LazSignerLowNativeBalance",
    anchors: [
      "laz_signer_native_balance_wei < laz_signer_min_native_balance_wei",
      "severity: page",
    ],
  },
  {
    alert: "LazIndexerPollFailing",
    anchors: [
      "laz_indexer_poll_success == 0",
      "laz_indexer_failure_since_timestamp_seconds > 0",
      "time() - laz_indexer_failure_since_timestamp_seconds",
      "> laz_indexer_poll_interval_seconds",
      "severity: page",
    ],
  },
  {
    alert: "LazIndexerPollStalled",
    anchors: [
      "laz_indexer_last_poll_timestamp_seconds > 0",
      "or laz_indexer_start_timestamp_seconds",
      "> 2 * laz_indexer_poll_interval_seconds",
      "severity: page",
    ],
  },
  {
    alert: "LazRPCProviderConflict",
    anchors: [
      'laz_rpc_provider_status{status="conflict"} == 1 or laz_rpc_provider_log_conflict == 1 or laz_rpc_provider_state_conflict == 1',
      "severity: page",
    ],
  },
  {
    alert: "LazTxOutboxOrphaned",
    anchors: [
      "laz_tx_outbox_orphaned_total > 0",
      "severity: page",
    ],
  },
  {
    alert: "LazPricingSnapshotNearStale",
    anchors: [
      "laz_pricing_snapshot_time_to_stale_seconds < 300",
      "severity: page",
    ],
  },
  {
    alert: "LazPricingPendingStalled",
    anchors: [
      "laz_pricing_pending_oldest_age_seconds > 300",
      "severity: page",
    ],
  },
  {
    alert: "LazRPCQuorumUnavailable",
    anchors: [
      '2 * count by (chain_eid, job, instance) (laz_rpc_provider_status{status="unavailable"}) >= count by (chain_eid, job, instance) (laz_rpc_provider_status)',
      "severity: page",
    ],
  },
];

export function validateRunbookReview(): string[] {
  const documents = new Map<string, string>();
  const errors: string[] = [];
  for (const doc of requiredDocs) {
    try {
      documents.set(doc.path, readFileSync(doc.path, "utf8"));
    } catch (error) {
      errors.push(`${doc.path}: cannot read file: ${(error as Error).message}`);
    }
  }
  try {
    documents.set(
      "docs/monitoring/prometheus-alerts.yml",
      readFileSync("docs/monitoring/prometheus-alerts.yml", "utf8")
    );
  } catch (error) {
    errors.push(
      `docs/monitoring/prometheus-alerts.yml: cannot read file: ${
        (error as Error).message
      }`
    );
  }
  if (errors.length > 0) {
    return errors;
  }
  return validateRunbookDocuments(documents);
}

export function validateRunbookDocuments(
  documents: Map<string, string>
): string[] {
  const errors: string[] = [];
  for (const doc of requiredDocs) {
    const body = documents.get(doc.path);
    if (body === undefined) {
      errors.push(`${doc.path}: cannot read file: missing document body`);
      continue;
    }
    for (const anchor of doc.anchors) {
      if (!body.includes(anchor)) {
        errors.push(
          `${doc.path}: missing required anchor ${JSON.stringify(anchor)}`
        );
      }
    }
  }
  validateAlertRules(documents, errors);
  return errors;
}

function validateAlertRules(
  documents: Map<string, string>,
  errors: string[]
): void {
  const path = "docs/monitoring/prometheus-alerts.yml";
  const body = documents.get(path);
  if (body === undefined) {
    errors.push(`${path}: cannot read file: missing document body`);
    return;
  }
  for (const rule of requiredAlertRules) {
    if (!body.includes(`alert: ${rule.alert}`)) {
      errors.push(`${path}: missing alert ${rule.alert}`);
      continue;
    }
    for (const anchor of rule.anchors) {
      if (!body.includes(anchor)) {
        errors.push(
          `${path}: alert ${
            rule.alert
          } missing required anchor ${JSON.stringify(anchor)}`
        );
      }
    }
  }
  if (!body.includes("runbook: docs/runbooks/monitoring.md")) {
    errors.push(`${path}: alert annotations must link the monitoring runbook`);
  }
}

export function checkRunbookReview(): void {
  const errors = validateRunbookReview();
  if (errors.length > 0) {
    throw new Error(`runbook review check failed:\n${errors.join("\n")}`);
  }
  console.log(
    `runbook review ok: ${requiredDocs.length} documents and ${requiredAlertRules.length} alert rules checked`
  );
}
