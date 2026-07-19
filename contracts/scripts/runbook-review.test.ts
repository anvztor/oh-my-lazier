import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  validateRunbookDocuments,
  validateRunbookReview,
} from "./runbook-review.js";

test("runbook review accepts current repository documents and alert rules", () => {
  assert.deepEqual(validateRunbookReview(), []);
});

test("runbook review rejects missing required alert rules", () => {
  const documents = currentDocuments();
  const alerts = documents.get("docs/monitoring/prometheus-alerts.yml");
  assert.ok(alerts);
  documents.set(
    "docs/monitoring/prometheus-alerts.yml",
    alerts.replace("alert: LazDVNQuorumConflict", "alert: MissingRule"),
  );

  assert.deepEqual(validateRunbookDocuments(documents), [
    "docs/monitoring/prometheus-alerts.yml: missing alert LazDVNQuorumConflict",
  ]);
});

test("runbook review rejects missing indexer poll alert", () => {
  const documents = currentDocuments();
  const alerts = documents.get("docs/monitoring/prometheus-alerts.yml");
  assert.ok(alerts);
  documents.set(
    "docs/monitoring/prometheus-alerts.yml",
    alerts.replace("alert: LazIndexerPollFailing", "alert: MissingRule"),
  );

  assert.deepEqual(validateRunbookDocuments(documents), [
    "docs/monitoring/prometheus-alerts.yml: missing alert LazIndexerPollFailing",
  ]);
});

test("runbook review rejects a fixed-delay indexer failure alert", () => {
  const documents = currentDocuments();
  const alerts = documents.get("docs/monitoring/prometheus-alerts.yml");
  assert.ok(alerts);
  documents.set(
    "docs/monitoring/prometheus-alerts.yml",
    alerts.replace(
      `        expr: |
          laz_indexer_poll_success == 0
          and laz_indexer_failure_since_timestamp_seconds > 0
          and time() - laz_indexer_failure_since_timestamp_seconds
            > laz_indexer_poll_interval_seconds
`,
      "        expr: laz_indexer_poll_success == 0\n",
    ),
  );

  assert.deepEqual(validateRunbookDocuments(documents), [
    'docs/monitoring/prometheus-alerts.yml: alert LazIndexerPollFailing missing required anchor "laz_indexer_failure_since_timestamp_seconds > 0"',
    'docs/monitoring/prometheus-alerts.yml: alert LazIndexerPollFailing missing required anchor "time() - laz_indexer_failure_since_timestamp_seconds"',
    'docs/monitoring/prometheus-alerts.yml: alert LazIndexerPollFailing missing required anchor "> laz_indexer_poll_interval_seconds"',
  ]);
});

test("runbook review rejects a fixed-window indexer stalled alert", () => {
  const documents = currentDocuments();
  const alerts = documents.get("docs/monitoring/prometheus-alerts.yml");
  assert.ok(alerts);
  documents.set(
    "docs/monitoring/prometheus-alerts.yml",
    alerts.replace(
      `        expr: |
          time() - (
            (laz_indexer_last_poll_timestamp_seconds > 0)
            or laz_indexer_start_timestamp_seconds
          ) > 2 * laz_indexer_poll_interval_seconds
`,
      "        expr: changes(laz_indexer_cursor_last_block[10m]) == 0\n",
    ),
  );

  assert.deepEqual(validateRunbookDocuments(documents), [
    'docs/monitoring/prometheus-alerts.yml: alert LazIndexerPollStalled missing required anchor "laz_indexer_last_poll_timestamp_seconds > 0"',
    'docs/monitoring/prometheus-alerts.yml: alert LazIndexerPollStalled missing required anchor "or laz_indexer_start_timestamp_seconds"',
    'docs/monitoring/prometheus-alerts.yml: alert LazIndexerPollStalled missing required anchor "> 2 * laz_indexer_poll_interval_seconds"',
  ]);
});

test("runbook review rejects missing runbook anchors", () => {
  const documents = currentDocuments();
  const monitoring = documents.get("docs/runbooks/monitoring.md");
  assert.ok(monitoring);
  documents.set(
    "docs/runbooks/monitoring.md",
    monitoring.replace("LazWorkerReadinessFailed", "MissingReadinessAlert"),
  );

  assert.deepEqual(validateRunbookDocuments(documents), [
    'docs/runbooks/monitoring.md: missing required anchor "LazWorkerReadinessFailed"',
  ]);
});

function currentDocuments(): Map<string, string> {
  return new Map([
    [
      "docs/runbooks/mainnet-readiness.md",
      readFileSync("docs/runbooks/mainnet-readiness.md", "utf8"),
    ],
    [
      "docs/runbooks/monitoring.md",
      readFileSync("docs/runbooks/monitoring.md", "utf8"),
    ],
    [
      "docs/runbooks/config-diff.md",
      readFileSync("docs/runbooks/config-diff.md", "utf8"),
    ],
    [
      "docs/runbooks/key-management.md",
      readFileSync("docs/runbooks/key-management.md", "utf8"),
    ],
    [
      "docs/runbooks/price-bot.md",
      readFileSync("docs/runbooks/price-bot.md", "utf8"),
    ],
    [
      "docs/runbooks/rate-limit.md",
      readFileSync("docs/runbooks/rate-limit.md", "utf8"),
    ],
    [
      "docs/deployments/sepolia-hoodi/migration-evidence.json",
      readFileSync(
        "docs/deployments/sepolia-hoodi/migration-evidence.json",
        "utf8",
      ),
    ],
    [
      "docs/monitoring/prometheus-alerts.yml",
      readFileSync("docs/monitoring/prometheus-alerts.yml", "utf8"),
    ],
  ]);
}
