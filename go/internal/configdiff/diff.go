package configdiff

import (
	"encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/islishude/oh-my-lazier/go/internal/config"
)

// Change describes one semantic configuration difference.
type Change struct {
	Path   string `json:"path"`
	Before any    `json:"before,omitempty"`
	After  any    `json:"after,omitempty"`
}

// Diff returns a deterministic semantic diff between two validated worker configs.
func Diff(before, after config.Config) []Change {
	var changes []Change
	compareRedactedURL(&changes, "database_url", before.DatabaseURL, after.DatabaseURL, redactDatabaseURL)
	compare(&changes, "metrics", before.Metrics, after.Metrics)
	compare(&changes, "services", services(before), services(after))
	compare(&changes, "tx_manager", before.TxManager, after.TxManager)
	compareProjected(&changes, "pricing", pricingGlobal(before.Pricing), pricingGlobal(after.Pricing), redactPricingGlobal)
	diffMaps(&changes, "pricing.chains", pricingChains(before.Pricing.Chains), pricingChains(after.Pricing.Chains))
	diffMapsProjected(&changes, "signers", signers(before.Signers), signers(after.Signers), redactSigner)
	diffMapsProjected(&changes, "chains", chains(before.Chains), chains(after.Chains), redactChain)
	diffMaps(&changes, "pathways", pathways(before.Pathways), pathways(after.Pathways))
	return changes
}

func compareProjected[T any](changes *[]Change, path string, before, after T, project func(T) T) {
	if reflect.DeepEqual(before, after) {
		return
	}
	*changes = append(*changes, Change{Path: path, Before: project(before), After: project(after)})
}

func diffMapsProjected[T any](changes *[]Change, prefix string, before, after map[string]T, project func(T) T) {
	keys := make([]string, 0, len(before)+len(after))
	seen := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys = append(keys, key)
		seen[key] = struct{}{}
	}
	for key := range after {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		beforeValue, beforeOK := before[key]
		afterValue, afterOK := after[key]
		path := fmt.Sprintf("%s[%s]", prefix, key)
		switch {
		case !beforeOK:
			*changes = append(*changes, Change{Path: path, After: project(afterValue)})
		case !afterOK:
			*changes = append(*changes, Change{Path: path, Before: project(beforeValue)})
		case !reflect.DeepEqual(beforeValue, afterValue):
			*changes = append(*changes, Change{Path: path, Before: project(beforeValue), After: project(afterValue)})
		}
	}
}

// RenderText renders a human-readable diff for review logs.
func RenderText(changes []Change) string {
	if len(changes) == 0 {
		return "no config changes\n"
	}
	var output strings.Builder
	for _, change := range changes {
		fmt.Fprintf(&output, "%s\n", change.Path)
		fmt.Fprintf(&output, "  before: %s\n", jsonValue(change.Before))
		fmt.Fprintf(&output, "  after:  %s\n", jsonValue(change.After))
	}
	return output.String()
}

func diffMaps[T any](changes *[]Change, prefix string, before, after map[string]T) {
	keys := make([]string, 0, len(before)+len(after))
	seen := make(map[string]struct{}, len(before)+len(after))
	for key := range before {
		keys = append(keys, key)
		seen[key] = struct{}{}
	}
	for key := range after {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		beforeValue, beforeOK := before[key]
		afterValue, afterOK := after[key]
		path := fmt.Sprintf("%s[%s]", prefix, key)
		switch {
		case !beforeOK:
			*changes = append(*changes, Change{Path: path, After: afterValue})
		case !afterOK:
			*changes = append(*changes, Change{Path: path, Before: beforeValue})
		default:
			compare(changes, path, beforeValue, afterValue)
		}
	}
}

func compare(changes *[]Change, path string, before, after any) {
	if reflect.DeepEqual(before, after) {
		return
	}
	*changes = append(*changes, Change{Path: path, Before: before, After: after})
}

func compareRedactedURL(changes *[]Change, path, before, after string, redact func(string) string) {
	if before == after {
		return
	}
	*changes = append(*changes, Change{Path: path, Before: redact(before), After: redact(after)})
}

func chains(items []config.ChainConfig) map[string]config.ChainConfig {
	result := make(map[string]config.ChainConfig, len(items))
	for _, item := range items {
		result[fmt.Sprintf("%d", item.EID)] = item
	}
	return result
}

func signers(items []config.SignerConfig) map[string]config.SignerConfig {
	result := make(map[string]config.SignerConfig, len(items))
	for _, item := range items {
		result[item.ID.Hex()] = item
	}
	return result
}

func redactSigner(item config.SignerConfig) config.SignerConfig {
	item.Keystore.PasswordEnv = redactSecretEnvironmentReference(item.Keystore.PasswordEnv)
	item.Keystore.PasswordFile = redactSecretFileReference(item.Keystore.PasswordFile)
	item.KMS.Endpoint = redactHTTPURL(item.KMS.Endpoint)
	return item
}

func redactChain(item config.ChainConfig) config.ChainConfig {
	item.RPCURLs = append([]string(nil), item.RPCURLs...)
	for index, rpcURL := range item.RPCURLs {
		item.RPCURLs[index] = redactRPCURL(rpcURL)
	}
	return item
}

func redactDatabaseURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Opaque != "" {
		return "[REDACTED]"
	}
	redactedQuery := url.Values{}
	if values := parsed.Query()["sslmode"]; len(values) == 1 && safePostgresSSLMode(values[0]) {
		redactedQuery.Set("sslmode", values[0])
	}
	parsed.User = nil
	parsed.RawQuery = redactedQuery.Encode()
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

func safePostgresSSLMode(value string) bool {
	switch value {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

func redactRPCURL(raw string) string {
	return redactEndpointURL(raw, "http", "https", "ws", "wss")
}

func redactHTTPURL(raw string) string {
	return redactEndpointURL(raw, "http", "https")
}

func redactEndpointURL(raw string, allowedSchemes ...string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" {
		return "[REDACTED]"
	}
	scheme := strings.ToLower(parsed.Scheme)
	if slices.Contains(allowedSchemes, scheme) {
		return scheme + "://[REDACTED]"
	}
	return "[REDACTED]"
}

func pathways(items []config.PathwayConfig) map[string]config.PathwayConfig {
	result := make(map[string]config.PathwayConfig, len(items))
	for _, item := range items {
		key := fmt.Sprintf("%d:%d:%s:%s", item.SrcEID, item.DstEID, item.SrcOApp, item.DstOApp)
		result[key] = item
	}
	return result
}

func pricingChains(items []config.PricingChainConfig) map[string]config.PricingChainConfig {
	result := make(map[string]config.PricingChainConfig, len(items))
	for _, item := range items {
		result[fmt.Sprintf("%d", item.EID)] = item
	}
	return result
}

type pricingConfigGlobal struct {
	Enabled                     bool   `json:"enabled"`
	Signer                      string `json:"signer"`
	IntervalSeconds             uint64 `json:"interval_seconds"`
	StaleAfterSeconds           uint64 `json:"stale_after_seconds"`
	MaxDeviationBps             uint64 `json:"max_deviation_bps"`
	SourceRequestTimeoutSeconds uint64 `json:"source_request_timeout_seconds"`
	GasSpikeBps                 uint64 `json:"gas_spike_bps"`
	CoinMarketCapBaseURL        string `json:"coinmarketcap_base_url"`
	CoinMarketCapAPIKeyEnv      string `json:"coinmarketcap_api_key_env"`
	CoinGeckoBaseURL            string `json:"coingecko_base_url"`
	CoinGeckoAPIKeyEnv          string `json:"coingecko_api_key_env"`
}

type servicesConfig struct {
	ExecutorEnabled bool `json:"executor_enabled"`
	DVNEnabled      bool `json:"dvn_enabled"`
}

func services(cfg config.Config) servicesConfig {
	return servicesConfig{
		ExecutorEnabled: cfg.ExecutorEnabled(),
		DVNEnabled:      cfg.DVNEnabled(),
	}
}

func pricingGlobal(pricing config.PricingConfig) pricingConfigGlobal {
	return pricingConfigGlobal{
		Enabled:                     pricing.Enabled,
		Signer:                      pricing.Signer.Hex(),
		IntervalSeconds:             pricing.IntervalSeconds,
		StaleAfterSeconds:           pricing.StaleAfterSeconds,
		MaxDeviationBps:             pricing.MaxDeviationBps,
		SourceRequestTimeoutSeconds: pricing.SourceRequestTimeoutSeconds,
		GasSpikeBps:                 pricing.GasSpikeBps,
		CoinMarketCapBaseURL:        pricing.CoinMarketCapBaseURL,
		CoinMarketCapAPIKeyEnv:      pricing.CoinMarketCapAPIKeyEnv,
		CoinGeckoBaseURL:            pricing.CoinGeckoBaseURL,
		CoinGeckoAPIKeyEnv:          pricing.CoinGeckoAPIKeyEnv,
	}
}

func redactPricingGlobal(pricing pricingConfigGlobal) pricingConfigGlobal {
	pricing.CoinMarketCapBaseURL = redactHTTPURL(pricing.CoinMarketCapBaseURL)
	pricing.CoinMarketCapAPIKeyEnv = redactSecretEnvironmentReference(pricing.CoinMarketCapAPIKeyEnv)
	pricing.CoinGeckoBaseURL = redactHTTPURL(pricing.CoinGeckoBaseURL)
	pricing.CoinGeckoAPIKeyEnv = redactSecretEnvironmentReference(pricing.CoinGeckoAPIKeyEnv)
	return pricing
}

func redactSecretEnvironmentReference(value string) string {
	if value == "" || config.IsValidEnvironmentVariableName(value) {
		return value
	}
	return "[REDACTED]"
}

func redactSecretFileReference(value string) string {
	if value == "" {
		return ""
	}
	return "[REDACTED]"
}

func jsonValue(value any) string {
	if value == nil {
		return "null"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(encoded)
}
