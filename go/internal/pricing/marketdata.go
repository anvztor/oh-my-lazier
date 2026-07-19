package pricing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultCoinMarketCapBaseURL    = "https://pro-api.coinmarketcap.com"
	defaultCoinGeckoBaseURL        = "https://api.coingecko.com"
	defaultCoinGeckoProBaseURL     = "https://pro-api.coingecko.com"
	maxMarketDataResponseBodyBytes = int64(1 << 20)
	maxMarketDataRedirects         = 10
)

// CoinMarketCapClient reads public USD prices from CoinMarketCap quotes.
type CoinMarketCapClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// CoinMarketCapPriceReader binds a CoinMarketCap asset ID to a reusable client.
type CoinMarketCapPriceReader struct {
	client *CoinMarketCapClient
	id     uint64
}

// CoinGeckoClient reads public USD prices from CoinGecko simple price.
type CoinGeckoClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// CoinGeckoPriceReader binds a CoinGecko coin id to a reusable client.
type CoinGeckoPriceReader struct {
	client *CoinGeckoClient
	coinID string
}

type marketDataHTTPStatusError struct {
	source     string
	statusCode int
}

func (e *marketDataHTTPStatusError) Error() string {
	return fmt.Sprintf("%s price request returned HTTP %d", e.source, e.statusCode)
}

func decodeMarketDataJSON(source string, body io.Reader, value any) error {
	limited := &io.LimitedReader{R: body, N: maxMarketDataResponseBodyBytes + 1}
	decoder := json.NewDecoder(limited)
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		if limited.N == 0 {
			return fmt.Errorf("%s price response body exceeds %d bytes", source, maxMarketDataResponseBodyBytes)
		}
		return err
	}
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return fmt.Errorf("%s price response body read failed: %w", source, err)
	}
	if limited.N == 0 {
		return fmt.Errorf("%s price response body exceeds %d bytes", source, maxMarketDataResponseBodyBytes)
	}
	return nil
}

// NewCoinMarketCapClient creates a CoinMarketCap quote client.
func NewCoinMarketCapClient(baseURL, apiKeyEnv string, httpClient *http.Client) (*CoinMarketCapClient, error) {
	if baseURL == "" {
		baseURL = defaultCoinMarketCapBaseURL
	}
	normalizedBaseURL, err := normalizeMarketDataBaseURL("coinmarketcap", baseURL)
	if err != nil {
		return nil, err
	}
	apiKey := ""
	if apiKeyEnv != "" {
		apiKey = os.Getenv(apiKeyEnv)
		if apiKey == "" {
			return nil, errors.New("coinmarketcap api key environment variable is empty")
		}
	}
	httpClient = marketDataHTTPClient(normalizedBaseURL, httpClient)
	return &CoinMarketCapClient{baseURL: normalizedBaseURL, apiKey: apiKey, httpClient: httpClient}, nil
}

// NewCoinMarketCapPriceReader creates a configured CoinMarketCap asset-ID reader.
func NewCoinMarketCapPriceReader(client *CoinMarketCapClient, id uint64) (*CoinMarketCapPriceReader, error) {
	if client == nil {
		return nil, errors.New("coinmarketcap client is required")
	}
	if id == 0 {
		return nil, errors.New("coinmarketcap id is required")
	}
	return &CoinMarketCapPriceReader{client: client, id: id}, nil
}

// PriceUSD fetches the configured asset ID's latest USD/native price.
func (r *CoinMarketCapPriceReader) PriceUSD(ctx context.Context) (SourcePrice, error) {
	return r.client.PriceUSD(ctx, r.id)
}

func (r *CoinMarketCapPriceReader) validateSourceConfiguration(ctx context.Context) error {
	_, err := r.PriceUSD(ctx)
	if _, ok := errors.AsType[*inconclusivePriceSourceConfigurationError](err); ok {
		return err
	}
	if isDeterministicMarketDataError(err) {
		return newPriceSourceConfigurationError(err)
	}
	return err
}

// PriceUSD fetches the latest cryptocurrency-ID price as USD/native.
func (c *CoinMarketCapClient) PriceUSD(ctx context.Context, id uint64) (SourcePrice, error) {
	if id == 0 {
		return SourcePrice{}, errors.New("coinmarketcap id is required")
	}
	endpoint, err := url.Parse(c.baseURL + "/v3/cryptocurrency/quotes/latest")
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("coinmarketcap", "build", err)
	}
	query := endpoint.Query()
	query.Set("id", strconv.FormatUint(id, 10))
	query.Set("convert", "USD")
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("coinmarketcap", "build", err)
	}
	if c.apiKey != "" {
		request.Header.Set("X-CMC_PRO_API_KEY", c.apiKey)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("coinmarketcap", "execute", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return SourcePrice{}, &marketDataHTTPStatusError{source: "coinmarketcap", statusCode: response.StatusCode}
	}
	var payload struct {
		Data []struct {
			ID    uint64 `json:"id"`
			Quote []struct {
				Symbol      string      `json:"symbol"`
				Price       json.Number `json:"price"`
				LastUpdated string      `json:"last_updated"`
			} `json:"quote"`
		} `json:"data"`
	}
	if err := decodeMarketDataJSON("coinmarketcap", response.Body, &payload); err != nil {
		return SourcePrice{}, err
	}
	if len(payload.Data) != 1 || payload.Data[0].ID != id {
		return SourcePrice{}, newInconclusivePriceSourceConfigurationError(fmt.Errorf("coinmarketcap returned no unique price for id %d", id))
	}
	var priceText, observedText string
	for _, quote := range payload.Data[0].Quote {
		if quote.Symbol == "USD" {
			priceText = quote.Price.String()
			observedText = quote.LastUpdated
			break
		}
	}
	price, ok := new(big.Rat).SetString(priceText)
	if !ok || price.Sign() <= 0 {
		return SourcePrice{}, fmt.Errorf("coinmarketcap returned invalid price %q", priceText)
	}
	observedAt, err := time.Parse(time.RFC3339Nano, observedText)
	if err != nil {
		return SourcePrice{}, errors.New("coinmarketcap returned invalid last_updated")
	}
	return SourcePrice{Source: "coinmarketcap", USD: price, ObservedAt: observedAt}, nil
}

// NewCoinGeckoClient creates a CoinGecko simple-price client.
func NewCoinGeckoClient(baseURL, apiKeyEnv string, httpClient *http.Client) (*CoinGeckoClient, error) {
	if baseURL == "" {
		if apiKeyEnv == "" {
			baseURL = defaultCoinGeckoBaseURL
		} else {
			baseURL = defaultCoinGeckoProBaseURL
		}
	}
	normalizedBaseURL, err := normalizeMarketDataBaseURL("coingecko", baseURL)
	if err != nil {
		return nil, err
	}
	apiKey := ""
	if apiKeyEnv != "" {
		apiKey = os.Getenv(apiKeyEnv)
		if apiKey == "" {
			return nil, errors.New("coingecko api key environment variable is empty")
		}
	}
	httpClient = marketDataHTTPClient(normalizedBaseURL, httpClient)
	return &CoinGeckoClient{baseURL: normalizedBaseURL, apiKey: apiKey, httpClient: httpClient}, nil
}

func normalizeMarketDataBaseURL(source, baseURL string) (string, error) {
	normalized := strings.TrimRight(baseURL, "/")
	parsed, err := url.Parse(normalized)
	if err != nil || strings.TrimSpace(baseURL) != baseURL || parsed.Opaque != "" || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.ForceQuery {
		return "", fmt.Errorf("%s base URL must be an absolute HTTPS URL without query or fragment", source)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "https" {
		return "", fmt.Errorf("%s base URL must be an absolute HTTPS URL without query or fragment", source)
	}
	parsed.Scheme = scheme
	return parsed.String(), nil
}

func marketDataHTTPClient(baseURL string, httpClient *http.Client) *http.Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	configured := *httpClient
	base, _ := url.Parse(baseURL)
	originalCheckRedirect := httpClient.CheckRedirect
	configured.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if !sameMarketDataOrigin(base, request.URL) {
			return http.ErrUseLastResponse
		}
		if originalCheckRedirect != nil {
			return originalCheckRedirect(request, via)
		}
		if len(via) >= maxMarketDataRedirects {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &configured
}

func sameMarketDataOrigin(left, right *url.URL) bool {
	if left == nil || right == nil || !strings.EqualFold(left.Scheme, right.Scheme) || !strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return marketDataOriginPort(left) == marketDataOriginPort(right)
}

func marketDataOriginPort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	switch strings.ToLower(value.Scheme) {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

// NewCoinGeckoPriceReader creates a configured CoinGecko coin-id reader.
func NewCoinGeckoPriceReader(client *CoinGeckoClient, coinID string) (*CoinGeckoPriceReader, error) {
	if client == nil {
		return nil, errors.New("coingecko client is required")
	}
	if coinID == "" {
		return nil, errors.New("coingecko coin id is required")
	}
	return &CoinGeckoPriceReader{client: client, coinID: coinID}, nil
}

// PriceUSD fetches the configured coin id's latest USD/native price.
func (r *CoinGeckoPriceReader) PriceUSD(ctx context.Context) (SourcePrice, error) {
	return r.client.PriceUSD(ctx, r.coinID)
}

func (r *CoinGeckoPriceReader) validateSourceConfiguration(ctx context.Context) error {
	_, err := r.PriceUSD(ctx)
	if _, ok := errors.AsType[*inconclusivePriceSourceConfigurationError](err); ok {
		return err
	}
	if isDeterministicMarketDataError(err) {
		return newPriceSourceConfigurationError(err)
	}
	return err
}

// PriceUSD fetches the latest coin id price as USD/native.
func (c *CoinGeckoClient) PriceUSD(ctx context.Context, coinID string) (SourcePrice, error) {
	if coinID == "" {
		return SourcePrice{}, errors.New("coingecko coin id is required")
	}
	endpoint, err := url.Parse(c.baseURL + "/api/v3/simple/price")
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("coingecko", "build", err)
	}
	query := endpoint.Query()
	query.Set("ids", coinID)
	query.Set("vs_currencies", "usd")
	query.Set("include_last_updated_at", "true")
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("coingecko", "build", err)
	}
	if c.apiKey != "" {
		request.Header.Set("x-cg-pro-api-key", c.apiKey)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return SourcePrice{}, wrapPriceSourceRequestError("coingecko", "execute", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return SourcePrice{}, &marketDataHTTPStatusError{source: "coingecko", statusCode: response.StatusCode}
	}
	var payload map[string]struct {
		USD           json.Number `json:"usd"`
		LastUpdatedAt int64       `json:"last_updated_at"`
	}
	if err := decodeMarketDataJSON("coingecko", response.Body, &payload); err != nil {
		return SourcePrice{}, err
	}
	entry, ok := payload[coinID]
	if !ok {
		return SourcePrice{}, newInconclusivePriceSourceConfigurationError(fmt.Errorf("coingecko returned no price for %s", coinID))
	}
	priceText := entry.USD.String()
	price, ok := new(big.Rat).SetString(priceText)
	if !ok || price.Sign() <= 0 {
		return SourcePrice{}, fmt.Errorf("coingecko returned invalid price %q", priceText)
	}
	if entry.LastUpdatedAt <= 0 {
		return SourcePrice{}, errors.New("coingecko returned invalid last_updated_at")
	}
	return SourcePrice{Source: "coingecko", USD: price, ObservedAt: time.Unix(entry.LastUpdatedAt, 0)}, nil
}

func isDeterministicMarketDataError(err error) bool {
	if err == nil || isPriceSourceConfigurationError(err) {
		return err != nil
	}
	var statusError *marketDataHTTPStatusError
	if !errors.As(err, &statusError) {
		return false
	}
	switch statusError.statusCode {
	case http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusPaymentRequired,
		http.StatusMethodNotAllowed,
		http.StatusGone,
		http.StatusUnprocessableEntity:
		return true
	default:
		return false
	}
}
