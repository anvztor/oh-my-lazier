package pricing

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

const (
	sourceConfigurationValidationAttempts = 3
	sourceConfigurationRetryDelay         = 100 * time.Millisecond
)

type sourceConfigurationValidator interface {
	validateSourceConfiguration(context.Context) error
}

type priceSourceConfigurationError struct {
	cause error
}

func (e *priceSourceConfigurationError) Error() string {
	if e == nil || e.cause == nil {
		return "price source configuration is invalid"
	}
	return e.cause.Error()
}

func (e *priceSourceConfigurationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newPriceSourceConfigurationError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*priceSourceConfigurationError](err); ok {
		return err
	}
	return &priceSourceConfigurationError{cause: err}
}

func isPriceSourceConfigurationError(err error) bool {
	var configurationError *priceSourceConfigurationError
	return errors.As(err, &configurationError)
}

// inconclusivePriceSourceConfigurationError marks a successful source response
// whose payload did not establish whether the configured identity is valid.
// Repeating the same result makes it a deterministic configuration mismatch.
type inconclusivePriceSourceConfigurationError struct {
	cause error
}

func (e *inconclusivePriceSourceConfigurationError) Error() string {
	if e == nil || e.cause == nil {
		return "price source configuration check was inconclusive"
	}
	return e.cause.Error()
}

func (e *inconclusivePriceSourceConfigurationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func newInconclusivePriceSourceConfigurationError(err error) error {
	if err == nil {
		return nil
	}
	return &inconclusivePriceSourceConfigurationError{cause: err}
}

func classifySourceIdentityCallError(message string, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if isPriceSourceRequestError(err) && !isPriceSourceContractRevert(err) {
		return err
	}
	return newPriceSourceConfigurationError(fmt.Errorf("%s: %w", message, err))
}

type sourceConfigurationCheck struct {
	eid      uint32
	source   string
	role     string
	validate sourceConfigurationValidator
}

type sourceConfigurationResult struct {
	index int
	err   error
}

// ValidateSourceConfigurations performs bounded source-identity checks before durable loops start.
// It returns deterministic configuration mismatches and defers transient availability failures to runtime loops.
func ValidateSourceConfigurations(ctx context.Context, sources map[uint32]ChainSources, policy PriceSelectionPolicy) error {
	if policy.SourceRequestTimeout <= 0 {
		return errors.New("price source request timeout must be positive")
	}
	checks := sourceConfigurationChecks(sources)
	if len(checks) == 0 {
		return nil
	}

	requestCtx, cancel := context.WithTimeout(ctx, policy.SourceRequestTimeout)
	defer cancel()
	results := make(chan sourceConfigurationResult, len(checks))
	for index, check := range checks {
		go func(index int, check sourceConfigurationCheck) {
			results <- sourceConfigurationResult{index: index, err: validateSourceConfigurationWithRetry(requestCtx, check.validate)}
		}(index, check)
	}

	completed := make([]bool, len(checks))
	checkErrors := make([]error, len(checks))
	remaining := len(checks)
	recordResult := func(result sourceConfigurationResult) {
		if completed[result.index] {
			return
		}
		completed[result.index] = true
		checkErrors[result.index] = result.err
		remaining--
	}
	for remaining > 0 {
		select {
		case result := <-results:
			recordResult(result)
		case <-requestCtx.Done():
			if err := ctx.Err(); err != nil {
				return err
			}
		drainResults:
			for {
				select {
				case result := <-results:
					recordResult(result)
				default:
					break drainResults
				}
			}
			for index := range checks {
				if completed[index] {
					continue
				}
				completed[index] = true
				checkErrors[index] = requestCtx.Err()
				remaining--
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var configurationErrors []error
	for index, check := range checks {
		err := checkErrors[index]
		if err == nil {
			continue
		}
		if isPriceSourceConfigurationError(err) {
			configurationErrors = append(configurationErrors, fmt.Errorf("%s source configuration for chain %d: %w", check.source, check.eid, err))
			continue
		}
		notifyPriceSourceFailure(policy, PriceSourceFailure{
			EID: check.eid, Source: check.source, Role: check.role, Category: priceSourceFailureCategory(err), Err: err,
		})
	}
	return errors.Join(configurationErrors...)
}

func validateSourceConfigurationWithRetry(ctx context.Context, validator sourceConfigurationValidator) error {
	for attempt := 1; attempt <= sourceConfigurationValidationAttempts; attempt++ {
		err := validator.validateSourceConfiguration(ctx)
		var inconclusive *inconclusivePriceSourceConfigurationError
		if !errors.As(err, &inconclusive) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt == sourceConfigurationValidationAttempts {
			return newPriceSourceConfigurationError(inconclusive.cause)
		}
		timer := time.NewTimer(sourceConfigurationRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func sourceConfigurationChecks(sources map[uint32]ChainSources) []sourceConfigurationCheck {
	eids := make([]uint32, 0, len(sources))
	for eid := range sources {
		eids = append(eids, eid)
	}
	slices.Sort(eids)

	var checks []sourceConfigurationCheck
	for _, eid := range eids {
		chainSources := sources[eid]
		checks = appendSourceConfigurationCheck(checks, eid, "primary", chainSources.Primary)
		for _, source := range chainSources.Sanity {
			checks = appendSourceConfigurationCheck(checks, eid, "sanity", source)
		}
	}
	return checks
}

func appendSourceConfigurationCheck(checks []sourceConfigurationCheck, eid uint32, role string, source ConfiguredPriceReader) []sourceConfigurationCheck {
	validator, ok := source.Reader.(sourceConfigurationValidator)
	if !ok {
		return checks
	}
	return append(checks, sourceConfigurationCheck{eid: eid, source: source.Name, role: role, validate: validator})
}
