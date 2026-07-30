package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/islishude/oh-my-lazier/go/internal/config"
	"github.com/islishude/oh-my-lazier/go/internal/db"
)

type retryOutput struct {
	Action string      `json:"action"`
	Before db.OutboxTx `json:"before"`
	After  db.OutboxTx `json:"after"`
}

func main() {
	configPath := flag.String("config", "config/example.yaml", "worker config path")
	action := flag.String("action", "", "retry action: retry-failed, replace, cancel-nonce, or resolve-external-nonce")
	id := flag.Int64("id", 0, "tx_outbox id")
	resolution := flag.String("resolution", "", "resolve-external-nonce resolution: retry or abandon")
	flag.Parse()

	if *id <= 0 {
		fmt.Fprintln(os.Stderr, "-id is required")
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect database: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping database: %v\n", err)
		os.Exit(1)
	}

	before, err := store.GetOutboxTx(ctx, *id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load outbox tx: %v\n", err)
		os.Exit(1)
	}

	switch *action {
	case "retry-failed":
		afterID, err := store.RetryFailedTx(ctx, *id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "retry failed tx: %v\n", err)
			os.Exit(1)
		}
		*id = afterID
	case "replace":
		if err := store.RequestTxReplacement(ctx, *id); err != nil {
			fmt.Fprintf(os.Stderr, "request replacement tx: %v\n", err)
			os.Exit(1)
		}
	case "cancel-nonce":
		if err := store.RequestTxCancel(ctx, *id); err != nil {
			fmt.Fprintf(os.Stderr, "request cancel: %v\n", err)
			os.Exit(1)
		}
	case "resolve-external-nonce":
		switch *resolution {
		case "retry":
			cloneID, err := store.ResolveExternalNonceRetry(ctx, *id)
			if err != nil {
				fmt.Fprintf(os.Stderr, "resolve external nonce (retry): %v\n", err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "created fresh clone %d\n", cloneID)
			// Report the clone that will actually run as After, mirroring
			// retry-failed, so automation gets the live row's ID from the JSON.
			*id = cloneID
		case "abandon":
			if err := store.ResolveExternalNonceAbandon(ctx, *id); err != nil {
				fmt.Fprintf(os.Stderr, "resolve external nonce (abandon): %v\n", err)
				os.Exit(1)
			}
		default:
			fmt.Fprintln(os.Stderr, "-resolution must be retry or abandon")
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "-action must be retry-failed, replace, cancel-nonce, or resolve-external-nonce")
		os.Exit(1)
	}

	after, err := store.GetOutboxTx(ctx, *id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reload outbox tx: %v\n", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(retryOutput{Action: *action, Before: before, After: after}); err != nil {
		fmt.Fprintf(os.Stderr, "encode result: %v\n", err)
		os.Exit(1)
	}
}
