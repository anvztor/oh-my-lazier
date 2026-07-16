package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/ethereum/go-ethereum/common"
	"github.com/islishude/oh-my-lazier/go/internal/chain"
	"github.com/islishude/oh-my-lazier/go/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the Postgres connection pool used by worker state machines.
type Store struct {
	pool *pgxpool.Pool
}

// Connect opens the Postgres pool for the configured database URL.
func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Ping verifies that the database connection is usable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Migrate applies embedded SQL migrations and refuses to continue if an already
// applied migration's checksum no longer matches the embedded file.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return err
	}
	entries, err := fs.Glob(migrations.Files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(entries)
	for _, name := range entries {
		body, err := migrations.Files.ReadFile(name)
		if err != nil {
			return err
		}
		checksum := migrationChecksum(body)
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if err := applyMigration(ctx, tx, name, checksum, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SyncConfig reconciles chain and pathway metadata from validated startup
// config: records present in the config are upserted and enabled, and any
// record no longer in the config is disabled so it is not treated as active.
func (s *Store) SyncConfig(ctx context.Context, registry *chain.Registry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Disable everything first, then re-enable only what the current validated
	// config declares. The repository keeps no compatibility burden, so a chain
	// or pathway removed from config must not linger as an active record that
	// readiness and the durable loops still treat as live.
	if _, err := tx.Exec(ctx, `UPDATE chains SET enabled = false`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE pathways SET enabled = false`); err != nil {
		return err
	}
	for _, configuredChain := range registry.All() {
		if _, err := tx.Exec(ctx, `
			INSERT INTO chains (eid, name, chain_id, endpoint_address, enabled)
			VALUES ($1, $2, $3, $4, true)
			ON CONFLICT (eid) DO UPDATE SET
				name = EXCLUDED.name,
				chain_id = EXCLUDED.chain_id,
				endpoint_address = EXCLUDED.endpoint_address,
				enabled = true
		`, configuredChain.EID, configuredChain.Name, configuredChain.ChainID.Int64(), addressBytes(configuredChain.EndpointAddress)); err != nil {
			return err
		}
	}
	for _, pathway := range registry.Pathways() {
		if _, err := tx.Exec(ctx, `
				INSERT INTO pathways (
					src_eid, dst_eid, src_oapp, dst_oapp, send_lib, receive_lib,
					open_executor, open_dvn, price_feed, destination_open_dvn, max_message_size, enabled
				)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
				ON CONFLICT (src_eid, dst_eid, src_oapp, dst_oapp) DO UPDATE SET
					send_lib = EXCLUDED.send_lib,
					receive_lib = EXCLUDED.receive_lib,
					open_executor = EXCLUDED.open_executor,
					open_dvn = EXCLUDED.open_dvn,
					price_feed = EXCLUDED.price_feed,
					destination_open_dvn = EXCLUDED.destination_open_dvn,
					max_message_size = EXCLUDED.max_message_size,
					enabled = EXCLUDED.enabled
			`, pathway.SrcEID, pathway.DstEID, addressBytes(pathway.SrcOApp), addressBytes(pathway.DstOApp), addressBytes(pathway.SendLib), addressBytes(pathway.ReceiveLib), addressBytes(pathway.SourceWorkers.OpenExecutor), addressBytes(pathway.SourceWorkers.OpenDVN), addressBytes(pathway.SourceWorkers.PriceFeed), addressBytes(pathway.DestinationWorkers.OpenDVN), pathway.MaxMessageSize, pathway.Enabled); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Close releases the database connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

func applyMigration(ctx context.Context, tx pgx.Tx, version, checksum, sql string) error {
	var appliedChecksum string
	err := tx.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE version = $1", version).Scan(&appliedChecksum)
	switch {
	case err == nil:
		if appliedChecksum != checksum {
			return fmt.Errorf("migration %s checksum mismatch", version)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)", version, checksum)
	return err
}

func migrationChecksum(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func addressBytes(address common.Address) []byte {
	bytes := address.Bytes()
	copied := make([]byte, len(bytes))
	copy(copied, bytes)
	return copied
}
