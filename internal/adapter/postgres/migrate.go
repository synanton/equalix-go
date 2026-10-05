package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/synanton/equalix-go/migrations"
)

// migrateLockKey is the well-known advisory-lock key serializing
// concurrent startups against the same database (single global
// namespace per DB — one migration stream, one key). Session-scoped:
// held on a dedicated connection for the migration duration, released
// by unlock or connection close (including process exit).
const migrateLockKey int64 = 727940425575961

// migrateStatementTimeout bounds any single migration statement: a
// table rewrite that outlives this fails fast instead of wedging
// startup behind an unbounded DDL. Migrations expected to exceed it
// run out-of-band (runbook), never with a raised timeout here.
const migrateStatementTimeout = "300s"

// Migrate applies pending migrations (embedded FS, or dir when set)
// under the advisory lock, then releases it. Fail-fast: any error
// returns before the caller binds any listener — no partial startup.
// Runs before readiness probes, so a migrated schema is a precondition
// of serving, not a background task. Opt-in at the call site (main
// gates on EQUALIX_MIGRATE_ON_STARTUP); this function unconditionally
// migrates when called.
func Migrate(ctx context.Context, dsn, dir string) error {
	lockConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("postgres: migrate lock connect: %w", err)
	}
	defer lockConn.Close(ctx)
	if _, err := lockConn.Exec(ctx,
		`SET SESSION statement_timeout = '`+migrateStatementTimeout+`'`); err != nil {
		return fmt.Errorf("postgres: migrate statement timeout: %w", err)
	}
	// Blocks until acquired: concurrent startups serialize here, the
	// loser migrates nothing (already-applied versions are no-ops).
	if _, err := lockConn.Exec(ctx,
		`SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("postgres: migrate lock acquire: %w", err)
	}
	defer func() {
		_, _ = lockConn.Exec(context.Background(),
			`SELECT pg_advisory_unlock($1)`, migrateLockKey)
	}()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("postgres: migrate connect: %w", err)
	}
	defer db.Close()
	// Provider (not the package-level Up): the package-level API keeps
	// global dialect/FS state, which leaks between calls in one process
	// (observed: an embedded run's BaseFS shadowing a later external-dir
	// run in tests). The provider carries dialect + filesystem per
	// call — no globals, no cross-call contamination.
	var fsys fs.FS = migrations.FS
	if dir != "" {
		fsys = os.DirFS(dir)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, fsys)
	if err != nil {
		return fmt.Errorf("postgres: migrate provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("postgres: migrate up: %w", err)
	}
	return nil
}
