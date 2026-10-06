package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/synanton/equalix-go/migrations"
)

// migrateLockKey is the well-known advisory-lock key serializing
// concurrent startups against the same database (single global
// namespace per DB — one migration stream, one key). Fixed constant,
// not derived: derivation (db name + schema + prefix hash) buys nothing
// when the namespace is already one-stream-per-database, and a fixed
// key is greppable in pg_locks. Collision semantics, stated plainly:
// another migrator using this key blocks (mutual serialization), never
// corrupts — the failure mode of sharing is waiting, not double-apply.
// Session-scoped: held on a dedicated NON-POOLED connection
// (pgx.Connect, never the pool) for the migration duration, explicitly
// unlocked first, then closed — LIFO defers below guarantee the order,
// and close releases even on failure paths (including process exit).
// PgBouncer warning: session-level locks require a DIRECT connection.
// Behind transaction-pooling PgBouncer the lock silently stops working
// (each statement may land on a different backend). The migration DSN
// must bypass the pooler; this is documented, not detected — there is
// no reliable in-protocol PgBouncer fingerprint worth asserting on.
const migrateLockKey int64 = 727940425575961

// migrateStatementTimeout bounds any single migration statement: a
// table rewrite that outlives this fails fast instead of wedging
// startup behind an unbounded DDL. Migrations expected to exceed it
// run out-of-band (runbook), never with a raised timeout here.
const migrateStatementTimeout = "300s"

// migrateLockTimeout bounds lock ACQUISITION waits during DDL: a
// migration blocked >30s on a busy table's ACCESS EXCLUSIVE fails fast
// instead of holding startup (and the advisory lock, stalling every
// other starting instance behind it). Distinct from statement_timeout,
// which bounds execution, not lock waits — with one PostgreSQL-specific
// exception that matters here: lock_timeout does NOT cover the advisory
// lock wait itself (advisory locks are outside its object list), so the
// pg_advisory_lock acquisition is bounded by statement_timeout (300s),
// not by this 30s. That is accepted, not fixed: migrations are expected
// to finish well under 300s, and a bounded retry loop around
// pg_try_advisory_lock would trade a documented 300s ceiling for
// retry-budget machinery with identical worst-case behavior.
const migrateLockTimeout = "30s"

// EnsureSchema applies the embedded set iff the tasks table is absent
// (fresh database from the compose pg-init path), else no-ops. Go-side
// ONLY: Java's schema belongs to Flyway — goose-migrating it would fork
// the schema source and fight Flyway validation on the next boot.
// Called after boot, before ResetDB: residue clearing assumes tables,
// schema creation precedes it.
func EnsureSchema(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("postgres: schema check connect: %w", err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT to_regclass('public.tasks') IS NOT NULL`).Scan(&exists); err != nil {
		return fmt.Errorf("postgres: schema check: %w", err)
	}
	if exists {
		return nil
	}
	return Migrate(ctx, dsn, "")
}

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
	if _, err := lockConn.Exec(ctx,
		`SET SESSION lock_timeout = '`+migrateLockTimeout+`'`); err != nil {
		return fmt.Errorf("postgres: migrate lock timeout: %w", err)
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

	dbCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("postgres: migrate dsn: %w", err)
	}
	// Timeouts as CONNECT-STARTUP params, not SET-on-a-conn: goose runs
	// DDL across pooled connections, and a SET would stick to whichever
	// checkout happened to run it. RuntimeParams ride every new backend
	// connection, so all DDL is bounded regardless of pooling.
	dbCfg.RuntimeParams["statement_timeout"] = migrateStatementTimeout
	dbCfg.RuntimeParams["lock_timeout"] = migrateLockTimeout
	db := stdlib.OpenDB(*dbCfg)
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
