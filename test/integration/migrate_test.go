//go:build integration

package integration

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	adapter "github.com/synanton/equalix-go/internal/adapter/postgres"
)

// freshDB creates an empty database on the shared container and returns
// a DSN for it. Each migrate test gets its own database: residue from
// one run must never leak into another's version table.
func freshDB(t *testing.T, name string) string {
	t.Helper()
	_, err := pool.Exec(ctx, `DROP DATABASE IF EXISTS `+name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatal(err)
	}
	base := pool.Config().ConnString()
	// base ends .../equalix?sslmode=disable — swap the database segment.
	idx := strings.LastIndex(base, "/")
	q := strings.Index(base[idx:], "?")
	dsn := base[:idx+1] + name + base[idx+q:]
	t.Cleanup(func() {
		// Best effort: drop the scratch database. A leftover only costs
		// disk, never correctness (names are unique per test).
		conn, err := pgx.Connect(ctx, base)
		if err != nil {
			return
		}
		defer conn.Close(ctx)
		_, _ = conn.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`)
	})
	return dsn
}

func tableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

func gooseVersions(t *testing.T, dsn string) int {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var n int
	// version_id 0 is goose's own baseline row, not a migration.
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM goose_db_version WHERE is_applied AND version_id > 0`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestMigrateFreshDBAppliesSchema is the opt-in path: an empty database
// gains the full schema plus the goose version history (5 versions).
func TestMigrateFreshDBAppliesSchema(t *testing.T) {
	dsn := freshDB(t, "mtest_schema")
	if err := adapter.Migrate(ctx, dsn, ""); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"tasks", "client_counts", "scheduler_virtual_clock"} {
		if !tableExists(t, dsn, tbl) {
			t.Fatalf("table %s missing after migrate", tbl)
		}
	}
	if got := gooseVersions(t, dsn); got != 5 {
		t.Fatalf("goose versions = %d, want 5", got)
	}
}

// TestMigrateIdempotent reruns against a migrated database: all versions
// applied is a no-op, never a re-apply or an error.
func TestMigrateIdempotent(t *testing.T) {
	dsn := freshDB(t, "mtest_idem")
	if err := adapter.Migrate(ctx, dsn, ""); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Migrate(ctx, dsn, ""); err != nil {
		t.Fatalf("second migrate errored: %v", err)
	}
	if got := gooseVersions(t, dsn); got != 5 {
		t.Fatalf("goose versions after rerun = %d, want 5", got)
	}
}

// TestMigrateConcurrentSerializes starts two migrations at once against
// one empty database: the advisory lock serializes them, both return
// nil, exactly 5 versions land. Without the lock the version-table
// check-then-insert races (double-apply or duplicate-key error).
func TestMigrateConcurrentSerializes(t *testing.T) {
	dsn := freshDB(t, "mtest_conc")
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Stagger slightly so both are inside the critical section
			// together rather than sequentially by luck.
			time.Sleep(time.Duration(i) * 200 * time.Millisecond)
			errs[i] = adapter.Migrate(ctx, dsn, "")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate %d: %v", i, err)
		}
	}
	if got := gooseVersions(t, dsn); got != 5 {
		t.Fatalf("goose versions after concurrent run = %d, want 5", got)
	}
}

// TestEnsureSchemaHealsFreshDB is the harness path: a database with
// roles but no tables (compose pg-init output) gains the schema on
// first call and no-ops after. This is what runOne/runGo invoke before
// ResetDB on Go sides — residue clearing assumes tables, schema
// creation precedes it.
func TestEnsureSchemaHealsFreshDB(t *testing.T) {
	dsn := freshDB(t, "mtest_ensure")
	if err := adapter.EnsureSchema(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	if !tableExists(t, dsn, "tasks") {
		t.Fatal("tasks missing after EnsureSchema")
	}
	if err := adapter.EnsureSchema(ctx, dsn); err != nil {
		t.Fatalf("second EnsureSchema errored: %v", err)
	}
}

// TestMigrateBadDSNFailsFast pins the fail-fast contract: an unreachable
// database errors before anything binds or serves — no partial startup.
func TestMigrateBadDSNFailsFast(t *testing.T) {
	if err := adapter.Migrate(ctx, "postgres://127.0.0.1:1/nodb?sslmode=disable", ""); err == nil {
		t.Fatal("migrate against unreachable DB succeeded")
	}
}

// TestMigrateExternalDir exercises the --migrations-dir override: same
// filenames read from disk instead of the embedded FS.
func TestMigrateExternalDir(t *testing.T) {
	dsn := freshDB(t, "mtest_ext")
	if err := adapter.Migrate(ctx, dsn, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if got := gooseVersions(t, dsn); got != 5 {
		t.Fatalf("goose versions via external dir = %d, want 5", got)
	}
}
