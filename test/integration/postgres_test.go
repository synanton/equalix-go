//go:build integration

// Package integration holds adapter tests against containerized
// infrastructure (testcontainers-go). Docker required:
//
//	make test-integration
package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	adapter "github.com/synanton/equalix-go/internal/adapter/postgres"
	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

var (
	ctx    = context.Background()
	pool   *pgxpool.Pool
	tasks  *adapter.TaskStore
	counts *adapter.CountsStore
	seqs   *adapter.SequenceStore
	vtime  *adapter.VirtualTimeStore
	stores *adapter.Stores
)

func TestMain(m *testing.M) {
	var cancel func()
	var err error
	pool, cancel, err = startPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	defer cancel()
	stores = adapter.NewStores(pool)
	tasks, counts, seqs, vtime = stores.Tasks, stores.Counts, stores.Sequences, stores.VirtualTime
	os.Exit(m.Run())
}

func startPostgres(ctx context.Context) (*pgxpool.Pool, func(), error) {
	pg, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("equalix"),
		postgres.WithUsername("equalix"),
		postgres.WithPassword("equalix"),
		// v0.33.0 sets no wait strategy by default; without this the
		// container returns while postgres is still booting.
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, nil, err
	}
	connStr, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, nil, err
	}
	for _, f := range []string{
		"00001_tasks_and_counts.sql",
		"00002_sequential.sql",
		"00003_virtual_time.sql",
		"00004_hierarchy.sql",
		"00005_db_clock_updated_at.sql",
		"00006_baseline_hardening.sql",
	} {
		if err := applyMigration(ctx, pool, f); err != nil {
			return nil, nil, err
		}
	}
	return pool, func() {
		pool.Close()
		_ = pg.Terminate(ctx)
	}, nil
}

// applyMigration executes the Up section of a goose SQL file.
func applyMigration(ctx context.Context, pool *pgxpool.Pool, file string) error {
	raw, err := os.ReadFile(filepath.Join("..", "..", "migrations", file))
	if err != nil {
		return err
	}
	up := strings.SplitN(string(raw), "-- +goose Down", 2)[0]
	up = strings.Replace(up, "-- +goose Up", "", 1)
	_, err = pool.Exec(ctx, up)
	return err
}

func clean(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(ctx, `TRUNCATE tasks, client_counts, client_sequence_state, client_virtual_time`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE scheduler_virtual_clock SET virtual_time = 0`); err != nil {
		t.Fatal(err)
	}
}

func uuid(i int) string { return fmt.Sprintf("11111111-1111-1111-1111-%012d", i) }

func queuedTask(i int, key string, priority int64) *domain.Task {
	return &domain.Task{
		ID: uuid(i), FairnessKey: key, Weight: 1.0, Status: domain.StatusQueued,
		Priority: priority, HasPriority: true, VirtualFinish: float64(priority),
		CreatedAt: time.Now(),
	}
}

func TestSaveFindRoundTrip(t *testing.T) {
	clean(t)
	in := queuedTask(1, "a", 100)
	in.RetryCount = 0
	if err := tasks.Save(ctx, in); err != nil {
		t.Fatal(err)
	}
	if in.Version != 0 {
		t.Fatalf("new task version = %d, want 0", in.Version)
	}
	got, err := tasks.FindByID(ctx, in.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.FairnessKey != "a" || got.Priority != 100 || !got.HasPriority ||
		got.Status != domain.StatusQueued || got.Weight != 1.0 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	// Update path bumps version.
	got.Status = domain.StatusDispatched
	if err := tasks.Save(ctx, got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 {
		t.Fatalf("version = %d, want 1", got.Version)
	}
}

func TestSaveVersionConflict(t *testing.T) {
	clean(t)
	if err := tasks.Save(ctx, queuedTask(2, "a", 10)); err != nil {
		t.Fatal(err)
	}
	a, _ := tasks.FindByID(ctx, uuid(2))
	b, _ := tasks.FindByID(ctx, uuid(2))
	a.Status, b.Status = domain.StatusDispatched, domain.StatusCommitted
	if err := tasks.Save(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := tasks.Save(ctx, b); !errors.Is(err, port.ErrVersionConflict) {
		t.Fatalf("second save err = %v, want ErrVersionConflict", err)
	}
}

func TestFindByIDNotFound(t *testing.T) {
	clean(t)
	if _, err := tasks.FindByID(ctx, uuid(999)); !errors.Is(err, port.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestFindReceivedOldestFirst(t *testing.T) {
	clean(t)
	now := time.Now()
	for i, age := range []time.Duration{3 * time.Hour, 1 * time.Hour, 2 * time.Hour} {
		tk := &domain.Task{ID: uuid(10 + i), FairnessKey: "a", Weight: 1,
			Status: domain.StatusReceived, CreatedAt: now.Add(-age)}
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	got, err := tasks.FindReceived(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != uuid(10) || got[1].ID != uuid(12) || got[2].ID != uuid(11) {
		t.Fatalf("order wrong: %v", ids(got))
	}
}

func TestDispatchSelectionQuotaAndOrder(t *testing.T) {
	clean(t)
	all := []*domain.Task{
		queuedTask(20, "a", 100),
		queuedTask(21, "a", 200),
		queuedTask(22, "b", 150),
	}
	all[2].Sequential = true // excluded from flat dispatch
	for _, tk := range all {
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	if err := counts.Increment(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	// Quota 1 with "a" holding 1 in-flight: only... "b" is sequential, so none.
	got, err := tasks.FindAndLockDispatchable(ctx, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("quota violated: %v", ids(got))
	}
	// No quota: priority order 100, 200.
	got, err = tasks.FindAndLockDispatchable(ctx, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != uuid(20) || got[1].ID != uuid(21) {
		t.Fatalf("order wrong: %v", ids(got))
	}
}

func TestDispatchSelectionPromotedBypassesQuota(t *testing.T) {
	clean(t)
	all := []*domain.Task{
		queuedTask(20, "a", 100),
		queuedTask(21, "a", 0), // starvation-promoted
	}
	for _, tk := range all {
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	if err := counts.Increment(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	// Quota 1 with "a" holding 1 in-flight: the promoted task still dispatches.
	got, err := tasks.FindAndLockDispatchable(ctx, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != uuid(21) {
		t.Fatalf("promoted bypass wrong: %v", ids(got))
	}
}

func TestSkipLockedHidesRowFromSecondTxn(t *testing.T) {
	clean(t)
	if err := tasks.Save(ctx, queuedTask(30, "a", 5)); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var locked string
	if err := tx.QueryRow(ctx, `SELECT id::text FROM tasks WHERE status = 'QUEUED'
		ORDER BY priority ASC FOR UPDATE`).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	// The pool-level dispatch select must skip the tx-locked row.
	got, err := tasks.FindAndLockDispatchable(ctx, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("locked row visible: %v", ids(got))
	}
}

func TestTransactAtomicity(t *testing.T) {
	clean(t)
	err := stores.Transact(ctx, func(tp port.TxPorts) error {
		tk := queuedTask(40, "a", 1)
		if err := tp.Tasks.Save(ctx, tk); err != nil {
			return err
		}
		if err := tp.Counts.Increment(ctx, "a"); err != nil {
			return err
		}
		if _, err := tp.VirtualTime.Reserve(ctx, "a", 1000, 1); err != nil {
			return err
		}
		return errors.New("boom")
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("transact err = %v, want boom", err)
	}
	if _, err := tasks.FindByID(ctx, uuid(40)); !errors.Is(err, port.ErrNotFound) {
		t.Fatalf("rolled-back task visible: %v", err)
	}
	if n, _ := counts.Get(ctx, "a"); n != 0 {
		t.Fatalf("rolled-back count = %d, want 0", n)
	}
}

func TestCountsFloorAndSet(t *testing.T) {
	clean(t)
	if err := counts.Decrement(ctx, "ghost"); err != nil {
		t.Fatal(err)
	}
	if err := counts.Increment(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := counts.Set(ctx, "a", -5); err != nil {
		t.Fatal(err)
	}
	if n, _ := counts.Get(ctx, "a"); n != 0 {
		t.Fatalf("set(-5) = %d, want 0", n)
	}
	if err := counts.Set(ctx, "a", 3); err != nil {
		t.Fatal(err)
	}
	all, err := counts.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if all["a"] != 3 {
		t.Fatalf("all = %v", all)
	}
}

func TestSequenceFindOrCreateAndSave(t *testing.T) {
	clean(t)
	st, err := seqs.FindOrCreate(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if st.NextSequence() != 1 || !st.Ready() {
		t.Fatalf("fresh = %+v", st)
	}
	st.OnDispatch(1, uuid(50))
	st.OnFailure(time.Now())
	if err := seqs.Save(ctx, st); err != nil {
		t.Fatal(err)
	}
	re, err := seqs.FindOrCreate(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if re.Ready() || !re.Blocked || re.LastDispatchedSequence != 1 {
		t.Fatalf("reloaded = %+v", re)
	}
}

func TestVirtualTimeReserveAndDispatch(t *testing.T) {
	clean(t)
	t1, err := vtime.Reserve(ctx, "a", 1000, 1)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := vtime.Reserve(ctx, "a", 1000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if t1 != 1000 || t2 != 2000 {
		t.Fatalf("tags = %v, %v; want 1000, 2000", t1, t2)
	}
	if err := vtime.RecordDispatch(ctx, map[string]float64{"a": t2}, nil); err != nil {
		t.Fatal(err)
	}
	v, err := vtime.SystemV(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != 2000 {
		t.Fatalf("V = %v, want 2000", v)
	}
}

func TestStarvedAndTimedOut(t *testing.T) {
	clean(t)
	old := queuedTask(60, "a", 999)
	old.CreatedAt = time.Now().Add(-2 * time.Hour)
	if err := tasks.Save(ctx, old); err != nil {
		t.Fatal(err)
	}
	starved, err := tasks.FindStarved(ctx, time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(starved) != 1 || starved[0].ID != uuid(60) {
		t.Fatalf("starved = %v", ids(starved))
	}
	// Make it in-flight long ago, then find via timeout scan.
	// The updated_at trigger would override a plain backdate UPDATE, so
	// the trigger is disabled for this statement only. Privilege needed is
	// table ownership (DISABLE TRIGGER is an owner-level operation, not
	// superuser) — satisfied by default in testcontainers; production code
	// never disables triggers.
	old.Status = domain.StatusDispatched
	if err := tasks.Save(ctx, old); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE tasks DISABLE TRIGGER trg_set_updated_at`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET updated_at = now() - interval '2 hours' WHERE id = $1::uuid`, uuid(60)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE tasks ENABLE TRIGGER trg_set_updated_at`); err != nil {
		t.Fatal(err)
	}
	timed, err := tasks.FindTimedOut(ctx, time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(timed) != 1 || timed[0].ID != uuid(60) {
		t.Fatalf("timed out = %v", ids(timed))
	}
}

func TestCountInFlightGroupsByKey(t *testing.T) {
	clean(t)
	for i, st := range []domain.Status{domain.StatusDispatched, domain.StatusCommitted, domain.StatusQueued} {
		tk := queuedTask(70+i, "a", int64(i))
		tk.Status = st
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	m, err := tasks.CountInFlight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m["a"] != 2 {
		t.Fatalf("in-flight = %v, want map[a:2]", m)
	}
}

func ids(ts []*domain.Task) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.ID
	}
	sort.Strings(out)
	return out
}

func TestListByKeyWithStatusFilter(t *testing.T) {
	clean(t)
	q := queuedTask(80, "a", 1)
	d := queuedTask(81, "a", 2)
	d.Status = domain.StatusDispatched
	o := queuedTask(82, "other", 3)
	for _, tk := range []*domain.Task{q, d, o} {
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	all, err := tasks.ListByKey(ctx, "a", nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("unfiltered = %v, %v", ids(all), err)
	}
	queued := domain.StatusQueued
	filtered, err := tasks.ListByKey(ctx, "a", &queued)
	if err != nil || len(filtered) != 1 || filtered[0].ID != uuid(80) {
		t.Fatalf("filtered = %v, %v", ids(filtered), err)
	}
}

func TestLockerMutualExclusion(t *testing.T) {
	clean(t)
	dsn := pool.Config().ConnString()
	a, err := adapter.NewLocker(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := adapter.NewLocker(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ok, release, err := a.Lock(ctx, "dispatcher")
	if err != nil || !ok {
		t.Fatalf("first lock = %v, %v", ok, err)
	}
	// Peer holder denied: non-blocking, no error.
	ok2, _, err := b.Lock(ctx, "dispatcher")
	if err != nil || ok2 {
		t.Fatalf("second lock = %v, %v; want denied", ok2, err)
	}
	// Different name unaffected.
	ok3, release3, err := b.Lock(ctx, "watchdog")
	if err != nil || !ok3 {
		t.Fatalf("other-name lock = %v, %v", ok3, err)
	}
	release3()
	// Release is idempotent; reacquire works.
	release()
	release()
	ok4, release4, err := b.Lock(ctx, "dispatcher")
	if err != nil || !ok4 {
		t.Fatalf("reacquire = %v, %v", ok4, err)
	}
	release4()
}

func TestPoolDropsClosedConn(t *testing.T) {
	// Pins the pgx v5.7.0 semantic the locker release path relies on:
	// releasing a closed underlying conn drops it instead of reusing it
	// (backend pid changes). If a pgx upgrade changes this, the locker
	// must be revisited.
	cfg, err := pgxpool.ParseConfig(pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	one, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	pid := func() int {
		c, err := one.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Release()
		var p int
		if err := c.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	c, err := one.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var before int
	if err := c.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_ = c.Conn().Close(ctx)
	c.Release()
	if after := pid(); after == before {
		t.Fatalf("pool reused closed conn (pid %d)", after)
	}
}
