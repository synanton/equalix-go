// Package postgres implements the port interfaces on PostgreSQL via pgx.
//
// Shape (spec §13 DECISION-3): one concrete store per aggregate, built over
// a narrow querier satisfied by both *pgxpool.Pool and pgx.Tx. Pool-bound
// instances are the only ones handed out; Transact builds tx-bound instances
// internally and never returns them, so callers cannot name a wrong
// connection. Behavioral reference: docs/schema.sql, docs/spec.md §§5, 8.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/synanton/equalix-go/internal/port"
)

// querier is the query surface shared by pools and transactions
// (*pgxpool.Pool and pgx.Tx both satisfy it).
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Stores bundles the pool-bound repositories and the Transactor.
// Obtain via NewStores; never construct stores directly.
type Stores struct {
	pool TxBeginner

	Tasks       *TaskStore
	Counts      *CountsStore
	Sequences   *SequenceStore
	VirtualTime *VirtualTimeStore
	Hierarchy   *HierarchyStores
}

// TxBeginner opens transactions; *pgxpool.Pool is the production
// implementation. Exported so tests can interpose a counting pool around
// the same Transact path the jobs use — the dispatch hot path pays nothing
// for the seam (one interface call per tick, same as the method call).
type TxBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// NewStores returns pool-bound repositories over pool.
func NewStores(pool TxBeginner) *Stores {
	return bind(pool, pool.(querier))
}

// bind builds stores over one querier, remembering pool for Transact.
func bind(pool TxBeginner, q querier) *Stores {
	return &Stores{
		pool:        pool,
		Tasks:       &TaskStore{q: q},
		Counts:      &CountsStore{q: q},
		Sequences:   &SequenceStore{q: q},
		VirtualTime: &VirtualTimeStore{q: q},
		Hierarchy:   &HierarchyStores{q: q},
	}
}

// Transact runs fn with repositories bound to a single transaction
// (port.Transactor). Rollback on fn error; commit errors surface as plain
// driver errors, never port.ErrVersionConflict (see port/tx.go contract).
func (s *Stores) Transact(ctx context.Context, fn func(port.TxPorts) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	bound := bind(nil, tx)
	if err := fn(port.TxPorts{
		Tasks:       bound.Tasks,
		Counts:      bound.Counts,
		VirtualTime: bound.VirtualTime,
		Leaves:      bound.Hierarchy,
		HStates:     bound.Hierarchy,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit tx: %w", err)
	}
	return nil
}
