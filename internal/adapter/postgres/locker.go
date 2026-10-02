package postgres

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/synanton/equalix-go/internal/port"
)

// Locker is the pg advisory-lock port.Locker (spec §9 Go replacement for
// ShedLock). Each Locker owns a separate single-connection pool: session
// scope means the lock lives on the connection, a crashed process releases
// on close (no TTL), and the job pool's sizing is unaffected (scope §6).
type Locker struct {
	pool *pgxpool.Pool
}

var _ port.Locker = (*Locker)(nil)

// NewLocker builds a Locker over its own 1-connection pool.
// connString is the same DSN shape as the job pool's.
// Use one Locker per job name: a shared Locker serializes concurrent ticks
// across jobs on its single connection.
func NewLocker(ctx context.Context, connString string) (*Locker, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("postgres: locker parse DSN: %w", err)
	}
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: locker pool: %w", err)
	}
	return &Locker{pool: pool}, nil
}

// lockKey hashes the job name to int64 deterministically across instances,
// restarts, and languages (FNV-1a 64 over "equalix:lock:"+name).
func lockKey(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("equalix:lock:" + name))
	return int64(binary.BigEndian.Uint64(h.Sum(nil)))
}

// Lock tries the advisory lock without blocking. Held-by-peer →
// (false, nil, nil). The returned release is idempotent; the connection is
// held until release, so the lock cannot outlive it.
func (l *Locker) Lock(ctx context.Context, name string) (bool, func(), error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("postgres: locker acquire: %w", err)
	}
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey(name)).Scan(&ok); err != nil {
		conn.Release()
		return false, nil, fmt.Errorf("postgres: try lock %s: %w", name, err)
	}
	if !ok {
		conn.Release()
		return false, nil, nil
	}
	var released bool
	return true, func() {
		if released {
			return
		}
		released = true
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey(name))
		conn.Release()
	}, nil
}

// Close drains the locker pool (process shutdown path).
func (l *Locker) Close() { l.pool.Close() }
