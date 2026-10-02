package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/synanton/equalix-go/internal/port"
)

// VirtualTimeStore is the pgx VirtualTimeRepository: Reserve via the
// atomic upsert (spec §2.3), RecordDispatch via monotonic upserts,
// SystemV with zero default. domain.Store is the reference implementation.
type VirtualTimeStore struct{ q querier }

var _ port.VirtualTimeRepository = (*VirtualTimeStore)(nil)

func (s *VirtualTimeStore) Reserve(ctx context.Context, key string, quantum, weight float64) (float64, error) {
	w := weight
	if w <= 0 {
		w = 1.0
	}
	// Snapshot V first (spec: tags start at max(finish, V)); the upsert's
	// GREATEST keeps concurrent reservers sequential.
	v, err := s.SystemV(ctx)
	if err != nil {
		return 0, err
	}
	var tag float64
	err = s.q.QueryRow(ctx, `INSERT INTO client_virtual_time AS cvt
        (fairness_key, virtual_time, virtual_finish, updated_at)
        VALUES ($1, $2::float8, $2::float8 + $3::float8, now())
        ON CONFLICT (fairness_key) DO UPDATE SET
            virtual_finish = GREATEST(cvt.virtual_finish, $2::float8) + $3::float8,
            updated_at = now()
        RETURNING virtual_finish`, key, v, quantum/w).Scan(&tag)
	if err != nil {
		return 0, fmt.Errorf("postgres: reserve tag %s: %w", key, err)
	}
	return tag, nil
}

func (s *VirtualTimeStore) RecordDispatch(ctx context.Context, tags, credits map[string]float64) error {
	peak := 0.0
	first := true
	for key, tag := range tags {
		_, err := s.q.Exec(ctx, `INSERT INTO client_virtual_time AS cvt
            (fairness_key, virtual_time, virtual_finish, updated_at)
            VALUES ($1, $2, $2, now())
            ON CONFLICT (fairness_key) DO UPDATE SET
                virtual_time = GREATEST(cvt.virtual_time, $2),
                virtual_finish = GREATEST(cvt.virtual_finish, $2),
                updated_at = now()`, key, tag)
		if err != nil {
			return fmt.Errorf("postgres: advance key %s: %w", key, err)
		}
		if aged := tag - credits[key]; first || aged > peak {
			peak, first = aged, false
		}
	}
	if first {
		return nil
	}
	_, err := s.q.Exec(ctx, `INSERT INTO scheduler_virtual_clock AS svc
        (id, virtual_time, updated_at)
        VALUES (1, $1, now())
        ON CONFLICT (id) DO UPDATE SET
            virtual_time = GREATEST(svc.virtual_time, $1),
            updated_at = now()`, peak)
	if err != nil {
		return fmt.Errorf("postgres: advance system V: %w", err)
	}
	return nil
}

func (s *VirtualTimeStore) SystemV(ctx context.Context) (float64, error) {
	var v *float64
	err := s.q.QueryRow(ctx,
		`SELECT virtual_time FROM scheduler_virtual_clock WHERE id = 1`).Scan(&v)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Absent row (pre-migration database): V = 0, matching the
			// zero state of domain.Store.
			return 0, nil
		}
		return 0, fmt.Errorf("postgres: system V: %w", err)
	}
	if v == nil {
		return 0, nil
	}
	return *v, nil
}
