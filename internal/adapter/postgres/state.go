package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// CountsStore is the pgx CountsRepository (client_counts).
type CountsStore struct{ q querier }

var _ port.CountsRepository = (*CountsStore)(nil)

func (s *CountsStore) Increment(ctx context.Context, key string) error {
	return s.add(ctx, key, 1)
}

func (s *CountsStore) Decrement(ctx context.Context, key string) error {
	return s.add(ctx, key, -1)
}

func (s *CountsStore) add(ctx context.Context, key string, delta int) error {
	// updated_at DB-owned (trigger): omitted on write, DEFAULT on insert.
	_, err := s.q.Exec(ctx, `INSERT INTO client_counts AS cc
        (fairness_key, in_flight_count)
        VALUES ($1, GREATEST(0, $2))
        ON CONFLICT (fairness_key) DO UPDATE SET
            in_flight_count = GREATEST(0, cc.in_flight_count + $2)`, key, delta)
	if err != nil {
		return fmt.Errorf("postgres: counts add %s: %w", key, err)
	}
	return nil
}

// AddBatch applies per-key deltas in one UNNEST upsert: a dispatch tick's N
// increments become one statement (same GREATEST floor per key).
func (s *CountsStore) AddBatch(ctx context.Context, deltas map[string]int) error {
	if len(deltas) == 0 {
		return nil
	}
	keys := make([]string, 0, len(deltas))
	amounts := make([]int32, 0, len(deltas))
	for k, d := range deltas {
		keys = append(keys, k)
		amounts = append(amounts, int32(d))
	}
	_, err := s.q.Exec(ctx, `INSERT INTO client_counts AS cc (fairness_key, in_flight_count)
        SELECT key, GREATEST(0, delta) FROM UNNEST($1::text[], $2::int[]) AS t(key, delta)
        ON CONFLICT (fairness_key) DO UPDATE SET
            in_flight_count = GREATEST(0, cc.in_flight_count + EXCLUDED.in_flight_count)`,
		keys, amounts)
	if err != nil {
		return fmt.Errorf("postgres: counts add batch %d keys: %w", len(deltas), err)
	}
	return nil
}

func (s *CountsStore) Get(ctx context.Context, key string) (int, error) {
	var n int
	err := s.q.QueryRow(ctx,
		`SELECT in_flight_count FROM client_counts WHERE fairness_key = $1`, key).Scan(&n)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("postgres: counts get %s: %w", key, err)
	}
	return n, nil
}

func (s *CountsStore) Set(ctx context.Context, key string, n int) error {
	if n < 0 {
		n = 0
	}
	_, err := s.q.Exec(ctx, `INSERT INTO client_counts AS cc
        (fairness_key, in_flight_count)
        VALUES ($1, $2)
        ON CONFLICT (fairness_key) DO UPDATE SET
            in_flight_count = $2`, key, n)
	if err != nil {
		return fmt.Errorf("postgres: counts set %s: %w", key, err)
	}
	return nil
}

func (s *CountsStore) All(ctx context.Context) (map[string]int, error) {
	rows, err := s.q.Query(ctx, `SELECT fairness_key, in_flight_count FROM client_counts`)
	if err != nil {
		return nil, fmt.Errorf("postgres: counts all: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var key string
		var n int
		if err := rows.Scan(&key, &n); err != nil {
			return nil, fmt.Errorf("postgres: scan counts: %w", err)
		}
		out[key] = n
	}
	return out, rows.Err()
}

// SequenceStore is the pgx SequenceStateRepository.
type SequenceStore struct{ q querier }

var _ port.SequenceStateRepository = (*SequenceStore)(nil)

func (s *SequenceStore) FindOrCreate(ctx context.Context, key string) (*domain.SequenceState, error) {
	// Single round-trip (no SELECT-then-INSERT): insert the zero row or
	// no-op on conflict, returning the row either way.
	var st domain.SequenceState
	var execID pgtype.Text
	var blockedAt pgtype.Timestamptz
	err := s.q.QueryRow(ctx, `INSERT INTO client_sequence_state AS css
        (fairness_key, last_completed_sequence, last_dispatched_sequence)
        VALUES ($1, 0, 0)
        ON CONFLICT (fairness_key) DO UPDATE SET fairness_key = EXCLUDED.fairness_key
        RETURNING css.fairness_key, css.last_completed_sequence,
            css.last_dispatched_sequence, css.current_executing_task_id::text,
            css.is_blocked, css.blocked_at`,
		key).Scan(
		&st.FairnessKey, &st.LastCompletedSequence, &st.LastDispatchedSequence,
		&execID, &st.Blocked, &blockedAt)
	if err != nil {
		return nil, fmt.Errorf("postgres: sequence ensure %s: %w", key, err)
	}
	if execID.Valid {
		st.CurrentExecutingID = execID.String
		st.HasExecuting = true
	}
	if blockedAt.Valid {
		st.BlockedAt = blockedAt.Time
	}
	return &st, nil
}

func (s *SequenceStore) Save(ctx context.Context, st *domain.SequenceState) error {
	_, err := s.q.Exec(ctx, `UPDATE client_sequence_state SET
            last_completed_sequence = $2, last_dispatched_sequence = $3,
            current_executing_task_id = $4::uuid, is_blocked = $5,
            blocked_at = $6
        WHERE fairness_key = $1`,
		st.FairnessKey, st.LastCompletedSequence, st.LastDispatchedSequence,
		nullableUUIDorNil(st.CurrentExecutingID, st.HasExecuting),
		st.Blocked, nullableTime(st.BlockedAt))
	if err != nil {
		return fmt.Errorf("postgres: sequence save %s: %w", st.FairnessKey, err)
	}
	return nil
}

func nullableUUIDorNil(id string, ok bool) any {
	if !ok || id == "" {
		return nil
	}
	return id
}
