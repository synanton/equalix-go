package postgres

import (
	"context"
	"fmt"
	"sort"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// HierarchyStores implements port.HierarchyStateStore + port.LeafStore
// (Java HierarchyStateRepositoryAdapter + the hierarchy task queries,
// EQX-7). Schema pre-exists (migrations 00004/00005 +
// idx_tasks_queued_by_key — the covering index serving the per-key
// head selection, cited here so the hot-path index has a second
// witness besides docs/schema.sql).
type HierarchyStores struct{ q querier }

var _ port.HierarchyStateStore = (*HierarchyStores)(nil)

// FindStates loads node states for keys; absent keys miss (fresh
// nodes start at their parent floor — never synthesize zeros here,
// the domain floor logic owns defaults).
func (s *HierarchyStores) FindStates(ctx context.Context, keys []string) (map[string]domain.HierarchyNodeState, error) {
	out := map[string]domain.HierarchyNodeState{}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := s.q.Query(ctx,
		`SELECT node_key, virtual_time, children_virtual_time FROM hierarchy_node WHERE node_key = ANY($1)`, keys)
	if err != nil {
		return nil, fmt.Errorf("postgres: hierarchy states: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var st domain.HierarchyNodeState
		if err := rows.Scan(&st.Key, &st.VirtualTime, &st.ChildrenVirtualTime); err != nil {
			return nil, fmt.Errorf("postgres: scan hierarchy state: %w", err)
		}
		out[st.Key] = st
	}
	return out, rows.Err()
}

// ChargeVirtualTime applies max(vt, floor) + delta, inserting absent
// nodes (mirrors the JPA upsert: floor + delta on insert, GREATEST on
// conflict — same arithmetic both paths).
func (s *HierarchyStores) ChargeVirtualTime(ctx context.Context, key string, floor, delta float64) error {
	_, err := s.q.Exec(ctx, `INSERT INTO hierarchy_node AS hn
        (node_key, virtual_time, children_virtual_time)
        VALUES ($1, $2::float8 + $3::float8, 0)
        ON CONFLICT (node_key) DO UPDATE SET
            virtual_time = GREATEST(hn.virtual_time, $2) + $3`,
		key, floor, delta)
	if err != nil {
		return fmt.Errorf("postgres: charge node %s: %w", key, err)
	}
	return nil
}

// RaiseChildrenFloor ratchets a parent's children floor monotonically.
func (s *HierarchyStores) RaiseChildrenFloor(ctx context.Context, key string, floor float64) error {
	_, err := s.q.Exec(ctx, `INSERT INTO hierarchy_node AS hn
        (node_key, virtual_time, children_virtual_time)
        VALUES ($1, 0, $2)
        ON CONFLICT (node_key) DO UPDATE SET
            children_virtual_time = GREATEST(hn.children_virtual_time, $2)`,
		key, floor)
	if err != nil {
		return fmt.Errorf("postgres: raise floor %s: %w", key, err)
	}
	return nil
}

var _ port.LeafStore = (*HierarchyStores)(nil)

// FindAndLockQueuedHeads locks up to perLeaf[key] head tasks per key
// by (priority, created_at, id) SKIP LOCKED, in one query (Java
// findAndLockQueuedHeads LATERAL shape). Keys sorted before locking:
// two dispatchers racing for the same rows must acquire in the same
// order or AB-BA deadlock. Call inside Transact.
func (s *HierarchyStores) FindAndLockQueuedHeads(ctx context.Context, perLeaf map[string]int) ([]*domain.Task, error) {
	keys := make([]string, 0, len(perLeaf))
	for k, n := range perLeaf {
		if n > 0 {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	sort.Strings(keys)
	limits := make([]int32, len(keys))
	for i, k := range keys {
		limits[i] = int32(perLeaf[k])
	}
	rows, err := s.q.Query(ctx, `SELECT t.* FROM
        unnest($1::text[], $2::int[]) AS want(key, lim)
        JOIN LATERAL (
            SELECT `+taskColumns+` FROM tasks
            WHERE status = 'QUEUED' AND is_sequential = false
                AND fairness_key = want.key
            ORDER BY priority ASC NULLS LAST, created_at ASC, id ASC
            LIMIT want.lim
            FOR UPDATE SKIP LOCKED
        ) t ON true`, keys, limits)
	if err != nil {
		return nil, fmt.Errorf("postgres: lock queued heads: %w", err)
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan locked head: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// FindQueuedLeaves aggregates per-key backlog for QUEUED
// non-sequential tasks: queued count, promoted (priority 0) count,
// max weight, and authoritative in-flight from client_counts (the
// hard quota reads authoritative counts, never CMS estimates —
// matches Java's QueuedLeaf.inFlight source exactly).
func (s *HierarchyStores) FindQueuedLeaves(ctx context.Context) ([]domain.QueuedLeaf, error) {
	rows, err := s.q.Query(ctx, `SELECT t.fairness_key,
            COUNT(*),
            COUNT(*) FILTER (WHERE t.priority = 0),
            MAX(t.weight),
            COALESCE(cc.in_flight_count, 0)
        FROM tasks t
        LEFT JOIN client_counts cc ON cc.fairness_key = t.fairness_key
        WHERE t.status = 'QUEUED' AND t.is_sequential = false
        GROUP BY t.fairness_key, cc.in_flight_count`)
	if err != nil {
		return nil, fmt.Errorf("postgres: queued leaves: %w", err)
	}
	defer rows.Close()
	var out []domain.QueuedLeaf
	for rows.Next() {
		var leaf domain.QueuedLeaf
		var maxWeight float64
		if err := rows.Scan(&leaf.FairnessKey, &leaf.Queued, &leaf.Promoted, &maxWeight, &leaf.InFlight); err != nil {
			return nil, fmt.Errorf("postgres: scan leaf: %w", err)
		}
		leaf.MaxWeight = maxWeight
		out = append(out, leaf)
	}
	return out, rows.Err()
}
