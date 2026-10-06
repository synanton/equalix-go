// Package hierarchical decorates a CMSStore with ancestor fan-out
// for hierarchical mode (Java HierarchicalCmsProvider, EQX-7): every
// update of a fairness key is also applied to its internal nodes and
// the root, because the selector reads in-flight pressure per node
// key. Flat mode is unaffected — the decorator is only constructed
// when hierarchy is enabled (nil hierarchy would be a wiring bug;
// New panics on it, same fail-loud rule as the metrics pair).
package hierarchical

import (
	"context"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// Store wraps an inner CMSStore with ancestor fan-out.
type Store struct {
	inner port.CMSStore
	hier  *domain.FairnessHierarchy
}

var _ port.CMSStore = (*Store)(nil)

// New wraps inner. hier must be non-nil and enabled.
func New(inner port.CMSStore, hier *domain.FairnessHierarchy) *Store {
	if inner == nil {
		panic("hierarchical: inner CMSStore must not be nil")
	}
	if hier == nil || !hier.Enabled() {
		panic("hierarchical: enabled hierarchy required (flat mode uses the inner store directly)")
	}
	return &Store{inner: inner, hier: hier}
}

func (s *Store) Add(ctx context.Context, key string, delta int64) error {
	if err := s.inner.Add(ctx, key, delta); err != nil {
		return err
	}
	for _, node := range s.hier.InternalNodeKeys(key) {
		if err := s.inner.Add(ctx, node, delta); err != nil {
			return err
		}
	}
	return s.inner.Add(ctx, domain.HierarchyRoot, delta)
}

func (s *Store) AddBatch(ctx context.Context, deltas map[string]int64) error {
	expanded := map[string]int64{}
	for key, delta := range deltas {
		expanded[key] += delta
		for _, node := range s.hier.InternalNodeKeys(key) {
			expanded[node] += delta
		}
		expanded[domain.HierarchyRoot] += delta
	}
	return s.inner.AddBatch(ctx, expanded)
}

func (s *Store) EstimateCount(ctx context.Context, key string) (int64, error) {
	return s.inner.EstimateCount(ctx, key)
}

func (s *Store) Total(ctx context.Context) (int64, error) {
	return s.inner.Total(ctx)
}

// Rebuild expands the snapshot with ancestor counts before delegating
// (Java rebuild(hierarchy.withAncestors(snapshot))). The inner total
// then includes every ancestor update — same accounting Java notes,
// not a discrepancy.
func (s *Store) Rebuild(ctx context.Context, counts map[string]int64) error {
	return s.inner.Rebuild(ctx, s.hier.WithAncestors(counts))
}
