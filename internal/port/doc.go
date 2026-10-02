// Package port declares the outbound boundaries of the domain core.
// Adapters (pgx, go-redis, HTTP executor, Prometheus) implement these
// interfaces; the domain and jobs depend only on the interfaces.
//
// Rules: ports reference domain types, never adapter types; ports perform
// no I/O themselves. Behavioral reference: docs/spec.md (sections cited
// per interface).
//
// Port map — which job uses which port:
//
//	dispatcher            → Transactor (Tasks + Counts + VirtualTime in one tx),
//	                        CMSStore, Executor, Metrics
//	priority calculator   → TaskRepository, CMSStore, Metrics (penalty factor read)
//	watchdog              → TaskRepository, CountsRepository, CMSStore, Metrics
//	completion handler    → TaskRepository, CountsRepository, CMSStore, Metrics
//	adaptive RPS          → Metrics
//	scheduled jobs        → Locker
package port
