package port

import "context"

// TxPorts bundles the repositories bound to one connection/transaction.
// The pgx adapter implements Transactor by checking out one connection;
// callers must not retain TxPorts past the Transact callback.
type TxPorts struct {
	Tasks       TaskRepository
	Counts      CountsRepository
	VirtualTime VirtualTimeRepository
}

// Transactor runs fn with repositories bound to a single transaction.
//
// B1 decision (atomic dispatch, Java parity): the dispatch path commits
// task status moves + client_counts increments + virtual-time advances
// atomically (Java: one @Transactional covering DispatcherService's save,
// incrementInFlight, and recordDispatch). CMS deltas and the executor call
// happen AFTER commit (spec §5.2: CMS via after-commit hook, executor send
// after save) — a crash between commit and flush is exactly the drift the
// watchdog repairs (spec §8), bounded by design rather than by a second
// transaction. Rollback on fn error discards all three writes together,
// so SKIP LOCKED never orphans a lock without its accounting.
type Transactor interface {
	Transact(ctx context.Context, fn func(TxPorts) error) error
}
