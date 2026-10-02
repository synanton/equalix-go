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

// Adapter contract notes (pgx implementation, EQLX-2):
//
//   - Context cancellation aborts the transaction: a cancelled ctx rolls
//     back and fn's error is not committed. Commit/rollback races surface
//     as driver errors (e.g. pgx.ErrTxCommitRollback); the adapter maps
//     them to plain errors — never to ErrVersionConflict, which is reserved
//     for optimistic-locking losses detected via Save.
//   - TxPorts must not escape the callback: retaining a tx-bound repository
//     past Transact's return is a use-after-commit. Adapters should make
//     escape a loud failure in tests (e.g. closed-connection error), not a
//     silent wrong-connection read.
//
// Open shape question for EQLX-2-pgx (record as DECISION once chosen):
// the adapter needs pool-bound repositories (calculator reads, watchdog
// snapshot, completion FindByID) and tx-bound ones (dispatch path) —
// either as an explicit interface pair (pool vs tx variants, misuse is a
// compile error) or one interface with two constructors (pool vs tx,
// misuse is a runtime error). Java's @Transactional hides this via
// per-thread proxied connections; Go must choose explicitly.
