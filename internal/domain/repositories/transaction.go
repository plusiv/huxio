package repositories

import "context"

// TxManager runs a unit of work inside a single database transaction. Every
// repository called with the derived context joins that transaction, which is
// how the message insert and the queue insert commit together.
type TxManager interface {
	WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}
