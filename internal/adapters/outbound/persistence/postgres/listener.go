package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// Listener implements repositories.NotificationListener on Postgres
// LISTEN/NOTIFY. It holds one dedicated connection outside the pooled budget
// for as long as it runs, and reconnects with backoff.
type Listener struct {
	pool           *pgxpool.Pool
	reconnectDelay time.Duration
	maxDelay       time.Duration
}

// NewListener builds a listener over the supplied pool.
func NewListener(pool *pgxpool.Pool) *Listener {
	return &Listener{pool: pool, reconnectDelay: 250 * time.Millisecond, maxDelay: 10 * time.Second}
}

// Listen blocks until ctx is cancelled, invoking handle for every notification.
func (l *Listener) Listen(
	ctx context.Context,
	channel string,
	handle func(ctx context.Context, payload string),
) error {
	log := logger.FromContext(ctx).With().Str("channel", channel).Logger()
	delay := l.reconnectDelay

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		err := l.listenOnce(ctx, channel, handle)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			log.Warn().Err(err).Dur("retry_in", delay).Msg("notification listener dropped, reconnecting")
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		if delay = delay * 2; delay > l.maxDelay {
			delay = l.maxDelay
		}
	}
}

func (l *Listener) listenOnce(
	ctx context.Context,
	channel string,
	handle func(ctx context.Context, payload string),
) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return eris.Wrap(err, "acquire listener connection")
	}
	defer conn.Release()

	// The channel name cannot be a bind parameter; it is a package constant,
	// never caller-supplied text.
	if _, err := conn.Exec(ctx, `LISTEN `+quoteIdent(channel)); err != nil {
		return eris.Wrap(err, "listen")
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return eris.Wrap(err, "wait for notification")
		}
		handle(ctx, notification.Payload)
	}
}

var _ repositories.NotificationListener = (*Listener)(nil)
