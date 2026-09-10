package repositories

import "context"

// NotificationListener subscribes to database notifications. Config and queue
// wakeups both ride on it. Implementations must reconnect on their own, and
// consumers must never rely on delivery: notifications drop silently on
// connection loss, which is why every consumer also refreshes on a timer.
type NotificationListener interface {
	// Listen blocks until ctx is cancelled, calling handle for every payload
	// received on the channel.
	Listen(ctx context.Context, channel string, handle func(ctx context.Context, payload string)) error
}
