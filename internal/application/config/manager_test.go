package config_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/rotisserie/eris"
)

// fakeSnapshotRepo serves configurable snapshot data and counts loads.
type fakeSnapshotRepo struct {
	mu    sync.Mutex
	data  *repositories.SnapshotData
	err   error
	loads atomic.Int32
}

func (f *fakeSnapshotRepo) LoadSnapshot(context.Context) (*repositories.SnapshotData, error) {
	f.loads.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.data, nil
}

func (f *fakeSnapshotRepo) NotifyConfigChanged(context.Context, string) error { return nil }

func (f *fakeSnapshotRepo) set(data *repositories.SnapshotData, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data, f.err = data, err
}

// fakeListener hands the test a way to push notifications.
type fakeListener struct {
	notify chan string
}

func (f *fakeListener) Listen(ctx context.Context, _ string, handle func(context.Context, string)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case payload := <-f.notify:
			handle(ctx, payload)
		}
	}
}

func withApps(ids ...string) *repositories.SnapshotData {
	data := &repositories.SnapshotData{}
	for _, id := range ids {
		data.Applications = append(data.Applications, &entities.Application{ID: id, OrgID: "org_1"})
	}
	return data
}

func TestManagerLoadAndCurrent(t *testing.T) {
	t.Parallel()

	repo := &fakeSnapshotRepo{data: withApps("app_1")}
	manager := config.NewManager(repo, &fakeListener{}, newSealer(t), telemetry.New().ConfigSnapshotAge, config.ManagerOptions{})

	if manager.Current() != nil {
		t.Error("Current must be nil before the first load")
	}
	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if manager.Current().App("app_1") == nil {
		t.Error("loaded snapshot is missing app_1")
	}
}

func TestManagerReloadsOnNotification(t *testing.T) {
	t.Parallel()

	repo := &fakeSnapshotRepo{data: withApps("app_1")}
	listener := &fakeListener{notify: make(chan string, 1)}
	manager := config.NewManager(repo, listener, newSealer(t), telemetry.New().ConfigSnapshotAge, config.ManagerOptions{
		RefreshInterval: time.Hour, // isolate the notification path
		Debounce:        5 * time.Millisecond,
	})

	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := manager.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	repo.set(withApps("app_1", "app_2"), nil)
	listener.notify <- "org_1"

	deadline := time.After(2 * time.Second)
	for manager.Current().App("app_2") == nil {
		select {
		case <-deadline:
			t.Fatal("snapshot was not reloaded after a notification")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done
}

func TestManagerKeepsPreviousSnapshotWhenReloadFails(t *testing.T) {
	t.Parallel()

	repo := &fakeSnapshotRepo{data: withApps("app_1")}
	listener := &fakeListener{notify: make(chan string, 1)}
	manager := config.NewManager(repo, listener, newSealer(t), telemetry.New().ConfigSnapshotAge, config.ManagerOptions{
		RefreshInterval: time.Hour,
		Debounce:        time.Millisecond,
	})
	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = manager.Run(ctx)
	}()

	repo.set(nil, eris.New("database down"))
	before := repo.loads.Load()
	listener.notify <- ""

	deadline := time.After(2 * time.Second)
	for repo.loads.Load() == before {
		select {
		case <-deadline:
			t.Fatal("reload was never attempted")
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Stale config beats no config: the previous snapshot must still serve.
	if manager.Current() == nil || manager.Current().App("app_1") == nil {
		t.Error("a failed reload must leave the previous snapshot in place")
	}

	cancel()
	<-done
}

func TestManagerRefreshIntervalIsTheBackstop(t *testing.T) {
	t.Parallel()

	repo := &fakeSnapshotRepo{data: withApps("app_1")}
	// No notifications at all: the timer alone must pick up the change, which
	// is what saves us when LISTEN dies silently.
	manager := config.NewManager(repo, &fakeListener{}, newSealer(t), telemetry.New().ConfigSnapshotAge, config.ManagerOptions{
		RefreshInterval: 20 * time.Millisecond,
	})
	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = manager.Run(ctx)
	}()

	repo.set(withApps("app_1", "app_late"), nil)

	deadline := time.After(2 * time.Second)
	for manager.Current().App("app_late") == nil {
		select {
		case <-deadline:
			t.Fatal("the unconditional refresh never ran")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done
}

func TestManagerCoalescesNotificationBursts(t *testing.T) {
	t.Parallel()

	repo := &fakeSnapshotRepo{data: withApps("app_1")}
	listener := &fakeListener{notify: make(chan string, 64)}
	manager := config.NewManager(repo, listener, newSealer(t), telemetry.New().ConfigSnapshotAge, config.ManagerOptions{
		RefreshInterval: time.Hour,
		Debounce:        50 * time.Millisecond,
	})
	if err := manager.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	loadsAfterInitial := repo.loads.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = manager.Run(ctx)
	}()

	// A bulk endpoint import fires one notification per row.
	for range 50 {
		listener.notify <- "org_1"
	}
	time.Sleep(400 * time.Millisecond)
	cancel()
	<-done

	reloads := repo.loads.Load() - loadsAfterInitial
	if reloads == 0 {
		t.Fatal("the burst produced no reload at all")
	}
	if reloads > 5 {
		t.Errorf("burst of 50 notifications caused %d reloads; they must coalesce", reloads)
	}
}
