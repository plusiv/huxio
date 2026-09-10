package entities_test

import (
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

func TestPartitionKeyForIsStableAndInRange(t *testing.T) {
	t.Parallel()

	ids := []string{"ep_2Xk1", "ep_2Xk2", "app_2Xk1", "", "ep_" + string(make([]byte, 64))}
	for _, id := range ids {
		first := entities.PartitionKeyFor(id)
		if first != entities.PartitionKeyFor(id) {
			t.Errorf("PartitionKeyFor(%q) is not deterministic", id)
		}
		if first < 0 || first >= entities.QueuePartitions {
			t.Errorf("PartitionKeyFor(%q) = %d, out of [0,%d)", id, first, entities.QueuePartitions)
		}
	}
}

func TestPartitionKeyForSpreadsAcrossPartitions(t *testing.T) {
	t.Parallel()

	seen := make(map[int16]int)
	for i := range 5000 {
		id := "ep_" + string(rune('a'+i%26)) + time.Duration(i).String()
		seen[entities.PartitionKeyFor(id)]++
	}
	// 5000 ids over 256 partitions: a healthy hash touches most partitions.
	if len(seen) < entities.QueuePartitions/2 {
		t.Errorf("only %d of %d partitions used", len(seen), entities.QueuePartitions)
	}
}

func TestTriggerTypeRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trigger entities.TriggerType
		want    bool
	}{
		{"scheduled retries", entities.TriggerScheduled, true},
		{"manual resend does not", entities.TriggerManual, false},
		{"bulk replay does not", entities.TriggerBulkReplay, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.trigger.Retryable(); got != tc.want {
				t.Errorf("Retryable() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPartitionLeaseExpired(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	ttl := 15 * time.Second

	tests := []struct {
		name  string
		lease entities.PartitionLease
		want  bool
	}{
		{"unowned", entities.PartitionLease{}, true},
		{"blank owner", entities.PartitionLease{OwnerID: utils.Ptr("")}, true},
		{"owner without heartbeat", entities.PartitionLease{OwnerID: utils.Ptr("wkr_1")}, true},
		{
			"fresh heartbeat",
			entities.PartitionLease{OwnerID: utils.Ptr("wkr_1"), HeartbeatAt: utils.Ptr(now.Add(-5 * time.Second))},
			false,
		},
		{
			"stale heartbeat",
			entities.PartitionLease{OwnerID: utils.Ptr("wkr_1"), HeartbeatAt: utils.Ptr(now.Add(-30 * time.Second))},
			true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.lease.Expired(now, ttl); got != tc.want {
				t.Errorf("Expired() = %v, want %v", got, tc.want)
			}
		})
	}
}
