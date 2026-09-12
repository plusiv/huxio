package repositories_test

import (
	"slices"
	"testing"

	"github.com/plusiv/huxio/internal/domain/repositories"
)

func TestDistinctPartitionsKeepsEachOnceInFirstSeenOrder(t *testing.T) {
	t.Parallel()

	tasks := []repositories.EnqueueTask{
		{PartitionKey: 42}, {PartitionKey: 7}, {PartitionKey: 42}, {PartitionKey: 7}, {PartitionKey: 0},
	}
	if got := repositories.DistinctPartitions(tasks); !slices.Equal(got, []int16{42, 7, 0}) {
		t.Errorf("DistinctPartitions = %v, want [42 7 0]", got)
	}
	if got := repositories.DistinctPartitions(nil); len(got) != 0 {
		t.Errorf("DistinctPartitions(nil) = %v, want empty", got)
	}
}
