/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// newCacheWithCQ returns a Cache with one ClusterQueue "cq" and N admitted
// workloads, used by the batch-delete tests and the benchmark.
func newCacheWithCQ(t testing.TB, n int) (*Cache, []workload.Reference) {
	t.Helper()
	ctx, log := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewFakeClient()
	c := New(cl)

	rf := utiltestingapi.MakeResourceFlavor("default").Obj()
	c.AddOrUpdateResourceFlavor(log, rf)

	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceCPU, "1000").Obj(),
		).
		NamespaceSelector(nil).
		Obj()
	if err := c.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}

	now := time.Now()
	keys := make([]workload.Reference, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("wl-%d", i)
		wl := utiltestingapi.MakeWorkload(name, "").
			PodSets(*utiltestingapi.MakePodSet("main", 1).
				Request(corev1.ResourceCPU, "10m").Obj()).
			SimpleReserveQuota("cq", "default", now).
			Obj()
		if !c.AddOrUpdateWorkload(log, wl) {
			t.Fatalf("AddOrUpdateWorkload(%s) failed", name)
		}
		keys[i] = workload.Key(wl)
	}
	return c, keys
}

// TestDeleteWorkloads_RemovesAllAndIsIdempotent verifies that DeleteWorkloads
// removes every input key from the cache and returns nil errors. A second
// call with the same keys returns nil errors too (idempotent for absent
// workloads, matching DeleteWorkload's contract).
func TestDeleteWorkloads_RemovesAllAndIsIdempotent(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	c, keys := newCacheWithCQ(t, 5)

	errs := c.DeleteWorkloads(log, keys)
	if len(errs) != len(keys) {
		t.Fatalf("errs len: got %d, want %d", len(errs), len(keys))
	}
	for i, err := range errs {
		if err != nil {
			t.Errorf("errs[%d]: got %v, want nil", i, err)
		}
	}
	for _, k := range keys {
		if _, assigned := c.workloadAssignedQueues[k]; assigned {
			t.Errorf("workload %s still assigned after DeleteWorkloads", k)
		}
	}

	// Second call must not panic and must return all-nil errors.
	errs2 := c.DeleteWorkloads(log, keys)
	for i, err := range errs2 {
		if err != nil {
			t.Errorf("errs2[%d]: got %v, want nil (idempotent re-delete)", i, err)
		}
	}
}

// TestDeleteWorkloads_PartialErrors verifies that an unknown key in the
// middle of the batch returns nil (idempotent), and that a ClusterQueue
// removal mid-batch is signalled per item without blocking other items.
func TestDeleteWorkloads_PartialErrors(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	c, keys := newCacheWithCQ(t, 3)

	// Mix: real key, unknown key, real key.
	mixed := []workload.Reference{
		keys[0],
		workload.Reference("/never-existed"),
		keys[1],
	}

	errs := c.DeleteWorkloads(log, mixed)
	if len(errs) != 3 {
		t.Fatalf("errs len: got %d, want 3", len(errs))
	}
	// All three should be nil — never-existed is just "not assigned" → nil.
	for i, err := range errs {
		if err != nil {
			t.Errorf("errs[%d]: got %v, want nil", i, err)
		}
	}
	// keys[0] and keys[1] removed, keys[2] still present.
	for i, k := range []workload.Reference{keys[0], keys[1]} {
		if _, assigned := c.workloadAssignedQueues[k]; assigned {
			t.Errorf("deleted[%d] %s still assigned", i, k)
		}
	}
	if _, assigned := c.workloadAssignedQueues[keys[2]]; !assigned {
		t.Errorf("untouched key %s missing", keys[2])
	}
}

// TestDeleteWorkloads_EmptyBatch handles the no-op case cleanly.
func TestDeleteWorkloads_EmptyBatch(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	c, _ := newCacheWithCQ(t, 1)
	errs := c.DeleteWorkloads(log, nil)
	if errs == nil {
		t.Errorf("expected non-nil empty slice for nil input, got nil")
	}
	if len(errs) != 0 {
		t.Errorf("expected len 0, got %d", len(errs))
	}
}

// TestDeleteWorkloads_ConcurrentReadsDontDeadlock spawns concurrent reads
// (Snapshot) and a batch delete and asserts both make progress without
// deadlock. It does not measure timing — that's the benchmark's job.
func TestDeleteWorkloads_ConcurrentReadsDontDeadlock(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	c, keys := newCacheWithCQ(t, 500)

	var wg sync.WaitGroup
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var snapshots atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-readCtx.Done():
					return
				default:
				}
				if snap, err := c.Snapshot(ctrl.LoggerInto(readCtx, log)); err == nil && snap != nil {
					snapshots.Add(1)
				}
			}
		}()
	}

	// One batch delete of all 500 keys.
	errs := c.DeleteWorkloads(log, keys)
	for i, err := range errs {
		if err != nil {
			t.Errorf("errs[%d]: %v", i, err)
		}
	}

	cancel()
	wg.Wait()

	if snapshots.Load() == 0 {
		t.Errorf("readers never observed a snapshot — deadlock suspected")
	}
}

// BenchmarkDeleteWorkloads_RawThroughput is intentionally NOT a contention
// benchmark — micro-benchmarks of RWMutex starvation are notoriously
// difficult to reproduce because Go's scheduler interleaves writers and
// readers efficiently when Snapshot() is fast (a few hundred µs here) and
// the writer's hold time is short. The production stall on cq-gke-01 only
// surfaced because Snapshot() in real workloads iterates many ClusterQueues
// and cohorts (~1ms+ per snapshot) AND sustained write pressure spanned
// many seconds. That combination is hard to reproduce in a unit benchmark.
//
// What this benchmark does measure: that DeleteWorkloads is no worse than
// repeated DeleteWorkload calls for raw write throughput. The production
// benefit (reduced reader-starvation probability) is observable in the
// kueue_admission_attempt_duration_seconds histogram once deployed.
func BenchmarkDeleteWorkloads_RawThroughput(b *testing.B) {
	const n = 1000
	for _, shape := range []string{"Single", "Batch"} {
		b.Run(shape, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				c, keys := newCacheWithCQ(b, n)
				_, log := utiltesting.ContextWithLog(b)
				b.StartTimer()
				if shape == "Single" {
					for _, k := range keys {
						_ = c.DeleteWorkload(log, k)
					}
				} else {
					_ = c.DeleteWorkloads(log, keys)
				}
			}
		})
	}
}

// Silence "imported and not used" if kueue import isn't referenced elsewhere
// (it is via utiltestingapi but be explicit).
var _ = kueue.ClusterQueue{}
