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

package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// setupReconcilerWithAdmittedWorkloads builds a WorkloadReconciler whose
// scheduler cache holds n admitted workloads, all under one ClusterQueue.
// Returns the reconciler and the workload keys so the test can drive the
// delete-flusher and assert removal.
func setupReconcilerWithAdmittedWorkloads(t *testing.T, n int) (*WorkloadReconciler, *schdcache.Cache, []workload.Reference) {
	t.Helper()
	ctx, log := utiltesting.ContextWithLog(t)

	cl := utiltesting.NewClientBuilder().Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)

	rf := utiltestingapi.MakeResourceFlavor("default").Obj()
	cqCache.AddOrUpdateResourceFlavor(log, rf)

	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceCPU, "1000").Obj(),
		).
		NamespaceSelector(nil).
		Obj()
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
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
		if !cqCache.AddOrUpdateWorkload(log, wl) {
			t.Fatalf("AddOrUpdateWorkload(%s) failed", name)
		}
		keys[i] = workload.Key(wl)
	}

	r := NewWorkloadReconciler(cl, qManager, cqCache, &utiltesting.EventRecorder{})
	return r, cqCache, keys
}

func cacheHas(c *schdcache.Cache, k workload.Reference) bool {
	// Cache doesn't export a "has key" helper; a snapshot is overkill but
	// reliable. Tests are not hot-path.
	snap, err := c.Snapshot(context.Background())
	if err != nil {
		return false
	}
	cqSnap := snap.ClusterQueue("cq")
	if cqSnap == nil {
		return false
	}
	_, ok := cqSnap.Workloads[k]
	return ok
}

// TestFlusher_FlushesOnTick verifies enqueued deletes land in the cache
// after one flush interval has passed.
func TestFlusher_FlushesOnTick(t *testing.T) {
	r, cqCache, keys := setupReconcilerWithAdmittedWorkloads(t, 5)
	_, log := utiltesting.ContextWithLog(t)

	// Tighten flush interval so the test runs fast.
	r.deleteFlushDur = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.runDeleteFlusher(ctx)

	for _, k := range keys {
		r.enqueueCacheDelete(log, k)
	}

	// Wait long enough for at least 2 ticks.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		allGone := true
		for _, k := range keys {
			if cacheHas(cqCache, k) {
				allGone = false
				break
			}
		}
		if allGone {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("flusher never drained all 5 keys within deadline")
}

// TestFlusher_FlushesOnBatchMax verifies that hitting the batch-max
// threshold triggers an immediate flush without waiting for the tick.
func TestFlusher_FlushesOnBatchMax(t *testing.T) {
	r, cqCache, keys := setupReconcilerWithAdmittedWorkloads(t, 100)
	_, log := utiltesting.ContextWithLog(t)

	// Set the batch max equal to len(keys) and the tick interval to a value
	// far longer than the test will run — only the batch-max path can fire.
	r.deleteFlushDur = 10 * time.Second
	r.deleteBatchMax = 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.runDeleteFlusher(ctx)

	for _, k := range keys {
		r.enqueueCacheDelete(log, k)
	}

	// Should drain within a few ms of the 100th enqueue, well before the
	// 10s tick. Allow 1s slack for goroutine scheduling.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		allGone := true
		for _, k := range keys {
			if cacheHas(cqCache, k) {
				allGone = false
				break
			}
		}
		if allGone {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("batch-max flush did not fire within 1s")
}

// TestFlusher_BufferFullFallback fills the buffer with no flusher running
// and confirms that the deleteBatchMax+1th enqueue falls back to the sync
// path (no drop, no panic, cache reflects the delete immediately).
func TestFlusher_BufferFullFallback(t *testing.T) {
	r, cqCache, keys := setupReconcilerWithAdmittedWorkloads(t, defaultDeleteBufferSize+1)
	_, log := utiltesting.ContextWithLog(t)

	// Do NOT start the flusher. Fill the buffer up to capacity.
	for i := 0; i < defaultDeleteBufferSize; i++ {
		r.enqueueCacheDelete(log, keys[i])
	}

	// The next enqueue must take the sync-fallback path and delete
	// immediately, regardless of the flusher being stopped.
	overflowKey := keys[defaultDeleteBufferSize]
	r.enqueueCacheDelete(log, overflowKey)
	if cacheHas(cqCache, overflowKey) {
		t.Fatalf("overflow key %s still in cache — sync fallback didn't fire", overflowKey)
	}

	// The buffered ones must NOT have been processed since no flusher ran.
	for i := 0; i < 3; i++ {
		if !cacheHas(cqCache, keys[i]) {
			t.Errorf("buffered key %s missing without flusher running", keys[i])
		}
	}
}

// TestFlusher_ContextCancelFlushesRemaining verifies that a final flush
// runs on context cancel so any in-flight enqueues are not lost.
func TestFlusher_ContextCancelFlushesRemaining(t *testing.T) {
	r, cqCache, keys := setupReconcilerWithAdmittedWorkloads(t, 7)
	_, log := utiltesting.ContextWithLog(t)

	// Use a long tick so the test wins/loses on the cancel-path flush.
	r.deleteFlushDur = 10 * time.Second
	r.deleteBatchMax = 1000

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.runDeleteFlusher(ctx)
		close(done)
	}()

	for _, k := range keys {
		r.enqueueCacheDelete(log, k)
	}

	// Give the flusher's select a chance to pick up all enqueues first.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("flusher didn't exit on context cancel")
	}

	for _, k := range keys {
		if cacheHas(cqCache, k) {
			t.Errorf("cancel-path flush left %s in cache", k)
		}
	}
}
