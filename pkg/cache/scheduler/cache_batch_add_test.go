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
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// TestAddOrUpdateWorkloads_BatchAddsAllUnderOneLock — batch add of 50
// workloads, all should commit (true) and end up in workloadAssignedQueues.
func TestAddOrUpdateWorkloads_BatchAddsAllUnderOneLock(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewFakeClient()
	c := New(cl)
	c.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	cq := utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "1000").Obj()).
		NamespaceSelector(nil).Obj()
	if err := c.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}
	now := time.Now()
	ws := make([]*kueue.Workload, 50)
	for i := range ws {
		ws[i] = utiltestingapi.MakeWorkload(fmt.Sprintf("wl-%d", i), "").
			PodSets(*utiltestingapi.MakePodSet("main", 1).Request(corev1.ResourceCPU, "10m").Obj()).
			SimpleReserveQuota("cq", "default", now).Obj()
	}
	results := c.AddOrUpdateWorkloads(log, ws)
	for i, ok := range results {
		if !ok {
			t.Errorf("results[%d]: got false, want true", i)
		}
	}
	for _, w := range ws {
		if _, assigned := c.workloadAssignedQueues[workload.Key(w)]; !assigned {
			t.Errorf("workload %s not assigned", workload.Key(w))
		}
	}
}

// TestAddOrUpdateWorkloads_FailsOnMissingCQ — per-item failure isolation.
func TestAddOrUpdateWorkloads_FailsOnMissingCQ(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewFakeClient()
	c := New(cl)
	c.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := c.AddClusterQueue(ctx, utiltestingapi.MakeClusterQueue("cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "1000").Obj()).
		NamespaceSelector(nil).Obj()); err != nil {
		t.Fatalf("AddClusterQueue: %v", err)
	}
	now := time.Now()
	good := utiltestingapi.MakeWorkload("good", "").
		PodSets(*utiltestingapi.MakePodSet("main", 1).Request(corev1.ResourceCPU, "10m").Obj()).
		SimpleReserveQuota("cq", "default", now).Obj()
	bad := utiltestingapi.MakeWorkload("bad", "").
		PodSets(*utiltestingapi.MakePodSet("main", 1).Request(corev1.ResourceCPU, "10m").Obj()).
		SimpleReserveQuota("does-not-exist", "default", now).Obj()
	results := c.AddOrUpdateWorkloads(log, []*kueue.Workload{good, bad})
	if !results[0] {
		t.Errorf("good: got false, want true")
	}
	if results[1] {
		t.Errorf("bad (unknown CQ): got true, want false")
	}
}
