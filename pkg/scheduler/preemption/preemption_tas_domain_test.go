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

package preemption

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utilslices "sigs.k8s.io/kueue/pkg/util/slices"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	"sigs.k8s.io/kueue/pkg/workload"
)

const tasGPU = corev1.ResourceName("nvidia.com/gpu")

// getTargetsForTAS builds the cluster from admitted (each a 1-GPU workload on a
// named node), then returns the preemption targets GetTargets picks for incoming
// on env-gilead, keyed as name:reason.
func getTargetsForTAS(t *testing.T, nodes []corev1.Node, admitted []kueue.Workload,
	clusterQueues []*kueue.ClusterQueue, incoming *kueue.Workload, count int32, fs *config.FairSharing) sets.Set[string] {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	topology := utiltestingapi.MakeDefaultOneLevelTopology("tas-single-level")
	flavor := utiltestingapi.MakeResourceFlavor("tas-default").
		NodeLabel("tas-node", "true").TopologyName("tas-single-level").Obj()
	for i := range admitted {
		admitted[i].UID = types.UID(admitted[i].Name)
	}

	ctx, log := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewClientBuilder().WithLists(&kueue.WorkloadList{Items: admitted}).Build()
	cqCache := schdcache.New(cl)
	for i := range nodes {
		cqCache.TASCache().SyncNode(&nodes[i])
	}
	cqCache.AddOrUpdateTopology(log, topology)
	cqCache.AddOrUpdateResourceFlavor(log, flavor)
	if err := cqCache.AddOrUpdateCohort(utiltestingapi.MakeCohort("shared").Obj()); err != nil {
		t.Fatalf("Couldn't add Cohort: %v", err)
	}
	for _, cq := range clusterQueues {
		if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Couldn't add ClusterQueue: %v", err)
		}
	}

	recorder := record.NewBroadcaster().NewRecorder(runtime.NewScheme(), corev1.EventSource{Component: constants.AdmissionName})
	preemptor := New(cl, workload.Ordering{}, recorder, fs, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	wlInfo := workload.NewInfo(incoming)
	wlInfo.ClusterQueue = "env-gilead"
	assignment := flavorassigner.Assignment{PodSets: []flavorassigner.PodSetAssignment{{
		Name: incoming.Spec.PodSets[0].Name, Count: count,
		Flavors: flavorassigner.ResourceAssignment{tasGPU: &flavorassigner.FlavorAssignment{Name: "tas-default", Mode: flavorassigner.Preempt}},
	}}}
	targets := preemptor.GetTargets(log, *wlInfo, assignment, snapshot)
	return sets.New(utilslices.Map(targets, func(t **Target) string {
		return targetKeyReason(workload.Key((*t).WorkloadInfo.Obj), (*t).Reason)
	})...)
}

func tasNode(name string) corev1.Node {
	return *testingnode.MakeNode(name).
		Label("tas-node", "true").Label(corev1.LabelHostname, name).
		StatusAllocatable(corev1.ResourceList{tasGPU: resource.MustParse("8"), corev1.ResourcePods: resource.MustParse("20")}).
		Ready().Obj()
}

func tasGPUWorkload(name, cq, node string, priority int32) kueue.Workload {
	topology := utiltestingapi.MakeDefaultOneLevelTopology("tas-single-level")
	now := time.Now().Truncate(time.Second)
	return *utiltestingapi.MakeWorkload(name, "default").
		Priority(priority).
		PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 1).
			Request(tasGPU, "1").PreferredTopologyRequest(corev1.LabelHostname).Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(cq)).
			PodSets(utiltestingapi.MakePodSetAssignment(kueue.DefaultPodSetName).
				Assignment(tasGPU, "tas-default", "1").Count(1).
				TopologyAssignment(utiltestingapi.MakeTopologyAssignment(utiltas.Levels(topology)).
					Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{node}, 1).Obj()).Obj()).
				Obj()).Obj(), now).
		Obj()
}

func tasCQ(name string, nominalGPU int, weight string) *kueue.ClusterQueue {
	b := utiltestingapi.MakeClusterQueue(name).Cohort("shared").
		Preemption(kueue.ClusterQueuePreemption{
			ReclaimWithinCohort: kueue.PreemptionPolicyLowerPriority,
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
		}).
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("tas-default").Resource(tasGPU, fmt.Sprintf("%d", nominalGPU)).Obj())
	if weight != "" {
		b = b.FairWeight(resource.MustParse(weight))
	}
	return b.Obj()
}

func gpuPodSet(name string, count int, perPod string, required bool) kueue.PodSet {
	ps := utiltestingapi.MakePodSet(kueue.PodSetReference(name), count).Request(tasGPU, perPod)
	if required {
		ps = ps.RequiredTopologyRequest(corev1.LabelHostname)
	} else {
		ps = ps.PreferredTopologyRequest(corev1.LabelHostname)
	}
	return *ps.Obj()
}

func nodeVictims(tenant, node string, n int) sets.Set[string] {
	s := sets.New[string]()
	for i := range n {
		s.Insert(targetKeyReason(workload.NewReference("default", fmt.Sprintf("%s-%s-%d", tenant, node, i)), kueue.InCohortReclamationReason))
	}
	return s
}

// The shared six-node layout: only node-c and node-e are held entirely by the
// borrowing tenant, so those are the only fully clearable hosts. playground
// borrows 16 over nominal 24; incyte sits within nominal 16 and is unreclaimable.
var sharedLayout = []struct {
	node               string
	playground, incyte int
}{
	{"node-a", 6, 2}, {"node-b", 7, 1}, {"node-c", 8, 0},
	{"node-d", 4, 4}, {"node-e", 8, 0}, {"node-f", 7, 1},
}

func sharedCluster() ([]corev1.Node, []kueue.Workload) {
	var nodes []corev1.Node
	var admitted []kueue.Workload
	for _, l := range sharedLayout {
		nodes = append(nodes, tasNode(l.node))
		for i := range l.playground {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-%s-%d", l.node, i), "env-playground", l.node, 29))
		}
		for i := range l.incyte {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("incyte-%s-%d", l.node, i), "env-incyte", l.node, 50))
		}
	}
	return nodes, admitted
}

func sharedCQs(gileadNominal int) []*kueue.ClusterQueue {
	return []*kueue.ClusterQueue{
		tasCQ("env-playground", 24, ""), tasCQ("env-incyte", 16, ""), tasCQ("env-gilead", gileadNominal, ""),
	}
}

func trainer(ps kueue.PodSet) *kueue.Workload {
	return utiltestingapi.MakeWorkload("trainer", "default").Priority(99).PodSets(ps).Obj()
}

// TestTASDomainRank_WholeNodeSinglePod is the reported case: a single 8-GPU pod
// needing a whole host.
func TestTASDomainRank_WholeNodeSinglePod(t *testing.T) {
	nodes, admitted := sharedCluster()
	got := getTargetsForTAS(t, nodes, admitted, sharedCQs(8), trainer(gpuPodSet("trainers", 1, "8", false)), 1, &config.FairSharing{})
	if diff := cmp.Diff(nodeVictims("playground", "node-c", 8), got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_WholeNodeMultiPod expresses the whole node as eight 1-GPU
// pods pinned to one host by a Required constraint.
func TestTASDomainRank_WholeNodeMultiPod(t *testing.T) {
	nodes, admitted := sharedCluster()
	got := getTargetsForTAS(t, nodes, admitted, sharedCQs(8), trainer(gpuPodSet("trainers", 8, "1", true)), 8, &config.FairSharing{})
	if diff := cmp.Diff(nodeVictims("playground", "node-c", 8), got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_TwoWholeNodes is the 16-GPU case: two 8-GPU pods, each of
// which needs a fully free host.
func TestTASDomainRank_TwoWholeNodes(t *testing.T) {
	nodes, admitted := sharedCluster()
	got := getTargetsForTAS(t, nodes, admitted, sharedCQs(16), trainer(gpuPodSet("trainers", 2, "8", false)), 2, &config.FairSharing{})
	want := nodeVictims("playground", "node-c", 8).Union(nodeVictims("playground", "node-e", 8))
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_TwoNodesAcrossCQs is the 16-GPU case where the two clearable
// hosts belong to two different borrowing tenants under fair weights.
func TestTASDomainRank_TwoNodesAcrossCQs(t *testing.T) {
	layout := []struct {
		node                       string
		playground, incyte, locked int
	}{
		{"node-1", 8, 0, 0}, {"node-2", 0, 8, 0},
		{"node-3", 7, 0, 1}, {"node-4", 7, 0, 1}, {"node-5", 7, 0, 1},
		{"node-6", 0, 7, 1}, {"node-7", 0, 7, 1}, {"node-8", 0, 7, 1},
	}
	var nodes []corev1.Node
	var admitted []kueue.Workload
	for _, l := range layout {
		nodes = append(nodes, tasNode(l.node))
		for i := range l.playground {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-%s-%d", l.node, i), "env-playground", l.node, 1029))
		}
		for i := range l.incyte {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("incyte-%s-%d", l.node, i), "env-incyte", l.node, 1050))
		}
		for i := range l.locked {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("locked-%s-%d", l.node, i), "env-locked", l.node, 1010))
		}
	}
	cqs := []*kueue.ClusterQueue{
		tasCQ("env-playground", 16, "4"), tasCQ("env-incyte", 16, "1"),
		tasCQ("env-locked", 16, "1"), tasCQ("env-gilead", 16, "1"),
	}
	fs := &config.FairSharing{PreemptionStrategies: []config.PreemptionStrategy{config.LessThanOrEqualToFinalShare, config.LessThanInitialShare}}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainerP(gpuPodSet("trainers", 2, "8", false), 1099), 2, fs)
	want := nodeVictims("playground", "node-1", 8).Union(nodeVictims("incyte", "node-2", 8))
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_ManyNodes is the multi-pod whole-node case on a cluster
// larger than eight partially-occupied hosts: only node-clear is fully held by
// the borrowing tenant.
func TestTASDomainRank_ManyNodes(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-clear")}
	admitted := []kueue.Workload{}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-node-clear-%d", i), "env-playground", "node-clear", 29))
	}
	for d := range 10 {
		n := fmt.Sprintf("node-decoy-%d", d)
		nodes = append(nodes, tasNode(n))
		for i := range 2 {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-%s-%d", n, i), "env-playground", n, 29))
		}
		for i := range 6 {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("locked-%s-%d", n, i), "env-locked", n, 10))
		}
	}
	cqs := []*kueue.ClusterQueue{tasCQ("env-playground", 20, ""), tasCQ("env-locked", 60, ""), tasCQ("env-gilead", 8, "")}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 8, "1", true)), 8, &config.FairSharing{})
	if diff := cmp.Diff(nodeVictims("playground", "node-clear", 8), got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

func trainerP(ps kueue.PodSet, prio int32) *kueue.Workload {
	return utiltestingapi.MakeWorkload("trainer", "default").Priority(prio).PodSets(ps).Obj()
}

// TestTASDomainRank_MixedTenantDomain puts a below-budget tenant on the same
// host as an above-budget one. node-x holds 4 borrowing env-playground pods plus
// 4 env-incyte pods that sit within nominal and so cannot be reclaimed; node-y
// holds 8 borrowing env-playground pods. Only node-y can be fully freed. The
// ranking must score node-x below node-y (its incyte pods are not candidates, so
// it can free at most 4 of the needed 8) and must not select the incyte pods.
func TestTASDomainRank_MixedTenantDomain(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-x"), tasNode("node-y")}
	var admitted []kueue.Workload
	for i := range 4 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-node-x-%d", i), "env-playground", "node-x", 29))
	}
	for i := range 4 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("incyte-node-x-%d", i), "env-incyte", "node-x", 50))
	}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-node-y-%d", i), "env-playground", "node-y", 29))
	}
	// playground borrows 8 over nominal 4; incyte uses 4 within nominal 16.
	cqs := []*kueue.ClusterQueue{tasCQ("env-playground", 4, ""), tasCQ("env-incyte", 16, ""), tasCQ("env-gilead", 8, "")}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 1, "8", false)), 1, &config.FairSharing{})
	if diff := cmp.Diff(nodeVictims("playground", "node-y", 8), got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_FairnessAcrossEqualDomains checks that when two hosts are
// each fully clearable but owned by different over-budget tenants, the fix does
// not let topology ordering override fair sharing's tenant choice. node-a holds
// env-light (fair weight 8, barely over its share); node-z holds env-heavy (fair
// weight 1, far over). Both are pure borrowers (nominal 0) so all eight pods on
// each are reclaimable. The domain tiebreak sorts node-a first by name, so if the
// ranking drove tenant selection it would evict env-light; fair sharing must
// still reclaim from env-heavy, the tenant furthest over its share.
//
// node-f is held by env-filler within its nominal, so it is neither reclaimable
// nor free -- it forces preemption without being a candidate. gilead lends 16
// (nominal 16, unused), exactly what env-light and env-heavy borrow, so cohort
// nominal equals physical and clearing one host frees enough quota for the
// trainer's eight GPUs -- isolating the tenant choice from quota effects.
func TestTASDomainRank_FairnessAcrossEqualDomains(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-a"), tasNode("node-f"), tasNode("node-z")}
	var admitted []kueue.Workload
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("light-node-a-%d", i), "env-light", "node-a", 29))
	}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("filler-node-f-%d", i), "env-filler", "node-f", 29))
	}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("heavy-node-z-%d", i), "env-heavy", "node-z", 29))
	}
	cqs := []*kueue.ClusterQueue{
		tasCQ("env-light", 0, "8"), tasCQ("env-heavy", 0, "1"),
		tasCQ("env-filler", 8, "1"), tasCQ("env-gilead", 16, "1"),
	}
	fs := &config.FairSharing{PreemptionStrategies: []config.PreemptionStrategy{config.LessThanOrEqualToFinalShare, config.LessThanInitialShare}}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 1, "8", false)), 1, fs)
	if diff := cmp.Diff(nodeVictims("heavy", "node-z", 8), got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_MultiTenantSingleDomain is the common production shape: the
// only freeable host is co-occupied by two different borrowing tenants (think
// gilead + incyte opinference pods), and the incoming rayjob needs the whole
// host. node-b is held by env-filler within nominal, so it forces preemption
// without being freeable. The incoming fits within its own nominal, so reclaim
// is unconditional and must evict every occupant of node-mix -- from both
// tenants -- to free it.
func TestTASDomainRank_MultiTenantSingleDomain(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-b"), tasNode("node-mix")}
	var admitted []kueue.Workload
	for i := range 4 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("teamA-node-mix-%d", i), "env-teamA", "node-mix", 29))
	}
	for i := range 4 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("teamB-node-mix-%d", i), "env-teamB", "node-mix", 50))
	}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("filler-node-b-%d", i), "env-filler", "node-b", 29))
	}
	// teamA and teamB are pure borrowers (nominal 0); filler sits within nominal.
	cqs := []*kueue.ClusterQueue{
		tasCQ("env-teamA", 0, ""), tasCQ("env-teamB", 0, ""),
		tasCQ("env-filler", 8, ""), tasCQ("env-gilead", 16, ""),
	}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 1, "8", false)), 1, &config.FairSharing{})
	want := nodeVictims("teamA", "node-mix", 4).Union(nodeVictims("teamB", "node-mix", 4))
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_BorrowingPreemptorFair is case #2 where fair sharing permits
// the preemption: the incoming rayjob (env-gilead here, high fair weight, pure
// borrower) is entitled, while the two co-located tenants env-opinfA/env-opinfB
// are far over their share (weight 1). The rayjob borrows over its own nominal,
// so reclaim is strategy-gated, and the only freeable host (node-t) is shared by
// both tenants. node-L is an env-lender within nominal that supplies the borrow.
// Freeing node-t requires assembling a two-tenant combination under the strategy.
func TestTASDomainRank_BorrowingPreemptorFair(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-L"), tasNode("node-t")}
	var admitted []kueue.Workload
	for i := range 4 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("opinfA-node-t-%d", i), "env-opinfA", "node-t", 29))
	}
	for i := range 4 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("opinfB-node-t-%d", i), "env-opinfB", "node-t", 29))
	}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("lender-node-L-%d", i), "env-lender", "node-L", 29))
	}
	// cohort nominal = 0+0+0+16 = 16 = physical; opinfA/opinfB borrow (weight 1,
	// high share), env-gilead is entitled (weight 100, low share), lender lends 8.
	cqs := []*kueue.ClusterQueue{
		tasCQ("env-opinfA", 0, "1"), tasCQ("env-opinfB", 0, "1"),
		tasCQ("env-lender", 16, "1"), tasCQ("env-gilead", 0, "100"),
	}
	fs := &config.FairSharing{PreemptionStrategies: []config.PreemptionStrategy{config.LessThanOrEqualToFinalShare, config.LessThanInitialShare}}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 1, "8", false)), 1, fs)
	// Strategy-gated reclaim reports InCohortFairSharing rather than reclamation.
	want := sets.New[string]()
	for i := range 4 {
		want.Insert(targetKeyReason(workload.NewReference("default", fmt.Sprintf("opinfA-node-t-%d", i)), kueue.InCohortFairSharingReason))
		want.Insert(targetKeyReason(workload.NewReference("default", fmt.Sprintf("opinfB-node-t-%d", i)), kueue.InCohortFairSharingReason))
	}
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_ThreeTenantsWithOwn adds the preemptor's own tenant's
// opinference pods to the shared host. node-t holds 3 env-gilead opinference
// pods (same ClusterQueue as the incoming rayjob, so within-queue preemption),
// 3 env-opinfA and 2 env-opinfB (cross-cohort reclaim). Freeing the host mixes
// within-queue preemption with cross-cohort reclaim across two other tenants;
// all eight occupants must be selected regardless of the eviction reason.
func TestTASDomainRank_ThreeTenantsWithOwn(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-L"), tasNode("node-t")}
	var admitted []kueue.Workload
	for i := range 3 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("gileadopinf-node-t-%d", i), "env-gilead", "node-t", 10))
	}
	for i := range 3 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("opinfA-node-t-%d", i), "env-opinfA", "node-t", 29))
	}
	for i := range 2 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("opinfB-node-t-%d", i), "env-opinfB", "node-t", 29))
	}
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("lender-node-L-%d", i), "env-lender", "node-L", 29))
	}
	cqs := []*kueue.ClusterQueue{
		tasCQ("env-opinfA", 0, "1"), tasCQ("env-opinfB", 0, "1"),
		tasCQ("env-lender", 8, "1"), tasCQ("env-gilead", 8, "100"),
	}
	fs := &config.FairSharing{PreemptionStrategies: []config.PreemptionStrategy{config.LessThanOrEqualToFinalShare, config.LessThanInitialShare}}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 1, "8", false)), 1, fs)
	gotNames := sets.New[string]()
	for k := range got {
		gotNames.Insert(k[:strings.LastIndex(k, ":")])
	}
	want := sets.New[string]()
	for i := range 3 {
		want.Insert(fmt.Sprintf("default/gileadopinf-node-t-%d", i))
		want.Insert(fmt.Sprintf("default/opinfA-node-t-%d", i))
	}
	for i := range 2 {
		want.Insert(fmt.Sprintf("default/opinfB-node-t-%d", i))
	}
	if diff := cmp.Diff(want, gotNames, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("evicted set (-want,+got):\n%s", diff)
	}
}

// TestTASDomainRank_PreferFewerEvictions checks the minimal-eviction preference:
// two hosts can each be made to fit the 8-GPU workload, but node-partial already
// has 2 GPUs free (6 preemptable) so it needs 6 evictions, while node-packed is
// full (8 preemptable) and needs 8. Preemption must fall on node-partial.
func TestTASDomainRank_PreferFewerEvictions(t *testing.T) {
	nodes := []corev1.Node{tasNode("node-packed"), tasNode("node-partial")}
	var admitted []kueue.Workload
	for i := range 8 {
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("teamX-packed-%d", i), "env-teamX", "node-packed", 29))
	}
	for i := range 6 { // 6 pods on an 8-GPU node -> 2 GPUs free
		admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("teamX-partial-%d", i), "env-teamX", "node-partial", 29))
	}
	// teamX borrows 8 over nominal 6; gilead lends and stays within its nominal 10.
	cqs := []*kueue.ClusterQueue{tasCQ("env-teamX", 6, ""), tasCQ("env-gilead", 10, "")}
	got := getTargetsForTAS(t, nodes, admitted, cqs, trainer(gpuPodSet("trainers", 1, "8", false)), 1, &config.FairSharing{})
	want := sets.New[string]()
	for i := range 6 {
		want.Insert(targetKeyReason(workload.NewReference("default", fmt.Sprintf("teamX-partial-%d", i)), kueue.InCohortReclamationReason))
	}
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}

// fsCQ mirrors the other patch's fair-weighted CQ (adds BorrowWithinCohort).
func fsCQ(name string, nominalGPU int, weight string) *kueue.ClusterQueue {
	return utiltestingapi.MakeClusterQueue(name).Cohort("shared").
		FairWeight(resource.MustParse(weight)).
		Preemption(kueue.ClusterQueuePreemption{
			ReclaimWithinCohort: kueue.PreemptionPolicyLowerPriority,
			BorrowWithinCohort:  &kueue.BorrowWithinCohort{Policy: kueue.BorrowWithinCohortPolicyLowerPriority},
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
		}).
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("tas-default").Resource(tasGPU, fmt.Sprintf("%d", nominalGPU)).Obj()).
		Obj()
}

// TestTASDomainRank_AcrossClusterQueues ports the other patch's mixed 3-tenant
// scenario: every node carries at least one env-incyte pod, and incyte borrows
// only 1 over its nominal, so node-b -- where incyte holds just 1 GPU -- is the
// only host that can be cleared. Clearing it spans env-playground (reclaim),
// env-incyte (reclaim) and env-gilead (within-queue, the preemptor's own).
func TestTASDomainRank_AcrossClusterQueues(t *testing.T) {
	layout := []struct {
		node                       string
		incyte, playground, gilead int
	}{
		{"node-a", 4, 4, 0}, {"node-b", 1, 6, 1}, {"node-c", 3, 5, 0},
		{"node-d", 2, 6, 0}, {"node-e", 2, 5, 1}, {"node-f", 5, 3, 0},
	}
	var nodes []corev1.Node
	var admitted []kueue.Workload
	for _, l := range layout {
		nodes = append(nodes, tasNode(l.node))
		for i := range l.incyte {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("incyte-%s-%d", l.node, i), "env-incyte", l.node, 1050))
		}
		for i := range l.playground {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("playground-%s-%d", l.node, i), "env-playground", l.node, 1029))
		}
		for i := range l.gilead {
			admitted = append(admitted, tasGPUWorkload(fmt.Sprintf("gilead-%s-%d", l.node, i), "env-gilead", l.node, 1010))
		}
	}
	cqs := []*kueue.ClusterQueue{fsCQ("env-playground", 16, "4"), fsCQ("env-incyte", 16, "1"), fsCQ("env-gilead", 16, "1")}
	fs := &config.FairSharing{PreemptionStrategies: []config.PreemptionStrategy{config.LessThanOrEqualToFinalShare, config.LessThanInitialShare}}
	incoming := utiltestingapi.MakeWorkload("trainer", "default").Priority(1099).
		PodSets(gpuPodSet("trainers", 1, "8", false)).Obj()
	got := getTargetsForTAS(t, nodes, admitted, cqs, incoming, 1, fs)
	want := sets.New[string]()
	for i := range 6 {
		want.Insert(targetKeyReason(workload.NewReference("default", fmt.Sprintf("playground-node-b-%d", i)), kueue.InCohortReclamationReason))
	}
	want.Insert(targetKeyReason(workload.NewReference("default", "incyte-node-b-0"), kueue.InCohortReclamationReason))
	want.Insert(targetKeyReason(workload.NewReference("default", "gilead-node-b-0"), kueue.InClusterQueueReason))
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("(-want,+got):\n%s", diff)
	}
}
