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

package tas

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	"sigs.k8s.io/kueue/test/util"
)

const (
	tasGPU = corev1.ResourceName("nvidia.com/gpu")

	// The priority band of the reported cluster: trainers above the scattered
	// single-GPU jobs they have to displace.
	donorPriority   = 29
	trainerPriority = 99
)

// A whole-node trainer contends with single-GPU jobs scattered over three hosts.
// Only node-b is held entirely by the borrowing donor queue, so node-b is the only
// host that reclaim can clear; the other two also hold jobs of a queue within its
// nominal quota, which are not preemptable.
//
// Fair Sharing ranks preemption candidates without a topology term, so it walks the
// donor's jobs in admission order, spends the donor's whole borrowing budget on
// node-c and node-a, frees no host, and admits nothing. The trainer then stays
// pending forever even though clearing node-b was within budget.
var _ = ginkgo.Describe("Topology Aware Scheduling with Fair Sharing whole-node preemption",
	ginkgo.Ordered, ginkgo.ContinueOnFailure, ginkgo.Label("feature:tas", "feature:fairsharing"), func() {
		var (
			ns          *corev1.Namespace
			nodes       []corev1.Node
			topology    *kueue.Topology
			tasFlavor   *kueue.ResourceFlavor
			donorCQ     *kueue.ClusterQueue
			steadyCQ    *kueue.ClusterQueue
			trainerCQ   *kueue.ClusterQueue
			donorLQ     *kueue.LocalQueue
			steadyLQ    *kueue.LocalQueue
			trainerLQ   *kueue.LocalQueue
			donorWls    map[string][]*kueue.Workload
			steadyWls   []*kueue.Workload
			nodeNames   = []string{"node-a", "node-b", "node-c"}
			gpusPerNode = 4
		)

		// Per-host occupancy: node-b is donor-only, so it is the sole clearable host.
		layout := map[string]struct{ donor, steady int }{
			"node-a": {donor: 3, steady: 1},
			"node-b": {donor: 4, steady: 0},
			"node-c": {donor: 2, steady: 2},
		}

		gpuWorkload := func(name string, lq *kueue.LocalQueue, node string, priority int32) *kueue.Workload {
			return utiltestingapi.MakeWorkload(name, ns.Name).
				Queue(kueue.LocalQueueName(lq.Name)).
				Priority(priority).
				PodSets(*utiltestingapi.MakePodSet("worker", 1).
					NodeSelector(map[string]string{corev1.LabelHostname: node}).
					RequiredTopologyRequest(corev1.LabelHostname).
					Request(tasGPU, "1").
					Obj()).
				Obj()
		}

		ginkgo.BeforeAll(func() {
			fwk.StartManager(ctx, cfg, managerSetupWithConfig(&config.Configuration{
				FairSharing: &config.FairSharing{},
			}))
		})

		ginkgo.AfterAll(func() {
			fwk.StopManager(ctx)
		})

		ginkgo.BeforeEach(func() {
			ns = util.CreateNamespaceFromPrefixWithLog(ctx, k8sClient, "tas-fs-whole-node-")

			nodes = nil
			for _, name := range nodeNames {
				nodes = append(nodes, *testingnode.MakeNode(name).
					Label("node-group", "tas").
					Label(corev1.LabelHostname, name).
					StatusAllocatable(corev1.ResourceList{
						tasGPU:              resource.MustParse(fmt.Sprintf("%d", gpusPerNode)),
						corev1.ResourcePods: resource.MustParse("10"),
					}).
					Ready().
					Obj())
			}
			util.CreateNodesWithStatus(ctx, k8sClient, nodes)

			topology = utiltestingapi.MakeDefaultOneLevelTopology("default")
			util.MustCreate(ctx, k8sClient, topology)

			tasFlavor = utiltestingapi.MakeResourceFlavor("tas-default").
				NodeLabel("node-group", "tas").
				TopologyName("default").Obj()
			util.MustCreate(ctx, k8sClient, tasFlavor)

			// Nominal quota splits the 12 GPUs three ways. The donor holds 9 and so
			// borrows 5 over nominal; the steady queue holds 3 and stays within
			// nominal, which makes its jobs unreclaimable; the trainer's 4 are its own.
			newCQ := func(name string) *kueue.ClusterQueue {
				return utiltestingapi.MakeClusterQueue(name).
					Cohort("gpu-cohort").
					Preemption(kueue.ClusterQueuePreemption{
						WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
						ReclaimWithinCohort: kueue.PreemptionPolicyLowerPriority,
					}).
					ResourceGroup(*utiltestingapi.MakeFlavorQuotas(tasFlavor.Name).
						Resource(tasGPU, "4").Obj()).
					Obj()
			}
			donorCQ, steadyCQ, trainerCQ = newCQ("env-donor"), newCQ("env-steady"), newCQ("env-trainer")
			for _, cq := range []*kueue.ClusterQueue{donorCQ, steadyCQ, trainerCQ} {
				util.MustCreate(ctx, k8sClient, cq)
			}
			util.ExpectClusterQueuesToBeActive(ctx, k8sClient, donorCQ, steadyCQ, trainerCQ)

			donorLQ = utiltestingapi.MakeLocalQueue("donor-lq", ns.Name).ClusterQueue(donorCQ.Name).Obj()
			steadyLQ = utiltestingapi.MakeLocalQueue("steady-lq", ns.Name).ClusterQueue(steadyCQ.Name).Obj()
			trainerLQ = utiltestingapi.MakeLocalQueue("trainer-lq", ns.Name).ClusterQueue(trainerCQ.Name).Obj()
			for _, lq := range []*kueue.LocalQueue{donorLQ, steadyLQ, trainerLQ} {
				util.MustCreate(ctx, k8sClient, lq)
			}

			ginkgo.By("filling the cohort with single-GPU jobs", func() {
				steadyWls = nil
				for _, node := range nodeNames {
					for i := range layout[node].steady {
						wl := gpuWorkload(fmt.Sprintf("steady-%s-%d", node, i), steadyLQ, node, donorPriority)
						util.MustCreate(ctx, k8sClient, wl)
						steadyWls = append(steadyWls, wl)
					}
				}
				util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, steadyWls...)

				// node-b first, so its jobs are the oldest of the donor's. The
				// candidate ordering prefers the most recently admitted, which puts
				// the scattered node-c and node-a jobs ahead of the clearable host.
				donorWls = map[string][]*kueue.Workload{}
				for _, node := range []string{"node-b", "node-a", "node-c"} {
					for i := range layout[node].donor {
						wl := gpuWorkload(fmt.Sprintf("donor-%s-%d", node, i), donorLQ, node, donorPriority)
						util.MustCreate(ctx, k8sClient, wl)
						util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, wl)
						donorWls[node] = append(donorWls[node], wl)
					}
				}
			})
		})

		ginkgo.AfterEach(func() {
			gomega.Expect(util.DeleteWorkloadsInNamespace(ctx, k8sClient, ns)).Should(gomega.Succeed())
			for _, lq := range []*kueue.LocalQueue{donorLQ, steadyLQ, trainerLQ} {
				gomega.Expect(util.DeleteObject(ctx, k8sClient, lq)).Should(gomega.Succeed())
			}
			for _, cq := range []*kueue.ClusterQueue{donorCQ, steadyCQ, trainerCQ} {
				util.ExpectObjectToBeDeleted(ctx, k8sClient, cq, true)
			}
			util.ExpectObjectToBeDeleted(ctx, k8sClient, tasFlavor, true)
			util.ExpectObjectToBeDeleted(ctx, k8sClient, topology, true)
			for _, node := range nodes {
				util.ExpectObjectToBeDeleted(ctx, k8sClient, &node, true)
			}
			gomega.Expect(util.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
		})

		// expectNodeBCleared asserts the reclaim that admits the trainer: every donor
		// job on node-b is preempted, the trainer lands on node-b, and nothing outside
		// node-b is touched.
		expectNodeBCleared := func(trainer *kueue.Workload, wantPods int32) {
			ginkgo.By("verifying the donor jobs on node-b are preempted", func() {
				util.ExpectWorkloadsToBePreempted(ctx, k8sClient, donorWls["node-b"]...)
				util.FinishEvictionForWorkloads(ctx, k8sClient, donorWls["node-b"]...)
			})

			ginkgo.By("verifying the trainer is admitted on node-b", func() {
				util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, trainer)
				gomega.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trainer), trainer)).To(gomega.Succeed())
				ta := utiltas.InternalFrom(trainer.Status.Admission.PodSetAssignments[0].TopologyAssignment)
				gomega.Expect(ta.Domains).To(gomega.HaveLen(1))
				gomega.Expect(ta.Domains[0].Values).To(gomega.Equal([]string{"node-b"}))
				gomega.Expect(ta.Domains[0].Count).To(gomega.Equal(wantPods))
			})

			ginkgo.By("verifying no job outside node-b is evicted", func() {
				kept := append(append([]*kueue.Workload{}, steadyWls...), donorWls["node-a"]...)
				kept = append(kept, donorWls["node-c"]...)
				util.ExpectWorkloadsToBeAdmitted(ctx, k8sClient, kept...)
			})
		}

		ginkgo.It("should clear one host for a trainer requesting the whole host in one pod", func() {
			trainer := utiltestingapi.MakeWorkload("trainer", ns.Name).
				Queue(kueue.LocalQueueName(trainerLQ.Name)).
				Priority(trainerPriority).
				PodSets(*utiltestingapi.MakePodSet("trainer", 1).
					RequiredTopologyRequest(corev1.LabelHostname).
					Request(tasGPU, fmt.Sprintf("%d", gpusPerNode)).
					Obj()).
				Obj()
			util.MustCreate(ctx, k8sClient, trainer)

			expectNodeBCleared(trainer, 1)
		})

		ginkgo.It("should clear one host for a trainer requesting the whole host as one pod per GPU", func() {
			trainer := utiltestingapi.MakeWorkload("trainer", ns.Name).
				Queue(kueue.LocalQueueName(trainerLQ.Name)).
				Priority(trainerPriority).
				PodSets(*utiltestingapi.MakePodSet("trainer", gpusPerNode).
					RequiredTopologyRequest(corev1.LabelHostname).
					Request(tasGPU, "1").
					Obj()).
				Obj()
			util.MustCreate(ctx, k8sClient, trainer)

			expectNodeBCleared(trainer, int32(gpusPerNode))
		})
	})
