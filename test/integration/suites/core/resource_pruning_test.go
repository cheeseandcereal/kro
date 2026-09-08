// Copyright 2025 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// Pruning a resource that left the definition must not depend on unrelated
// resources being ready.
//
// Removing a resource from a ResourceGraphDefinition is how an author retires
// something, and it has to take effect on existing instances. Readiness of the
// *other* resources in the graph is a separate concern: a graph that contains
// one permanently-unready resource is normal in a degraded environment, and it
// must not pin retired resources in the cluster indefinitely.
//
// The failure mode is a silent leak. Nothing reports that a prune was skipped,
// so the orphaned object simply stays, and the instance keeps reporting
// whatever its readiness state was.
var _ = Describe("ResourcePruning", func() {
	var namespace string

	BeforeEach(func(ctx SpecContext) {
		namespace = fmt.Sprintf("test-%s", rand.String(5))
		Expect(env.Client.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	AfterEach(func(ctx SpecContext) {
		Expect(env.Client.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	It("prunes a removed resource while another resource is not ready", func(ctx SpecContext) {
		// "gate" never satisfies its readyWhen, so the instance stays
		// IN_PROGRESS for the whole spec. "retired" is removed from the
		// definition partway through and must still be cleaned up.
		newRGD := func(withRetired bool) *krov1alpha1.ResourceGraphDefinition {
			opts := []generator.ResourceGraphDefinitionOption{
				generator.WithSchema(
					"TestPruneWhileUnready", "v1alpha1",
					map[string]any{
						"name": "string",
					},
					nil,
				),
				generator.WithResource("gate", map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name": "${schema.spec.name}-gate",
					},
					"data": map[string]any{
						"ready": "false",
					},
				}, []string{`${gate.data.ready == "true"}`}, nil),
			}
			if withRetired {
				opts = append(opts, generator.WithResource("retired", map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"name": "${schema.spec.name}-retired",
					},
					"data": map[string]any{
						"keep": "for-now",
					},
				}, nil, nil))
			}
			return generator.NewResourceGraphDefinition("test-prune-while-unready", opts...)
		}

		rgd := newRGD(true)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		name := "prune-unready"
		instance := newInstance("TestPruneWhileUnready", name, namespace, map[string]any{
			"name": name,
		})
		Expect(env.Client.Create(ctx, instance)).To(Succeed())

		// Both resources exist, and the instance is held IN_PROGRESS by "gate".
		for _, suffix := range []string{"-gate", "-retired"} {
			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(env.Client.Get(ctx, types.NamespacedName{
					Name:      name + suffix,
					Namespace: namespace,
				}, &corev1.ConfigMap{})).To(Succeed())
			}, 30*time.Second, time.Second).WithContext(ctx).Should(Succeed())
		}
		waitForInstanceState(ctx, instance, name, namespace, "IN_PROGRESS")

		// Retire the resource.
		Eventually(func(g Gomega, ctx SpecContext) {
			current := &krov1alpha1.ResourceGraphDefinition{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, current)).To(Succeed())
			current.Spec.Resources = newRGD(false).Spec.Resources
			g.Expect(env.Client.Update(ctx, current)).To(Succeed())
		}, 20*time.Second, time.Second).WithContext(ctx).Should(Succeed())
		waitForRGDActive(ctx, rgd.Name)

		// The retired resource is cleaned up even though "gate" is still not
		// ready, and the still-declared resource is left alone.
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name + "-retired",
				Namespace: namespace,
			}, &corev1.ConfigMap{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"resource removed from the definition was not pruned (err=%v); instance conditions: %s",
				err, instanceConditions(instance))
		}, 60*time.Second, 2*time.Second).WithContext(ctx).Should(Succeed())

		Expect(env.Client.Get(ctx, types.NamespacedName{
			Name:      name + "-gate",
			Namespace: namespace,
		}, &corev1.ConfigMap{})).To(Succeed(), "still-declared resource must not be pruned")
	})

	// Pruning is decided per node: an unresolved node protects only its own
	// members, while retired members of nodes that did resolve are deleted. A
	// dependent that reads into a collection becomes unresolvable exactly when
	// the collection shrinks to nothing, so an instance-wide veto would leak
	// every retired member for as long as the collection stays empty.
	It("prunes retired collection members while a dependent node is unresolved", func(ctx SpecContext) {
		// "cms" renders one ConfigMap per spec.values entry; "summary" reads
		// cms[0], so it resolves only while the collection is non-empty.
		rgd := generator.NewResourceGraphDefinition("test-prune-per-node",
			generator.WithSchema(
				"TestPrunePerNode", "v1alpha1",
				map[string]any{
					"name":   "string",
					"values": "[]string",
				},
				nil,
			),
			generator.WithResourceCollection("cms", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-cp-${v}",
				},
				"data": map[string]any{
					"value": "${v}",
				},
			},
				[]krov1alpha1.ForEachDimension{
					{"v": "${schema.spec.values}"},
				},
				nil, nil),
			generator.WithResource("summary", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-summary",
				},
				"data": map[string]any{
					"first": "${cms[0].metadata.name}",
				},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		name := "prune-per-node"
		instance := newInstance("TestPrunePerNode", name, namespace, map[string]any{
			"name":   name,
			"values": []any{"a1", "a2"},
		})
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			_ = env.Client.Delete(ctx, instance)
		})

		getCM := func(ctx SpecContext, suffix string) (*corev1.ConfigMap, error) {
			cm := &corev1.ConfigMap{}
			err := env.Client.Get(ctx, types.NamespacedName{
				Name:      name + suffix,
				Namespace: namespace,
			}, cm)
			return cm, err
		}
		setValues := func(ctx SpecContext, values []string) {
			Eventually(func(g Gomega, ctx SpecContext) {
				g.Expect(env.Client.Get(ctx, types.NamespacedName{
					Name:      name,
					Namespace: namespace,
				}, instance)).To(Succeed())
				g.Expect(unstructured.SetNestedStringSlice(instance.Object, values, "spec", "values")).To(Succeed())
				g.Expect(env.Client.Update(ctx, instance)).To(Succeed())
			}, 20*time.Second, time.Second).WithContext(ctx).Should(Succeed())
		}

		By("converging with two collection members and a resolved summary")
		waitForInstanceState(ctx, instance, name, namespace, "ACTIVE")
		for _, suffix := range []string{"-cp-a1", "-cp-a2"} {
			_, err := getCM(ctx, suffix)
			Expect(err).ToNot(HaveOccurred(), "collection member %s must exist once ACTIVE", name+suffix)
		}
		summary, err := getCM(ctx, "-summary")
		Expect(err).ToNot(HaveOccurred())
		Expect(summary.Data).To(HaveKeyWithValue("first", name+"-cp-a1"))

		By("shrinking the collection to nothing, which leaves summary unresolvable")
		setValues(ctx, []string{})

		// The retired members are pruned while summary is data-pending.
		for _, suffix := range []string{"-cp-a1", "-cp-a2"} {
			Eventually(func(g Gomega, ctx SpecContext) {
				_, err := getCM(ctx, suffix)
				if !apierrors.IsNotFound(err) {
					// Refresh so the failure output shows this cycle's conditions.
					_ = env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, instance)
				}
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"retired collection member %s was not pruned (err=%v); instance conditions: %s",
					name+suffix, err, instanceConditions(instance))
			}, 60*time.Second, 2*time.Second).WithContext(ctx).Should(Succeed())
		}

		// Orphans are deleted dependents-first in one pass, so had summary been
		// targeted it would already be gone.
		_, err = getCM(ctx, "-summary")
		Expect(err).ToNot(HaveOccurred(), "the unresolved node's own resource must be retained")

		// The status write lands after the prune in the same reconcile.
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, instance)).To(Succeed())
			state, _, err := unstructured.NestedString(instance.Object, "status", "state")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(state).ToNot(Equal("ACTIVE"),
				"an instance with an unresolved node must not report ACTIVE; conditions: %s", instanceConditions(instance))
		}, 20*time.Second, time.Second).WithContext(ctx).Should(Succeed())

		By("growing the collection back, which lets summary resolve again")
		setValues(ctx, []string{"a1"})
		waitForInstanceState(ctx, instance, name, namespace, "ACTIVE")

		_, err = getCM(ctx, "-cp-a1")
		Expect(err).ToNot(HaveOccurred(), "re-added collection member must be recreated")
		_, err = getCM(ctx, "-cp-a2")
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "member that stayed retired must not reappear")
		Eventually(func(g Gomega, ctx SpecContext) {
			summary, err := getCM(ctx, "-summary")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(summary.Data).To(HaveKeyWithValue("first", name+"-cp-a1"))
		}, 30*time.Second, time.Second).WithContext(ctx).Should(Succeed())
	})
})
