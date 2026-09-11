// Copyright 2026 The Kube Resource Orchestrator Authors
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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	ctrlinstance "github.com/kubernetes-sigs/kro/pkg/controller/instance"
	"github.com/kubernetes-sigs/kro/pkg/features"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

func findInstanceConditionByType(inst *unstructured.Unstructured, condType string) map[string]any {
	statusConditions, found, _ := unstructured.NestedSlice(inst.Object, "status", "conditions")
	if !found {
		return nil
	}
	for _, c := range statusConditions {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := m["type"].(string); t == condType {
			return m
		}
	}
	return nil
}

func getConditionTypes(inst *unstructured.Unstructured) []string {
	statusConditions, found, _ := unstructured.NestedSlice(inst.Object, "status", "conditions")
	if !found {
		return nil
	}
	var out []string
	for _, c := range statusConditions {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := m["type"].(string); ok {
			out = append(out, t)
		}
	}
	return out
}

// createInstanceWithCleanup creates the instance and registers a cleanup
// that deletes it and waits for finalization. Instances must be fully
// deleted before the RGD's own cleanup runs: RGD deletion deregisters the
// instance micro-controller before deleting the CRD, so an instance still
// carrying kro's finalizer at that point can never be finalized — its CRD
// then wedges the apiserver's CRD finalizer workers, stalling every later
// CRD deletion in the suite. DeferCleanup's LIFO order runs this before the
// RGD delete registered at RGD creation time.
func createInstanceWithCleanup(ctx SpecContext, instance *unstructured.Unstructured) {
	GinkgoHelper()
	Expect(env.Client.Create(ctx, instance)).To(Succeed())
	key := types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()}
	DeferCleanup(func(ctx SpecContext) {
		if err := env.Client.Delete(ctx, instance); err != nil && !errors.IsNotFound(err) {
			Expect(err).ToNot(HaveOccurred(), "deleting instance %s", key)
		}
		Eventually(func(g Gomega) {
			err := env.Client.Get(ctx, key, instance)
			g.Expect(err).To(MatchError(errors.IsNotFound, "instance should be fully finalized"))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})
}

var _ = Describe("Instance Custom Conditions", func() {
	var namespace string

	BeforeEach(func(ctx SpecContext) {
		namespace = fmt.Sprintf("test-cc-%s", rand.String(5))
		Expect(env.Client.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	AfterEach(func(ctx SpecContext) {
		Expect(env.Client.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	It("writes author-defined conditions to status.conditions", func(ctx SpecContext) {
		rgdName := "test-cc-happy"
		instanceKind := "TestCcHappy"

		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{"name": "string"},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'PrimaryReady', status: 'True', reason: 'OK',
							message: 'always healthy in this test'})}`,
						`${runtime.newCondition({type: 'AppReady', status: 'False', reason: 'Init', message: ''})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"foo": "${schema.spec.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{
				Name: "demo", Namespace: namespace,
			}, instance)).To(Succeed())

			primary := findInstanceConditionByType(instance, "PrimaryReady")
			g.Expect(primary).ToNot(BeNil(), "PrimaryReady should be on the wire")
			g.Expect(primary["status"]).To(Equal("True"))
			g.Expect(primary["reason"]).To(Equal("OK"))
			g.Expect(primary["message"]).To(Equal("always healthy in this test"))
			g.Expect(primary["lastTransitionTime"]).ToNot(BeNil())
			g.Expect(primary["observedGeneration"]).To(Equal(instance.GetGeneration()))

			appReady := findInstanceConditionByType(instance, "AppReady")
			g.Expect(appReady).ToNot(BeNil())
			g.Expect(appReady["status"]).To(Equal("False"))
			g.Expect(appReady["reason"]).To(Equal("Init"))

			types := getConditionTypes(instance)
			g.Expect(types).ToNot(ContainElement(ctrlinstance.InstanceManaged))
			g.Expect(types).ToNot(ContainElement(ctrlinstance.GraphResolved))
			g.Expect(types).ToNot(ContainElement(ctrlinstance.ResourcesReady))
			g.Expect(types).ToNot(ContainElement(ctrlinstance.Ready),
				"kro lifecycle Ready must not appear when the author did not declare a Ready condition")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("preserves lastTransitionTime when condition status is unchanged", func(ctx SpecContext) {
		rgdName := "test-cc-ltt"
		instanceKind := "TestCcLtt"

		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{"name": "string", "label": "string | default=initial"},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'AlwaysTrue', status: 'True', reason: 'X', message: ''})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":   "${schema.spec.name}",
					"labels": map[string]any{"app": "${schema.spec.label}"},
				},
				"data": map[string]any{"foo": "${schema.spec.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo", "label": "initial"},
		}}
		createInstanceWithCleanup(ctx, instance)

		var firstLTT any
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, "AlwaysTrue")
			g.Expect(c).ToNot(BeNil())
			firstLTT = c["lastTransitionTime"]
			g.Expect(firstLTT).ToNot(BeNil())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
		Expect(unstructured.SetNestedField(instance.Object, "updated", "spec", "label")).To(Succeed())
		Expect(env.Client.Update(ctx, instance)).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, "AlwaysTrue")
			g.Expect(c).ToNot(BeNil())
			g.Expect(c["observedGeneration"]).To(Equal(instance.GetGeneration()),
				"observedGeneration should track the new generation")
			g.Expect(c["lastTransitionTime"]).To(Equal(firstLTT),
				"lastTransitionTime should be preserved when status hasn't changed")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("emits kro built-in conditions when no conditions: block is present", func(ctx SpecContext) {
		rgdName := "test-cc-backcompat"
		instanceKind := "TestCcBackCompat"

		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{"name": "string"},
				nil,
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"foo": "${schema.spec.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			ts := getConditionTypes(instance)
			g.Expect(ts).To(ContainElement(ctrlinstance.Ready))
			g.Expect(ts).To(ContainElement(ctrlinstance.InstanceManaged))
			g.Expect(ts).To(ContainElement(ctrlinstance.GraphResolved))
			g.Expect(ts).To(ContainElement(ctrlinstance.ResourcesReady))

			ready := findInstanceConditionByType(instance, ctrlinstance.Ready)
			g.Expect(ready["status"]).To(Equal("True"))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("honors an author-defined Ready that overrides kro's lifecycle Ready", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("test-cc-author-ready",
			generator.WithSchema(
				"TestCcAuthorReady", "v1alpha1",
				map[string]any{"name": "string"},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'Ready', status: 'False', reason: 'AuthorSaysNo', message: 'this is a test'})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"foo": "${schema.spec.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       "TestCcAuthorReady",
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			ready := findInstanceConditionByType(instance, ctrlinstance.Ready)
			g.Expect(ready).ToNot(BeNil())
			g.Expect(ready["status"]).To(Equal("False"),
				"author Ready=False must win, even when kro lifecycle would otherwise say True")
			g.Expect(ready["reason"]).To(Equal("AuthorSaysNo"))
			g.Expect(ready["message"]).To(Equal("this is a test"))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("evaluates conditions against schema.spec fields and updates them when spec changes", func(ctx SpecContext) {
		rgdName := "test-cc-schema-read"
		instanceKind := "TestCcSchemaRead"

		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{
					"name":    "string",
					"healthy": "boolean | default=true",
				},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'AppReady', status: schema.spec.healthy ? 'True' : 'False',
							reason: 'CheckedSpec', message: ''})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"foo": "${schema.spec.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo", "healthy": true},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, "AppReady")
			g.Expect(c).ToNot(BeNil())
			g.Expect(c["status"]).To(Equal("True"))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
		Expect(unstructured.SetNestedField(instance.Object, false, "spec", "healthy")).To(Succeed())
		Expect(env.Client.Update(ctx, instance)).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, "AppReady")
			g.Expect(c).ToNot(BeNil())
			g.Expect(c["status"]).To(Equal("False"),
				"condition status should follow spec.healthy")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("runtime.condition reads kro's internal value, not the author's wire override", func(ctx SpecContext) {
		rgdName := "test-cc-internal-not-wire"
		instanceKind := "TestCcInternalNotWire"

		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{"name": "string"},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({
							type: 'ResourcesReady',
							status: 'True',
							reason: 'AuthorOverride',
							message: 'author claims ready',
						})}`,
						`${runtime.newCondition({
							type: 'DerivedReady',
							status: runtime.condition(schema, 'ResourcesReady').status,
							reason: 'MirrorsKroInternal',
							message: '',
						})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"foo": "${schema.spec.name}"},
			}, []string{`${configmap.data["ready"] == "true"}`}, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())

			rr := findInstanceConditionByType(instance, ctrlinstance.ResourcesReady)
			g.Expect(rr).ToNot(BeNil(), "author's ResourcesReady override should appear on the wire")
			g.Expect(rr["status"]).To(Equal("True"),
				"author's declaration should appear unchanged on the wire")
			g.Expect(rr["reason"]).To(Equal("AuthorOverride"))

			derived := findInstanceConditionByType(instance, "DerivedReady")
			g.Expect(derived).ToNot(BeNil())
			g.Expect(derived["status"]).To(Equal("False"),
				"runtime.condition(schema, 'ResourcesReady') must reflect kro's internal value (False), "+
					"not the author's overriding 'True' on the wire")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("drops runtime-duplicate condition types, sets state=Error, keeps survivors", func(ctx SpecContext) {
		rgdName := "test-cc-dup-type"
		instanceKind := "TestCcDupType"

		// The duplicate types are computed, so the build-time literal check
		// cannot catch them; runtime dedup drops both copies.
		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{"name": "string"},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'Survivor', status: 'True', reason: '', message: ''})}`,
						`${runtime.newCondition({type: 'Same-' + schema.spec.name, status: 'True', reason: '', message: ''})}`,
						`${runtime.newCondition({type: 'Same-' + schema.spec.name, status: 'False', reason: '', message: ''})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"foo": "${schema.spec.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())

			state, _, _ := unstructured.NestedString(instance.Object, "status", "state")
			g.Expect(state).To(Equal(string(krov1alpha1.InstanceStateError)),
				"duplicate condition type must surface as state=Error")

			g.Expect(findInstanceConditionByType(instance, "Survivor")).ToNot(BeNil(),
				"Survivor must remain on the wire")
			g.Expect(findInstanceConditionByType(instance, "Same-demo")).To(BeNil(),
				"both copies of the duplicated type must be dropped")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("preserves lastTransitionTime for author conditions overriding built-in types", func(ctx SpecContext) {
		rgdName := "test-cc-override-ltt"
		instanceKind := "TestCcOverrideLtt"

		// The author's ResourcesReady=True disagrees with kro's internal
		// value (the readyWhen below never passes), which previously caused
		// lastTransitionTime to churn on every reconcile.
		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{"name": "string", "label": "string | default=initial"},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'ResourcesReady', status: 'True', reason: 'AuthorOverride', message: ''})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":   "${schema.spec.name}",
					"labels": map[string]any{"app": "${schema.spec.label}"},
				},
				"data": map[string]any{"foo": "${schema.spec.name}"},
			}, []string{`${configmap.data["ready"] == "true"}`}, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo", "label": "initial"},
		}}
		createInstanceWithCleanup(ctx, instance)

		var firstLTT any
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, ctrlinstance.ResourcesReady)
			g.Expect(c).ToNot(BeNil())
			g.Expect(c["status"]).To(Equal("True"))
			firstLTT = c["lastTransitionTime"]
			g.Expect(firstLTT).ToNot(BeNil())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		// Force another reconcile via a spec change; the override's
		// lastTransitionTime must survive it.
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
		Expect(unstructured.SetNestedField(instance.Object, "updated", "spec", "label")).To(Succeed())
		Expect(env.Client.Update(ctx, instance)).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, ctrlinstance.ResourcesReady)
			g.Expect(c).ToNot(BeNil())
			g.Expect(c["observedGeneration"]).To(Equal(instance.GetGeneration()))
			g.Expect(c["lastTransitionTime"]).To(Equal(firstLTT),
				"lastTransitionTime must not churn when the author's status is stable, "+
					"even though it disagrees with kro's internal value")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("preserves a previously written condition while its data is pending", func(ctx SpecContext) {
		rgdName := "test-cc-data-pending"
		instanceKind := "TestCcDataPending"

		rgd := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(
				instanceKind, "v1alpha1",
				map[string]any{
					"name": "string",
					"key":  "string",
				},
				map[string]any{
					"conditions": []any{
						`${runtime.newCondition({type: 'Static', status: 'True', reason: '', message: ''})}`,
						`${runtime.newCondition({type: 'DataDriven',
							status: configmap.data[schema.spec.key] == 'x' ? 'True' : 'False',
							reason: 'FromConfigMap', message: ''})}`,
					},
				},
			),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"phase": "x"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo", "key": "phase"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, "DataDriven")
			g.Expect(c).ToNot(BeNil())
			g.Expect(c["status"]).To(Equal("True"))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		// Point the expression at a key that doesn't exist: evaluation is now
		// data-pending, and the previously written condition must survive.
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
		Expect(unstructured.SetNestedField(instance.Object, "missing", "spec", "key")).To(Succeed())
		Expect(env.Client.Update(ctx, instance)).To(Succeed())

		Consistently(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			c := findInstanceConditionByType(instance, "DataDriven")
			g.Expect(c).ToNot(BeNil(), "condition must not disappear while its data is pending")
			g.Expect(c["status"]).To(Equal("True"), "the last written value must be preserved")
			for _, builtin := range []string{
				ctrlinstance.InstanceManaged, ctrlinstance.GraphResolved,
				ctrlinstance.ResourcesReady, ctrlinstance.Ready,
			} {
				g.Expect(findInstanceConditionByType(instance, builtin)).To(BeNil(),
					"kro's built-ins must not leak onto the wire while data is pending")
			}
		}).WithContext(ctx).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(Succeed())
	})

	It("removes leftover author conditions after the conditions block is removed", func(ctx SpecContext) {
		rgdName := "test-cc-block-removed"
		instanceKind := "TestCcBlockRemoved"

		withConditions := generator.WithSchema(
			instanceKind, "v1alpha1",
			map[string]any{"name": "string"},
			map[string]any{
				"conditions": []any{
					`${runtime.newCondition({type: 'AppReady', status: 'True', reason: '', message: ''})}`,
				},
			},
		)
		configmap := generator.WithResource("configmap", map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "${schema.spec.name}"},
			"data":       map[string]any{"foo": "${schema.spec.name}"},
		}, nil, nil)

		rgd := generator.NewResourceGraphDefinition(rgdName, withConditions, configmap)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       instanceKind,
			"metadata":   map[string]any{"name": "demo", "namespace": namespace},
			"spec":       map[string]any{"name": "demo"},
		}}
		createInstanceWithCleanup(ctx, instance)

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			g.Expect(findInstanceConditionByType(instance, "AppReady")).ToNot(BeNil())
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		// Drop the conditions block; kro's built-ins take the wire back and
		// the author condition is cleaned up.
		withoutConditions := generator.NewResourceGraphDefinition(rgdName,
			generator.WithSchema(instanceKind, "v1alpha1", map[string]any{"name": "string"}, nil),
			configmap,
		)
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgdName}, rgd)).To(Succeed())
		rgd.Spec = withoutConditions.Spec
		Expect(env.Client.Update(ctx, rgd)).To(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "demo", Namespace: namespace}, instance)).To(Succeed())
			g.Expect(findInstanceConditionByType(instance, "AppReady")).To(BeNil(),
				"leftover author condition must be removed from the wire")
			g.Expect(findInstanceConditionByType(instance, ctrlinstance.Ready)).ToNot(BeNil(),
				"kro's built-in conditions must return")
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})
})

var _ = Describe("Suspended author conditions", Serial, func() {
	It("reprojects author conditions while suspended with default feature gates", func(ctx SpecContext) {
		// The shared suite enables alpha gates. Use a cold, isolated environment
		// to exercise the default RGD path without changing the suite's wiring.
		featuregatetesting.SetFeatureGateDuringTest(GinkgoT(), features.FeatureGate, features.GraphKind, false)
		featuregatetesting.SetFeatureGateDuringTest(GinkgoT(), features.FeatureGate, features.CELOmitFunction, false)
		testEnv, err := environment.New(ctx, environment.ControllerConfig{
			AllowCRDDeletion: true,
			ReconcileConfig:  ctrlinstance.ReconcileConfig{DefaultRequeueDuration: time.Second},
			LogWriter:        GinkgoWriter,
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(stopEnvironmentWithRetry(testEnv)).To(Succeed()) })
		Expect(testEnv.Router).To(BeNil(), "GraphKind is disabled in this environment")
		Expect(testEnv.SchemaWatcher).To(BeNil())

		namespace := "suspended-author-conditions"
		Expect(testEnv.Client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})).To(Succeed())
		rgd := generator.NewResourceGraphDefinition("suspended-author-conditions",
			generator.WithSchema("SuspendedAuthorConditions", "v1alpha1",
				map[string]any{"healthy": "boolean", "value": "string"},
				map[string]any{
					"value": "${configmap.data.value}",
					"conditions": []any{
						`${runtime.newCondition({type: 'AppReady', status: schema.spec.healthy ? 'True' : 'False', reason: 'CheckedSpec'})}`,
						`${runtime.newCondition({type: 'CmReady', status: configmap.data.value == schema.spec.value ? 'True' : 'False', reason: 'FromConfigMap'})}`,
						`${runtime.newCondition({type: 'Paused', status: runtime.condition(schema, 'ResourcesReady').reason == 'ReconciliationSuspended' ? 'True' : 'False', reason: runtime.condition(schema, 'ResourcesReady').reason})}`,
					},
				}),
			generator.WithResource("configmap", map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": "${schema.metadata.name}"},
				"data":     map[string]any{"value": "${schema.spec.value}"},
			}, nil, nil),
		)
		Expect(testEnv.Client.Create(ctx, rgd)).To(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kro.run/v1alpha1", "kind": "SuspendedAuthorConditions",
			"metadata": map[string]any{"name": "demo", "namespace": namespace},
			"spec":     map[string]any{"healthy": true, "value": "v1"},
		}}
		key := types.NamespacedName{Name: "demo", Namespace: namespace}
		Expect(testEnv.Client.Create(ctx, instance)).To(Succeed())
		cm := &corev1.ConfigMap{}
		Eventually(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, key, instance)).To(Succeed())
			g.Expect(getConditionTypes(instance)).To(ConsistOf("AppReady", "CmReady", "Paused"))
			for condType, status := range map[string]string{"AppReady": "True", "CmReady": "True", "Paused": "False"} {
				condition := findInstanceConditionByType(instance, condType)
				g.Expect(condition["status"]).To(Equal(status))
				g.Expect(condition["observedGeneration"]).To(Equal(instance.GetGeneration()))
			}
			value, _, _ := unstructured.NestedString(instance.Object, "status", "value")
			g.Expect(value).To(Equal("v1"))
			g.Expect(testEnv.Client.Get(ctx, key, cm)).To(Succeed())
			g.Expect(cm.Data["value"]).To(Equal("v1"))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
		previousGeneration := instance.GetGeneration()
		previousCmReady := findInstanceConditionByType(instance, "CmReady")
		previousCM := cm.DeepCopy()

		By("changing the desired child value and health atomically with suspension")
		Eventually(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, key, instance)).To(Succeed())
			g.Expect(unstructured.SetNestedField(instance.Object, false, "spec", "healthy")).To(Succeed())
			g.Expect(unstructured.SetNestedField(instance.Object, "v2", "spec", "value")).To(Succeed())
			annotations := instance.GetAnnotations()
			annotations[krov1alpha1.InstanceReconcileAnnotation] = krov1alpha1.ReconcileSuspended
			instance.SetAnnotations(annotations)
			g.Expect(testEnv.Client.Update(ctx, instance)).To(Succeed())
		}).WithContext(ctx).WithTimeout(20 * time.Second).WithPolling(time.Second).Should(Succeed())

		Eventually(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, key, instance)).To(Succeed())
			g.Expect(instance.GetGeneration()).To(BeNumerically(">", previousGeneration))
			appReady := findInstanceConditionByType(instance, "AppReady")
			g.Expect(appReady["status"]).To(Equal("False"), "a resolvable author condition must follow the suspended spec change")
			g.Expect(appReady["observedGeneration"]).To(Equal(instance.GetGeneration()))
			paused := findInstanceConditionByType(instance, "Paused")
			g.Expect(paused["status"]).To(Equal("True"))
			g.Expect(paused["reason"]).To(Equal("ReconciliationSuspended"))
			g.Expect(paused["observedGeneration"]).To(Equal(instance.GetGeneration()))
			g.Expect(findInstanceConditionByType(instance, "CmReady")).To(Equal(previousCmReady))
			g.Expect(getConditionTypes(instance)).To(ConsistOf("AppReady", "CmReady", "Paused"))
			status, _, _ := unstructured.NestedMap(instance.Object, "status")
			g.Expect(status["state"]).To(Equal(string(krov1alpha1.InstanceStateActive)))
			g.Expect(status["value"]).To(Equal("v1"))
			g.Expect(testEnv.Client.Get(ctx, key, cm)).To(Succeed())
			g.Expect(cm.Data).To(Equal(previousCM.Data))
			g.Expect(cm.UID).To(Equal(previousCM.UID))
			g.Expect(cm.ResourceVersion).To(Equal(previousCM.ResourceVersion))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
		suspendedStatus, _, _ := unstructured.NestedMap(instance.Object, "status")
		suspendedRV := instance.GetResourceVersion()
		Consistently(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, key, instance)).To(Succeed())
			status, _, _ := unstructured.NestedMap(instance.Object, "status")
			g.Expect(status).To(Equal(suspendedStatus))
			g.Expect(instance.GetResourceVersion()).To(Equal(suspendedRV))
			g.Expect(testEnv.Client.Get(ctx, key, cm)).To(Succeed())
			g.Expect(cm.ResourceVersion).To(Equal(previousCM.ResourceVersion))
		}).WithContext(ctx).WithTimeout(5 * time.Second).WithPolling(time.Second).Should(Succeed())

		By("resuming and applying the spec change that was held during suspension")
		Eventually(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, key, instance)).To(Succeed())
			annotations := instance.GetAnnotations()
			delete(annotations, krov1alpha1.InstanceReconcileAnnotation)
			instance.SetAnnotations(annotations)
			g.Expect(testEnv.Client.Update(ctx, instance)).To(Succeed())
		}).WithContext(ctx).WithTimeout(20 * time.Second).WithPolling(time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			g.Expect(testEnv.Client.Get(ctx, key, instance)).To(Succeed())
			g.Expect(getConditionTypes(instance)).To(ConsistOf("AppReady", "CmReady", "Paused"))
			for condType, status := range map[string]string{"AppReady": "False", "CmReady": "True", "Paused": "False"} {
				condition := findInstanceConditionByType(instance, condType)
				g.Expect(condition["status"]).To(Equal(status))
				g.Expect(condition["observedGeneration"]).To(Equal(instance.GetGeneration()))
			}
			value, _, _ := unstructured.NestedString(instance.Object, "status", "value")
			g.Expect(value).To(Equal("v2"))
			g.Expect(testEnv.Client.Get(ctx, key, cm)).To(Succeed())
			g.Expect(cm.Data["value"]).To(Equal("v2"))
			g.Expect(cm.UID).To(Equal(previousCM.UID))
		}).WithContext(ctx).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	})
})
