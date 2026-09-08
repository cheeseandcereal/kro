// Copyright 2026 The Kubernetes Authors.
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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// describeConditions renders RGD conditions with reason and message so a
// failed wait shows why the RGD is not Active.
func describeConditions(conds []krov1alpha1.Condition) string {
	out := make([]string, 0, len(conds))
	for _, c := range conds {
		reason, message := "", ""
		if c.Reason != nil {
			reason = *c.Reason
		}
		if c.Message != nil {
			message = *c.Message
		}
		out = append(out, fmt.Sprintf("%s=%s (%s: %s)", c.Type, c.Status, reason, message))
	}
	return strings.Join(out, "; ")
}

// Every RGD here is accepted by the RGD-level validator; its instances must
// then compile and converge on the graphengine runtime as well.
var _ = Describe("RGD validator / graphengine runtime parity", func() {
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

	// waitForRGDActive blocks until the RGD reports Active.
	waitForRGDActive := func(ctx SpecContext, name string) {
		Eventually(func(g Gomega, ctx SpecContext) {
			rgd := &krov1alpha1.ResourceGraphDefinition{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive),
				"RGD %s should be Active: %s", name, describeConditions(rgd.Status.Conditions))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	}

	// waitForInstanceActive blocks until the instance reports status.state
	// ACTIVE and returns the instance as last observed.
	waitForInstanceActive := func(ctx SpecContext, kind, name string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		Eventually(func(g Gomega, ctx SpecContext) {
			obj.SetAPIVersion("kro.run/v1alpha1")
			obj.SetKind(kind)
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj)).To(Succeed())
			state, _, _ := unstructured.NestedString(obj.Object, "status", "state")
			g.Expect(state).To(Equal("ACTIVE"), "instance status: %v", obj.Object["status"])
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		return obj
	}

	// "graphengine" is an ordinary resource id, not a reserved word.
	It("accepts an RGD whose resource id is 'graphengine' and reconciles its instance", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("test-parity-graphengine-id",
			generator.WithSchema("ParityGraphEngineID", "v1alpha1",
				map[string]any{"name": "string"},
				nil,
			),
			generator.WithResource("graphengine", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"id": "graphengine"},
			}, nil, nil),
			// References the `graphengine` node so the id is exercised as a CEL variable too.
			generator.WithResource("dependent", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}-dep"},
				"data":       map[string]any{"upstream": "${graphengine.metadata.name}"},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kro.run/v1alpha1",
			"kind":       "ParityGraphEngineID",
			"metadata":   map[string]any{"name": "ge", "namespace": namespace},
			"spec":       map[string]any{"name": "ge-cm"},
		}}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		Eventually(func(g Gomega, ctx SpecContext) {
			cm := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "ge-cm", Namespace: namespace}, cm)).To(Succeed())
			g.Expect(cm.Data).To(HaveKeyWithValue("id", "graphengine"))
			dep := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "ge-cm-dep", Namespace: namespace}, dep)).To(Succeed())
			g.Expect(dep.Data).To(HaveKeyWithValue("upstream", "ge-cm"))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		waitForInstanceActive(ctx, "ParityGraphEngineID", "ge")
	})

	// optional<bool> conditions compile; an empty optional reads as false
	// (includeWhen → node skipped; readyWhen → not ready).
	It("accepts optional<bool> readyWhen/includeWhen conditions and treats an empty optional as false", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("test-parity-optional-bool",
			generator.WithSchema("ParityOptionalBool", "v1alpha1",
				map[string]any{
					"name": "string",
					// No default, so ${schema.spec.?enabled} is empty when unset.
					"enabled": "boolean",
				},
				nil,
			),
			// `${cm.?immutable}` is optional<bool>; the template sets it, so the node becomes ready.
			generator.WithResource("cm", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"immutable":  true,
				"data":       map[string]any{"k": "v"},
			}, []string{"${cm.?immutable}"}, nil),
			generator.WithResource("guarded", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}-guarded"},
				"data":       map[string]any{"k": "v"},
			}, nil, []string{"${schema.spec.?enabled}"}),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kro.run/v1alpha1",
			"kind":       "ParityOptionalBool",
			"metadata":   map[string]any{"name": "opt", "namespace": namespace},
			// spec.enabled unset.
			"spec": map[string]any{"name": "opt-cm"},
		}}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		By("the readyWhen ${cm.?immutable} node is created and becomes ready (instance ACTIVE)")
		Eventually(func(g Gomega, ctx SpecContext) {
			cm := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "opt-cm", Namespace: namespace}, cm)).To(Succeed())
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		waitForInstanceActive(ctx, "ParityOptionalBool", "opt")

		By("the includeWhen ${schema.spec.?enabled} node is skipped while spec.enabled is unset")
		Consistently(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: "opt-cm-guarded", Namespace: namespace}, &corev1.ConfigMap{})
			g.Expect(errors.IsNotFound(err)).To(BeTrue(), "guarded ConfigMap must not exist while the optional is empty")
		}, 3*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("setting spec.enabled=true includes the guarded node")
		Eventually(func(g Gomega, ctx SpecContext) {
			current := &unstructured.Unstructured{}
			current.SetAPIVersion("kro.run/v1alpha1")
			current.SetKind("ParityOptionalBool")
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "opt", Namespace: namespace}, current)).To(Succeed())
			g.Expect(unstructured.SetNestedField(current.Object, true, "spec", "enabled")).To(Succeed())
			g.Expect(env.Client.Update(ctx, current)).To(Succeed())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega, ctx SpecContext) {
			cm := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "opt-cm-guarded", Namespace: namespace}, cm)).To(Succeed())
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		waitForInstanceActive(ctx, "ParityOptionalBool", "opt")
	})

	// metadata.creationTimestamp is format: date-time, so getFullYear() only
	// resolves while the published `schema` value keeps its declared typing.
	It("keeps the schema node typed so schema.metadata.creationTimestamp is a timestamp", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("test-parity-schema-typing",
			generator.WithSchema("ParitySchemaTyping", "v1alpha1",
				map[string]any{"name": "string"},
				nil,
			),
			generator.WithResource("cm", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data": map[string]any{
					"year": "${string(schema.metadata.creationTimestamp.getFullYear())}",
				},
			}, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kro.run/v1alpha1",
			"kind":       "ParitySchemaTyping",
			"metadata":   map[string]any{"name": "typed", "namespace": namespace},
			"spec":       map[string]any{"name": "typed-cm"},
		}}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		created := waitForInstanceActive(ctx, "ParitySchemaTyping", "typed")
		wantYear := strconv.Itoa(created.GetCreationTimestamp().UTC().Year())

		Eventually(func(g Gomega, ctx SpecContext) {
			cm := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "typed-cm", Namespace: namespace}, cm)).To(Succeed())
			g.Expect(cm.Data).To(HaveKeyWithValue("year", wantYear))
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})

	// The environment sets the RGD-level axis cap to 11 (environment/setup.go),
	// one above the runtime default, so this instance converges only if that
	// value reaches the instance runtime through the RGD reconciler.
	It("expands a forEach collection with as many axes as --rgd-max-collection-dimension-size admits", func(ctx SpecContext) {
		const axes = 11
		forEach := make([]krov1alpha1.ForEachDimension, 0, axes)
		nameParts := make([]string, 0, axes)
		for i := 0; i < axes; i++ {
			iter := fmt.Sprintf("d%d", i)
			forEach = append(forEach, krov1alpha1.ForEachDimension{iter: `${["x"]}`})
			nameParts = append(nameParts, "${"+iter+"}")
		}
		rgd := generator.NewResourceGraphDefinition("test-parity-dimension-cap",
			generator.WithSchema("ParityDimensionCap", "v1alpha1",
				map[string]any{"name": "string"},
				nil,
			),
			generator.WithResourceCollection("cms", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}-" + strings.Join(nameParts, "-")},
				"data":       map[string]any{"k": "v"},
			}, forEach, nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		instance := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kro.run/v1alpha1",
			"kind":       "ParityDimensionCap",
			"metadata":   map[string]any{"name": "dims", "namespace": namespace},
			"spec":       map[string]any{"name": "dims"},
		}}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		})

		waitForInstanceActive(ctx, "ParityDimensionCap", "dims")
		Eventually(func(g Gomega, ctx SpecContext) {
			cm := &corev1.ConfigMap{}
			name := "dims-" + strings.TrimSuffix(strings.Repeat("x-", axes), "-")
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, cm)).To(Succeed())
		}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})
