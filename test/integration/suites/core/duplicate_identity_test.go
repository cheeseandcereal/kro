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
	"github.com/kubernetes-sigs/kro/pkg/applyset"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// Two resources of one RGD that render the same object identity are rejected
// before any write: a pre-existing user object with that identity is neither
// modified nor adopted, and survives the instance's deletion.
var _ = Describe("Duplicate resource identity", func() {
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

	It("rejects the instance before any write and never adopts or prunes the user's object", func(ctx SpecContext) {
		const sharedName = "dup-shared"

		By("pre-creating a user-owned ConfigMap with the identity both resources will render")
		userCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: sharedName, Namespace: namespace},
			Data:       map[string]string{"from": "previous"},
		}
		Expect(env.Client.Create(ctx, userCM)).To(Succeed())
		pristine := &corev1.ConfigMap{}
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: sharedName, Namespace: namespace}, pristine)).To(Succeed())

		By("creating an RGD whose two ConfigMap resources both render metadata.name=${schema.spec.name}")
		sharedConfigMap := func(from string) map[string]any {
			return map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "${schema.spec.name}"},
				"data":       map[string]any{"from": from},
			}
		}
		rgd := generator.NewResourceGraphDefinition("test-dup-identity",
			generator.WithSchema(
				"TestDupIdentity", "v1alpha1",
				map[string]any{"name": "string"},
				nil,
			),
			generator.WithResource("cma", sharedConfigMap("cma"), nil, nil),
			generator.WithResource("cmb", sharedConfigMap("cmb"), nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})

		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, rgd)).To(Succeed())
			g.Expect(rgd.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("creating an instance that names the pre-existing ConfigMap")
		instance := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       "TestDupIdentity",
				"metadata":   map[string]any{"name": "dup-instance", "namespace": namespace},
				"spec":       map[string]any{"name": sharedName},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())

		By("waiting for the instance to report ERROR with the duplicate-identity message")
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: "dup-instance", Namespace: namespace}, instance)).To(Succeed())

			state, found, err := unstructured.NestedString(instance.Object, "status", "state")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(found).To(BeTrue())
			g.Expect(state).To(Equal("ERROR"))

			conditions, found, err := unstructured.NestedSlice(instance.Object, "status", "conditions")
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(found).To(BeTrue())
			var resourcesReady map[string]any
			for _, cond := range conditions {
				if c, ok := cond.(map[string]any); ok && c["type"] == "ResourcesReady" {
					resourcesReady = c
					break
				}
			}
			g.Expect(resourcesReady).ToNot(BeNil())
			g.Expect(resourcesReady["status"]).To(Equal("False"))
			g.Expect(resourcesReady["message"]).To(ContainSubstring("duplicate resource identity"))
			g.Expect(resourcesReady["message"]).To(ContainSubstring(`"cma"`))
			g.Expect(resourcesReady["message"]).To(ContainSubstring(`"cmb"`))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("verifying the user's ConfigMap was never written, adopted, or labelled")
		assertUntouched := func(g Gomega, ctx SpecContext) {
			live := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: sharedName, Namespace: namespace}, live)).To(Succeed())
			g.Expect(live.Data).To(HaveKeyWithValue("from", "previous"))
			g.Expect(live.Labels).ToNot(HaveKey(applyset.ApplysetPartOfLabel))
			g.Expect(live.Labels).ToNot(HaveKey(metadata.OwnedLabel))
			g.Expect(live.Labels).ToNot(HaveKey(metadata.NodeIDLabel))
			g.Expect(live.Labels).ToNot(HaveKey(metadata.InstanceLabel))
			g.Expect(live.OwnerReferences).To(BeEmpty())
			// No write at all: the resourceVersion is the one from creation.
			g.Expect(live.ResourceVersion).To(Equal(pristine.ResourceVersion))
		}
		Consistently(assertUntouched, 3*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("deleting the ERROR instance and verifying the user's ConfigMap survives")
		Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: "dup-instance", Namespace: namespace}, instance)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "instance should be gone, got err=%v", err)
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Consistently(assertUntouched, 3*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})

// A forEach iterator that varies only `kind` renders one distinct identity per
// kind, so the Graph compiles and converges one ConfigMap and one Secret
// sharing a fixed name.
var _ = Describe("Graph heterogeneous-kind forEach", func() {
	It("accepts an iterator that appears only in kind and converges one object per kind", func() {
		t := GinkgoT()
		ns := env.CreateNamespace(t)

		g := &krov1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "hetero", Namespace: ns},
			Spec: krov1alpha1.GraphSpec{
				Nodes: []krov1alpha1.Node{
					{
						ID:  "kinds",
						Def: environment.RawExt(t, map[string]any{"items": []string{"ConfigMap", "Secret"}}),
					},
					{
						ID:      "res",
						ForEach: []krov1alpha1.ForEachDimension{{"k": "${kinds.items}"}},
						Template: environment.RawExt(t, map[string]any{
							"apiVersion": "v1",
							"kind":       "${k}",
							// The name is FIXED: only the kind distinguishes the rows.
							"metadata": map[string]any{"name": "hetero-shared"},
						}),
					},
				},
			},
		}
		env.CreateGraph(t, g)

		env.AwaitCondition(t,
			types.NamespacedName{Namespace: ns, Name: "hetero"},
			krov1alpha1.GraphConditionTypeReady,
			metav1.ConditionTrue, 20*time.Second)

		environment.Eventually(t, 15*time.Second, 100*time.Millisecond, func() error {
			ctx := env.Context()
			cm := &corev1.ConfigMap{}
			if err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "hetero-shared"}, cm); err != nil {
				return fmt.Errorf("ConfigMap: %w", err)
			}
			sec := &corev1.Secret{}
			if err := env.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: "hetero-shared"}, sec); err != nil {
				return fmt.Errorf("Secret: %w", err)
			}
			for kind, labels := range map[string]map[string]string{"ConfigMap": cm.Labels, "Secret": sec.Labels} {
				if labels[metadata.NodeIDLabel] != "res" {
					return fmt.Errorf("%s: node-id label=%q want %q", kind, labels[metadata.NodeIDLabel], "res")
				}
			}
			return nil
		})

		got := env.GetGraph(t, types.NamespacedName{Namespace: ns, Name: "hetero"})
		kinds := make([]string, 0, len(got.Status.ManagedResources))
		for _, mr := range got.Status.ManagedResources {
			kinds = append(kinds, mr.Kind)
		}
		Expect(kinds).To(ConsistOf("ConfigMap", "Secret"), "one managed resource per rendered kind")
	})
})
