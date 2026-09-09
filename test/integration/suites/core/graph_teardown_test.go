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
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

// Graph teardown must neither wedge on a resource type the cluster no longer
// serves nor orphan children whose inventory entry never received a UID.
var _ = Describe("Graph Teardown Safety", func() {
	It("releases the finalizer after the CRD behind a managed resource was deleted", func(ctx SpecContext) {
		t := GinkgoT()
		ns := env.CreateNamespace(t)

		group := fmt.Sprintf("teardown-%s.kro.run", rand.String(5))
		kind := fmt.Sprintf("TdWidget%s", rand.String(5))
		crd := installCRD(t, env, group, kind, "v0")
		widgetGVK := schema.GroupVersionKind{Group: group, Version: "v1", Kind: kind}
		widgetKey := types.NamespacedName{Namespace: ns, Name: "w"}

		// The Widget reads an external ConfigMap ("gate"); deleting gate later
		// makes the node Unresolved, so the inventory entry is preserved and the
		// Widget is not re-created while its CRD is removed.
		gate := &unstructured.Unstructured{}
		gate.SetGroupVersionKind(configMapGVK)
		gate.SetNamespace(ns)
		gate.SetName("gate")
		if err := unstructured.SetNestedField(gate.Object, "x", "data", "k"); err != nil {
			t.Fatalf("set gate data: %v", err)
		}
		if err := env.Client.Create(ctx, gate); err != nil {
			t.Fatalf("create gate: %v", err)
		}

		g := &expv1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "crd-gone", Namespace: ns},
			Spec: expv1alpha1.GraphSpec{
				Nodes: []expv1alpha1.Node{
					{
						ID: "gate",
						Ref: &expv1alpha1.ExternalRef{
							APIVersion: "v1",
							Kind:       "ConfigMap",
							Metadata:   expv1alpha1.ExternalRefMetadata{Name: "gate"},
						},
					},
					{
						ID: "w",
						Template: environment.RawExt(t, map[string]any{
							"apiVersion": group + "/v1",
							"kind":       kind,
							"metadata":   map[string]any{"name": "w"},
							"spec":       map[string]any{"value": "${gate.data.k}"},
						}),
					},
				},
			},
		}
		env.CreateGraph(t, g)
		graphKey := types.NamespacedName{Namespace: ns, Name: "crd-gone"}
		env.AwaitCondition(t, graphKey, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 30*time.Second)
		env.AwaitObject(t, widgetGVK, widgetKey, nil, 15*time.Second)
		environment.Eventually(t, 10*time.Second, 100*time.Millisecond, func() error {
			got := env.GetGraph(t, graphKey)
			if len(got.Status.ManagedResources) != 1 || got.Status.ManagedResources[0].UID == "" {
				return fmt.Errorf("inventory not yet recorded with a UID: %+v", got.Status.ManagedResources)
			}
			return nil
		})

		if err := env.Client.Delete(ctx, gate); err != nil {
			t.Fatalf("delete gate: %v", err)
		}
		env.AwaitCondition(t, graphKey, expv1alpha1.GraphConditionTypeReady, metav1.ConditionFalse, 30*time.Second)
		if got := env.GetGraph(t, graphKey); len(got.Status.ManagedResources) != 1 {
			t.Fatalf("inventory must be preserved while the node is unresolved, got %+v", got.Status.ManagedResources)
		}

		// Remove the CRD: delete the lone instance ourselves and drop the
		// apiserver's cleanup finalizer, which starves under the shared control
		// plane's CRD churn (as crd_test.go does).
		w := &unstructured.Unstructured{}
		w.SetGroupVersionKind(widgetGVK)
		w.SetNamespace(ns)
		w.SetName("w")
		if err := env.Client.Delete(ctx, w); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("delete widget instance: %v", err)
		}
		env.AwaitDeleted(t, widgetGVK, widgetKey, 15*time.Second)
		if err := env.Client.Delete(ctx, crd); err != nil {
			t.Fatalf("delete CRD: %v", err)
		}
		unstickTerminatingCRD(ctx, crd.Name)
		environment.Eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
			err := env.Client.Get(ctx, types.NamespacedName{Name: crd.Name}, &apiextensionsv1.CustomResourceDefinition{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("CRD %s still present", crd.Name)
		})

		// The shared manager's mapper is warm, so the stale mapping 404s here; the
		// cold-mapper NoMatch case is covered in the executor's envtest suite.
		if err := env.Client.Delete(ctx, env.GetGraph(t, graphKey)); err != nil {
			t.Fatalf("delete graph: %v", err)
		}
		env.AwaitGraphGone(t, graphKey, 30*time.Second)
	})

	It("tears down UID-free write-ahead entries only for objects it verifiably applied", func() {
		t := GinkgoT()
		ns := env.CreateNamespace(t)
		ctx := env.Context()

		// The template node reads a ConfigMap that never exists, so it stays
		// Unresolved and the controller preserves staged inventory entries
		// verbatim instead of repairing their UIDs.
		g := &expv1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "write-ahead", Namespace: ns},
			Spec: expv1alpha1.GraphSpec{
				Nodes: []expv1alpha1.Node{
					{
						ID: "ext",
						Ref: &expv1alpha1.ExternalRef{
							APIVersion: "v1",
							Kind:       "ConfigMap",
							Metadata:   expv1alpha1.ExternalRefMetadata{Name: "never-created"},
						},
					},
					{
						ID: "owned",
						Template: environment.RawExt(t, map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata":   map[string]any{"name": "wa-owned"},
							"data":       map[string]any{"v": "${ext.data.k}"},
						}),
					},
				},
			},
		}
		env.CreateGraph(t, g)
		graphKey := types.NamespacedName{Namespace: ns, Name: "write-ahead"}
		env.AwaitCondition(t, graphKey, expv1alpha1.GraphConditionTypeAccepted, metav1.ConditionTrue, 30*time.Second)
		environment.Eventually(t, 15*time.Second, 100*time.Millisecond, func() error {
			if got := env.GetGraph(t, graphKey); got.Status.AppliedServiceAccount == "" {
				return fmt.Errorf("applied identity not yet recorded")
			}
			return nil
		})
		graphUID := env.GetGraph(t, graphKey).GetUID()

		// Live objects: one under this Graph's template manager, one under a peer
		// Graph's, one unmarked.
		ssa := func(name, manager string) {
			cm := &unstructured.Unstructured{}
			cm.SetGroupVersionKind(configMapGVK)
			cm.SetNamespace(ns)
			cm.SetName(name)
			if err := unstructured.SetNestedField(cm.Object, "v", "data", "k"); err != nil {
				t.Fatalf("set data: %v", err)
			}
			if err := env.Client.Patch(ctx, cm, client.Apply, client.FieldOwner(manager)); err != nil {
				t.Fatalf("ssa %s: %v", name, err)
			}
		}
		ssa("wa-owned", executor.TemplateFieldManager(graphUID))
		ssa("wa-peer", executor.TemplateFieldManager(types.UID("some-other-graph-uid")))
		bystander := &unstructured.Unstructured{}
		bystander.SetGroupVersionKind(configMapGVK)
		bystander.SetNamespace(ns)
		bystander.SetName("wa-bystander")
		if err := env.Client.Create(ctx, bystander); err != nil {
			t.Fatalf("create bystander: %v", err)
		}

		// UID-free entries for all three, under the Unresolved node's ID.
		entry := func(name string) expv1alpha1.ManagedResource {
			return expv1alpha1.ManagedResource{
				NodeID: "owned", APIVersion: "v1", Kind: "ConfigMap", Namespace: ns, Name: name,
			}
		}
		staged := []expv1alpha1.ManagedResource{entry("wa-owned"), entry("wa-peer"), entry("wa-bystander")}
		stagedPersisted := func() error {
			got := env.GetGraph(t, graphKey)
			if len(got.Status.ManagedResources) != len(staged) {
				return fmt.Errorf("staged inventory not in status: %+v", got.Status.ManagedResources)
			}
			for i, mr := range got.Status.ManagedResources {
				if mr != staged[i] {
					return fmt.Errorf("entry %d = %+v, want %+v", i, mr, staged[i])
				}
			}
			return nil
		}
		// A reconcile in flight before the patch may overwrite it; re-stage until
		// the entries survive a settling window.
		environment.Eventually(t, 30*time.Second, 200*time.Millisecond, func() error {
			current := env.GetGraph(t, graphKey)
			dc := current.DeepCopy()
			dc.Status.ManagedResources = staged
			if err := env.Client.Status().Patch(ctx, dc, client.MergeFrom(current)); err != nil {
				return fmt.Errorf("stage status: %w", err)
			}
			deadline := time.Now().Add(1500 * time.Millisecond)
			for time.Now().Before(deadline) {
				if err := stagedPersisted(); err != nil {
					return err
				}
				time.Sleep(100 * time.Millisecond)
			}
			return nil
		})

		if err := env.Client.Delete(ctx, env.GetGraph(t, graphKey)); err != nil {
			t.Fatalf("delete graph: %v", err)
		}
		env.AwaitGraphGone(t, graphKey, 30*time.Second)
		env.AwaitDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "wa-owned"}, 15*time.Second)

		for _, name := range []string{"wa-peer", "wa-bystander"} {
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"})
			if err := env.Client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
				t.Fatalf("%s must survive teardown of a Graph that never applied it: %v", name, err)
			}
		}
	})
})
