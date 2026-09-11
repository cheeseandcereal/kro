// Copyright 2025 The Kube Resource Orchestrator Authors
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
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

var _ = Describe("Graph write-ahead teardown", func() {
	DescribeTable("cleans up an applied child whose inventory UID was not persisted", func(prune bool) {
		t := GinkgoT()
		ctx := env.Context()
		ns := env.CreateNamespace(t)
		source := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"namespace": ns, "name": "source"},
			"data":     map[string]any{"value": "ready"},
		}}
		require.NoError(t, env.Client.Create(ctx, source))
		g := env.CreateGraph(t, &expv1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "writeahead", Namespace: ns},
			Spec: expv1alpha1.GraphSpec{Nodes: []expv1alpha1.Node{
				{ID: "source", Ref: &expv1alpha1.ExternalRef{
					APIVersion: "v1", Kind: "ConfigMap",
					Metadata: expv1alpha1.ExternalRefMetadata{Name: "source"},
				}},
				{ID: "child", Template: environment.RawExt(t, map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": "writeahead-child"},
					"data":     map[string]any{"value": "${source.data.value}"},
				})},
			}},
		})
		key := client.ObjectKeyFromObject(g)
		env.AwaitCondition(t, key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 20*time.Second)
		childKey := client.ObjectKey{Namespace: ns, Name: "writeahead-child"}
		child := env.AwaitObject(t, configMapGVK, childKey, nil, 5*time.Second)
		t.Cleanup(func() { _ = env.Client.Delete(ctx, child) })
		current := env.GetGraph(t, key)
		require.Len(t, current.Status.ManagedResources, 1)
		require.Equal(t, string(child.GetUID()), current.Status.ManagedResources[0].UID)
		require.NotEmpty(t, child.GetManagedFields(), "use the real Apply ownership metadata")

		// Hold the node unresolved so no subsequent Apply restores its UID.
		require.NoError(t, env.Client.Delete(ctx, source))
		env.AwaitCondition(t, key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionFalse, 20*time.Second)
		environment.Eventually(t, 10*time.Second, 100*time.Millisecond, func() error {
			current = env.GetGraph(t, key)
			if len(current.Status.ManagedResources) != 1 {
				return fmt.Errorf("expected retained child inventory, got %v", current.Status.ManagedResources)
			}
			current.Status.ManagedResources[0].UID = ""
			return env.Client.Status().Update(ctx, current)
		})
		environment.Consistently(t, time.Second, 100*time.Millisecond, func() error {
			current = env.GetGraph(t, key)
			if len(current.Status.ManagedResources) != 1 || current.Status.ManagedResources[0].UID != "" {
				return fmt.Errorf("write-ahead inventory was replaced: %v", current.Status.ManagedResources)
			}
			return nil
		})

		if prune {
			env.UpdateGraphSpec(t, key, func(g *expv1alpha1.Graph) {
				g.Spec.Nodes = g.Spec.Nodes[:1] // retire only the child's template
			})
			environment.Eventually(t, 10*time.Second, 100*time.Millisecond, func() error {
				current = env.GetGraph(t, key)
				if len(current.Status.ManagedResources) != 0 {
					return fmt.Errorf("retired child is still tracked: %v", current.Status.ManagedResources)
				}
				return nil
			})
		} else {
			require.NoError(t, env.Client.Delete(ctx, current))
			require.Eventually(t, func() bool {
				return apierrors.IsNotFound(env.Client.Get(ctx, key, &expv1alpha1.Graph{}))
			}, 15*time.Second, 100*time.Millisecond, "Graph must release its finalizer")
		}
		require.Eventually(t, func() bool {
			return apierrors.IsNotFound(env.Client.Get(ctx, childKey, child))
		}, 5*time.Second, 100*time.Millisecond, "UID-free owned child must not be orphaned")
	},
		Entry("on Graph deletion", false),
		Entry("when its node is retired", true),
	)
})
