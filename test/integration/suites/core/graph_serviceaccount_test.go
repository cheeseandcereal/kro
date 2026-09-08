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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
)

// graphWithServiceAccountName builds a Graph as an unstructured object so the
// serviceAccountName value is sent verbatim (the typed struct's omitempty would
// drop an empty string).
func graphWithServiceAccountName(ns, name, sa string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": expv1alpha1.GroupVersion.String(),
		"kind":       "Graph",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"serviceAccountName": sa,
			"nodes": []any{map[string]any{
				"id": "cm",
				"template": map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": name + "-cm"},
					"data":       map[string]any{"hello": "world"},
				},
			}},
		},
	}}
}

var _ = Describe("Graph ServiceAccount", func() {
	// An explicit serviceAccountName: "" (what Helm/Kustomize render for an unset
	// value) must be admitted and behave like an omitted field.
	It("admits an explicit empty serviceAccountName and applies as the namespace default", func() {
		t := GinkgoT()
		ns := env.CreateNamespace(t)
		ctx := env.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		u := graphWithServiceAccountName(ns, "empty-sa", "")
		if err := env.Client.Create(ctx, u); err != nil {
			t.Fatalf("create Graph with serviceAccountName \"\": %v (the CRD pattern must admit the explicit empty value)", err)
		}
		key := types.NamespacedName{Namespace: ns, Name: "empty-sa"}
		t.Cleanup(func() {
			cur := &expv1alpha1.Graph{}
			if err := env.Client.Get(context.Background(), key, cur); err == nil {
				_ = env.Client.Delete(context.Background(), cur)
			}
		})

		env.AwaitCondition(t, key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 15*time.Second)
		env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "empty-sa-cm"}, nil, 15*time.Second)

		// The empty value resolved to the namespace default ServiceAccount.
		got := env.GetGraph(t, key)
		want := fmt.Sprintf("system:serviceaccount:%s:default", ns)
		if got.Status.AppliedServiceAccount != want {
			t.Fatalf("status.appliedServiceAccount=%q want %q", got.Status.AppliedServiceAccount, want)
		}
	})
})
