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
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
)

// Use unstructured data so the typed field's omitempty cannot erase an explicit
// empty string before admission. A nil serviceAccountName means omission.
func graphWithServiceAccountName(ns, name string, serviceAccountName *string) *unstructured.Unstructured {
	spec := map[string]any{
		"nodes": []any{map[string]any{
			"id": "cm",
			"template": map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": name + "-cm"},
				"data":       map[string]any{"hello": "world"},
			},
		}},
	}
	if serviceAccountName != nil {
		spec["serviceAccountName"] = *serviceAccountName
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": expv1alpha1.GroupVersion.String(),
		"kind":       "Graph",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       spec,
	}}
}

var _ = Describe("Graph ServiceAccount", func() {
	DescribeTable("admits supported names and applies with the selected identity",
		func(serviceAccountName *string, appliedName string) {
			t := GinkgoT()
			ns := env.CreateNamespace(t)
			ctx := env.Context()
			g := graphWithServiceAccountName(ns, "serviceaccount", serviceAccountName)
			if err := env.Client.Create(ctx, g); err != nil {
				t.Fatalf("create Graph: %v", err)
			}
			key := types.NamespacedName{Namespace: ns, Name: g.GetName()}
			t.Cleanup(func() {
				if err := env.Client.Delete(context.Background(), g); err != nil && !apierrors.IsNotFound(err) {
					t.Fatalf("delete Graph: %v", err)
				}
				env.AwaitGraphGone(t, key, 15*time.Second)
			})

			value, found, err := unstructured.NestedString(g.Object, "spec", "serviceAccountName")
			if err != nil || found != (serviceAccountName != nil) || (found && value != *serviceAccountName) {
				t.Fatalf("admission changed serviceAccountName: value=%q, found=%v, err=%v", value, found, err)
			}
			env.AwaitCondition(t, key, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 15*time.Second)
			env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "serviceaccount-cm"},
				func(u *unstructured.Unstructured) error {
					value, _, _ := unstructured.NestedString(u.Object, "data", "hello")
					if value != "world" {
						return fmt.Errorf("data.hello=%q, want world", value)
					}
					return nil
				}, 15*time.Second)

			got := env.GetGraph(t, key)
			want := fmt.Sprintf("system:serviceaccount:%s:%s", ns, appliedName)
			if got.Status.AppliedServiceAccount != want {
				t.Fatalf("status.appliedServiceAccount=%q, want %q", got.Status.AppliedServiceAccount, want)
			}
			t.Logf("Graph %s: serviceAccountName=%q (present=%v), Ready=True, ConfigMap applied, identity=%s", key, value, found, want)
		},
		Entry("omitted name uses the namespace default", (*string)(nil), "default"),
		Entry("explicit empty string uses the namespace default", new(""), "default"),
		Entry("dotted name", new("my.service.account"), "my.service.account"),
		Entry("253-character name", new(strings.Repeat("a", 253)), strings.Repeat("a", 253)),
	)

	DescribeTable("rejects invalid names at admission", func(serviceAccountName string) {
		t := GinkgoT()
		ns := env.CreateNamespace(t)
		g := graphWithServiceAccountName(ns, "invalid-serviceaccount", &serviceAccountName)
		err := env.Client.Create(env.Context(), g, client.DryRunAll)
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.serviceAccountName") {
			t.Fatalf("create Graph with serviceAccountName %q: want Invalid on that field, got %v", serviceAccountName, err)
		}
	},
		Entry("whitespace", " "),
		Entry("malformed name", "Bad_Name"),
		Entry("254-character name", strings.Repeat("a", 254)),
	)
})
