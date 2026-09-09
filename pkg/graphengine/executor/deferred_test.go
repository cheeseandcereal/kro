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

package executor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	krotruntime "github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

var graphGVK = schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "Graph"}

// newGraphAwareCompiler extends the fake resolver and discovery with the
// kro.run/v1alpha1 Graph kind (spec is preserve-unknown-fields, like the real
// CRD) so a parent Graph can template child Graph objects in tests.
func newGraphAwareCompiler(t *testing.T) *compiler.Compiler {
	t.Helper()
	r, disco := testk8s.NewFakeResolver()
	r.AddSchema(graphGVK, &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"apiVersion": {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
				"kind":       {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
				"metadata": {
					VendorExtensible: spec.VendorExtensible{Extensions: spec.Extensions{"x-kubernetes-preserve-unknown-fields": true}},
					SchemaProps:      spec.SchemaProps{Type: []string{"object"}},
				},
				"spec": {
					VendorExtensible: spec.VendorExtensible{Extensions: spec.Extensions{"x-kubernetes-preserve-unknown-fields": true}},
					SchemaProps:      spec.SchemaProps{Type: []string{"object"}},
				},
			},
		},
	})
	disco.Resources = append(disco.Resources, &metav1.APIResourceList{
		GroupVersion: "kro.run/v1alpha1",
		APIResources: []metav1.APIResource{{
			Name: "graphs", Namespaced: true, Kind: "Graph",
			Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		}},
	})
	rm := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	return compiler.NewCompilerWithDependencies(r, rm)
}

// TestSimple_Apply_DeferredExpressions stamps child Graph objects whose
// template mixes parent-evaluated and $${...} deferred expressions, and
// checks the applied child carries the deferred text verbatim — exactly one
// dollar sign peeled — while parent-evaluated fields are resolved.
func TestSimple_Apply_DeferredExpressions(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("parent",
		generator.WithNamespace("default"),
		generator.WithDef("teams", map[string]any{"names": []any{"alpha", "beta"}}),
		generator.WithTemplate("children", map[string]any{
			"apiVersion": "kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata":   map[string]any{"name": "team-${team}"},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{"id": "cfg", "def": map[string]any{"team": "${team}"}},
					map[string]any{
						"id": "cm",
						"template": map[string]any{
							"apiVersion": "v1", "kind": "ConfigMap",
							"metadata": map[string]any{"name": "$${cfg.team}"},
							"data": map[string]any{
								"label":  "${team}-$${cfg.team}",
								"upper":  "${'${' + 'cfg.team.upperAscii()' + '}'}",
								"deep":   "$$${leaf.value}",
								"quoted": `$${cfg.team == "alpha" ? 'yes' : 'no'}`,
							},
						},
					},
				},
			},
		}, expv1alpha1.ForEachDimension{"team": "${teams.names}"}),
	)

	prog, err := newGraphAwareCompiler(t).Compile(g)
	require.NoError(t, err)
	rt := krotruntime.New(prog, g)
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()

	res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.NoError(t, err)
	require.Len(t, res.Applied, 2)

	for _, team := range []string{"alpha", "beta"} {
		child := &unstructured.Unstructured{}
		child.SetGroupVersionKind(graphGVK)
		require.NoError(t, cl.Get(context.Background(),
			types.NamespacedName{Namespace: "default", Name: "team-" + team}, child))

		nodes, _, err := unstructured.NestedSlice(child.Object, "spec", "nodes")
		require.NoError(t, err)
		require.Len(t, nodes, 2)

		cfg := nodes[0].(map[string]any)
		assert.Equal(t, team, cfg["def"].(map[string]any)["team"], "parent-evaluated def is baked in")

		tmpl := nodes[1].(map[string]any)["template"].(map[string]any)
		assert.Equal(t, "${cfg.team}", tmpl["metadata"].(map[string]any)["name"],
			"deferred name reaches the child as an expression")
		data := tmpl["data"].(map[string]any)
		assert.Equal(t, team+"-${cfg.team}", data["label"], "mixed: parent half evaluated, child half deferred")
		assert.Equal(t, "${cfg.team.upperAscii()}", data["upper"], "dynamically built child expression")
		assert.Equal(t, "$${leaf.value}", data["deep"], "two-level deferral peels exactly one dollar")
		assert.Equal(t, `${cfg.team == "alpha" ? 'yes' : 'no'}`, data["quoted"], "quotes in the body survive verbatim")
	}
}
