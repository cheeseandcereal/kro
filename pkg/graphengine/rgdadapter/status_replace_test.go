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

package rgdadapter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
	"k8s.io/kube-openapi/pkg/validation/spec"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

// newInstanceAwareCompiler builds a real compiler whose fake discovery and
// schema resolver also know the WebApp instance kind, so the synthesized
// author-status patch node (which targets the instance's own GVK) compiles.
func newInstanceAwareCompiler(t *testing.T) *compiler.Compiler {
	t.Helper()
	fakeResolver, disco := testk8s.NewFakeResolver()
	str := func() spec.Schema { return spec.Schema{SchemaProps: spec.SchemaProps{Type: []string{"string"}}} }
	fakeResolver.AddSchema(
		schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "WebApp"},
		&spec.Schema{SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"apiVersion": str(),
				"kind":       str(),
				"metadata": {SchemaProps: spec.SchemaProps{
					Type:       []string{"object"},
					Properties: map[string]spec.Schema{"name": str(), "namespace": str()},
				}},
				"spec": {SchemaProps: spec.SchemaProps{
					Type: []string{"object"},
					Properties: map[string]spec.Schema{
						"replicas": {SchemaProps: spec.SchemaProps{Type: []string{"integer"}}},
					},
				}},
				"status": {SchemaProps: spec.SchemaProps{
					Type:       []string{"object"},
					Properties: map[string]spec.Schema{"cmName": str()},
				}},
			},
		}},
	)
	disco.Resources = append(disco.Resources, &metav1.APIResourceList{
		GroupVersion: "kro.run/v1alpha1",
		APIResources: []metav1.APIResource{{
			Name:       "webapps",
			Namespaced: true,
			Kind:       "WebApp",
			Verbs:      []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		}},
	})
	rm := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	return compiler.NewCompilerWithDependencies(fakeResolver, rm)
}

// The synthesized author-status node must compile status-replace (alongside
// soft-deps, data-pending tolerance and self-watch exemption), and no other
// node may.
func TestBuildRuntimeForInstance_StatusNodeIsStatusReplace(t *testing.T) {
	t.Parallel()

	rgd := testRGD(&v1alpha1.Schema{
		Kind:       "WebApp",
		APIVersion: "v1alpha1",
		Group:      "kro.run",
		Spec:       apimachineryruntime.RawExtension{Raw: []byte(`{"replicas":"integer"}`)},
		Status:     apimachineryruntime.RawExtension{Raw: []byte(`{"cmName":"${cm.metadata.name}"}`)},
	})

	rt, g, err := BuildRuntimeForInstance(rgd, testInstance("demo", "default"), newInstanceAwareCompiler(t))
	require.NoError(t, err)
	require.NotNil(t, rt)

	var found bool
	for _, n := range g.Spec.Nodes {
		if n.ID == StatusPatchNodeID {
			found = n.Patch != nil
		}
	}
	require.True(t, found, "an RGD with author status fields must synthesize the status patch node")

	node := rt.Node(StatusPatchNodeID)
	require.NotNil(t, node, "the synthesized status node must be part of the compiled program")
	assert.Equal(t, compiler.NodeKindPatch, node.Kind())
	assert.Equal(t, "status", node.Subresource(), "the node targets the instance's status subresource")
	assert.True(t, node.StatusReplace(), "the author-status writeback must be status-replace")
	assert.True(t, node.SelfWatchExempt(), "the existing self-watch exemption must be preserved")
	assert.True(t, node.Spec().TolerateDataPending, "per-field data-pending tolerance must be preserved")
	assert.Empty(t, node.Spec().HardDepIDs(), "the status node never gates on the resources it reads")

	for _, id := range []string{SchemaNodeID, "cm"} {
		other := rt.Node(id)
		require.NotNil(t, other)
		assert.False(t, other.StatusReplace(), "node %q must not be status-replace", id)
	}
}
