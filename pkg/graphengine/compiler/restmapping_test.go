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

package compiler

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
	"k8s.io/kube-openapi/pkg/validation/spec"

	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

// scriptedMapper answers RESTMapping from a per-call script and counts calls.
type scriptedMapper struct {
	*meta.DefaultRESTMapper
	script  []error // per-call error; nil means return mapping
	mapping *meta.RESTMapping
	calls   int
}

func (m *scriptedMapper) RESTMapping(schema.GroupKind, ...string) (*meta.RESTMapping, error) {
	i := m.calls
	m.calls++
	if i < len(m.script) && m.script[i] != nil {
		return nil, m.script[i]
	}
	return m.mapping, nil
}

// resettableScriptedMapper adds Reset() (meta.ResettableRESTMapper) and counts resets.
type resettableScriptedMapper struct {
	*scriptedMapper
	resets int
}

func (m *resettableScriptedMapper) Reset() { m.resets++ }

var _ meta.ResettableRESTMapper = (*resettableScriptedMapper)(nil)

func noMatchErr(gk schema.GroupKind) error {
	return &meta.NoKindMatchError{GroupKind: gk, SearchedVersions: []string{"v1"}}
}

// TestRestMapping_NoMatchResetsAndRetriesOnce pins the retry contract: exactly
// one Reset and one retry on a NoMatch, none for any other error.
func TestRestMapping_NoMatchResetsAndRetriesOnce(t *testing.T) {
	t.Parallel()

	gvk := schema.GroupVersionKind{Group: "spot.example.com", Version: "v1", Kind: "Widget"}
	want := &meta.RESTMapping{
		Resource:         schema.GroupVersionResource{Group: "spot.example.com", Version: "v1", Resource: "widgets"},
		GroupVersionKind: gvk,
		Scope:            meta.RESTScopeNamespace,
	}
	boom := errors.New("discovery: connection refused")

	cases := []struct {
		name        string
		script      []error
		resettable  bool
		wantErr     error // nil means success
		wantNoMatch bool
		wantCalls   int
		wantResets  int
	}{
		{
			name:       "success first try: no reset",
			script:     nil,
			resettable: true,
			wantCalls:  1,
			wantResets: 0,
		},
		{
			name:       "nomatch then success: one reset, one retry",
			script:     []error{noMatchErr(gvk.GroupKind()), nil},
			resettable: true,
			wantCalls:  2,
			wantResets: 1,
		},
		{
			name:        "persistent nomatch: retried exactly once, error surfaces",
			script:      []error{noMatchErr(gvk.GroupKind()), noMatchErr(gvk.GroupKind()), nil},
			resettable:  true,
			wantNoMatch: true,
			wantCalls:   2,
			wantResets:  1,
		},
		{
			name:       "non-nomatch error: not retried, no reset",
			script:     []error{boom, nil},
			resettable: true,
			wantErr:    boom,
			wantCalls:  1,
			wantResets: 0,
		},
		{
			name:        "nomatch on a mapper without Reset: retried once without a reset, nomatch surfaces",
			script:      []error{noMatchErr(gvk.GroupKind()), noMatchErr(gvk.GroupKind()), nil},
			resettable:  false,
			wantNoMatch: true,
			wantCalls:   2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := &scriptedMapper{
				DefaultRESTMapper: meta.NewDefaultRESTMapper(nil),
				script:            tc.script,
				mapping:           want,
			}
			var rm meta.RESTMapper = inner
			var resettable *resettableScriptedMapper
			if tc.resettable {
				resettable = &resettableScriptedMapper{scriptedMapper: inner}
				rm = resettable
			}
			ctx := newRootContext(nil, rm)

			got, err := ctx.restMapping(gvk)

			switch {
			case tc.wantNoMatch:
				require.Error(t, err)
				assert.True(t, meta.IsNoMatchError(err), "expected a NoMatch error, got %v", err)
				assert.Nil(t, got)
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
				assert.Nil(t, got)
			default:
				require.NoError(t, err)
				assert.Equal(t, want, got)
			}
			assert.Equal(t, tc.wantCalls, inner.calls, "RESTMapping call count")
			if resettable != nil {
				assert.Equal(t, tc.wantResets, resettable.resets, "Reset call count")
			}
		})
	}
}

// TestCompile_KindAddedAfterDiscoveryWarmsIsMappable compiles against the real
// DeferredDiscoveryRESTMapper: a Kind added to discovery after the first
// compile must be mappable by the next one without an external Reset.
func TestCompile_KindAddedAfterDiscoveryWarmsIsMappable(t *testing.T) {
	resolver, disco := testk8s.NewFakeResolver()
	rm := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	c := NewCompilerWithDependencies(resolver, rm)

	// First compile warms the discovery cache.
	_, err := c.Compile(generator.NewGraph("g", generator.WithTemplate("cm", configMap("cfg"))))
	require.NoError(t, err)

	// A CRD lands AFTER the cache is warm.
	widgetGVK := schema.GroupVersionKind{Group: "spot.example.com", Version: "v1", Kind: "Widget"}
	disco.Resources = append(disco.Resources, &metav1.APIResourceList{
		GroupVersion: "spot.example.com/v1",
		APIResources: []metav1.APIResource{{
			Name: "widgets", Namespaced: true, Kind: "Widget",
			Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		}},
	})
	resolver.AddSchema(widgetGVK, &spec.Schema{SchemaProps: spec.SchemaProps{
		Type: []string{"object"},
		Properties: map[string]spec.Schema{
			"apiVersion": {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
			"kind":       {SchemaProps: spec.SchemaProps{Type: []string{"string"}}},
			"metadata": {SchemaProps: spec.SchemaProps{
				Type:       []string{"object"},
				Properties: map[string]spec.Schema{"name": {SchemaProps: spec.SchemaProps{Type: []string{"string"}}}},
			}},
			"spec": {SchemaProps: spec.SchemaProps{
				Type:       []string{"object"},
				Properties: map[string]spec.Schema{"size": {SchemaProps: spec.SchemaProps{Type: []string{"integer"}}}},
			}},
		},
	}})

	// Premise: the warm mapper alone does not see the new Kind.
	_, err = rm.RESTMapping(widgetGVK.GroupKind(), widgetGVK.Version)
	require.True(t, meta.IsNoMatchError(err), "expected the warm deferred mapper to return NoMatch for a kind added after its refresh, got %v", err)

	prog, err := c.Compile(generator.NewGraph("g", generator.WithTemplate("w", map[string]any{
		"apiVersion": "spot.example.com/v1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "w1"},
		"spec":       map[string]any{"size": int64(3)},
	})))
	require.NoError(t, err, "a Kind created after the first compile must be mappable without a controller restart")
	n := prog.Nodes["w"]
	require.NotNil(t, n)
	assert.Equal(t, "widgets", n.GVR.Resource)
	assert.True(t, n.Namespaced)
}
