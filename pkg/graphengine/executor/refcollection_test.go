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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
)

// refCollectionSelector turns the rendered metadata.selector of an external
// collection into a label selector. A missing or empty selector selects
// everything rather than nothing, so an author who omits it reads the whole
// kind instead of silently getting an empty list.
func TestRefCollectionSelector(t *testing.T) {
	t.Parallel()

	asUnstructured := func(t *testing.T, sel *metav1.LabelSelector) *unstructured.Unstructured {
		t.Helper()
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{},
		}}
		if sel != nil {
			raw, err := apimachineryruntime.DefaultUnstructuredConverter.ToUnstructured(sel)
			require.NoError(t, err)
			require.NoError(t, unstructured.SetNestedMap(obj.Object, raw, "metadata", "selector"))
		}
		return obj
	}

	t.Run("a missing selector selects everything", func(t *testing.T) {
		t.Parallel()
		sel, err := refCollectionSelector("coll", asUnstructured(t, nil))
		require.NoError(t, err)
		assert.True(t, sel.Empty(), "an absent selector must match everything")
	})

	t.Run("matchLabels is honoured", func(t *testing.T) {
		t.Parallel()
		sel, err := refCollectionSelector("coll", asUnstructured(t, &metav1.LabelSelector{
			MatchLabels: map[string]string{"tier": "db"},
		}))
		require.NoError(t, err)
		assert.True(t, sel.Matches(labels.Set{"tier": "db"}))
		assert.False(t, sel.Matches(labels.Set{"tier": "web"}))
	})

	t.Run("matchExpressions is honoured", func(t *testing.T) {
		t.Parallel()
		sel, err := refCollectionSelector("coll", asUnstructured(t, &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "tier",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"db", "cache"},
			}},
		}))
		require.NoError(t, err)
		assert.True(t, sel.Matches(labels.Set{"tier": "cache"}))
		assert.False(t, sel.Matches(labels.Set{"tier": "web"}))
	})

	t.Run("an invalid operator is an error, not a match-everything", func(t *testing.T) {
		t.Parallel()
		_, err := refCollectionSelector("coll", asUnstructured(t, &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "tier",
				Operator: metav1.LabelSelectorOperator("Bogus"),
			}},
		}))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "coll", "the error must name the offending node")
	})
}

// TestRefCollectionSelector_FailsClosed pins the strict decode of a rendered
// metadata.selector: malformed shapes are errors, not labels.Everything(). The
// objects are raw maps because a typed metav1.LabelSelector cannot express them.
func TestRefCollectionSelector_FailsClosed(t *testing.T) {
	t.Parallel()

	// withSelector sets metadata.selector even when nil (present-but-null).
	withSelector := func(selector any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{
				"namespace": "default",
				"selector":  selector,
			},
		}}
	}

	t.Run("malformed selectors are errors naming the node and the fault", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name     string
			selector any
			wantErr  string
		}{
			{
				name:     "misspelled matchLabels key",
				selector: map[string]any{"matchLabel": map[string]any{"tier": "db"}},
				wantErr:  `unknown field "matchLabel"`,
			},
			{
				name:     "bare label map without matchLabels",
				selector: map[string]any{"tier": "db"},
				wantErr:  `unknown field "tier"`,
			},
			{
				name: "unknown field in a matchExpressions item",
				selector: map[string]any{"matchExpressions": []any{
					map[string]any{"key": "tier", "operator": "In", "values": []any{"db"}, "bogus": true},
				}},
				wantErr: `unknown field "matchExpressions[0].bogus"`,
			},
			{
				name:     "matchLabels holding a string instead of a map",
				selector: map[string]any{"matchLabels": "tier=db"},
				wantErr:  "not a valid LabelSelector object",
			},
			{
				name:     "matchLabels value that is not a string",
				selector: map[string]any{"matchLabels": map[string]any{"tier": int64(3)}},
				wantErr:  "not a valid LabelSelector object",
			},
			{
				name:     "selector that is a string",
				selector: "tier=db",
				wantErr:  "must be a LabelSelector object",
			},
			{
				name:     "selector that is a list",
				selector: []any{map[string]any{"tier": "db"}},
				wantErr:  "must be a LabelSelector object",
			},
			{
				name:     "selector that is present but null",
				selector: nil,
				wantErr:  "must be a LabelSelector object",
			},
			{
				name:     "malformed label key",
				selector: map[string]any{"matchLabels": map[string]any{"bad key!": "db"}},
				wantErr:  "invalid label selector",
			},
			{
				name: "In operator without values",
				selector: map[string]any{"matchExpressions": []any{
					map[string]any{"key": "tier", "operator": "In"},
				}},
				wantErr: "invalid label selector",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				sel, err := refCollectionSelector("coll", withSelector(tc.selector))
				require.Error(t, err,
					"selector %#v resolved to %v, which would list every ConfigMap in scope", tc.selector, sel)
				assert.Contains(t, err.Error(), tc.wantErr)
				assert.Contains(t, err.Error(), `"coll"`, "the error must name the offending node")
			})
		}
	})

	t.Run("empty selectors select everything", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name string
			ref  *unstructured.Unstructured
		}{
			{name: "empty selector object", ref: withSelector(map[string]any{})},
			{name: "empty matchLabels", ref: withSelector(map[string]any{"matchLabels": map[string]any{}})},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				sel, err := refCollectionSelector("coll", tc.ref)
				require.NoError(t, err)
				assert.Equal(t, labels.Everything(), sel)
			})
		}
	})

	t.Run("well-formed raw selectors decode to the expected requirements", func(t *testing.T) {
		t.Parallel()
		sel, err := refCollectionSelector("coll", withSelector(map[string]any{
			"matchLabels": map[string]any{"tier": "db"},
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "NotIn", "values": []any{"test"}},
			},
		}))
		require.NoError(t, err)
		assert.Equal(t, "env notin (test),tier=db", sel.String())
	})
}

// TestSimple_ApplyRefCollectionMalformedSelectorFailsClosed drives a CEL-derived
// malformed selector through Apply: a hard error (not ErrNotReady, since a typo
// never self-heals), no drift watch, nothing published into scope.
func TestSimple_ApplyRefCollectionMalformedSelectorFailsClosed(t *testing.T) {
	t.Parallel()

	seed := func(name, tier string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{
				"name": name, "namespace": "default",
				"labels": map[string]any{"tier": tier},
			},
		}}
	}

	cases := []struct {
		name     string
		selector any
		wantErr  string
	}{
		{
			name:     "misspelled matchLabels key",
			selector: map[string]any{"matchLabel": map[string]any{"tier": "db"}},
			wantErr:  `unknown field "matchLabel"`,
		},
		{
			name:     "selector resolves to a string",
			selector: "tier=db",
			wantErr:  "must be a LabelSelector object",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithObjects(seed("db-a", "db"), seed("web", "web")).Build()

			// Route the selector through a def node so its value only exists
			// at apply time, like ${schema.spec.sel}.
			rt := compileAndBuild(t, generator.NewGraph("g",
				generator.WithNamespace("default"),
				generator.WithDef("input", map[string]any{"selector": tc.selector}),
				generator.WithRef("coll", &expv1alpha1.ExternalRef{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Metadata: expv1alpha1.ExternalRefMetadata{
						Namespace: "default",
						Selector:  apimachineryruntime.RawExtension{Raw: []byte(`"${input.selector}"`)},
					},
				}),
			))

			w := &recordingWatcher{}
			_, err := NewSimple(cl).Apply(context.Background(), rt, w)
			require.Error(t, err)
			assert.False(t, errors.Is(err, ErrNotReady),
				"a malformed selector is a permanent authoring error, not a soft not-ready")
			assert.Contains(t, err.Error(), `"coll"`)
			assert.Contains(t, err.Error(), tc.wantErr)

			assert.Empty(t, w.reqs, "no drift watch may be registered for a selector that failed to decode")
			_, published := rt.Scope()["coll"]
			assert.False(t, published, "a failed collection must not publish a match-everything list into scope")
		})
	}
}

// An external collection is read-only: kro lists the matching objects and
// publishes them into scope, but must never record them as managed resources.
// Recording one would make an object kro does not own a prune candidate.
func TestSimple_ApplyRefCollectionIsReadOnly(t *testing.T) {
	t.Parallel()

	seed := func(name, tier string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{
				"name": name, "namespace": "default",
				"labels": map[string]any{"tier": tier},
			},
			"data": map[string]any{"k": name},
		}}
		return obj
	}

	collectionGraph := func(sel *metav1.LabelSelector) *expv1alpha1.Graph {
		raw, err := json.Marshal(sel)
		require.NoError(t, err)
		return generator.NewGraph("g",
			generator.WithNamespace("default"),
			generator.WithRef("coll", &expv1alpha1.ExternalRef{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Metadata: expv1alpha1.ExternalRefMetadata{
					Namespace: "default",
					Selector:  apimachineryruntime.RawExtension{Raw: raw},
				},
			}),
		)
	}

	t.Run("matching objects are listed and none are recorded as managed", func(t *testing.T) {
		t.Parallel()
		cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(seed("db-a", "db"), seed("db-b", "db"), seed("web", "web")).Build()

		w := &recordingWatcher{}
		res, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, collectionGraph(&metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "db"},
			})), w)
		require.NoError(t, err)

		assert.Empty(t, res.Applied,
			"external collection members must never be recorded as managed resources")
		assert.Empty(t, res.Unresolved,
			"a successfully listed collection is resolved even when read-only")
	})

	t.Run("the collection watch carries the selector and scope", func(t *testing.T) {
		t.Parallel()
		cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(seed("db-a", "db")).Build()

		w := &recordingWatcher{}
		_, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, collectionGraph(&metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "db"},
			})), w)
		require.NoError(t, err)

		require.Len(t, w.reqs, 1, "a collection node declares exactly one watch")
		req := w.reqs[0]
		assert.Equal(t, "coll", req.NodeID)
		assert.Equal(t, "default", req.Namespace)
		require.NotNil(t, req.Selector, "a collection watch must carry a selector")
		assert.True(t, req.Selector.Matches(labels.Set{"tier": "db"}))
		assert.False(t, req.Selector.Matches(labels.Set{"tier": "web"}))
	})

	t.Run("an empty match set is not an error", func(t *testing.T) {
		t.Parallel()
		cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(seed("web", "web")).Build()

		res, err := NewSimple(cl).Apply(context.Background(),
			compileAndBuild(t, collectionGraph(&metav1.LabelSelector{
				MatchLabels: map[string]string{"tier": "db"},
			})), &recordingWatcher{})
		require.NoError(t, err, "matching nothing is a valid empty collection")
		assert.Empty(t, res.Applied)
	})
}
