// Copyright 2026 The Kube Resource Orchestrator Authors.
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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kubernetes-sigs/kro/pkg/applyset"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// sharedCM renders a ConfigMap template named "shared" whose data.from records
// which node produced it.
func sharedCM(from string) map[string]any {
	return map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "shared"},
		"data":     map[string]any{"from": from},
	}
}

// previousSharedCM is a user-owned live ConfigMap default/shared (no kro labels).
func previousSharedCM() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "shared", "namespace": "default"},
		"data":     map[string]any{"from": "previous"},
	}}
}

// writeCounter counts every mutating request issued through the fake client.
type writeCounter struct{ n atomic.Int64 }

func (c *writeCounter) funcs() interceptor.Funcs {
	count := func() { c.n.Add(1) }
	return interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			count()
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			count()
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			count()
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Apply: func(ctx context.Context, cl client.WithWatch, obj apimachineryruntime.ApplyConfiguration, opts ...client.ApplyOption) error {
			count()
			return cl.Apply(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			count()
			return cl.Delete(ctx, obj, opts...)
		},
		DeleteAllOf: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteAllOfOption) error {
			count()
			return cl.DeleteAllOf(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			count()
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			count()
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
}

// rgdPathLabeler stamps the labels the instance controller's labeler would
// (applyset part-of, kro.run/owned); a live object carrying them was adopted.
func rgdPathLabeler(obj *unstructured.Unstructured) {
	l := obj.GetLabels()
	if l == nil {
		l = map[string]string{}
	}
	l[applyset.ApplysetPartOfLabel] = "applyset-test"
	l[metadata.OwnedLabel] = "true"
	obj.SetLabels(l)
}

func getSharedCM(t *testing.T, cl client.Client) *unstructured.Unstructured {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(configMapGVK)
	require.NoError(t, cl.Get(context.Background(),
		types.NamespacedName{Namespace: "default", Name: "shared"}, got))
	return got
}

// TestApply_RejectsDuplicateIdentityBeforeWrite: two template nodes with static
// names rendering the same object are rejected by the pre-walk pass before ANY
// write — a pre-existing user object is neither modified nor adopted (labelled).
func TestApply_RejectsDuplicateIdentityBeforeWrite(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm1", sharedCM("cm1")),
		generator.WithTemplate("cm2", sharedCM("cm2")),
	)

	writes := &writeCounter{}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(previousSharedCM()).
		WithInterceptorFuncs(writes.funcs()).
		Build()
	before := getSharedCM(t, cl)

	res, err := NewSimple(cl).ApplyWithLabeler(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{}, rgdPathLabeler)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateIdentity),
		"a cross-node duplicate identity must surface ErrDuplicateIdentity, got %v", err)
	assert.False(t, errors.Is(err, ErrNotReady),
		"a duplicate identity is a permanent graph error, not a soft not-ready")
	assert.Contains(t, err.Error(), `nodes "cm1" and "cm2" both render v1/ConfigMap/default/shared`)

	assert.Empty(t, res.Applied, "nothing may be recorded as applied when the collision is caught pre-walk")
	assert.Zero(t, writes.n.Load(), "no apply/patch/create request may reach the cluster")

	after := getSharedCM(t, cl)
	assert.Equal(t, before.Object, after.Object, "the user's live object must be byte-identical")
	data, _, _ := unstructured.NestedStringMap(after.Object, "data")
	assert.Equal(t, "previous", data["from"])
	assert.NotContains(t, after.GetLabels(), applyset.ApplysetPartOfLabel, "the user's object must not be adopted into the applyset")
	assert.NotContains(t, after.GetLabels(), metadata.OwnedLabel)
	assert.NotContains(t, after.GetLabels(), metadata.NodeIDLabel)
}

// TestApply_RejectsDuplicateIdentityWithinOneCollection pins finding 3901183693:
// two rows of a SINGLE forEach collection node that resolve to the same final
// object identity must be rejected with a hard ErrDuplicateIdentity, not applied
// concurrently with a nondeterministic last-writer win reported as success.
// The forEach iterator appears in metadata.namespace (satisfying the compile-
// time uniqueness guard), but the values "" and "default" both default to the
// Graph namespace "default", so both rows resolve to ConfigMap default/shared.
// (The Def-sourced axis is not resolvable pre-walk; the per-item claim catches it.)
func TestApply_RejectsDuplicateIdentityWithinOneCollection(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("src", map[string]any{"namespaces": []any{"", "default"}}),
		generator.WithTemplate("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "shared", "namespace": "${ns}"},
			"data":     map[string]any{"k": "v"},
		}, generator.ForEachDim("ns", "${src.namespaces}")),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	_, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateIdentity),
		"two collection rows colliding on one identity must surface ErrDuplicateIdentity, got %v", err)
}

// TestApply_RejectsResolvableCollectionSelfCollisionBeforeWrite: with a literal
// (pre-walk resolvable) axis, the same collision is caught by the pre-walk pass
// before either row is written.
func TestApply_RejectsResolvableCollectionSelfCollisionBeforeWrite(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "shared", "namespace": "${ns}"},
			"data":     map[string]any{"k": "v"},
		}, generator.ForEachDim("ns", "${['', 'default']}")),
	)

	writes := &writeCounter{}
	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithInterceptorFuncs(writes.funcs()).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateIdentity), "got %v", err)
	assert.Contains(t, err.Error(), `node "cm" renders v1/ConfigMap/default/shared more than once`)
	assert.Empty(t, res.Applied)
	assert.Zero(t, writes.n.Load(), "neither row may be written")

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(configMapGVK)
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "shared"}, got)
	assert.True(t, apierrors.IsNotFound(err), "the object must not exist, got err=%v", err)
}

// TestApply_DynamicallyNamedDuplicateIsStillRejectedDuringWalk pins the
// coverage limit of the pre-walk pass: cm2's name depends on cm1's published
// output, so it is only claimed per item during the walk — after cm1 has been
// applied — and the collision still rejects cm2 before its write.
func TestApply_DynamicallyNamedDuplicateIsStillRejectedDuringWalk(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm1", sharedCM("cm1")),
		generator.WithTemplate("cm2", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${cm1.metadata.name}"},
			"data":     map[string]any{"from": "cm2"},
		}),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(previousSharedCM()).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateIdentity), "got %v", err)
	assert.Contains(t, err.Error(), `nodes "cm1" and "cm2" both render v1/ConfigMap/default/shared`)

	// cm1 landed; cm2 was refused before its write.
	require.Len(t, res.Applied, 1)
	assert.Equal(t, "cm1", res.Applied[0].NodeID)
	data, _, _ := unstructured.NestedStringMap(getSharedCM(t, cl).Object, "data")
	assert.Equal(t, "cm1", data["from"], "cm2 must never overwrite the object")
}

// TestApply_WalkClaimCannotConsumeAnotherNodesReservation: a dynamically named
// node reached before the static node that reserved the same identity is
// refused (a reservation is only consumable by its owner), so the reserving
// node still writes its object.
func TestApply_WalkClaimCannotConsumeAnotherNodesReservation(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("src", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "src"},
			"data":     map[string]any{"target": "shared"},
		}),
		generator.WithTemplate("dyn", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${src.data.target}"},
			"data":     map[string]any{"from": "dyn"},
		}),
		generator.WithTemplate("static", sharedCM("static")),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `nodes "static" and "dyn" both render v1/ConfigMap/default/shared`)
	ids := make([]string, 0, len(res.Applied))
	for _, mr := range res.Applied {
		ids = append(ids, mr.NodeID)
	}
	assert.ElementsMatch(t, []string{"src", "static"}, ids)
	data, _, _ := unstructured.NestedStringMap(getSharedCM(t, cl).Object, "data")
	assert.Equal(t, "static", data["from"], "dyn must not write under static's reservation")
}

// TestApply_ChildFrameCollisionIsRejectedBeforeAnyChildWrite: a subgraph child
// frame runs the pre-walk pass too, so two colliding child nodes are refused
// before any child write while the parent's own node is unaffected.
func TestApply_ChildFrameCollisionIsRejectedBeforeAnyChildWrite(t *testing.T) {
	t.Parallel()

	childCM := func(from string) map[string]any {
		return map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "child-shared"},
			"data":     map[string]any{"from": from},
		}
	}
	child := generator.NewGraph("child",
		generator.WithNamespace("default"),
		generator.WithTemplate("resA", childCM("sub/resA")),
		generator.WithTemplate("resB", childCM("sub/resB")),
	)
	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm", sharedCM("cm")),
		generator.WithSubgraph("sub", child),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateIdentity), "got %v", err)
	assert.Contains(t, err.Error(), `nodes "sub/resA" and "sub/resB" both render v1/ConfigMap/default/child-shared`)

	require.Len(t, res.Applied, 1, "only the parent's own node may have been applied")
	assert.Equal(t, "cm", res.Applied[0].NodeID)
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(configMapGVK)
	getErr := cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "child-shared"}, got)
	assert.True(t, apierrors.IsNotFound(getErr), "neither child node may write, got err=%v", getErr)
}

// TestApply_ChildFrameCollisionWithParentIdentityIsRejected: a child node's
// pre-walk reservation fails against an identity the parent frame already
// claimed (shared claim set), so the child never writes.
func TestApply_ChildFrameCollisionWithParentIdentityIsRejected(t *testing.T) {
	t.Parallel()

	child := generator.NewGraph("child",
		generator.WithNamespace("default"),
		generator.WithTemplate("res", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			// Captured from the parent frame, so resolvable in the child's pre-pass.
			"metadata": map[string]any{"name": "${cm.metadata.name}"},
			"data":     map[string]any{"from": "sub/res"},
		}),
	)
	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm", sharedCM("cm")),
		generator.WithSubgraph("sub", child),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDuplicateIdentity), "got %v", err)
	assert.Contains(t, err.Error(), `nodes "cm" and "sub/res" both render v1/ConfigMap/default/shared`)

	require.Len(t, res.Applied, 1, "only the parent node may have been applied")
	assert.Equal(t, "cm", res.Applied[0].NodeID)
	data, _, _ := unstructured.NestedStringMap(getSharedCM(t, cl).Object, "data")
	assert.Equal(t, "cm", data["from"], "the child node must never write")
}

// TestApply_AllowsDistinctIdentities confirms the guard does not false-positive
// on two nodes that render DIFFERENT identities.
func TestApply_AllowsDistinctIdentities(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("cm1", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "one"},
			"data":     map[string]any{"k": "v"},
		}),
		generator.WithTemplate("cm2", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "two"},
			"data":     map[string]any{"k": "v"},
		}),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.NoError(t, err, "distinct identities must not trip the duplicate guard")
	assert.Len(t, res.Applied, 2, "both distinct objects apply")
}

// TestApply_WalkReclaimOfPreWalkReservationIsNotADuplicate: the walk's per-item
// claim consumes the node's own pre-walk reservation (scalar and every
// collection row) instead of reporting it as a duplicate.
func TestApply_WalkReclaimOfPreWalkReservationIsNotADuplicate(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("single", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "single"},
			"data":     map[string]any{"k": "v"},
		}),
		generator.WithTemplate("many", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "many-${n}"},
			"data":     map[string]any{"k": "v"},
		}, generator.ForEachDim("n", "${['a', 'b', 'c']}")),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.NoError(t, err, "a node re-claiming what it reserved pre-walk is not a duplicate")
	assert.Len(t, res.Applied, 4)
	for _, name := range []string{"single", "many-a", "many-b", "many-c"} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(configMapGVK)
		require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, got), "missing %q", name)
	}
}

// TestApply_ExcludedNodeReservesNoIdentity: an includeWhen:false node reserves
// nothing, so a sibling rendering the same identity applies cleanly.
func TestApply_ExcludedNodeReservesNoIdentity(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithTemplate("off", sharedCM("off")),
		generator.WithIncludeWhen("${false}"),
		generator.WithTemplate("on", sharedCM("on")),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.NoError(t, err)
	require.Len(t, res.Applied, 1)
	assert.Equal(t, "on", res.Applied[0].NodeID)
	data, _, _ := unstructured.NestedStringMap(getSharedCM(t, cl).Object, "data")
	assert.Equal(t, "on", data["from"])
}

// TestApply_UndecidableIncludeWhenReservesNoIdentity: a node whose includeWhen
// is still data-pending before the walk (here on a Def not yet published)
// reserves nothing, so two mutually exclusive writers of one identity are not
// a false duplicate.
func TestApply_UndecidableIncludeWhenReservesNoIdentity(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("selector", map[string]any{"mode": "b"}),
		generator.WithTemplate("modeA", sharedCM("modeA")),
		generator.WithIncludeWhen("${selector.mode == 'a'}"),
		generator.WithTemplate("modeB", sharedCM("modeB")),
		generator.WithIncludeWhen("${selector.mode == 'b'}"),
	)

	cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuild(t, g), watchrouter.NoopWatcher{})

	require.NoError(t, err, "mutually exclusive writers of one identity must not be reported as a duplicate")
	require.Len(t, res.Applied, 1)
	assert.Equal(t, "modeB", res.Applied[0].NodeID)
	data, _, _ := unstructured.NestedStringMap(getSharedCM(t, cl).Object, "data")
	assert.Equal(t, "modeB", data["from"])
}

// TestApply_SoftDepPlaceholderIsNotReserved pins the pre-walk pass's purity
// gate. "summary" soft-references template "a" (WithSoftDependencies, as the RGD
// adapter compiles its status node), so a is seeded {} before the walk; "b"
// names itself from a via optional chaining (fallback against the placeholder,
// "real" against a's published value) and "c" statically renders the fallback.
// The pass must not reserve b's placeholder-derived identity, or b/c would be a
// false duplicate. The Def-mediated row pins that purity is transitive.
func TestApply_SoftDepPlaceholderIsNotReserved(t *testing.T) {
	t.Parallel()

	realCM := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": "a"},
		"data":     map[string]any{"k": "real"},
	}
	namedCM := func(nameExpr string) map[string]any {
		return map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": nameExpr},
			"data":     map[string]any{"k": "v"},
		}
	}
	cases := []struct {
		name string
		mid  []generator.GraphOption // the node(s) between a and c
	}{
		{
			name: "identity optional-chains the soft-seeded template directly",
			mid:  []generator.GraphOption{generator.WithTemplate("b", namedCM("${a.?data.k.orValue('fallback')}"))},
		},
		{
			name: "identity reads a Def that optional-chains the soft-seeded template",
			mid: []generator.GraphOption{
				generator.WithDef("d", map[string]any{"name": "${a.?data.k.orValue('fallback')}"}),
				generator.WithTemplate("b", namedCM("${d.name}")),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := []generator.GraphOption{
				generator.WithNamespace("default"),
				generator.WithTemplate("a", realCM),
			}
			opts = append(opts, tc.mid...)
			opts = append(opts,
				generator.WithTemplate("c", namedCM("fallback")),
				generator.WithDef("summary", map[string]any{"aVal": "${a.data.k}"}),
			)
			rt := compileAndBuild(t, generator.NewGraph("g", opts...), compiler.WithSoftDependencies("summary"))
			require.Equal(t, map[string]any{}, rt.Scope()["a"], "precondition: a is seeded as a soft-dependency placeholder")
			// Seed Def nodes into scope exactly as both controllers do before Apply.
			for _, n := range rt.Nodes() {
				if n.Kind() != compiler.NodeKindDef {
					continue
				}
				if desired, err := n.Resolve(); err == nil && len(desired) > 0 {
					n.SetObserved(desired, desired)
					rt.Set(n.ID(), desired[0].Object)
				}
			}

			cl := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
			res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})

			require.NoError(t, err, "a placeholder-derived identity must not be reserved against a node that really renders it")
			ids := make([]string, 0, len(res.Applied))
			for _, mr := range res.Applied {
				ids = append(ids, mr.NodeID)
			}
			assert.ElementsMatch(t, []string{"a", "b", "c"}, ids)
			for _, name := range []string{"a", "real", "fallback"} {
				got := &unstructured.Unstructured{}
				got.SetGroupVersionKind(configMapGVK)
				require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, got), "missing %q", name)
			}
		})
	}
}
