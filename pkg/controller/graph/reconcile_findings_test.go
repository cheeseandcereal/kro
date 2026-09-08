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

package graph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	memory "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/restmapper"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
	krotruntime "github.com/kubernetes-sigs/kro/pkg/graphengine/runtime"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
	testk8s "github.com/kubernetes-sigs/kro/pkg/testutil/k8s"
)

// templateProgram builds a minimal compiled Program with a single static
// Template node whose rendered identity is (apiVersion, kind, namespace,
// name). No Variables/ForEach/IncludeWhen, so the node resolves in memory to
// exactly that object — enough for intendedManagedResources to project it.
func templateProgram(nodeID, apiVersion, kind, namespace, name string) *compiler.Program {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
	}}
	gv, _ := schema.ParseGroupVersion(apiVersion)
	node := &compiler.Node{
		ID:         nodeID,
		Kind:       compiler.NodeKindTemplate,
		GVR:        gv.WithKind(kind).GroupVersion().WithResource(kind),
		Namespaced: namespace != "",
		Object:     obj,
	}
	return &compiler.Program{
		Nodes:            map[string]*compiler.Node{nodeID: node},
		TopologicalOrder: []string{nodeID},
	}
}

// emptyNodeProgram builds a Program whose single node has no payload, so
// intendedManagedResources projects nothing and the pre-apply write-ahead is
// skipped. Used to isolate reconcile paths from the Finding A write-ahead.
func emptyNodeProgram(nodeID string) *compiler.Program {
	node := &compiler.Node{ID: nodeID, Kind: compiler.NodeKindTemplate}
	return &compiler.Program{
		Nodes:            map[string]*compiler.Node{nodeID: node},
		TopologicalOrder: []string{nodeID},
	}
}

// applyObservingExecutor records the ManagedResources and Contributions
// persisted on the API server at the moment Apply is entered, so a test can
// assert the write-ahead landed before any resource was applied.
type applyObservingExecutor struct {
	fakeExecutor
	cl  client.Client
	key types.NamespacedName
	// persistedAtApply is the server-side inventory observed when Apply ran.
	persistedAtApply []expv1alpha1.ManagedResource
	// contribsAtApply is the server-side ledger observed when Apply ran.
	contribsAtApply []expv1alpha1.Contribution
	observed        bool
}

func (e *applyObservingExecutor) Apply(ctx context.Context, rt *krotruntime.Runtime, w watchrouter.Watcher) (executor.ApplyResult, error) {
	got := &expv1alpha1.Graph{}
	if err := e.cl.Get(ctx, e.key, got); err == nil {
		e.persistedAtApply = got.Status.ManagedResources
		e.contribsAtApply = got.Status.Contributions
		e.observed = true
	}
	return e.fakeExecutor.Apply(ctx, rt, w)
}

// TestReconcile_WriteAheadIntentPersistedBeforeApply is the Finding A
// regression: the inventory teardown depends on must be durable on the API
// server BEFORE Apply creates any child. Before the fix the reconciler applied
// first and persisted the inventory only afterwards, so a lost status write
// after apply orphaned children (delete would see 0 entries). The fix
// write-aheads the union of previous + intended identities before Apply.
func TestReconcile_WriteAheadIntentPersistedBeforeApply(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	g := graph("g", withFinalizer)
	cl := newClient(t, g)

	obs := &applyObservingExecutor{cl: cl, key: key}
	fc := &fakeCompiler{program: templateProgram("widget", "example.com/v1", "Widget", "default", "w")}
	r := &Reconciler{Client: cl, Compiler: fc, Registry: registry.New(), Executor: obs}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	require.True(t, obs.observed, "Apply must have been called")
	// The server-side inventory observed AT apply time must already contain
	// the intended Widget identity. Pre-fix this slice is empty.
	require.Len(t, obs.persistedAtApply, 1,
		"pre-apply intent must be persisted to the API server before Apply runs")
	mr := obs.persistedAtApply[0]
	assert.Equal(t, "example.com/v1", mr.APIVersion)
	assert.Equal(t, "Widget", mr.Kind)
	assert.Equal(t, "w", mr.Name)
	assert.Equal(t, "widget", mr.NodeID)
}

// TestIntendedManagedResources_ProjectsTemplateIdentities is a focused unit
// test for the projection helper Finding A relies on.
func TestIntendedManagedResources_ProjectsTemplateIdentities(t *testing.T) {
	t.Parallel()
	g := graph("g")
	prog := templateProgram("widget", "example.com/v1", "Widget", "default", "w")
	rt := krotruntime.New(prog, g)

	got := intendedManagedResources(rt)
	require.Len(t, got, 1)
	assert.Equal(t, "example.com/v1", got[0].APIVersion)
	assert.Equal(t, "Widget", got[0].Kind)
	assert.Equal(t, "w", got[0].Name)
	assert.Empty(t, got[0].UID, "pre-apply intent carries no UID")
}

// TestIntendedManagedResources_SkipsDynamicGVKWithoutNamespace pins the
// tracking.go:161 fix: a dynamic-GVK node has no compile-time REST scope
// (Namespaced()==false), so a rendered object with NO explicit namespace can't
// be namespace-defaulted in the projection the way the executor will at apply
// time. Emitting a ns="" intent entry would never dedup against the applied
// entry (ns=graph), churning status every cycle — so it must be skipped. A
// dynamic node that DOES set an explicit namespace keeps its intent entry.
func TestIntendedManagedResources_SkipsDynamicGVKWithoutNamespace(t *testing.T) {
	t.Parallel()
	g := graph("g") // namespace "default"

	dynNoNS := func(nodeID, name, namespace string) *compiler.Node {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "example.com/v1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": name},
		}}
		if namespace != "" {
			_ = unstructured.SetNestedField(obj.Object, namespace, "metadata", "namespace")
		}
		return &compiler.Node{
			ID:         nodeID,
			Kind:       compiler.NodeKindTemplate,
			DynamicGVK: true,
			Namespaced: false, // dynamic: unknown at compile time
			Object:     obj,
		}
	}

	t.Run("dynamic node without explicit namespace is skipped", func(t *testing.T) {
		n := dynNoNS("dyn", "w", "")
		prog := &compiler.Program{
			Nodes:            map[string]*compiler.Node{"dyn": n},
			TopologicalOrder: []string{"dyn"},
		}
		got := intendedManagedResources(krotruntime.New(prog, g))
		assert.Empty(t, got, "a dynamic-GVK node with no explicit namespace must not emit a ns=\"\" intent entry")
	})

	t.Run("dynamic node with explicit namespace is kept", func(t *testing.T) {
		n := dynNoNS("dyn", "w", "other-ns")
		prog := &compiler.Program{
			Nodes:            map[string]*compiler.Node{"dyn": n},
			TopologicalOrder: []string{"dyn"},
		}
		got := intendedManagedResources(krotruntime.New(prog, g))
		require.Len(t, got, 1, "an explicit namespace is a stable identity and must be tracked")
		assert.Equal(t, "other-ns", got[0].Namespace)
		assert.Equal(t, "w", got[0].Name)
	})
}

// TestIntendedContributions_MatchesExecutorFieldManager pins the contribution
// write-ahead (graph/controller.go:314): the projected FieldManager MUST equal
// what the executor applies under, or the write-ahead ledger entry would never
// correlate with the contribution Release later looks for. Both derive it from
// the single shared executor.PatchFieldManager(graphUID, nodeID), so this
// asserts the projection reproduces that exact identity for a patch node.
func TestIntendedContributions_MatchesExecutorFieldManager(t *testing.T) {
	t.Parallel()
	g := graph("g") // namespace "default"
	g.SetUID(types.UID("graph-uid-123"))

	patchObj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "target", "namespace": "default"},
		"data":       map[string]any{"k": "v"},
	}}
	patchNode := &compiler.Node{
		ID:         "p",
		Kind:       compiler.NodeKindPatch,
		Namespaced: true,
		Object:     patchObj,
	}
	prog := &compiler.Program{
		Nodes:            map[string]*compiler.Node{"p": patchNode},
		TopologicalOrder: []string{"p"},
	}
	rt := krotruntime.New(prog, g)

	got := intendedContributions(rt)
	require.Len(t, got, 1, "the patch node's contribution must be projected")
	c := got[0]
	assert.Equal(t, "v1", c.APIVersion)
	assert.Equal(t, "ConfigMap", c.Kind)
	assert.Equal(t, "default", c.Namespace)
	assert.Equal(t, "target", c.Name)
	// The crux: the projected field manager is byte-identical to the executor's.
	assert.Equal(t, executor.PatchFieldManager("graph-uid-123", "p"), c.FieldManager,
		"write-ahead FieldManager must match the executor's, or Release cannot correlate the ledger entry")
}

// subgraphProgram wraps one or more child programs as inline subgraph
// (NodeKindGraph) nodes at the root, mirroring how the compiler emits a
// `graph:` node (Kind=NodeKindGraph, SubProgram=<child>). Each entry's key is
// the subgraph node ID; the value is the compiled child Program. Used to build
// realistic nested-frame runtimes for the write-ahead projection tests.
func subgraphProgram(children map[string]*compiler.Program) *compiler.Program {
	nodes := make(map[string]*compiler.Node, len(children))
	order := make([]string, 0, len(children))
	for id, child := range children {
		nodes[id] = &compiler.Node{ID: id, Kind: compiler.NodeKindGraph, SubProgram: child}
		order = append(order, id)
	}
	return &compiler.Program{Nodes: nodes, TopologicalOrder: order}
}

// patchProgram builds a minimal compiled Program with a single static Patch
// node whose rendered target identity is (apiVersion, kind, namespace, name).
func patchProgram(nodeID, apiVersion, kind, namespace, name string) *compiler.Program {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"data": map[string]any{"k": "v"},
	}}
	node := &compiler.Node{
		ID:         nodeID,
		Kind:       compiler.NodeKindPatch,
		Namespaced: namespace != "",
		Object:     obj,
	}
	return &compiler.Program{
		Nodes:            map[string]*compiler.Node{nodeID: node},
		TopologicalOrder: []string{nodeID},
	}
}

// TestIntendedManagedResources_RecursesSubgraphs pins the tracking.go:137 fix:
// a template node declared inside an inline subgraph is applied by the executor
// (applySubgraph) but, before the fix, had NO write-ahead inventory entry — a
// crash between Apply and the post-apply status persist would orphan it. The
// projection must recurse subgraph frames to arbitrary depth, qualifying each
// child NodeID with the subgraph prefix exactly as the executor records it.
func TestIntendedManagedResources_RecursesSubgraphs(t *testing.T) {
	t.Parallel()
	g := graph("g") // namespace "default"

	t.Run("one level deep qualifies sub/child", func(t *testing.T) {
		t.Parallel()
		child := templateProgram("child", "example.com/v1", "Widget", "default", "w")
		prog := subgraphProgram(map[string]*compiler.Program{"sub": child})
		rt := krotruntime.New(prog, g)

		got := intendedManagedResources(rt)
		require.Len(t, got, 1, "the subgraph's template node must be projected")
		assert.Equal(t, "sub/child", got[0].NodeID, "child NodeID is qualified with the subgraph prefix")
		assert.Equal(t, "example.com/v1", got[0].APIVersion)
		assert.Equal(t, "Widget", got[0].Kind)
		assert.Equal(t, "default", got[0].Namespace)
		assert.Equal(t, "w", got[0].Name)
		assert.Empty(t, got[0].UID, "pre-apply intent carries no UID")
	})

	t.Run("two levels deep qualifies subA/subB/leaf", func(t *testing.T) {
		t.Parallel()
		leaf := templateProgram("leaf", "example.com/v1", "Widget", "default", "w")
		inner := subgraphProgram(map[string]*compiler.Program{"subB": leaf})
		prog := subgraphProgram(map[string]*compiler.Program{"subA": inner})
		rt := krotruntime.New(prog, g)

		got := intendedManagedResources(rt)
		require.Len(t, got, 1, "a template nested two subgraphs deep must be projected")
		assert.Equal(t, "subA/subB/leaf", got[0].NodeID, "nested NodeIDs stack the subgraph prefix")
		assert.Equal(t, "Widget", got[0].Kind)
		assert.Equal(t, "w", got[0].Name)
	})

	t.Run("top-level and nested template both projected", func(t *testing.T) {
		t.Parallel()
		child := templateProgram("child", "example.com/v1", "Gadget", "default", "gg")
		prog := subgraphProgram(map[string]*compiler.Program{"sub": child})
		// Add a top-level template alongside the subgraph node.
		prog.Nodes["top"] = templateProgram("top", "example.com/v1", "Widget", "default", "w").Nodes["top"]
		prog.TopologicalOrder = []string{"top", "sub"}
		rt := krotruntime.New(prog, g)

		got := intendedManagedResources(rt)
		require.Len(t, got, 2)
		byNode := map[string]expv1alpha1.ManagedResource{}
		for _, mr := range got {
			byNode[mr.NodeID] = mr
		}
		require.Contains(t, byNode, "top")
		require.Contains(t, byNode, "sub/child")
		assert.Equal(t, "Widget", byNode["top"].Kind)
		assert.Equal(t, "Gadget", byNode["sub/child"].Kind)
	})

	t.Run("dynamic-no-namespace child is still skipped inside a subgraph", func(t *testing.T) {
		t.Parallel()
		dynObj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "example.com/v1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": "w"},
		}}
		dynNode := &compiler.Node{
			ID:         "dyn",
			Kind:       compiler.NodeKindTemplate,
			DynamicGVK: true,
			Namespaced: false,
			Object:     dynObj,
		}
		child := &compiler.Program{
			Nodes:            map[string]*compiler.Node{"dyn": dynNode},
			TopologicalOrder: []string{"dyn"},
		}
		prog := subgraphProgram(map[string]*compiler.Program{"sub": child})
		rt := krotruntime.New(prog, g)

		got := intendedManagedResources(rt)
		assert.Empty(t, got, "the dynamic-no-namespace skip must hold inside a subgraph frame too")
	})
}

// TestIntendedContributions_RecursesSubgraphs is the patch twin of
// TestIntendedManagedResources_RecursesSubgraphs: a patch node inside an inline
// subgraph must be projected, and its FieldManager MUST equal the executor's
// for the QUALIFIED node path (prefix+localID) — the executor derives it from
// patchFieldManager(uid, s.qualifiedPath(n.ID())) where qualifiedPath =
// nodePrefix+id, and applySubgraph extends nodePrefix by "<subID>/". If this
// drifts, the write-ahead ledger entry never correlates with the contribution
// Release later looks for.
func TestIntendedContributions_RecursesSubgraphs(t *testing.T) {
	t.Parallel()
	g := graph("g") // namespace "default"
	g.SetUID(types.UID("graph-uid-123"))

	t.Run("one level deep field manager matches executor for sub/patch", func(t *testing.T) {
		t.Parallel()
		child := patchProgram("patch", "v1", "ConfigMap", "default", "target")
		prog := subgraphProgram(map[string]*compiler.Program{"sub": child})
		rt := krotruntime.New(prog, g)

		got := intendedContributions(rt)
		require.Len(t, got, 1, "the subgraph's patch node contribution must be projected")
		c := got[0]
		assert.Equal(t, "v1", c.APIVersion)
		assert.Equal(t, "ConfigMap", c.Kind)
		assert.Equal(t, "default", c.Namespace)
		assert.Equal(t, "target", c.Name)
		// The crux: field manager is byte-identical to the executor's for the
		// QUALIFIED path "sub/patch", not the bare local id.
		assert.Equal(t, executor.PatchFieldManager("graph-uid-123", "sub/patch"), c.FieldManager,
			"write-ahead FieldManager must match the executor's qualified-path derivation")
		// And it must NOT be the (wrong) bare-id derivation.
		assert.NotEqual(t, executor.PatchFieldManager("graph-uid-123", "patch"), c.FieldManager,
			"a bare-id field manager would not correlate with the executor's qualified apply")
	})

	t.Run("two levels deep field manager matches executor for subA/subB/patch", func(t *testing.T) {
		t.Parallel()
		leaf := patchProgram("patch", "v1", "ConfigMap", "default", "target")
		inner := subgraphProgram(map[string]*compiler.Program{"subB": leaf})
		prog := subgraphProgram(map[string]*compiler.Program{"subA": inner})
		rt := krotruntime.New(prog, g)

		got := intendedContributions(rt)
		require.Len(t, got, 1)
		assert.Equal(t, executor.PatchFieldManager("graph-uid-123", "subA/subB/patch"), got[0].FieldManager,
			"nested patch field managers stack the subgraph prefix, matching the executor")
	})

	t.Run("dynamic-no-namespace patch child is still skipped inside a subgraph", func(t *testing.T) {
		t.Parallel()
		dynObj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "example.com/v1",
			"kind":       "Widget",
			"metadata":   map[string]any{"name": "target"},
			"data":       map[string]any{"k": "v"},
		}}
		dynNode := &compiler.Node{
			ID:         "dynp",
			Kind:       compiler.NodeKindPatch,
			DynamicGVK: true,
			Namespaced: false,
			Object:     dynObj,
		}
		child := &compiler.Program{
			Nodes:            map[string]*compiler.Node{"dynp": dynNode},
			TopologicalOrder: []string{"dynp"},
		}
		prog := subgraphProgram(map[string]*compiler.Program{"sub": child})
		rt := krotruntime.New(prog, g)

		got := intendedContributions(rt)
		assert.Empty(t, got, "the dynamic-no-namespace skip must hold for patch nodes inside a subgraph too")
	})
}

// the reconciler believes they succeeded) then delegates. It simulates a lost
// status write — the exact crash window Finding A guards.
//
// (Retained as documentation of the crash model; Finding B's regression uses
// patchErrClient to fail the terminal status write directly.)

// TestReconcile_StatusWriteErrorNotDiscardedOnNotReady is the Finding B
// regression: when Apply returns a soft ErrNotReady AND updateStatus fails,
// the joined error still matched errors.Is(ErrNotReady) and the reconcile
// returned nil, silently discarding the status-write failure. The fix keeps
// the status-write error separate and surfaces it regardless of the not-ready
// branch.
func TestReconcile_StatusWriteErrorNotDiscardedOnNotReady(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	g := graph("g", withFinalizer)
	cl := newClient(t, g)
	// updateStatus is the reconcile's terminal status Patch. Fail it.
	wrapped := &patchErrClient{Client: cl, statusErr: errors.New("status boom")}

	exec := &fakeExecutor{applyErr: fmt.Errorf("apply %q: %w", "n", executor.ErrNotReady)}
	// Use an empty (payload-less) node so the pre-apply write-ahead projects
	// nothing and does NOT fire — this isolates the failing write to the
	// TERMINAL updateStatus, which is exactly the path Finding B guards.
	r := &Reconciler{Client: wrapped, Compiler: &fakeCompiler{program: emptyNodeProgram("n")}, Registry: registry.New(), Executor: exec}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	// Pre-fix: err == nil (status failure swallowed by the ErrNotReady branch).
	require.Error(t, err, "a failed status write must never be discarded, even on soft not-ready")
	assert.Contains(t, err.Error(), "status boom")
}

// TestReconcile_ReleaseReachableOnSoftNotReady is the reachability half of
// Finding C: when Apply returns a soft ErrNotReady, the early apply-error
// TestReconcile_NoReleaseOnSoftNotReady pins the corrected Finding C contract:
// release of patch contributions runs on the CLEAN-apply path ONLY. On a soft
// ErrNotReady, a patch node's contribution is absent from result.Contributions
// whether it was genuinely removed OR is merely data-pending this cycle, and
// executor.Contribution carries no NodeID to tell those apart — so releasing
// here would drop fields a still-wanted patch node set (a transient flap). The
// field-manager-identity-change deadlock this path once tried to break is now
// fixed at the source in the executor (contributeApply force-reclaims a
// same-Graph stale patch identity), so no controller-side release on soft
// errors is needed.
func TestReconcile_NoReleaseOnSoftNotReady(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	// Prior contribution recorded on the Graph; this cycle Apply reports NO
	// contributions and a soft ErrNotReady with the patch node Unresolved
	// (data-pending, still wanted).
	prior := []executor.Contribution{{
		APIVersion:   "v1",
		Kind:         "ConfigMap",
		Namespace:    "default",
		Name:         "target",
		FieldManager: "kro-graphengine.patch.oldidentity",
	}}

	g := graph("g", withFinalizer, func(g *expv1alpha1.Graph) {
		g.Status.Contributions = toAPIContributions(prior)
	})
	cl := newClient(t, g)

	exec := &fakeExecutor{
		applyErr:    fmt.Errorf("apply %q (patch): %w", "p", executor.ErrNotReady),
		applyResult: executor.ApplyResult{Unresolved: []string{"p"}},
	}
	r := &Reconciler{Client: cl, Compiler: &fakeCompiler{program: emptyNodeProgram("n")}, Registry: registry.New(), Executor: exec}

	_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})

	// The prior contribution must NOT be released on a soft not-ready cycle:
	// a data-pending patch node is still wanted, and releasing its fields would
	// flap them until the node resolves next cycle.
	assert.Empty(t, exec.releaseCalls, "release must not fire on soft not-ready (would flap a data-pending patch's fields)")
}

// TestReconcile_SoftNotReadyStillPrunesRetiredNode pins finding 357: a node
// that is soft not-ready this cycle must NOT veto pruning of an UNRELATED
// resource whose owning node was removed from the spec. Previously all pruning
// was gated on a fully clean apply, so one never-ready node leaked every
// retired resource until it resolved. diffManagedResources keeps unresolved
// nodes' entries, so a prune candidate on a soft cycle is genuinely retired and
// safe to delete.
func TestReconcile_SoftNotReadyStillPrunesRetiredNode(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	// Previously-tracked resource owned by node "gone", which is no longer in
	// the graph. A separate node "widget" is not-ready this cycle.
	g := graph("g", withFinalizer, func(g *expv1alpha1.Graph) {
		g.Status.ManagedResources = []expv1alpha1.ManagedResource{{
			NodeID:     "gone",
			APIVersion: "v1",
			Kind:       "ConfigMap",
			Namespace:  "default",
			Name:       "retired-cm",
			UID:        "uid-retired",
		}}
	})
	cl := newClient(t, g)

	// Apply: soft not-ready, node "widget" Unresolved, nothing applied. The
	// "gone" resource is neither Applied nor Unresolved -> a prune candidate.
	exec := &fakeExecutor{
		applyErr:    fmt.Errorf("apply %q: %w", "widget", executor.ErrNotReady),
		applyResult: executor.ApplyResult{Unresolved: []string{"widget"}},
	}
	r := &Reconciler{Client: cl, Compiler: &fakeCompiler{program: emptyNodeProgram("widget")}, Registry: registry.New(), Executor: exec}

	_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})

	// The retired resource must have been pruned despite the soft not-ready.
	require.Len(t, exec.deleteCalls, 1, "prune must run on a soft not-ready cycle for a retired node")
	require.Len(t, exec.deleteCalls[0], 1)
	assert.Equal(t, "retired-cm", exec.deleteCalls[0][0].Name,
		"the retired node's resource is the prune candidate")

	// Persisted status must no longer track the pruned resource.
	got := &expv1alpha1.Graph{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	for _, mr := range got.Status.ManagedResources {
		assert.NotEqual(t, "retired-cm", mr.Name, "a successfully pruned resource must drop from status")
	}
}

// TestReconcile_ErrorPathKeepsIntentSuperset guards the Finding A hardening:
// on a soft apply error the in-memory status (which the terminal updateStatus
// overwrites onto the server) must not shrink below the written-ahead intent,
// so a partially-applied resource still has a durable inventory entry.
func TestReconcile_ErrorPathKeepsIntentSuperset(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	g := graph("g", withFinalizer)
	cl := newClient(t, g)

	// Apply reports a soft not-ready and an EMPTY Applied set (nothing observed
	// this cycle), simulating a crash/partial apply. The intent projected from
	// the template must still land in persisted status.
	exec := &fakeExecutor{
		applyErr:    fmt.Errorf("apply %q: %w", "widget", executor.ErrNotReady),
		applyResult: executor.ApplyResult{Unresolved: []string{"widget"}},
	}
	fc := &fakeCompiler{program: templateProgram("widget", "example.com/v1", "Widget", "default", "w")}
	r := &Reconciler{Client: cl, Compiler: fc, Registry: registry.New(), Executor: exec}

	_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})

	got := &expv1alpha1.Graph{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	require.Len(t, got.Status.ManagedResources, 1,
		"intent superset must survive a soft-error cycle in persisted status")
	assert.Equal(t, "Widget", got.Status.ManagedResources[0].Kind)
	assert.Equal(t, "w", got.Status.ManagedResources[0].Name)
}

// statusPatchGate wraps a client and refuses every Status().Patch whose merge
// body touches the named status field, letting all other status writes through
// (an apiserver that rejects one inventory write but accepts the conditions).
type statusPatchGate struct {
	client.Client
	field    string
	err      error
	rejected int // number of status patches refused
}

func (c *statusPatchGate) Status() client.StatusWriter {
	return &gatedStatusWriter{StatusWriter: c.Client.Status(), gate: c}
}

type gatedStatusWriter struct {
	client.StatusWriter
	gate *statusPatchGate
}

func (w *gatedStatusWriter) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	data, err := p.Data(obj)
	if err != nil {
		return err
	}
	if bytes.Contains(data, []byte(`"`+w.gate.field+`"`)) {
		w.gate.rejected++
		return w.gate.err
	}
	return w.StatusWriter.Patch(ctx, obj, p, opts...)
}

// manyManagedResources fabricates n distinct, UID-bearing ManagedResource entries.
func manyManagedResources(nodeID string, n int) []expv1alpha1.ManagedResource {
	out := make([]expv1alpha1.ManagedResource, 0, n)
	for i := range n {
		out = append(out, expv1alpha1.ManagedResource{
			NodeID:     nodeID,
			APIVersion: "v1",
			Kind:       "ConfigMap",
			Namespace:  "default",
			Name:       fmt.Sprintf("cm-%d", i),
			UID:        fmt.Sprintf("uid-%d", i),
		})
	}
	return out
}

// manyContributions fabricates n distinct ledger rows under one field manager.
func manyContributions(fieldManager string, n int) []executor.Contribution {
	out := make([]executor.Contribution, 0, n)
	for i := range n {
		out = append(out, executor.Contribution{
			APIVersion:   "v1",
			Kind:         "ConfigMap",
			Namespace:    "default",
			Name:         fmt.Sprintf("target-%d", i),
			FieldManager: fieldManager,
		})
	}
	return out
}

// requireConverged asserts the ResourcesConverged condition on g has the given
// status and reason, and that the Ready root followed it.
func requireConverged(t *testing.T, g *expv1alpha1.Graph, status metav1.ConditionStatus, reason string) *expv1alpha1.Condition {
	t.Helper()
	rc := findCondition(g.Status.Conditions, ResourcesConverged)
	require.NotNil(t, rc, "ResourcesConverged must be present")
	assert.Equal(t, status, rc.Status)
	require.NotNil(t, rc.Reason)
	assert.Equal(t, reason, *rc.Reason)
	ready := findCondition(g.Status.Conditions, Ready)
	require.NotNil(t, ready, "Ready must be present")
	assert.Equal(t, status, ready.Status, "Ready must follow ResourcesConverged")
	return rc
}

// compileGraph compiles g with the real compiler bound to the fake schema
// resolver, so projection tests exercise the compiled forEach/CEL machinery.
func compileGraph(t *testing.T, g *expv1alpha1.Graph) *compiler.Program {
	t.Helper()
	r, disco := testk8s.NewFakeResolver()
	rm := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disco))
	p, err := compiler.NewCompilerWithDependencies(r, rm).Compile(g)
	require.NoError(t, err)
	return p
}

// forEachPatchGraph builds a Graph whose forEach patch node "p" contributes data
// to the ConfigMap named by each element of a Def node's list.
func forEachPatchGraph(ns string, names []any) *expv1alpha1.Graph {
	g := generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithDef("src", map[string]any{"names": names}),
		generator.WithPatchManifest("p", map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "${n}"},
			"data":       map[string]any{"patched": "yes"},
		}),
	)
	g.Spec.Nodes[len(g.Spec.Nodes)-1].ForEach = []expv1alpha1.ForEachDimension{{"n": "${src.names}"}}
	return g
}

// TestReconcile_ForEachPatchWriteAheadPersistsEveryTarget: for a forEach patch
// node the server-side ledger must hold one row per target before Apply runs.
func TestReconcile_ForEachPatchWriteAheadPersistsEveryTarget(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	spec := forEachPatchGraph("default", []any{"claim-a", "claim-b", "claim-c"})
	g := graph("g", withFinalizer, func(g *expv1alpha1.Graph) {
		g.SetUID("uid-foreach-reconcile")
		g.Spec = spec.Spec
	})
	cl := newClient(t, g)

	obs := &applyObservingExecutor{cl: cl, key: key}
	fc := &fakeCompiler{program: compileGraph(t, g)}
	r := &Reconciler{Client: cl, Compiler: fc, Registry: registry.New(), Executor: obs}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.True(t, obs.observed, "Apply must have been called")

	require.Len(t, obs.contribsAtApply, 3,
		"the write-ahead ledger must hold one row per forEach target BEFORE Apply runs")
	wantFM := executor.PatchFieldManager("uid-foreach-reconcile", "p")
	names := make([]string, 0, 3)
	for _, c := range obs.contribsAtApply {
		names = append(names, c.Name)
		assert.Equal(t, wantFM, c.FieldManager, "every row carries the executor's per-node field manager")
		assert.Equal(t, "default", c.Namespace)
	}
	assert.ElementsMatch(t, []string{"claim-a", "claim-b", "claim-c"}, names)
	assert.Empty(t, obs.persistedAtApply, "a patch node owns nothing; no managed-resource intent")
}

// ledgerWriteRecorder wraps a client and records the row count carried by every
// Status().Patch that touches status.contributions, so a test can assert the
// write pattern, not just the final ledger (every status write re-enqueues).
type ledgerWriteRecorder struct {
	client.Client
	writes []int
}

func (c *ledgerWriteRecorder) Status() client.StatusWriter {
	return &recordingStatusWriter{StatusWriter: c.Client.Status(), rec: c}
}

type recordingStatusWriter struct {
	client.StatusWriter
	rec *ledgerWriteRecorder
}

func (w *recordingStatusWriter) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	data, err := p.Data(obj)
	if err != nil {
		return err
	}
	if bytes.Contains(data, []byte(`"contributions"`)) {
		n := -1
		if g, ok := obj.(*expv1alpha1.Graph); ok {
			n = len(g.Status.Contributions)
		}
		w.rec.writes = append(w.rec.writes, n)
	}
	return w.StatusWriter.Patch(ctx, obj, p, opts...)
}

// TestReconcile_SoftPathKeepsWriteAheadLedger: a patch node whose target does
// not exist yet (ErrNotReady, fewer rows observed than projected) must persist
// exactly one ledger write per Graph — the write-ahead — and never shrink it;
// writing it back down each cycle re-enqueues the Graph in a hot loop.
func TestReconcile_SoftPathKeepsWriteAheadLedger(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}
	fm := executor.PatchFieldManager("uid-soft-ledger", "p")
	contrib := func(name string) executor.Contribution {
		return executor.Contribution{APIVersion: "v1", Kind: "ConfigMap", Namespace: "default", Name: name, FieldManager: fm}
	}

	cases := []struct {
		name      string
		program   func(t *testing.T, g *expv1alpha1.Graph) *compiler.Program
		observed  []executor.Contribution // what the executor reports alongside ErrNotReady
		wantRows  int                     // write-ahead rows == steady-state ledger size
		wantNames []string
	}{
		{
			// Three targets, none present yet.
			name: "forEach patch over absent targets",
			program: func(t *testing.T, g *expv1alpha1.Graph) *compiler.Program {
				g.Spec = forEachPatchGraph("default", []any{"claim-a", "claim-b", "claim-c"}).Spec
				return compileGraph(t, g)
			},
			wantRows:  3,
			wantNames: []string{"claim-a", "claim-b", "claim-c"},
		},
		{
			name: "singleton patch over an absent target",
			program: func(*testing.T, *expv1alpha1.Graph) *compiler.Program {
				return patchProgram("p", "v1", "ConfigMap", "default", "target")
			},
			wantRows:  1,
			wantNames: []string{"target"},
		},
		{
			// Two of three targets exist; the third row stays as a write-ahead ghost.
			name: "forEach patch with a subset of targets present",
			program: func(t *testing.T, g *expv1alpha1.Graph) *compiler.Program {
				g.Spec = forEachPatchGraph("default", []any{"claim-a", "claim-b", "claim-c"}).Spec
				return compileGraph(t, g)
			},
			observed:  []executor.Contribution{contrib("claim-a"), contrib("claim-b")},
			wantRows:  3,
			wantNames: []string{"claim-a", "claim-b", "claim-c"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := graph("g", withFinalizer, func(g *expv1alpha1.Graph) { g.SetUID("uid-soft-ledger") })
			prog := tc.program(t, g)
			cl := newClient(t, g)
			rec := &ledgerWriteRecorder{Client: cl}
			exec := &fakeExecutor{
				applyErr:    fmt.Errorf("apply %q: patch target not found: %w", "p", executor.ErrNotReady),
				applyResult: executor.ApplyResult{Contributions: tc.observed},
			}
			r := &Reconciler{Client: rec, Compiler: &fakeCompiler{program: prog}, Registry: registry.New(), Executor: exec}

			// First cycle: one write-ahead write, and no write back down.
			res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err, "a not-ready target is a soft requeue")
			assert.Positive(t, res.RequeueAfter)
			assert.Equal(t, []int{tc.wantRows}, rec.writes,
				"exactly one ledger write (the write-ahead) per cycle; a second, smaller write is the N->0 drop")

			got := &expv1alpha1.Graph{}
			require.NoError(t, cl.Get(context.Background(), key, got))
			require.Len(t, got.Status.Contributions, tc.wantRows, "the soft path must not shrink the ledger below the write-ahead")
			names := make([]string, 0, len(got.Status.Contributions))
			for _, c := range got.Status.Contributions {
				names = append(names, c.Name)
				assert.Equal(t, fm, c.FieldManager)
			}
			assert.ElementsMatch(t, tc.wantNames, names)

			// Second cycle: the server already holds the intent; nothing to write.
			_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)
			assert.Equal(t, []int{tc.wantRows}, rec.writes, "a steady-state soft cycle must issue no ledger write at all")
			require.NoError(t, cl.Get(context.Background(), key, got))
			assert.Len(t, got.Status.Contributions, tc.wantRows)
		})
	}
}

// TestReconcile_ContributionPersistFailureFlipsConverged: a refused post-apply
// ledger write after a clean apply must flip ResourcesConverged to
// False/StatusWriteFailed and be returned, not leave Ready=True with an empty ledger.
func TestReconcile_ContributionPersistFailureFlipsConverged(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	g := graph("g", withFinalizer)
	cl := newClient(t, g)
	// Refuse only ledger writes; the conditions write must still land.
	gate := &statusPatchGate{Client: cl, field: "contributions", err: errors.New("etcdserver: request is too large")}

	exec := &fakeExecutor{applyResult: executor.ApplyResult{
		Contributions: manyContributions("kro-graphengine.patch.abc.def", 3),
	}}
	// A payload-less node has no write-ahead, so the only ledger write is the
	// post-apply persist under test.
	r := &Reconciler{Client: gate, Compiler: &fakeCompiler{program: emptyNodeProgram("p")}, Registry: registry.New(), Executor: exec}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.Error(t, err, "a refused contribution-ledger write must be returned, not swallowed")
	assert.Contains(t, err.Error(), "persist contributions")
	assert.Contains(t, err.Error(), "request is too large")
	assert.GreaterOrEqual(t, gate.rejected, 1, "the ledger write must have been attempted and refused")

	got := &expv1alpha1.Graph{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	rc := requireConverged(t, got, metav1.ConditionFalse, "StatusWriteFailed")
	require.NotNil(t, rc.Message)
	assert.Contains(t, *rc.Message, "request is too large")
	acc := findCondition(got.Status.Conditions, GraphAccepted)
	require.NotNil(t, acc)
	assert.Equal(t, metav1.ConditionTrue, acc.Status, "the Graph still compiled")
	assert.Empty(t, got.Status.Contributions, "nothing landed on the server; status must not pretend otherwise")
}

// TestReconcile_WriteAheadRejectedStillPublishesConditions: a refused write-ahead
// must publish Accepted=True and ResourcesConverged=False/WriteAheadFailed with
// nothing applied, and the conditions write must not re-send the refused list.
func TestReconcile_WriteAheadRejectedStillPublishesConditions(t *testing.T) {
	t.Parallel()
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	g := graph("g", withFinalizer)
	cl := newClient(t, g)
	rejection := apierrors.NewInvalid(
		schema.GroupKind{Group: "kro.run", Kind: "Graph"}, "g",
		field.ErrorList{field.TooMany(field.NewPath("status", "managedResources"), 6000, 5000)})
	// Refuse the write-ahead (carries managedResources); let conditions through.
	gate := &statusPatchGate{Client: cl, field: "managedResources", err: rejection}

	exec := &fakeExecutor{}
	fc := &fakeCompiler{program: templateProgram("widget", "example.com/v1", "Widget", "default", "w")}
	r := &Reconciler{Client: gate, Compiler: fc, Registry: registry.New(), Executor: exec}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write-ahead managed-resource intent")
	assert.Equal(t, 0, exec.applyCalls, "a refused write-ahead must fail closed: nothing applied")
	assert.Equal(t, 1, gate.rejected, "exactly the write-ahead was refused; the conditions write must not carry the refused list")

	got := &expv1alpha1.Graph{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	require.NotEmpty(t, got.Status.Conditions, "a Graph whose write-ahead was refused must not be left with no status at all")
	acc := findCondition(got.Status.Conditions, GraphAccepted)
	require.NotNil(t, acc)
	assert.Equal(t, metav1.ConditionTrue, acc.Status, "Accepted/Compiled is still reported")
	rc := requireConverged(t, got, metav1.ConditionFalse, "WriteAheadFailed")
	require.NotNil(t, rc.Message)
	assert.Contains(t, *rc.Message, "must have at most 5000 items")
	assert.Empty(t, got.Status.ManagedResources, "the refused inventory must not have landed")
}

// TestReconcile_InventoryTooLargeFailsClosed: an inventory over
// GraphInventoryMaxItems yields ResourcesConverged=False/InventoryTooLarge naming
// the count and cap; before apply the executor is not run, and the server-side
// inventory never grows past what it held.
func TestReconcile_InventoryTooLargeFailsClosed(t *testing.T) {
	t.Parallel()
	limit := expv1alpha1.GraphInventoryMaxItems
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	cases := []struct {
		name         string
		initial      func(*expv1alpha1.Graph)
		program      *compiler.Program
		exec         *fakeExecutor
		wantApplied  bool   // was the executor run this cycle?
		wantField    string // status field named in the condition message
		wantCount    int    // entry count named in the condition message
		wantManaged  int    // persisted managedResources length after reconcile
		wantContribs int    // persisted contributions length after reconcile
	}{
		{
			// previous holds exactly the cap; the template intends one more.
			name: "managed-resource write-ahead over the cap refuses to apply",
			initial: func(g *expv1alpha1.Graph) {
				g.Status.ManagedResources = manyManagedResources("cms", limit)
			},
			program:     templateProgram("widget", "example.com/v1", "Widget", "default", "w"),
			exec:        &fakeExecutor{},
			wantApplied: false,
			wantField:   "managedResources",
			wantCount:   limit + 1,
			wantManaged: limit,
		},
		{
			// prior ledger holds exactly the cap; the patch intends one more target.
			name: "contribution write-ahead over the cap refuses to apply",
			initial: func(g *expv1alpha1.Graph) {
				g.SetUID("uid-contrib-cap")
				g.Status.Contributions = toAPIContributions(manyContributions("kro-graphengine.patch.prior.x", limit))
			},
			program:      patchProgram("p", "v1", "ConfigMap", "default", "target"),
			exec:         &fakeExecutor{},
			wantApplied:  false,
			wantField:    "contributions",
			wantCount:    limit + 1,
			wantContribs: limit,
		},
		{
			// No write-ahead (payload-less node); apply reports cap+1 identities
			// the projection could not see. Condition set, no prune, server
			// inventory unchanged.
			name:    "post-apply applied set over the cap surfaces InventoryTooLarge",
			program: emptyNodeProgram("cms"),
			exec: &fakeExecutor{applyResult: executor.ApplyResult{
				Applied: manyManagedResources("cms", limit+1),
			}},
			wantApplied: true,
			wantField:   "managedResources",
			wantCount:   limit + 1,
			wantManaged: 0,
		},
		{
			// Same for contributions observed after apply.
			name:    "post-apply contribution set over the cap surfaces InventoryTooLarge",
			program: emptyNodeProgram("p"),
			exec: &fakeExecutor{applyResult: executor.ApplyResult{
				Contributions: manyContributions("kro-graphengine.patch.abc.def", limit+1),
			}},
			wantApplied:  true,
			wantField:    "contributions",
			wantCount:    limit + 1,
			wantContribs: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := graph("g", withFinalizer)
			if tc.initial != nil {
				tc.initial(g)
			}
			cl := newClient(t, g)
			r := &Reconciler{Client: cl, Compiler: &fakeCompiler{program: tc.program}, Registry: registry.New(), Executor: tc.exec}

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.Error(t, err, "an over-cap inventory must be returned as an error so the Graph is retried")
			var tooLarge *inventoryTooLargeError
			require.ErrorAs(t, err, &tooLarge, "the error must be the typed cap error, not a raw apiserver rejection")
			assert.Equal(t, tc.wantApplied, tc.exec.applyCalls > 0, "executor run")
			assert.Empty(t, tc.exec.deleteCalls, "no prune may run on a cycle whose bookkeeping cannot be persisted")

			got := &expv1alpha1.Graph{}
			require.NoError(t, cl.Get(context.Background(), key, got))
			rc := requireConverged(t, got, metav1.ConditionFalse, "InventoryTooLarge")
			require.NotNil(t, rc.Message)
			assert.Contains(t, *rc.Message, "status."+tc.wantField)
			assert.Contains(t, *rc.Message, fmt.Sprintf("%d entries", tc.wantCount))
			assert.Contains(t, *rc.Message, fmt.Sprintf("cap of %d", limit))
			// The message says whether anything reached the cluster.
			if tc.wantApplied {
				assert.Contains(t, *rc.Message, "applied resources exceed the trackable inventory")
			} else {
				assert.Contains(t, *rc.Message, "refusing to apply")
			}
			acc := findCondition(got.Status.Conditions, GraphAccepted)
			require.NotNil(t, acc)
			assert.Equal(t, metav1.ConditionTrue, acc.Status, "the Graph still compiled")
			assert.Len(t, got.Status.ManagedResources, tc.wantManaged, "server-side inventory must not grow past what it held")
			assert.Len(t, got.Status.Contributions, tc.wantContribs, "server-side ledger must not grow past what it held")
		})
	}
}

// TestReconcile_PostApplyOverCapKeepsWriteAheadUIDs: when the applied set is
// over the cap the inventory stays at the write-ahead union but adopts the
// observed UIDs, since Delete skips UID-free entries.
func TestReconcile_PostApplyOverCapKeepsWriteAheadUIDs(t *testing.T) {
	t.Parallel()
	limit := expv1alpha1.GraphInventoryMaxItems
	key := types.NamespacedName{Namespace: "default", Name: "g"}

	g := graph("g", withFinalizer)
	cl := newClient(t, g)

	// Write-ahead projects "w" UID-free; apply reports it with a UID plus cap
	// more identities the projection never saw.
	applied := append([]expv1alpha1.ManagedResource{{
		NodeID: "widget", APIVersion: "example.com/v1", Kind: "Widget", Namespace: "default", Name: "w", UID: "uid-w",
	}}, manyManagedResources("cms", limit)...)
	exec := &fakeExecutor{applyResult: executor.ApplyResult{Applied: applied}}
	fc := &fakeCompiler{program: templateProgram("widget", "example.com/v1", "Widget", "default", "w")}
	r := &Reconciler{Client: cl, Compiler: fc, Registry: registry.New(), Executor: exec}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	var tooLarge *inventoryTooLargeError
	require.ErrorAs(t, err, &tooLarge)

	got := &expv1alpha1.Graph{}
	require.NoError(t, cl.Get(context.Background(), key, got))
	requireConverged(t, got, metav1.ConditionFalse, "InventoryTooLarge")
	require.Len(t, got.Status.ManagedResources, 1, "the inventory stays at the write-ahead union, never the over-cap applied set")
	assert.Equal(t, "w", got.Status.ManagedResources[0].Name)
	assert.Equal(t, "uid-w", got.Status.ManagedResources[0].UID,
		"the write-ahead entry must adopt the observed UID so teardown can still delete it")
}
