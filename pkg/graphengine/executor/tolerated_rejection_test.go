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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// TestClassifyRejection covers the permanent-vs-transient classification that
// enriches the tolerated-update-rejection signal (finding 886, Opt 2). The axis
// is NOT "immutable vs still-reconciling" (a reconciling object is an accepted
// write, never an error here) but "permanent vs transient rejection".
func TestClassifyRejection(t *testing.T) {
	t.Parallel()

	immutable := apierrors.NewInvalid(
		schema.GroupKind{Kind: "Service"}, "svc",
		field.ErrorList{field.Invalid(field.NewPath("spec", "clusterIP"), "10.0.0.9", "field is immutable")},
	)
	otherInvalid := apierrors.NewInvalid(
		schema.GroupKind{Kind: "ConfigMap"}, "cm",
		field.ErrorList{field.Invalid(field.NewPath("data"), "x", "too long")},
	)

	tests := []struct {
		name          string
		err           error
		wantReason    string
		wantPermanent bool
	}{
		{"immutable field", immutable, "field immutable", true},
		{"other invalid", otherInvalid, "invalid request", true},
		{"bad request", apierrors.NewBadRequest("nope"), "invalid request", true},
		{"conflict is transient", apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "cm", errors.New("rv")), "field-manager conflict, will retry", false},
		{"throttled is transient", apierrors.NewTooManyRequestsError("slow down"), "throttled, will retry", false},
		{"unavailable is transient", apierrors.NewServiceUnavailable("down"), "transient server error, will retry", false},
		{"unknown is transient", errors.New("something odd"), "rejected, will retry", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reason, permanent := classifyRejection(tc.err)
			assert.Equal(t, tc.wantReason, reason)
			assert.Equal(t, tc.wantPermanent, permanent)
		})
	}
}

// TestApply_OnToleratedRejectionHookFires verifies that a tolerated collection
// update-rejection on an already-existing item invokes OnToleratedRejection
// with the target identity + classification, while the node still converges
// (the hook is observational — it must not make Apply fail).
func TestApply_OnToleratedRejectionHookFires(t *testing.T) {
	t.Parallel()

	// Both members already exist, so every SSA is an update; reject with an
	// immutable-field Invalid so the tolerate path fires.
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(liveCM("cm-alpha"), liveCM("cm-beta")).Build()
	immutable := apierrors.NewInvalid(
		schema.GroupKind{Kind: "ConfigMap"}, "cm-alpha",
		field.ErrorList{field.Invalid(field.NewPath("data"), "x", "field is immutable")},
	)
	cl := &patchFailClient{Client: base, err: immutable}

	var mu sync.Mutex
	var got []ToleratedRejection
	s := NewSimple(cl)
	s.OnToleratedRejection = func(r ToleratedRejection) {
		mu.Lock()
		got = append(got, r)
		mu.Unlock()
	}

	res, err := s.Apply(context.Background(),
		compileAndBuild(t, collectionCMGraph()), watchrouter.NoopWatcher{})

	require.NoError(t, err, "a tolerated update-rejection must not fail Apply (the node converges)")
	// Both items converge into Applied (live identities recorded).
	assert.Len(t, res.Applied, 2)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2, "the hook must fire once per tolerated item")
	for _, r := range got {
		assert.Equal(t, "v1", r.APIVersion)
		assert.Equal(t, "ConfigMap", r.Kind)
		assert.Equal(t, "default", r.Namespace)
		assert.Equal(t, "field immutable", r.Reason)
		assert.Contains(t, r.Cause, "immutable")
	}
}

// TestApply_NoHookWhenNil confirms a nil hook is a safe no-op (the Graph
// controller path leaves it nil and relies on the log line).
func TestApply_NoHookWhenNil(t *testing.T) {
	t.Parallel()
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(liveCM("cm-alpha"), liveCM("cm-beta")).Build()
	cl := &patchFailClient{Client: base, err: apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "cm-alpha", nil)}

	s := NewSimple(cl) // OnToleratedRejection nil
	_, err := s.Apply(context.Background(),
		compileAndBuild(t, collectionCMGraph()), watchrouter.NoopWatcher{})
	require.NoError(t, err, "a nil hook must be a safe no-op")
}

// collectionCMGraphWithDependent is collectionCMGraph plus a scalar node "dep"
// that reads the collection, so a test can observe whether the collection node
// reached ready (dep applied) or was held not-ready (dep gated).
func collectionCMGraphWithDependent() *expv1alpha1.Graph {
	return generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("src", map[string]any{"names": []any{"alpha", "beta"}}),
		generator.WithTemplate("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${'cm-' + n}"},
			"data":     map[string]any{"k": "new"},
		}, generator.ForEachDim("n", "${src.names}")),
		generator.WithTemplate("dep", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "dep"},
			"data":     map[string]any{"count": "${string(size(cm))}"},
		}),
	)
}

// failOnlyClient fails SSA for the named object only and delegates every other
// patch.
type failOnlyClient struct {
	client.Client
	name string
	err  error
}

func (f *failOnlyClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if obj.GetName() == f.name {
		return f.err
	}
	return f.Client.Patch(ctx, obj, patch, opts...)
}

// appliedNames projects the names in an ApplyResult.Applied inventory.
func appliedNames(res ApplyResult) []string {
	names := make([]string, 0, len(res.Applied))
	for _, a := range res.Applied {
		names = append(names, a.Name)
	}
	return names
}

// TestApply_TransientUpdateRejectionHoldsNodeNotReady: a rejection of an UPDATE
// to an already-existing member that classifyRejection cannot prove permanent
// keeps the live identity tracked but is not converged — Apply returns
// ErrNotReady with the cause, the node is Unresolved, dependents gate, and the
// observational hook does not fire. One row per transient class.
func TestApply_TransientUpdateRejectionHoldsNodeNotReady(t *testing.T) {
	t.Parallel()

	webhookDown := apierrors.NewInternalError(errors.New(
		`failed calling webhook "block.example.com": failed to call webhook: service "does-not-exist" not found`))

	tests := []struct {
		name  string
		err   error
		cause string // must reach the returned error (and so the condition message)
	}{
		{"internal error (webhook backend unreachable)", webhookDown, "failed calling webhook"},
		{"throttled", apierrors.NewTooManyRequestsError("slow down"), "slow down"},
		{"conflict", apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "cm-alpha", errors.New("resource version mismatch")), "resource version mismatch"},
		{"forbidden (a denial is not proven permanent)", apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "cm-alpha", errors.New("denied by policy")), "denied by policy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Both members exist, so every SSA is an UPDATE; only cm-alpha's fails.
			base := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithObjects(liveCM("cm-alpha"), liveCM("cm-beta")).Build()
			cl := &failOnlyClient{Client: base, name: "cm-alpha", err: tc.err}

			var hookCalls atomic.Int32
			s := NewSimple(cl)
			s.GateReadiness = true
			s.OnToleratedRejection = func(ToleratedRejection) { hookCalls.Add(1) }

			res, err := s.Apply(context.Background(),
				compileAndBuild(t, collectionCMGraphWithDependent()), watchrouter.NoopWatcher{})

			require.Error(t, err, "a transient update rejection must not be reported as converged")
			assert.True(t, errors.Is(err, ErrNotReady),
				"the failure must be a soft not-ready so the reconcile requeues, got %v", err)
			assert.False(t, errors.Is(err, ErrResourceDeleting))
			assert.Contains(t, err.Error(), "collection \"cm\": 1 item(s) failed to apply")
			assert.Contains(t, err.Error(), "item default/cm-alpha")
			assert.Contains(t, err.Error(), tc.cause, "the rejection cause must reach the condition message")

			assert.ElementsMatch(t, []string{"cm-alpha", "cm-beta"}, appliedNames(res),
				"the rejected member's live identity must stay in Applied")
			assert.Contains(t, res.Unresolved, "cm",
				"the collection node must be Unresolved so prune is withheld")

			assert.Contains(t, res.Unresolved, "dep",
				"a dependent of the not-ready collection must be gated (Unresolved)")
			assert.False(t, cmExists(t, base, "dep"),
				"a dependent must not be created while the collection is not ready")

			assert.Zero(t, hookCalls.Load(),
				"a transient failure is retried, not tolerated: OnToleratedRejection must not fire")
		})
	}
}

// TestApply_PermanentUpdateRejectionStillTolerated: a permanent rejection
// (Invalid/BadRequest) of an UPDATE to an already-existing member is tolerated —
// live object recorded as applied, hook fired once, Apply nil, dependents
// released — so one unfixable member does not wedge the node.
func TestApply_PermanentUpdateRejectionStillTolerated(t *testing.T) {
	t.Parallel()

	immutable := apierrors.NewInvalid(
		schema.GroupKind{Kind: "ConfigMap"}, "cm-alpha",
		field.ErrorList{field.Invalid(field.NewPath("data"), "x", "field is immutable")},
	)
	tests := []struct {
		name       string
		err        error
		wantReason string
	}{
		{"immutable field (Invalid)", immutable, "field immutable"},
		{"other Invalid", apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "cm-alpha", nil), "invalid request"},
		{"BadRequest", apierrors.NewBadRequest("malformed"), "invalid request"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := fake.NewClientBuilder().WithScheme(newScheme(t)).
				WithObjects(liveCM("cm-alpha"), liveCM("cm-beta")).Build()
			cl := &failOnlyClient{Client: base, name: "cm-alpha", err: tc.err}

			var mu sync.Mutex
			var got []ToleratedRejection
			s := NewSimple(cl)
			s.GateReadiness = true
			s.OnToleratedRejection = func(r ToleratedRejection) {
				mu.Lock()
				got = append(got, r)
				mu.Unlock()
			}

			res, err := s.Apply(context.Background(),
				compileAndBuild(t, collectionCMGraphWithDependent()), watchrouter.NoopWatcher{})

			require.NoError(t, err, "a permanent update rejection on an existing member must still be tolerated")
			assert.ElementsMatch(t, []string{"cm-alpha", "cm-beta", "dep"}, appliedNames(res),
				"the live member, its sibling and the released dependent are all tracked")
			assert.Empty(t, res.Unresolved, "a converged node has nothing unresolved")
			assert.True(t, cmExists(t, base, "dep"),
				"the node converged, so its dependent must have been applied")

			mu.Lock()
			defer mu.Unlock()
			require.Len(t, got, 1, "the hook must fire exactly once, for the rejected member only")
			assert.Equal(t, "cm-alpha", got[0].Name)
			assert.Equal(t, "cm", got[0].NodeID)
			assert.Equal(t, tc.wantReason, got[0].Reason)
		})
	}
}

// TestApply_TransientCreateFailureUnchanged: when the member does not exist and
// CREATE fails transiently, the node is held soft not-ready, nothing is
// advertised as applied, and the hook (about rejected UPDATES) does not fire.
func TestApply_TransientCreateFailureUnchanged(t *testing.T) {
	t.Parallel()

	base := fake.NewClientBuilder().WithScheme(newScheme(t)).Build() // no members exist
	cl := &failOnlyClient{Client: base, name: "cm-alpha", err: apierrors.NewInternalError(errors.New("failed calling webhook"))}

	var hookCalls atomic.Int32
	s := NewSimple(cl)
	s.GateReadiness = true
	s.OnToleratedRejection = func(ToleratedRejection) { hookCalls.Add(1) }

	res, err := s.Apply(context.Background(),
		compileAndBuild(t, collectionCMGraphWithDependent()), watchrouter.NoopWatcher{})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotReady), "a transient create failure stays a soft requeue, got %v", err)
	assert.Contains(t, err.Error(), "failed calling webhook")
	assert.ElementsMatch(t, []string{"cm-beta"}, appliedNames(res),
		"only the member that actually landed may be advertised as applied")
	assert.Contains(t, res.Unresolved, "cm")
	assert.Contains(t, res.Unresolved, "dep")
	assert.False(t, cmExists(t, base, "dep"))
	assert.Zero(t, hookCalls.Load(), "a failed create is not a tolerated update rejection")
}
