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

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// Fake-client coverage of replaceStatus: the write guard and the conflict
// retry. Managed-field behaviour is covered by the envtest cases.

// statusWriteRecorder counts status Updates and can inject a one-shot 409
// that first mutates the stored object, simulating a concurrent writer.
type statusWriteRecorder struct {
	client.Client
	updates      int
	conflictOnce bool
	onConflict   func(ctx context.Context, base client.Client)
}

func (r *statusWriteRecorder) Status() client.SubResourceWriter {
	return &recordingStatusWriter{rec: r, inner: r.Client.Status()}
}

type recordingStatusWriter struct {
	rec   *statusWriteRecorder
	inner client.SubResourceWriter
}

func (w *recordingStatusWriter) Create(ctx context.Context, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
	return w.inner.Create(ctx, obj, sub, opts...)
}

func (w *recordingStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.rec.updates++
	if w.rec.conflictOnce {
		w.rec.conflictOnce = false
		if w.rec.onConflict != nil {
			w.rec.onConflict(ctx, w.rec.Client)
		}
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, obj.GetName(), errors.New("the object has been modified"))
	}
	return w.inner.Update(ctx, obj, opts...)
}

func (w *recordingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return w.inner.Patch(ctx, obj, patch, opts...)
}

func (w *recordingStatusWriter) Apply(ctx context.Context, obj k8sruntime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return w.inner.Apply(ctx, obj, opts...)
}

func livePodWithStatus(name string, status map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": name, "namespace": "default"},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "c", "image": "img"}}},
		"status":   status,
	}}
}

func getPod(t *testing.T, cl client.Client, name string) *unstructured.Unstructured {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(podGVK)
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, got))
	return got
}

// statusReplacePodGraph renders `status.phase` from a def onto the named Pod.
func statusReplacePodGraph(target, phase string) *expv1alpha1.Graph {
	return generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("src", map[string]any{"phase": phase}),
		generator.WithPatch("p", "v1", "Pod", target, map[string]any{
			"status": map[string]any{"phase": "${src.phase}"},
		}),
	)
}

// An identical status must not be written at all (not merely no-op'd by the
// server): zero Updates, resourceVersion untouched.
func TestReplaceStatus_NoWriteWhenIdentical(t *testing.T) {
	t.Parallel()
	pod := livePodWithStatus("same", map[string]any{
		"phase":      "Running",
		"conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
		"state":      "ACTIVE",
	})
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(pod).WithStatusSubresource(pod).Build()
	rec := &statusWriteRecorder{Client: base}
	rv := getPod(t, base, "same").GetResourceVersion()

	g := statusReplacePodGraph("same", "Running")
	g.SetUID("uid-replace-identical")
	_, err := NewSimple(rec).Apply(context.Background(),
		compileAndBuild(t, g, compiler.WithStatusReplace("p")), watchrouter.NoopWatcher{})
	require.NoError(t, err)

	assert.Zero(t, rec.updates, "an identical status must not be written")
	assert.Equal(t, rv, getPod(t, base, "same").GetResourceVersion())
}

// The live status holds whole numbers as int64 while a render yields float64;
// they serialize identically and must not trigger a write.
func TestReplaceStatus_NoWriteWhenNumbersDifferOnlyByGoType(t *testing.T) {
	t.Parallel()
	pod := livePodWithStatus("num", map[string]any{"phase": "Running"})
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(pod).WithStatusSubresource(pod).Build()
	rec := &statusWriteRecorder{Client: base}

	live := getPod(t, base, "num")
	live.Object["status"] = map[string]any{"phase": "Running", "restarts": int64(3)}
	rendered := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "num", "namespace": "default"},
		"status":   map[string]any{"phase": "Running", "restarts": float64(3)},
	}}
	require.NoError(t, NewSimple(rec).replaceStatus(context.Background(), rendered, live))
	assert.Zero(t, rec.updates, "3 (int64) and 3.0 (float64) serialize identically: no write")
}

// A 409 must be retried from a fresh read, so the concurrent writer's
// conditions/state are carried over rather than our stale snapshot.
func TestReplaceStatus_ConflictRereadsAndRetries(t *testing.T) {
	t.Parallel()
	pod := livePodWithStatus("racy", map[string]any{
		"phase":      "Pending",
		"conditions": []any{map[string]any{"type": "Ready", "status": "False"}},
		"state":      "IN_PROGRESS",
	})
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(pod).WithStatusSubresource(pod).Build()
	freshConditions := []any{map[string]any{"type": "Ready", "status": "True"}}
	rec := &statusWriteRecorder{
		Client:       base,
		conflictOnce: true,
		onConflict: func(ctx context.Context, cl client.Client) {
			cur := getPod(t, cl, "racy")
			cur.Object["status"] = map[string]any{
				"phase":      "Pending",
				"conditions": freshConditions,
				"state":      "ACTIVE",
			}
			require.NoError(t, cl.Status().Update(ctx, cur))
		},
	}

	g := statusReplacePodGraph("racy", "Running")
	g.SetUID("uid-replace-conflict")
	res, err := NewSimple(rec).Apply(context.Background(),
		compileAndBuild(t, g, compiler.WithStatusReplace("p")), watchrouter.NoopWatcher{})
	require.NoError(t, err, "a conflict is retried, not surfaced")
	require.Len(t, res.Contributions, 1)
	assert.Equal(t, 2, rec.updates, "the first Update conflicted, the retry succeeded")

	status, _, _ := unstructured.NestedMap(getPod(t, base, "racy").Object, "status")
	assert.Equal(t, "Running", status["phase"], "our author field landed on the retry")
	assert.Equal(t, freshConditions, status["conditions"],
		"the retry carried over the conditions written by the concurrent writer, not our stale snapshot")
	assert.Equal(t, "ACTIVE", status["state"], "same for state")
}

// A target that vanishes between the conflict and the re-read is soft
// not-ready, not a hard error.
func TestReplaceStatus_TargetDeletedDuringRetryIsSoft(t *testing.T) {
	t.Parallel()
	pod := livePodWithStatus("gone", map[string]any{"phase": "Pending"})
	base := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(pod).WithStatusSubresource(pod).Build()
	rec := &statusWriteRecorder{
		Client:       base,
		conflictOnce: true,
		onConflict: func(ctx context.Context, cl client.Client) {
			require.NoError(t, cl.Delete(ctx, getPod(t, cl, "gone")))
		},
	}

	g := statusReplacePodGraph("gone", "Running")
	g.SetUID("uid-replace-gone")
	res, err := NewSimple(rec).Apply(context.Background(),
		compileAndBuild(t, g, compiler.WithStatusReplace("p")), watchrouter.NoopWatcher{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotReady), "a vanished target is soft not-ready, got %v", err)
	assert.Contains(t, res.Unresolved, "p")
	assert.Empty(t, res.Contributions)
}

func TestReplacementStatus(t *testing.T) {
	t.Parallel()

	t.Run("carries conditions and state from the live status", func(t *testing.T) {
		t.Parallel()
		rendered := map[string]any{"phase": "Running"}
		live := map[string]any{
			"phase":      "Pending",
			"message":    "stale",
			"conditions": []any{"c"},
			"state":      "ACTIVE",
		}
		got := replacementStatus(rendered, live)
		assert.Equal(t, map[string]any{
			"phase":      "Running",
			"conditions": []any{"c"},
			"state":      "ACTIVE",
		}, got, "rendered fields + carried conditions/state; the un-rendered live field is dropped")
		assert.Equal(t, map[string]any{"phase": "Running"}, rendered, "the rendered map is not mutated")
	})

	t.Run("no conditions or state when the live status has none", func(t *testing.T) {
		t.Parallel()
		got := replacementStatus(map[string]any{"phase": "Running"}, nil)
		assert.Equal(t, map[string]any{"phase": "Running"}, got)
	})

	t.Run("an empty render leaves only the controller-owned keys", func(t *testing.T) {
		t.Parallel()
		got := replacementStatus(nil, map[string]any{"phase": "Pending", "state": "ACTIVE"})
		assert.Equal(t, map[string]any{"state": "ACTIVE"}, got)
	})
}

func TestStatusJSONEqual(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b map[string]any
		want bool
	}{
		{"nil and empty are the same status", nil, map[string]any{}, true},
		{"identical maps", map[string]any{"a": "x", "n": int64(1)}, map[string]any{"a": "x", "n": int64(1)}, true},
		{"int64 vs float64 whole number", map[string]any{"n": int64(3)}, map[string]any{"n": float64(3)}, true},
		{"key order is irrelevant", map[string]any{"a": "x", "b": "y"}, map[string]any{"b": "y", "a": "x"}, true},
		{"different value", map[string]any{"a": "x"}, map[string]any{"a": "y"}, false},
		{"missing key", map[string]any{"a": "x", "b": "y"}, map[string]any{"a": "x"}, false},
		{"nested slice order matters", map[string]any{"c": []any{"1", "2"}}, map[string]any{"c": []any{"2", "1"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, statusJSONEqual(tt.a, tt.b))
			assert.Equal(t, tt.want, statusJSONEqual(tt.b, tt.a), "symmetric")
		})
	}
}
