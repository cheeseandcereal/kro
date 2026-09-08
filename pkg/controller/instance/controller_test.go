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

package instance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apimachineryruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance/applyset"
	"github.com/kubernetes-sigs/kro/pkg/graph/revisions"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/rgdadapter"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/requeue"
)

// TestReconcileInstanceLoad exercises the engine-agnostic instance-load path
// that runs before the deletion/graph-engine branches.
func TestReconcileInstanceLoad(t *testing.T) {
	tests := []struct {
		name    string
		objects []apimachineryruntime.Object
		getErr  string
		request types.NamespacedName
		wantErr string
	}{
		{
			name:    "instance not found",
			request: types.NamespacedName{Name: "missing", Namespace: "default"},
		},
		{
			name:    "load errors are returned",
			objects: []apimachineryruntime.Object{newInstanceObject("demo", "default")},
			getErr:  "get failed",
			request: types.NamespacedName{Name: "demo", Namespace: "default"},
			wantErr: "get failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := newControllerTestDynamicClient(t, tt.objects...)
			if tt.getErr != "" {
				raw.PrependReactor("get", "webapps", func(action k8stesting.Action) (bool, apimachineryruntime.Object, error) {
					return true, nil, errors.New(tt.getErr)
				})
			}

			controller, _ := newControllerUnderTest(t, raw, newTestGraph())
			err := controller.Reconcile(context.Background(), ctrl.Request{NamespacedName: tt.request})

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestReconcileDeletionRemovesFinalizer(t *testing.T) {
	instance := newInstanceObject("demo", "default")
	addEmptyDeletionScope(instance)
	metadata.SetInstanceFinalizer(instance)
	instance.SetDeletionTimestamp(new(metav1.NewTime(time.Now())))

	raw := newControllerTestDynamicClient(t, instance.DeepCopy())
	controller, _ := newControllerUnderTest(t, raw, newTestGraph())

	err := controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()},
	})
	require.NoError(t, err)

	stored := getStoredParentObject(t, raw)
	assert.False(t, metadata.HasInstanceFinalizer(stored))
	assert.Equal(t, metav1.ConditionUnknown, conditionByType(t, stored, ResourcesReady).Status)
}

func TestReconcileDeletionPreservesAuthorStatusWithoutRuntime(t *testing.T) {
	instance := newInstanceObject("demo", "default")
	addEmptyDeletionScope(instance)
	metadata.SetInstanceFinalizer(instance)
	instance.SetDeletionTimestamp(new(metav1.NewTime(time.Now())))
	require.NoError(t, unstructured.SetNestedMap(instance.Object, map[string]any{
		"state":    string(v1alpha1.InstanceStateActive),
		"endpoint": "https://example.test",
		"conditions": []any{map[string]any{
			"type":               "AuthorHealthy",
			"status":             "True",
			"reason":             "Healthy",
			"lastTransitionTime": "2026-01-01T00:00:00Z",
		}},
	}, "status"))

	raw := newControllerTestDynamicClient(t, instance.DeepCopy())
	controller, _ := newControllerUnderTest(t, raw, newTestGraph())
	controller.reconcileConfig.HasAuthorConditions = true

	require.NoError(t, controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()},
	}))

	stored := getStoredParentObject(t, raw)
	assert.False(t, metadata.HasInstanceFinalizer(stored))
	assert.Equal(t, "https://example.test", stored.Object["status"].(map[string]any)["endpoint"])
	conditions := conditionsFromInstance(stored)
	require.Len(t, conditions, 2)
	authorHealthy := conditionByType(t, stored, "AuthorHealthy")
	require.NotNil(t, authorHealthy.LastTransitionTime)
	assert.Equal(t, "2026-01-01T00:00:00Z", authorHealthy.LastTransitionTime.UTC().Format(time.RFC3339))
	resourcesReady := conditionByType(t, stored, ResourcesReady)
	assert.Equal(t, metav1.ConditionUnknown, resourcesReady.Status)
	assert.Equal(t, new("UnderDeletion"), resourcesReady.Reason)
}

func TestReconcileDeletionSurfacesErrorsWithAuthorConditions(t *testing.T) {
	instance := newInstanceObject("demo", "default")
	metadata.SetInstanceFinalizer(instance)
	instance.SetDeletionTimestamp(new(metav1.NewTime(time.Now())))
	require.NoError(t, unstructured.SetNestedMap(instance.Object, map[string]any{
		"state": string(v1alpha1.InstanceStateActive),
		"conditions": []any{map[string]any{
			"type":               "AuthorHealthy",
			"status":             "True",
			"reason":             "Healthy",
			"lastTransitionTime": "2026-01-01T00:00:00Z",
		}},
	}, "status"))

	raw := newControllerTestDynamicClient(t, instance.DeepCopy())
	controller, _ := newControllerUnderTest(t, raw, newTestGraph())
	controller.reconcileConfig.HasAuthorConditions = true

	err := controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()},
	})
	require.Error(t, err)

	stored := getStoredParentObject(t, raw)
	assert.True(t, metadata.HasInstanceFinalizer(stored))
	assert.Equal(t, metav1.ConditionTrue, conditionByType(t, stored, "AuthorHealthy").Status)
	resourcesReady := conditionByType(t, stored, ResourcesReady)
	assert.Equal(t, metav1.ConditionUnknown, resourcesReady.Status)
	require.NotNil(t, resourcesReady.Message)
	assert.Contains(t, *resourcesReady.Message, "deletion blocked")
	assert.Contains(t, *resourcesReady.Message, applyset.ApplySetParentIDLabel)
}

func TestReconcileApplySetInventory_FullyResolvedGate(t *testing.T) {
	// A hard apply error (pruneOK=false) withholds pruning entirely, even with
	// unresolved nodes; a fully resolved cycle (pruneOK, nil retain) prunes.
	t.Run("hard error does not prune orphans", func(t *testing.T) {
		instance := newInstanceObject("demo", "default")
		addDeletionScope(instance, controllerTestDeployGVK, "default")

		orphan := newManagedObject(newDeploymentObject("orphan-deploy", "default"), instance, "deploy", 1)
		raw := newControllerTestDynamicClient(t, instance.DeepCopy(), orphan)
		controller, _ := newControllerUnderTest(t, raw, newTestGraph())

		// applied is empty (orphan is not in applied), but apply hit a hard error
		applied := []v1alpha1.ManagedResource{}
		err := controller.reconcileApplySetInventory(context.Background(), controller.log, instance, nil, applied, applyset.Metadata{}, false, nil)
		require.NoError(t, err)

		// The orphan must NOT be deleted from the dynamic client
		stored, err := raw.Tracker().Get(controllerTestDeployGVR, "default", "orphan-deploy")
		require.NoError(t, err, "orphan resource must not be pruned on a hard apply error")
		require.NotNil(t, stored)
	})

	t.Run("hard error with unresolved nodes does not prune orphans", func(t *testing.T) {
		instance := newInstanceObject("demo", "default")
		addDeletionScope(instance, controllerTestDeployGVK, "default")

		orphan := newManagedObject(newDeploymentObject("orphan-deploy", "default"), instance, "deploy", 1)
		raw := newControllerTestDynamicClient(t, instance.DeepCopy(), orphan)
		controller, _ := newControllerUnderTest(t, raw, newTestGraph())

		// "deploy" resolved (its orphan would be prunable) but the hard error dominates.
		pruneOK, retain := pruneGate(true, []string{"other"})
		err := controller.reconcileApplySetInventory(context.Background(), controller.log, instance, nil, nil, applyset.Metadata{}, pruneOK, retain)
		require.NoError(t, err)

		_, err = raw.Tracker().Get(controllerTestDeployGVR, "default", "orphan-deploy")
		require.NoError(t, err, "a hard apply error must veto pruning even for resolved nodes' orphans")
	})

	t.Run("fullyResolved true prunes orphans", func(t *testing.T) {
		instance := newInstanceObject("demo", "default")
		addDeletionScope(instance, controllerTestDeployGVK, "default")

		orphan := newManagedObject(newDeploymentObject("orphan-deploy", "default"), instance, "deploy", 1)
		raw := newControllerTestDynamicClient(t, instance.DeepCopy(), orphan)
		controller, _ := newControllerUnderTest(t, raw, newTestGraph())

		// applied is empty (orphan is not in applied), and the cycle is fully resolved
		applied := []v1alpha1.ManagedResource{}
		err := controller.reconcileApplySetInventory(context.Background(), controller.log, instance, nil, applied, applyset.Metadata{}, true, nil)
		require.NoError(t, err)

		// The orphan must be deleted from the dynamic client
		_, err = raw.Tracker().Get(controllerTestDeployGVR, "default", "orphan-deploy")
		require.Error(t, err, "orphan resource must be pruned when fully resolved")
	})
}

// TestPruneGate pins the wiring that combines the hard-error signal and the
// owning-Unresolved set into the ApplySet prune decision: only a hard error
// withholds pruning; an unresolved owning node yields a retain predicate that
// covers exactly its members (and its subgraph descendants) plus members that
// cannot be attributed (no label, hashed token).
func TestPruneGate(t *testing.T) {
	cases := []struct {
		name       string
		hardErr    bool
		unresolved []string
		wantPrune  bool
		wantRetain bool
	}{
		{name: "resolved and no hard error prunes without retain", hardErr: false, unresolved: nil, wantPrune: true, wantRetain: false},
		{name: "hard error blocks prune", hardErr: true, unresolved: nil, wantPrune: false, wantRetain: false},
		{name: "unresolved nodes retain but do not block prune", hardErr: false, unresolved: []string{"nodeA"}, wantPrune: true, wantRetain: true},
		{name: "hard error and unresolved block prune", hardErr: true, unresolved: []string{"nodeA"}, wantPrune: false, wantRetain: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pruneOK, retain := pruneGate(tc.hardErr, tc.unresolved)
			if pruneOK != tc.wantPrune {
				t.Fatalf("pruneGate(%v, %v) pruneOK = %v, want %v", tc.hardErr, tc.unresolved, pruneOK, tc.wantPrune)
			}
			if (retain != nil) != tc.wantRetain {
				t.Fatalf("pruneGate(%v, %v) retain != nil = %v, want %v", tc.hardErr, tc.unresolved, retain != nil, tc.wantRetain)
			}
		})
	}

	member := func(nodeIDLabel string) *unstructured.Unstructured {
		obj := newConfigMapObject("m", "default")
		if nodeIDLabel != "" {
			obj.SetLabels(map[string]string{metadata.NodeIDLabel: nodeIDLabel})
		}
		return obj
	}
	retainFor := func(unresolved ...string) func(*unstructured.Unstructured) bool {
		_, retain := pruneGate(false, unresolved)
		require.NotNil(t, retain)
		return retain
	}
	hashed := metadata.NodeIDToken("outer/" + strings.Repeat("Xy", 34))
	require.True(t, metadata.IsHashedNodeIDToken(hashed), "test precondition: over-long path must hash")

	t.Run("retain keeps only the unresolved node's members", func(t *testing.T) {
		retain := retainFor("summary")
		assert.True(t, retain(member("summary")), "member of the unresolved node must be retained")
		assert.False(t, retain(member("cms")), "member of a resolved node must stay prunable")
		assert.False(t, retain(member("summaryx")), "a sibling sharing a prefix is not the same node")
	})

	t.Run("retain keeps unattributable members", func(t *testing.T) {
		retain := retainFor("summary")
		assert.True(t, retain(member("")), "a member with no node-id label cannot be attributed and must be retained")
		assert.True(t, retain(member(hashed)), "a hashed token carries no structure and must be retained")
	})

	t.Run("retain covers a subgraph node's descendants", func(t *testing.T) {
		// An unresolved subgraph node is reported by its own id; its children's
		// members carry the '.'-qualified child tokens.
		retain := retainFor("sub")
		assert.True(t, retain(member("sub")))
		assert.True(t, retain(member("sub.child")), "child of the unresolved subgraph must be retained")
		assert.True(t, retain(member("sub.child.grandchild")), "nested descendant must be retained")
		assert.False(t, retain(member("subx.child")), "a different subgraph sharing a prefix must stay prunable")
		assert.False(t, retain(member("other")))
	})

	t.Run("retain derives the token from a prefixed child id", func(t *testing.T) {
		// applySubgraph reports a child's unresolved id as "sub/child", and the
		// executor stamped that child's members with NodeIDToken("sub/child").
		retain := retainFor("sub/child")
		assert.True(t, retain(member("sub.child")))
		assert.False(t, retain(member("sub.sibling")), "a resolved sibling in the same subgraph must stay prunable")
		assert.False(t, retain(member("sub")), "the enclosing subgraph's other members are not the child's")
	})
}

// TestOwnedUnresolved pins that only UNRESOLVED nodes that actually OWN managed
// resources influence the prune decision. An ownerless node — the synthesized
// `instance` status patch node, any other patch node, a read-only ref node, or
// a def node — owns no cluster resource, so its being Unresolved must neither
// retain anything nor hold back the inventory shrink. ownedUnresolved is the
// filter applied to ApplyResult.Unresolved before it reaches pruneGate.
//
// Before the fix (pruneGate fed the raw Unresolved set) an unresolved ownerless
// node vetoes every prune and a resource removed from the RGD is never deleted;
// after the fix ownedUnresolved drops it and pruning proceeds.
func TestOwnedUnresolved(t *testing.T) {
	comp := newTestRealCompiler(t)
	inst := newInstanceObject("demo", "default")

	// RGD with a template resource (owns a ConfigMap) AND a read-only
	// externalRef resource (a ref node, which owns nothing — kro never applies
	// or prunes it). Both target the ConfigMap kind, which the fake resolver
	// knows, so the runtime compiles without a synthesized instance schema.
	rgd := &v1alpha1.ResourceGraphDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "webapp"},
		Spec: v1alpha1.ResourceGraphDefinitionSpec{
			Schema: &v1alpha1.Schema{
				APIVersion: "v1alpha1",
				Kind:       "WebApp",
				Group:      "kro.run",
				Spec:       apimachineryruntime.RawExtension{Raw: []byte(`{}`)},
			},
			Resources: []*v1alpha1.Resource{
				{
					ID: "cm",
					Template: apimachineryruntime.RawExtension{
						Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-1"}}`),
					},
				},
				{
					ID: "existing",
					ExternalRef: &v1alpha1.ExternalRef{
						APIVersion: "v1",
						Kind:       "ConfigMap",
						Metadata:   v1alpha1.ExternalRefMetadata{Name: "imported", Namespace: "default"},
					},
				},
			},
		},
	}
	rt, _, err := rgdadapter.BuildRuntimeForInstance(rgd, inst, comp)
	require.NoError(t, err)

	// Sanity: the runtime carries both an owning template node ("cm") and the
	// ownerless read-only ref node ("existing").
	cmNode := rt.Node("cm")
	require.NotNil(t, cmNode, "template node must exist")
	require.Equal(t, compiler.NodeKindTemplate, cmNode.Kind(), "cm is an owning template node")
	refNode := rt.Node("existing")
	require.NotNil(t, refNode, "externalRef node must exist")
	require.Equal(t, compiler.NodeKindRef, refNode.Kind(), "externalRef compiles to an ownerless ref node")

	t.Run("ownerless ref node is dropped from the unresolved set", func(t *testing.T) {
		// Only the ownerless ref node is unresolved: prune is unrestricted.
		owning := ownedUnresolved(rt, []string{"existing"})
		assert.Empty(t, owning, "ownerless ref node must not influence pruning")
		pruneOK, retain := pruneGate(false, owning)
		assert.True(t, pruneOK, "prune must proceed when only ownerless nodes are unresolved")
		assert.Nil(t, retain, "nothing is retained when only ownerless nodes are unresolved")
	})

	t.Run("owning template node retains its members", func(t *testing.T) {
		owning := ownedUnresolved(rt, []string{"cm"})
		assert.Equal(t, []string{"cm"}, owning, "an unresolved owning template node must remain in the set")
		pruneOK, retain := pruneGate(false, owning)
		assert.True(t, pruneOK, "other nodes' orphans stay prunable")
		require.NotNil(t, retain, "an unresolved owning node must retain its members")
	})

	t.Run("mixed set keeps only owning nodes", func(t *testing.T) {
		owning := ownedUnresolved(rt, []string{"existing", "cm"})
		assert.Equal(t, []string{"cm"}, owning, "only the owning node survives the filter")
		_, retain := pruneGate(false, owning)
		assert.NotNil(t, retain)
	})

	t.Run("unknown node id is conservatively kept and retains exactly its members", func(t *testing.T) {
		// A subgraph-prefixed child ID cannot be resolved back to a node of this
		// runtime and is treated as owning; the child executor stamped that
		// child's members with the token of the same prefixed id.
		owning := ownedUnresolved(rt, []string{"sub/child"})
		assert.Equal(t, []string{"sub/child"}, owning, "unclassifiable node id must remain in the set")
		_, retain := pruneGate(false, owning)
		require.NotNil(t, retain)
		child := newConfigMapObject("child-cm", "default")
		child.SetLabels(map[string]string{metadata.NodeIDLabel: metadata.NodeIDToken("sub/child")})
		assert.True(t, retain(child), "the prefixed child's own members are retained")
		other := newConfigMapObject("cm-1", "default")
		other.SetLabels(map[string]string{metadata.NodeIDLabel: "cm"})
		assert.False(t, retain(other), "a resolved root node's orphan stays prunable")
	})
}

// TestReconcile_EmitsInitialConditionEventsOnFirstReconcile asserts that on the
// very first reconcile (where stampInstanceMetadata patches metadata and
// updates the in-memory instance), condition-transition events are emitted for
// the full initial condition set (InstanceManaged, GraphResolved, ResourcesReady, Ready).
func TestReconcile_EmitsInitialConditionEventsOnFirstReconcile(t *testing.T) {
	comp := newTestRealCompiler(t)
	inst := newInstanceObject("demo", "default")
	raw := newControllerTestDynamicClient(t, inst.DeepCopy())
	controller, _ := newGraphEngineControllerUnderTest(t, raw, testEmptyRGDSpec(), revisions.RevisionStateActive, comp, nil)

	fakeRecorder := record.NewFakeRecorder(100)
	controller.eventRecorder = fakeRecorder
	controller.eventsEnabled = true

	err := controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	})
	require.NoError(t, err)

	events := drainEvents(fakeRecorder)
	require.NotEmpty(t, events, "initial condition set must emit transition events on first reconcile")

	// Verify events for all four built-in conditions were emitted
	var hasManaged, hasResolved, hasResourcesReady, hasReady bool
	for _, e := range events {
		if strings.Contains(e, InstanceManaged) {
			hasManaged = true
		}
		if strings.Contains(e, GraphResolved) {
			hasResolved = true
		}
		if strings.Contains(e, ResourcesReady) {
			hasResourcesReady = true
		}
		if strings.Contains(e, Ready) {
			hasReady = true
		}
	}
	assert.True(t, hasManaged, "must emit event for InstanceManaged")
	assert.True(t, hasResolved, "must emit event for GraphResolved")
	assert.True(t, hasResourcesReady, "must emit event for ResourcesReady")
	assert.True(t, hasReady, "must emit event for Ready")

	// On second reconcile without changes, no new events should be emitted
	err = controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	})
	require.NoError(t, err)
	events2 := drainEvents(fakeRecorder)
	assert.Empty(t, events2, "no events should be emitted when conditions have not changed")
}

// TestReconcile_FailedRevisionDoesNotDelayedRequeue asserts that when the latest
// revision is in a failed state, Reconcile returns requeue.None (not a delayed requeue)
// and marks the instance condition explaining the failed revision.
func TestReconcile_FailedRevisionDoesNotDelayedRequeue(t *testing.T) {
	comp := newTestRealCompiler(t)
	inst := newInstanceObject("demo", "default")
	raw := newControllerTestDynamicClient(t, inst.DeepCopy())
	controller, _ := newGraphEngineControllerUnderTest(t, raw, testEmptyRGDSpec(), revisions.RevisionStateFailed, comp, nil)

	err := controller.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"},
	})
	require.Error(t, err)
	assert.False(t, requeue.IsRequeueError(err), "failed revision must return requeue.None, not a requeue error")
	var noReq *requeue.NoRequeue
	assert.True(t, errors.As(err, &noReq), "error must be *requeue.NoRequeue")
	assert.Contains(t, err.Error(), "latest issued revision 1 failed")

	stored := getStoredParentObject(t, raw)
	cond := conditionByType(t, stored, GraphResolved)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	require.NotNil(t, cond.Message)
	assert.Contains(t, *cond.Message, "latest issued revision 1 failed")
}
