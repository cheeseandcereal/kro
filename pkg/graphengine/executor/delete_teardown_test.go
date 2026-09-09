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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// resettableMapper is a meta.ResettableRESTMapper whose Reset is observable,
// standing in for a deferred-discovery mapper.
type resettableMapper struct {
	meta.RESTMapper
	resets int
	// onReset is invoked on every Reset so the wrapping client can "heal".
	onReset func()
}

func (m *resettableMapper) Reset() {
	m.resets++
	if m.onReset != nil {
		m.onReset()
	}
}

// noMatchClient fails Get/Delete with a raw *meta.NoKindMatchError until healed;
// with healOnReset the mapper's Reset heals it (a stale mapper predating a
// re-created CRD), otherwise the NoMatch persists (the CRD is gone).
type noMatchClient struct {
	client.Client
	mapper      *resettableMapper
	healOnReset bool
	healed      bool
	deletes     int
	gets        int
	// deleteErr, when set, replaces the NoMatch on Delete (any error class).
	deleteErr error
}

func newNoMatchClient(t *testing.T, healOnReset bool) *noMatchClient {
	t.Helper()
	inner := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	c := &noMatchClient{Client: inner, healOnReset: healOnReset}
	c.mapper = &resettableMapper{RESTMapper: inner.RESTMapper(), onReset: func() {
		if c.healOnReset {
			c.healed = true
		}
	}}
	return c
}

func (c *noMatchClient) RESTMapper() meta.RESTMapper { return c.mapper }

func (c *noMatchClient) noMatch(obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	return &meta.NoKindMatchError{GroupKind: gvk.GroupKind(), SearchedVersions: []string{gvk.Version}}
}

func (c *noMatchClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	c.deletes++
	if c.deleteErr != nil {
		return c.deleteErr
	}
	if !c.healed {
		return c.noMatch(obj)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *noMatchClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets++
	if !c.healed {
		return c.noMatch(obj)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// TestSimple_Delete_NoMatch pins the NoMatch handling: one mapper reset and one
// retry, then a persistent NoMatch is "already gone" while a healed one deletes.
func TestSimple_Delete_NoMatch(t *testing.T) {
	t.Parallel()

	entryWithUID := func(name string) expv1alpha1.ManagedResource {
		return expv1alpha1.ManagedResource{
			NodeID: "w", APIVersion: "e2.test/v1", Kind: "Widget",
			Namespace: "default", Name: name, UID: "uid-" + name,
		}
	}
	entryNoUID := func(name string) expv1alpha1.ManagedResource {
		mr := entryWithUID(name)
		mr.UID = ""
		return mr
	}
	seedLabelledCM := func(t *testing.T, c client.Client, name string) expv1alpha1.ManagedResource {
		cm := &unstructured.Unstructured{}
		cm.SetGroupVersionKind(configMapGVK)
		cm.SetNamespace("default")
		cm.SetName(name)
		cm.SetUID(types.UID("uid-" + name))
		cm.SetLabels(map[string]string{
			metadata.InstanceIDLabel: string(testOwnerUID),
			metadata.NodeIDLabel:     nodeIDTokenForPath("items"),
		})
		require.NoError(t, c.Create(context.Background(), cm))
		return expv1alpha1.ManagedResource{
			NodeID: "items", APIVersion: "v1", Kind: "ConfigMap",
			Namespace: "default", Name: name,
		}
	}

	cases := []struct {
		name        string
		healOnReset bool
		deleteErr   error
		seed        func(t *testing.T, c client.Client) []expv1alpha1.ManagedResource
		wantErr     string
		wantResets  int
		wantDeletes int
		wantGets    int
		after       func(t *testing.T, c client.Client)
	}{
		{
			name:        "persistent NoMatch on a UID entry is tolerated after one mapper refresh",
			healOnReset: false,
			seed: func(*testing.T, client.Client) []expv1alpha1.ManagedResource {
				return []expv1alpha1.ManagedResource{entryWithUID("gone")}
			},
			wantResets:  1,
			wantDeletes: 2, // first attempt + the post-refresh retry
		},
		{
			name:        "NoMatch that heals after the mapper refresh is retried and deletes",
			healOnReset: true,
			seed: func(t *testing.T, c client.Client) []expv1alpha1.ManagedResource {
				return []expv1alpha1.ManagedResource{newSeededCM(t, c, "revived", "default")}
			},
			wantResets:  1,
			wantDeletes: 2,
			after: func(t *testing.T, c client.Client) {
				assertCMGone(t, c, "revived", "default")
			},
		},
		{
			name:        "persistent NoMatch on a UID-free entry's GET is tolerated without any delete",
			healOnReset: false,
			seed: func(*testing.T, client.Client) []expv1alpha1.ManagedResource {
				return []expv1alpha1.ManagedResource{entryNoUID("gone")}
			},
			wantResets:  1,
			wantGets:    2,
			wantDeletes: 0,
		},
		{
			name:        "NoMatch on a UID-free entry's GET that heals proceeds to verify and delete",
			healOnReset: true,
			seed: func(t *testing.T, c client.Client) []expv1alpha1.ManagedResource {
				return []expv1alpha1.ManagedResource{seedLabelledCM(t, c, "member")}
			},
			wantResets:  1,
			wantGets:    2,
			wantDeletes: 1,
			after: func(t *testing.T, c client.Client) {
				assertCMGone(t, c, "member", "default")
			},
		},
		{
			name:      "a non-NoMatch error is neither retried nor tolerated, and later entries are still visited",
			deleteErr: apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "cm", fmt.Errorf("denied")),
			seed: func(*testing.T, client.Client) []expv1alpha1.ManagedResource {
				return []expv1alpha1.ManagedResource{entryWithUID("first"), entryWithUID("second")}
			},
			wantErr:     "forbidden",
			wantResets:  0,
			wantDeletes: 2, // both entries attempted exactly once each, no retry
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cl := newNoMatchClient(t, tc.healOnReset)
			cl.deleteErr = tc.deleteErr
			resources := tc.seed(t, cl.Client)

			err := NewSimple(cl).Delete(context.Background(), testOwnerUID, resources)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantResets, cl.mapper.resets, "mapper resets")
			assert.Equal(t, tc.wantDeletes, cl.deletes, "Delete calls")
			if tc.wantGets > 0 {
				assert.Equal(t, tc.wantGets, cl.gets, "Get calls")
			}
			if tc.after != nil {
				tc.after(t, cl.Client)
			}
		})
	}
}

// TestOwnedByGraph pins the marker matrix a UID-free entry is verified against.
func TestOwnedByGraph(t *testing.T) {
	t.Parallel()
	self := types.UID("graph-self")
	peer := types.UID("graph-peer")
	newCM := func(labels map[string]string, managers ...string) *unstructured.Unstructured {
		o := obj("cm")
		o.SetNamespace("default")
		if labels != nil {
			o.SetLabels(labels)
		}
		return withManagedFields(o, managers...)
	}
	memberLabels := func(uid types.UID, nodePath string) map[string]string {
		return map[string]string{
			metadata.InstanceIDLabel: string(uid),
			metadata.NodeIDLabel:     nodeIDTokenForPath(nodePath),
		}
	}
	legacyPerNode := templateFieldManagerPrefix + graphManagerSegment(self) + ".deadbeef"

	cases := []struct {
		name   string
		live   *unstructured.Unstructured
		owner  types.UID
		nodeID string
		want   bool
	}{
		{name: "nil object is never owned", live: nil, owner: self, nodeID: "n"},
		{name: "empty owner UID verifies nothing", live: newCM(nil, templateFieldManager(self)), owner: "", nodeID: "n"},
		{name: "no markers at all", live: newCM(nil), owner: self, nodeID: "n"},
		{name: "own template manager", live: newCM(nil, templateFieldManager(self)), owner: self, nodeID: "n", want: true},
		{name: "own template manager among other managers", live: newCM(nil, "kubectl-client-side-apply", FieldManager, templateFieldManager(self)), owner: self, nodeID: "n", want: true},
		{name: "legacy per-node manager of the same Graph", live: newCM(nil, legacyPerNode), owner: self, nodeID: "n", want: true},
		{name: "peer Graph's template manager", live: newCM(nil, templateFieldManager(peer)), owner: self, nodeID: "n"},
		{name: "shared RGD field manager is not Graph ownership", live: newCM(nil, FieldManager), owner: self, nodeID: "n"},
		{name: "non-kro manager", live: newCM(nil, "kubectl-client-side-apply"), owner: self, nodeID: "n"},
		{name: "collection member labels match", live: newCM(memberLabels(self, "sub/items")), owner: self, nodeID: "sub/items", want: true},
		{name: "collection member of a peer Graph", live: newCM(memberLabels(peer, "sub/items")), owner: self, nodeID: "sub/items"},
		{name: "collection member of another node", live: newCM(memberLabels(self, "sub/other")), owner: self, nodeID: "sub/items"},
		{name: "instance-id alone without node-id is not enough", live: newCM(map[string]string{metadata.InstanceIDLabel: string(self)}), owner: self, nodeID: "items"},
		{name: "an entry without a node ID never matches an unlabelled node-id", live: newCM(map[string]string{metadata.InstanceIDLabel: string(self)}), owner: self, nodeID: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ownedByGraph(tc.live, tc.owner, tc.nodeID))
		})
	}
}

// TestNodeIDTokenForPath_MatchesNodeIDToken pins that the token teardown derives
// from a persisted NodeID equals the one the executor stamps through its frame.
func TestNodeIDTokenForPath_MatchesNodeIDToken(t *testing.T) {
	t.Parallel()
	root := &Simple{}
	child := &Simple{nodePrefix: "subA/subB/"}
	assert.Equal(t, root.nodeIDToken("res"), nodeIDTokenForPath("res"))
	assert.Equal(t, child.nodeIDToken("res"), nodeIDTokenForPath("subA/subB/res"))
	long := &Simple{nodePrefix: "aVeryLongSubgraphNameThatKeepsGoing/andAnotherEquallyLongNestedFrame/"}
	assert.Equal(t, long.nodeIDToken("someLeafNodeWithALongIdentifier"),
		nodeIDTokenForPath("aVeryLongSubgraphNameThatKeepsGoing/andAnotherEquallyLongNestedFrame/someLeafNodeWithALongIdentifier"),
		"the hashed fallback must agree too")
}

// TestSimple_Delete_UIDFreeEntries tears down an inventory of UID-free entries
// against a real apiserver (real SSA managedFields, real UID preconditions):
// objects this Graph applied are deleted, everything else named survives.
func TestSimple_Delete_UIDFreeEntries(t *testing.T) {
	cl := patchEnvClient(t)
	ctx := context.Background()
	ns := "default"
	self := types.UID("uid-graph-teardown-self")
	peer := types.UID("uid-graph-teardown-peer")

	ssaCM := func(name, manager string) {
		cm := &unstructured.Unstructured{}
		cm.SetGroupVersionKind(configMapGVK)
		cm.SetNamespace(ns)
		cm.SetName(name)
		require.NoError(t, unstructured.SetNestedField(cm.Object, "v", "data", "k"))
		require.NoError(t, cl.Patch(ctx, cm, client.Apply, client.FieldOwner(manager)))
	}
	createCM := func(name string, labels map[string]string) {
		cm := &unstructured.Unstructured{}
		cm.SetGroupVersionKind(configMapGVK)
		cm.SetNamespace(ns)
		cm.SetName(name)
		if labels != nil {
			cm.SetLabels(labels)
		}
		require.NoError(t, cl.Create(ctx, cm))
	}
	memberLabels := func(uid types.UID, nodePath string) map[string]string {
		return map[string]string{
			metadata.InstanceIDLabel: string(uid),
			metadata.NodeIDLabel:     nodeIDTokenForPath(nodePath),
		}
	}
	entry := func(name, nodeID, uid string) expv1alpha1.ManagedResource {
		return expv1alpha1.ManagedResource{
			NodeID: nodeID, APIVersion: "v1", Kind: "ConfigMap",
			Namespace: ns, Name: name, UID: uid,
		}
	}
	exists := func(name string) bool {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(configMapGVK)
		err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, got)
		if apierrors.IsNotFound(err) {
			return false
		}
		require.NoError(t, err)
		return true
	}

	// Objects this Graph applied: template manager, legacy per-node manager,
	// stamped collection member.
	ssaCM("wa-owned", TemplateFieldManager(self))
	ssaCM("wa-legacy-owned", templateFieldManagerPrefix+graphManagerSegment(self)+".abcdef")
	createCM("wa-member", memberLabels(self, "sub/items"))
	// Objects this Graph must NOT touch.
	createCM("wa-control", nil)
	ssaCM("wa-peer", TemplateFieldManager(peer))
	createCM("wa-peer-member", memberLabels(peer, "sub/items"))
	createCM("wa-other-node-member", memberLabels(self, "sub/other"))
	createCM("wa-impostor", nil)
	require.True(t, hasFieldManager(getConfigMap(t, cl, ns, "wa-owned"), TemplateFieldManager(self)),
		"precondition: SSA landed the Graph's template field manager")

	err := NewSimple(cl).Delete(ctx, self, []expv1alpha1.ManagedResource{
		entry("wa-owned", "owned", ""),
		entry("wa-legacy-owned", "owned2", ""),
		entry("wa-member", "sub/items", ""),
		entry("wa-control", "owned", ""),
		entry("wa-peer", "owned", ""),
		entry("wa-peer-member", "sub/items", ""),
		entry("wa-other-node-member", "sub/items", ""),
		entry("wa-never-landed", "owned", ""),
		// A stale recorded UID: the apiserver rejects the precondition (Conflict).
		entry("wa-impostor", "owned", "00000000-stale-uid-0000"),
	})
	require.NoError(t, err, "no entry in this inventory is an error condition")

	assert.False(t, exists("wa-owned"), "object under this Graph's template manager must be deleted")
	assert.False(t, exists("wa-legacy-owned"), "object under this Graph's legacy per-node manager must be deleted")
	assert.False(t, exists("wa-member"), "this Graph's collection member must be deleted")

	assert.True(t, exists("wa-control"), "an unmarked object named by a UID-free entry must survive")
	assert.True(t, exists("wa-peer"), "a peer Graph's object must survive")
	assert.True(t, exists("wa-peer-member"), "a peer Graph's collection member must survive")
	assert.True(t, exists("wa-other-node-member"), "another node's collection member must survive")
	assert.True(t, exists("wa-impostor"), "a UID precondition mismatch must leave the live object alone")
}

// TestSimple_Delete_ColdMapperTreatsMissingTypeAsGone deletes the CRD behind an
// inventory entry and tears down with a fresh client (cold lazy mapper), which
// surfaces the missing mapping as a raw NoMatch; Delete must treat it as gone.
func TestSimple_Delete_ColdMapperTreatsMissingTypeAsGone(t *testing.T) {
	cl := patchEnvClient(t)
	ctx := context.Background()
	ns := "default"

	const group = "teardown.test.kro.run"
	gizmoGVK := schema.GroupVersionKind{Group: group, Version: "v1", Kind: "Gizmo"}
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "gizmos." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind: "Gizmo", ListKind: "GizmoList", Plural: "gizmos", Singular: "gizmo",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec": {Type: "object", XPreserveUnknownFields: new(true)},
						},
					},
				},
			}},
		},
	}
	require.NoError(t, cl.Create(ctx, crd))
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		got := &apiextensionsv1.CustomResourceDefinition{}
		if err := cl.Get(ctx, types.NamespacedName{Name: crd.Name}, got); err != nil {
			return false, nil
		}
		for _, c := range got.Status.Conditions {
			if c.Type == apiextensionsv1.Established && c.Status == apiextensionsv1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	}), "CRD must become Established")

	gizmo := &unstructured.Unstructured{}
	gizmo.SetGroupVersionKind(gizmoGVK)
	gizmo.SetNamespace(ns)
	gizmo.SetName("g1")
	gizmo.Object["spec"] = map[string]any{"field": "val"}
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		// Discovery of a freshly established CRD can lag by a moment.
		err := cl.Create(ctx, gizmo)
		return err == nil, nil
	}), "Gizmo instance must be creatable once the CRD is served")
	require.NotEmpty(t, gizmo.GetUID())
	inventory := []expv1alpha1.ManagedResource{
		{
			NodeID: "gizmo", APIVersion: group + "/v1", Kind: "Gizmo",
			Namespace: ns, Name: "g1", UID: string(gizmo.GetUID()),
		},
		{
			NodeID: "gizmo", APIVersion: group + "/v1", Kind: "Gizmo",
			Namespace: ns, Name: "g2",
		},
	}

	require.NoError(t, cl.Delete(ctx, crd))
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		err := cl.Get(ctx, types.NamespacedName{Name: crd.Name}, &apiextensionsv1.CustomResourceDefinition{})
		return apierrors.IsNotFound(err), nil
	}), "CRD must be fully removed")

	// A fresh client whose lazy mapper has never seen the group; poll until a
	// direct Delete surfaces the raw NoMatch (discovery of the removal can lag).
	scheme := k8sruntime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	var cold client.Client
	require.NoError(t, wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		fresh, err := client.New(patchEnvCfg, client.Options{Scheme: scheme})
		if err != nil {
			return false, err
		}
		probe := &unstructured.Unstructured{}
		probe.SetGroupVersionKind(gizmoGVK)
		probe.SetNamespace(ns)
		probe.SetName("g1")
		if err := fresh.Delete(ctx, probe); meta.IsNoMatchError(err) {
			cold = fresh
			return true, nil
		}
		return false, nil
	}), "a cold mapper must report the removed type as NoMatch")

	err := NewSimple(cold).Delete(ctx, types.UID("uid-graph-cold-mapper"), inventory)
	require.NoError(t, err, "a resource type the cluster no longer serves must not wedge teardown")
}
