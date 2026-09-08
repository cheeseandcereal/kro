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
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// These tests pin the status-replace write path against the real API server's
// managed-field bookkeeping (how an Update interacts with an existing
// {kro, Update, status} entry), so they run under envtest.

// widgetGVK is a namespaced test CRD with a status subresource, like every kro
// instance CRD.
var widgetGVK = schema.GroupVersionKind{Group: "test.kro.run", Version: "v1", Kind: "Widget"}

// legacyControllerStatusManager mirrors the instance controller's manager for
// conditions + state (pkg/controller/instance.instanceStatusFieldManager).
const legacyControllerStatusManager = "kro-instance-status"

// ensureWidgetCRD installs the Widget CRD (idempotent) and waits for it to be
// Established.
func ensureWidgetCRD(t *testing.T, cl client.Client) {
	t.Helper()
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "widgets.test.kro.run"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "test.kro.run",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Kind: "Widget", ListKind: "WidgetList", Plural: "widgets", Singular: "widget",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type: "object",
						Properties: map[string]apiextensionsv1.JSONSchemaProps{
							"spec":   {Type: "object", XPreserveUnknownFields: new(true)},
							"status": {Type: "object", XPreserveUnknownFields: new(true)},
						},
					},
				},
			}},
		},
	}
	if err := cl.Create(context.Background(), crd); err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
	require.NoError(t, wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
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
	}))
}

func createWidget(t *testing.T, cl client.Client, ns, name string) *unstructured.Unstructured {
	t.Helper()
	w := &unstructured.Unstructured{}
	w.SetGroupVersionKind(widgetGVK)
	w.SetNamespace(ns)
	w.SetName(name)
	w.Object["spec"] = map[string]any{"field": "val"}
	require.NoError(t, cl.Create(context.Background(), w))
	return w
}

func getWidget(t *testing.T, cl client.Client, ns, name string) *unstructured.Unstructured {
	t.Helper()
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(widgetGVK)
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got))
	return got
}

// writeLegacyStatus does what a pre-graph-engine release did on every
// reconcile: UpdateStatus of the whole status under the implicit "kro" manager.
func writeLegacyStatus(t *testing.T, cl client.Client, w *unstructured.Unstructured, status map[string]any) {
	t.Helper()
	w.Object["status"] = status
	require.NoError(t, cl.Status().Update(context.Background(), w, client.FieldOwner(legacyStatusFieldManager)))
}

// takeOverConditionsAndState does what the controller's persistConditionsAndState
// does: a forced SSA of conditions + state under the instance-status manager.
func takeOverConditionsAndState(t *testing.T, cl client.Client, ns, name string, conditions []any, state string) {
	t.Helper()
	patch := &unstructured.Unstructured{}
	patch.SetGroupVersionKind(widgetGVK)
	patch.SetNamespace(ns)
	patch.SetName(name)
	patch.Object["status"] = map[string]any{
		"conditions": conditions,
		"state":      state,
	}
	require.NoError(t, cl.Status().Patch(context.Background(), patch, client.Apply,
		client.FieldOwner(legacyControllerStatusManager), client.ForceOwnership))
}

// statusOwners maps "<manager>/<operation>" to the sorted .status keys that
// managedFields entry owns.
func statusOwners(t *testing.T, obj *unstructured.Unstructured) map[string][]string {
	t.Helper()
	owners := map[string][]string{}
	for _, mf := range obj.GetManagedFields() {
		if mf.Subresource != "status" || mf.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		require.NoError(t, json.Unmarshal(mf.FieldsV1.Raw, &fields))
		statusFields, _ := fields["f:status"].(map[string]any)
		var keys []string
		for k := range statusFields {
			if k == "." {
				continue
			}
			keys = append(keys, strings.TrimPrefix(k, "f:"))
		}
		sort.Strings(keys)
		owners[mf.Manager+"/"+string(mf.Operation)] = keys
	}
	return owners
}

// statusReplaceGraph builds a Graph whose patch node "p" writes only `phase`
// to the Widget's status — the shape of the author-status node after a second
// author field (`message`) stopped resolving.
func statusReplaceGraph(ns, target, phase string) *expv1alpha1.Graph {
	return generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithDef("src", map[string]any{"phase": phase}),
		generator.WithPatch("p", "test.kro.run/v1", "Widget", target, map[string]any{
			"status": map[string]any{"phase": "${src.phase}"},
		}),
	)
}

// An instance whose author fields are owned by a pre-upgrade {kro, Update,
// status} entry: the omitted field must be removed, the legacy entry continued
// (no sibling owner), conditions/state and their ownership untouched.
func TestPatch_StatusReplace_RemovesFieldOwnedByLegacyUpdateManager(t *testing.T) {
	cl := patchEnvClient(t)
	ensureWidgetCRD(t, cl)
	ns := "default"
	const name = "legacy-widget"

	w := createWidget(t, cl, ns, name)
	writeLegacyStatus(t, cl, w, map[string]any{
		"phase":   "Pending",
		"message": "legacy-value",
		"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "reason": "Legacy"},
		},
		"state": "ACTIVE",
	})
	// Changed conditions are taken over by the controller manager; the
	// unchanged state stays co-owned by the legacy entry.
	conditions := []any{map[string]any{"type": "Ready", "status": "True", "reason": "Upgraded"}}
	takeOverConditionsAndState(t, cl, ns, name, conditions, "ACTIVE")

	owners := statusOwners(t, getWidget(t, cl, ns, name))
	require.Equal(t, []string{"message", "phase", "state"}, owners[legacyStatusFieldManager+"/Update"],
		"precondition: the legacy kro/Update entry owns the author fields (and co-owns the unchanged state)")
	require.Equal(t, []string{"conditions", "state"}, owners[legacyControllerStatusManager+"/Apply"],
		"precondition: the controller manager owns conditions + state")

	g := statusReplaceGraph(ns, name, "Running")
	g.SetUID("uid-status-replace-legacy")
	rt := compileAndBuildEnv(t, patchEnvCfg, g, compiler.WithStatusReplace("p"))
	res, err := NewSimple(cl).Apply(context.Background(), rt, watchrouter.NoopWatcher{})
	require.NoError(t, err)

	require.Len(t, res.Contributions, 1)
	assert.Equal(t, patchFieldManager(g.GetUID(), "p"), res.Contributions[0].FieldManager,
		"the ledger entry must be the same per-node patch manager the SSA path records")
	assert.Equal(t, "status", res.Contributions[0].Subresource)
	assert.Equal(t, name, res.Contributions[0].Name)
	assert.Empty(t, res.Applied, "a status-replace node is still a patch: never an owned resource")

	after := getWidget(t, cl, ns, name)
	got, _, _ := unstructured.NestedMap(after.Object, "status")
	assert.Equal(t, "Running", got["phase"], "the resolved author field is updated")
	_, hasMessage := got["message"]
	assert.False(t, hasMessage, "the omitted author field must be REMOVED even though the legacy kro/Update entry owned it")
	assert.Equal(t, conditions, got["conditions"], "controller-owned conditions are carried over untouched")
	assert.Equal(t, "ACTIVE", got["state"], "controller-owned state is carried over untouched")

	owners = statusOwners(t, after)
	assert.Equal(t, []string{"phase", "state"}, owners[legacyStatusFieldManager+"/Update"],
		"the legacy kro/Update entry is continued: message gone, phase kept, unchanged co-owned state untouched")
	assert.Equal(t, []string{"conditions", "state"}, owners[legacyControllerStatusManager+"/Apply"],
		"carrying conditions/state over unchanged must not move their ownership")
	assert.False(t, hasFieldManager(after, patchFieldManager(g.GetUID(), "p")),
		"the status-replace path must not add a per-node SSA owner to the instance status")
	var kroUpdateEntries int
	for _, mf := range after.GetManagedFields() {
		if mf.Manager == legacyStatusFieldManager && mf.Operation == metav1.ManagedFieldsOperationUpdate && mf.Subresource == "status" {
			kroUpdateEntries++
		}
	}
	assert.Equal(t, 1, kroUpdateEntries, "exactly one {kro, Update, status} entry")
}

// An instance whose author fields are owned by a per-node patch manager (the
// node previously applied by forced SSA): the omitted field is still removed,
// the changed field moves to kro/Update, and the ledger entry is unchanged so
// nothing is released.
func TestPatch_StatusReplace_RemovesFieldOwnedByHeadPatchManager(t *testing.T) {
	cl := patchEnvClient(t)
	ensureWidgetCRD(t, cl)
	ns := "default"
	const name = "head-widget"
	createWidget(t, cl, ns, name)

	headGraph := generator.NewGraph("g",
		generator.WithNamespace(ns),
		generator.WithDef("src", map[string]any{"phase": "Pending", "message": "head-state"}),
		generator.WithPatch("p", "test.kro.run/v1", "Widget", name, map[string]any{
			"status": map[string]any{"phase": "${src.phase}", "message": "${src.message}"},
		}),
	)
	headGraph.SetUID("uid-status-replace-head")
	_, err := NewSimple(cl).Apply(context.Background(), compileAndBuildEnv(t, patchEnvCfg, headGraph), watchrouter.NoopWatcher{})
	require.NoError(t, err)
	patchManager := patchFieldManager(headGraph.GetUID(), "p")
	owners := statusOwners(t, getWidget(t, cl, ns, name))
	require.Equal(t, []string{"message", "phase"}, owners[patchManager+"/Apply"],
		"precondition: the per-node patch manager owns both author fields")

	g := statusReplaceGraph(ns, name, "Running")
	g.SetUID(headGraph.GetUID())
	res, err := NewSimple(cl).Apply(context.Background(),
		compileAndBuildEnv(t, patchEnvCfg, g, compiler.WithStatusReplace("p")), watchrouter.NoopWatcher{})
	require.NoError(t, err)
	require.Len(t, res.Contributions, 1)
	assert.Equal(t, patchManager, res.Contributions[0].FieldManager,
		"the ledger entry is unchanged across the switch, so nothing is diffed out and released")

	after := getWidget(t, cl, ns, name)
	got, _, _ := unstructured.NestedMap(after.Object, "status")
	assert.Equal(t, "Running", got["phase"])
	_, hasMessage := got["message"]
	assert.False(t, hasMessage, "the omitted field is gone although the head patch manager owned it")

	owners = statusOwners(t, after)
	assert.Equal(t, []string{"phase"}, owners[legacyStatusFieldManager+"/Update"],
		"the changed field moves to the kro/Update entry")
	_, patchStillOwns := owners[patchManager+"/Apply"]
	assert.False(t, patchStillOwns,
		"the head patch manager lost phase (changed by Update) and message (removed), so its entry is gone")
}

// A status patch node WITHOUT the flag (a standalone Graph's) still applies by
// forced SSA: it reclaims the field it sets and leaves a field it does not set
// with its legacy owner.
func TestPatch_StatusSubresource_WithoutStatusReplaceKeepsForcedSSA(t *testing.T) {
	cl := patchEnvClient(t)
	ensureWidgetCRD(t, cl)
	ns := "default"
	const name = "graph-path-widget"
	w := createWidget(t, cl, ns, name)
	writeLegacyStatus(t, cl, w, map[string]any{"phase": "Pending", "message": "legacy-value"})

	g := statusReplaceGraph(ns, name, "Running") // same shape, NO WithStatusReplace
	g.SetUID("uid-status-graph-path")
	res, err := NewSimple(cl).Apply(context.Background(), compileAndBuildEnv(t, patchEnvCfg, g), watchrouter.NoopWatcher{})
	require.NoError(t, err)
	require.Len(t, res.Contributions, 1)

	after := getWidget(t, cl, ns, name)
	got, _, _ := unstructured.NestedMap(after.Object, "status")
	assert.Equal(t, "Running", got["phase"], "forced SSA reclaims and updates the field it sets")
	assert.Equal(t, "legacy-value", got["message"],
		"forced SSA leaves a field it does not set in place — the legacy owner keeps it")

	owners := statusOwners(t, after)
	assert.Equal(t, []string{"phase"}, owners[patchFieldManager(g.GetUID(), "p")+"/Apply"],
		"the Graph path still applies under the per-node patch manager")
	assert.Equal(t, []string{"message"}, owners[legacyStatusFieldManager+"/Update"],
		"the legacy entry keeps the field the patch did not claim")
}
