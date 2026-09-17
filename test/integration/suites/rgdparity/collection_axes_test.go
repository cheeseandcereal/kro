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

package rgdparity_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/controller/instance"
	"github.com/kubernetes-sigs/kro/pkg/features"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

func TestRGDCollectionDimensions(t *testing.T) {
	if testing.Short() {
		t.Skip("requires envtest")
	}
	require.False(t, features.FeatureGate.Enabled(features.GraphKind))
	require.False(t, features.FeatureGate.Enabled(features.CELOmitFunction))

	for _, tc := range []struct {
		name       string
		configured int
		limit      int
	}{
		{name: "configured", configured: 11, limit: 11},
		{name: "default", limit: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			env, err := environment.New(ctx, environment.ControllerConfig{
				ReconcileConfig: instance.ReconcileConfig{
					DefaultRequeueDuration:     time.Second,
					MaxCollectionDimensionSize: tc.configured,
				},
			})
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, env.Stop()) })
			namespace := env.CreateNamespace(t)

			t.Run("above_limit_rejected", func(t *testing.T) {
				rgd := collectionAxesRGD(tc.limit + 1)
				require.NoError(t, env.Client.Create(ctx, rgd))
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					require.NoError(c, env.Client.Get(ctx, client.ObjectKeyFromObject(rgd), rgd))
					require.Equal(c, krov1alpha1.ResourceGraphDefinitionStateInactive, rgd.Status.State)
					i := slices.IndexFunc(rgd.Status.Conditions, func(cond krov1alpha1.Condition) bool {
						return cond.Type == krov1alpha1.RGDConditionTypeGraphAccepted
					})
					require.NotEqual(c, -1, i)
					cond := rgd.Status.Conditions[i]
					assert.Equal(c, metav1.ConditionFalse, cond.Status)
					assert.Equal(c, rgd.Generation, cond.ObservedGeneration)
					require.NotNil(c, cond.Message)
					assert.Contains(c, *cond.Message, fmt.Sprintf(
						"forEach cannot have more than %d dimensions, got %d", tc.limit, tc.limit+1))
				}, 30*time.Second, 100*time.Millisecond)
			})

			t.Run("at_limit_executes", func(t *testing.T) {
				rgd := collectionAxesRGD(tc.limit)
				require.NoError(t, env.Client.Create(ctx, rgd))
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					require.NoError(c, env.Client.Get(ctx, client.ObjectKeyFromObject(rgd), rgd))
					assert.Equal(c, krov1alpha1.ResourceGraphDefinitionStateActive, rgd.Status.State,
						"RGD conditions: %+v", rgd.Status.Conditions)
				}, 30*time.Second, 100*time.Millisecond)

				inst := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "kro.run/v1alpha1", "kind": rgd.Spec.Schema.Kind,
					"metadata": map[string]any{"name": "dims", "namespace": namespace},
					"spec":     map[string]any{"name": "dims"},
				}}
				require.NoError(t, env.Client.Create(ctx, inst))
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					require.NoError(c, env.Client.Get(ctx, client.ObjectKeyFromObject(inst), inst))
					state, _, err := unstructured.NestedString(inst.Object, "status", "state")
					require.NoError(c, err)
					require.Equal(c, "ACTIVE", state, "instance status: %v", inst.Object["status"])
					conditions, _, err := unstructured.NestedSlice(inst.Object, "status", "conditions")
					require.NoError(c, err)
					for _, raw := range conditions {
						cond := raw.(map[string]any)
						if cond["type"] == "Ready" {
							assert.Equal(c, "True", cond["status"])
							assert.Equal(c, inst.GetGeneration(), cond["observedGeneration"])
							return
						}
					}
					assert.Fail(c, "instance Ready condition missing")
				}, 30*time.Second, 100*time.Millisecond)

				cms := &corev1.ConfigMapList{}
				require.NoError(t, env.Client.List(ctx, cms, client.InNamespace(namespace)))
				require.Len(t, cms.Items, 1)
				assert.Equal(t, "dims"+strings.Repeat("-x", tc.limit), cms.Items[0].Name)
				assert.Equal(t, map[string]string{"value": "expanded"}, cms.Items[0].Data)
			})
		})
	}
}

func collectionAxesRGD(dimensions int) *krov1alpha1.ResourceGraphDefinition {
	axes := make([]krov1alpha1.ForEachDimension, dimensions)
	name := "${schema.spec.name}"
	for i := range axes {
		iterator := fmt.Sprintf("d%d", i)
		axes[i] = krov1alpha1.ForEachDimension{iterator: `${["x"]}`}
		name += "-${" + iterator + "}"
	}
	return generator.NewResourceGraphDefinition(fmt.Sprintf("collection-axes-%d", dimensions),
		generator.WithSchema(fmt.Sprintf("CollectionAxes%d", dimensions), "v1alpha1",
			map[string]any{"name": "string"}, nil),
		generator.WithResourceCollection("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name},
			"data":     map[string]any{"value": "expanded"},
		}, axes, nil, nil),
	)
}
