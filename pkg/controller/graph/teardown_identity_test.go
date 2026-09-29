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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
)

func TestReconcile_AppliedIdentity(t *testing.T) {
	t.Parallel()

	const current = "system:serviceaccount:default:deployer"
	const prior = "system:serviceaccount:default:old-sa"
	hard := errors.New("apply forbidden")
	resource := executor.ApplyResult{Applied: []expv1alpha1.ManagedResource{{
		NodeID: "cm", APIVersion: "v1", Kind: "ConfigMap", Namespace: "default", Name: "child", UID: "child-uid",
	}}}
	contribution := executor.ApplyResult{Contributions: []executor.Contribution{{
		APIVersion: "v1", Kind: "ConfigMap", Namespace: "default", Name: "target",
		FieldManager: executor.PatchFieldManager("graph-uid", "patch"),
	}}}

	cases := []struct {
		name            string
		previous        string
		result          executor.ApplyResult
		applyErr        error
		noImpersonation bool
		wantIdentity    string
	}{
		{
			name: "first partial resource", result: resource, applyErr: hard, wantIdentity: current,
		},
		{
			name: "first partial contribution", result: contribution, applyErr: hard, wantIdentity: current,
		},
		{
			name: "empty hard failure with intent records nothing", applyErr: hard,
		},
		{
			name: "empty hard failure preserves prior identity", previous: prior, applyErr: hard, wantIdentity: prior,
		},
		{
			name: "partial resource preserves prior identity", previous: prior,
			result: resource, applyErr: hard, wantIdentity: prior,
		},
		{
			name: "partial contribution preserves prior identity", previous: prior,
			result: contribution, applyErr: hard, wantIdentity: prior,
		},
		{
			name: "clean empty apply updates identity", previous: prior, wantIdentity: current,
		},
		{
			name: "soft empty apply updates identity", previous: prior, applyErr: executor.ErrNotReady, wantIdentity: current,
		},
		{
			name: "inactive impersonation records nothing", result: resource, applyErr: hard, noImpersonation: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			g := graph("g", withFinalizer)
			g.UID = "graph-uid"
			g.Spec.ServiceAccountName = "deployer"
			g.Status.AppliedServiceAccount = tc.previous
			cl := newClient(t, g)
			exec := &fakeExecutor{applyResult: tc.result, applyErr: tc.applyErr}
			// Intent alone must not record an identity after an empty hard failure.
			prog := templateProgram("cm", "v1", "ConfigMap", "default", "child")
			if len(tc.result.Contributions) > 0 {
				prog = patchProgram("patch", "v1", "ConfigMap", "default", "target")
			}
			r := &Reconciler{
				Client: cl, Compiler: &fakeCompiler{program: prog}, Registry: registry.New(), Executor: exec,
			}
			if !tc.noImpersonation {
				r.Impersonation = &impersonationCache{
					newExec: func(user string) (executor.Interface, error) {
						assert.Equal(t, current, user)
						return exec, nil
					},
				}
			}
			key := client.ObjectKeyFromObject(g)
			result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
			if tc.applyErr == hard {
				require.ErrorIs(t, err, hard)
			} else {
				require.NoError(t, err)
				if tc.applyErr != nil {
					assert.Positive(t, result.RequeueAfter)
				}
			}

			got := &expv1alpha1.Graph{}
			require.NoError(t, cl.Get(ctx, key, got))
			assert.Equal(t, tc.wantIdentity, got.Status.AppliedServiceAccount)
			for _, mr := range tc.result.Applied {
				assert.Contains(t, got.Status.ManagedResources, mr)
			}
			assert.Equal(t, toAPIContributions(tc.result.Contributions), got.Status.Contributions)
			if tc.applyErr == hard {
				condition := findCondition(got.Status.Conditions, ResourcesConverged)
				require.NotNil(t, condition)
				assert.Equal(t, metav1.ConditionFalse, condition.Status)
				require.NotNil(t, condition.Reason)
				assert.Equal(t, "ApplyFailed", *condition.Reason)
				if len(tc.result.Applied) == 0 && len(tc.result.Contributions) == 0 {
					require.Len(t, got.Status.ManagedResources, 1)
					assert.Empty(t, got.Status.ManagedResources[0].UID, "only write-ahead intent was persisted")
				}
			}
		})
	}
}
