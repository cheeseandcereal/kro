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

package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
)

// fakeImpersonation resolves every identity to exec and records the identities
// asked for, so a test can observe which one a reconcile ran under.
func fakeImpersonation(exec executor.Interface) (*impersonationCache, *[]string) {
	var built []string
	return &impersonationCache{
		newExec: func(user string) (executor.Interface, error) {
			built = append(built, user)
			return exec, nil
		},
	}, &built
}

func withServiceAccount(sa string) func(*expv1alpha1.Graph) {
	return func(g *expv1alpha1.Graph) { g.Spec.ServiceAccountName = sa }
}

func withAppliedIdentity(user string) func(*expv1alpha1.Graph) {
	return func(g *expv1alpha1.Graph) { g.Status.AppliedServiceAccount = user }
}

func withUID(uid string) func(*expv1alpha1.Graph) {
	return func(g *expv1alpha1.Graph) { g.UID = types.UID(uid) }
}

// TestReconcile_AppliedIdentityOnHardFailure pins that the applying identity is
// recorded when a hard failure still landed resources or contributions (the
// inventory persists them, so teardown needs the identity), and left untouched
// when nothing landed.
func TestReconcile_AppliedIdentityOnHardFailure(t *testing.T) {
	t.Parallel()

	partial := []expv1alpha1.ManagedResource{{
		NodeID: "cm", APIVersion: "v1", Kind: "ConfigMap", Namespace: "default", Name: "landed", UID: "uid-landed",
	}}
	hard := errors.New(`clusterroles.rbac.authorization.k8s.io "cr" is forbidden`)

	cases := []struct {
		name         string
		graph        *expv1alpha1.Graph
		exec         *fakeExecutor
		wantIdentity string
	}{
		{
			name:  "hard failure with a partial Applied set records the identity",
			graph: graph("g", withFinalizer, withServiceAccount("deployer")),
			exec: &fakeExecutor{
				applyErr:    hard,
				applyResult: executor.ApplyResult{Applied: partial},
			},
			wantIdentity: "system:serviceaccount:default:deployer",
		},
		{
			name:  "hard failure that landed only patch contributions records the identity",
			graph: graph("g", withFinalizer, withServiceAccount("deployer")),
			exec: &fakeExecutor{
				applyErr: hard,
				applyResult: executor.ApplyResult{Contributions: []executor.Contribution{{
					APIVersion: "v1", Kind: "ConfigMap", Namespace: "default", Name: "target",
					FieldManager: executor.PatchFieldManager("uid", "p"),
				}}},
			},
			wantIdentity: "system:serviceaccount:default:deployer",
		},
		{
			name: "hard failure that applied nothing preserves the last-good identity",
			graph: graph("g", withFinalizer, withServiceAccount("new-sa"),
				withAppliedIdentity("system:serviceaccount:default:old-sa")),
			exec:         &fakeExecutor{applyErr: hard},
			wantIdentity: "system:serviceaccount:default:old-sa",
		},
		{
			name:         "hard failure that applied nothing on a never-applied Graph records nothing",
			graph:        graph("g", withFinalizer, withServiceAccount("deployer")),
			exec:         &fakeExecutor{applyErr: hard},
			wantIdentity: "",
		},
		{
			name:  "partial Applied under a NEW identity moves the recorded identity to it",
			graph: graph("g", withFinalizer, withServiceAccount("new-sa"), withAppliedIdentity("system:serviceaccount:default:old-sa")),
			exec: &fakeExecutor{
				applyErr:    hard,
				applyResult: executor.ApplyResult{Applied: partial},
			},
			// The scalar cannot represent a mixed-identity inventory; the latest wins.
			wantIdentity: "system:serviceaccount:default:new-sa",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cl := newClient(t, tc.graph)
			imp, _ := fakeImpersonation(tc.exec)
			r := &Reconciler{
				Client:        cl,
				Compiler:      &fakeCompiler{program: emptyNodeProgram("cm")},
				Registry:      registry.New(),
				Executor:      tc.exec,
				Impersonation: imp,
			}
			key := types.NamespacedName{Namespace: "default", Name: "g"}
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.Error(t, err, "a hard apply failure is surfaced")

			got := &expv1alpha1.Graph{}
			require.NoError(t, cl.Get(context.Background(), key, got))
			assert.Equal(t, tc.wantIdentity, got.Status.AppliedServiceAccount)
			for _, mr := range tc.exec.applyResult.Applied {
				assert.Contains(t, got.Status.ManagedResources, mr,
					"the partially-applied resource is in the persisted inventory, so teardown needs its identity")
			}
		})
	}
}

// TestReconcileDelete_RecordedIdentityAndGraphUID pins that teardown runs under
// Status.AppliedServiceAccount (not the current spec) and hands Delete the
// Graph's UID.
func TestReconcileDelete_RecordedIdentityAndGraphUID(t *testing.T) {
	t.Parallel()

	g := graph("g", withFinalizer, withDeletionTimestamp, withManagedResource,
		withUID("graph-uid-123"),
		withServiceAccount("rights-less-sa"),
		withAppliedIdentity("system:serviceaccount:default:deployer"))
	cl := newClient(t, g)
	exec := &fakeExecutor{}
	imp, built := fakeImpersonation(exec)
	r := &Reconciler{
		Client:        cl,
		Compiler:      &fakeCompiler{err: errors.New("delete never compiles")},
		Registry:      registry.New(),
		Executor:      exec,
		Impersonation: imp,
	}
	key := types.NamespacedName{Namespace: "default", Name: "g"}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	assert.Equal(t, []string{"system:serviceaccount:default:deployer"}, *built,
		"teardown must impersonate the recorded applied identity, never the current spec")
	require.Len(t, exec.deleteCalls, 1)
	assert.Equal(t, g.Status.ManagedResources, exec.deleteCalls[0])
	assert.Equal(t, []types.UID{"graph-uid-123"}, exec.deleteOwners)

	err = cl.Get(context.Background(), key, &expv1alpha1.Graph{})
	assert.True(t, apierrors.IsNotFound(err), "finalizer released after teardown")
}
