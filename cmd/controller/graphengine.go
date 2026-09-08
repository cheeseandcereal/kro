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

package main

import (
	"fmt"
	"net/http"

	"github.com/go-logr/logr"
	"k8s.io/client-go/kubernetes"
	authorizationv1client "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ctrlgraph "github.com/kubernetes-sigs/kro/pkg/controller/graph"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/executor"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/registry"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/schemawatcher"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/watchrouter"
)

// The Graph kind (kro.run/v1alpha1) is registered in the scheme by
// api/v1alpha1's SchemeBuilder, which main.go already adds — no separate
// registration is needed here.

// setupGraphEngine builds the graph-engine compiler, the Graph compile cache
// and the CRD schema watcher (added to the manager) and returns them. It runs
// regardless of the GraphKind gate: the compiler serves every RGD instance
// controller, and the watcher is what drives Compiler.InvalidateSchema.
func setupGraphEngine(
	mgr ctrl.Manager,
	restConfig *rest.Config,
	httpClient *http.Client,
	logger logr.Logger,
	celCostLimit uint64,
) (*compiler.Compiler, *registry.Registry, *schemawatcher.SchemaWatcher, error) {
	cmp, err := compiler.NewCompiler(restConfig, httpClient)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build graph-engine compiler: %w", err)
	}
	cmp.WithCostLimit(celCostLimit)

	// The registry lives here so the watcher can invalidate it (empty while the
	// gate is off). The watcher needs full CRD objects, so this adds a structured
	// CRD informer next to the RGD reconciler's metadata-only one.
	reg := registry.New()
	sw := schemawatcher.New(logger.WithName("graph-engine"), schemawatcher.Config{
		Cache:   mgr.GetCache(),
		Graphs:  reg,
		Schemas: cmp,
	})
	if err := mgr.Add(sw); err != nil {
		return nil, nil, nil, fmt.Errorf("add graph-engine schema watcher: %w", err)
	}
	return cmp, reg, sw, nil
}

// setupGraphController wires the Graph controller into the manager alongside
// the ResourceGraphDefinition stack, reusing the compiler, compile cache and
// CRD schema watcher from setupGraphEngine. It builds the resource-drift watch
// router (a manager Runnable feeding the Graph work queue) and the impersonated
// executor clients, then registers the reconciler.
func setupGraphController(
	mgr ctrl.Manager,
	cmp *compiler.Compiler,
	reg *registry.Registry,
	sw *schemawatcher.SchemaWatcher,
	metaClient metadata.Interface,
	logger logr.Logger,
	concurrentReconciles int,
	maxCollectionSize int,
	applyConcurrency int,
	controllerServiceAccount string,
) error {
	router := watchrouter.NewRouter(logger.WithName("graph-watch-router"), watchrouter.Config{}, metaClient)
	if err := mgr.Add(router); err != nil {
		return fmt.Errorf("add graph watch router: %w", err)
	}

	exec := executor.NewSimple(mgr.GetClient())
	exec.ApplyConcurrency = applyConcurrency
	// Standalone Graph objects carry no ApplySet part-of ownership label, so
	// per-Graph field-manager conflict detection is what keeps two Graphs that
	// template the same object from flip-flopping its fields. The RGD/instance
	// path leaves this off and relies on its ApplySet part-of guard instead.
	exec.ConflictDetection = true

	// A namespaced Graph applies its resources while impersonating a
	// ServiceAccount in the Graph's namespace (default, or spec.serviceAccountName).
	// Build impersonated controller-runtime clients from the manager's REST
	// config; they share the compiler's REST mapper — the one the schema watcher
	// resets — so discovery is not repeated per ServiceAccount and a recreated
	// CRD's new plural/scope is picked up. The kro controller SA needs the
	// "impersonate" verb on serviceaccounts for this to take effect.
	baseCfg := mgr.GetConfig()
	mapper := cmp.RESTMapper()
	impersonation := ctrlgraph.NewImpersonation(exec, func(user string) (client.Client, error) {
		cfg := rest.CopyConfig(baseCfg)
		cfg.Impersonate = rest.ImpersonationConfig{UserName: user}
		return client.New(cfg, client.Options{Mapper: mapper})
	}, func(user string) (authorizationv1client.AuthorizationV1Interface, error) {
		// Same impersonated config drives the SelfSubjectAccessReview gate, so
		// "self" is the Graph's ServiceAccount. A client-go Clientset is used
		// (rather than the controller-runtime client) to avoid scheme wiring for
		// the SSAR type.
		cfg := rest.CopyConfig(baseCfg)
		cfg.Impersonate = rest.ImpersonationConfig{UserName: user}
		cs, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			return nil, err
		}
		return cs.AuthorizationV1(), nil
	})
	// Cached clients memoize each GVK's mapping for their lifetime, so CRD
	// changes must also purge them (see impersonationCache.InvalidateSchema).
	sw.AddSchemaInvalidator(impersonation)

	reconciler := &ctrlgraph.Reconciler{
		Client:                   mgr.GetClient(),
		Compiler:                 cmp,
		Registry:                 reg,
		Executor:                 exec,
		Router:                   router,
		SchemaWatcher:            sw,
		MaxConcurrentReconciles:  concurrentReconciles,
		MaxCollectionSize:        maxCollectionSize,
		Impersonation:            impersonation,
		RequireImpersonation:     true,
		ControllerServiceAccount: controllerServiceAccount,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup graph reconciler: %w", err)
	}
	return nil
}
