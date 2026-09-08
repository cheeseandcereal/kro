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

package core_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// A resource-backed includeWhen must be decided only once the upstream it reads
// is ready — never against the interim value of an applied-but-not-ready
// object, and never against the `{}` placeholder a soft-dependency target is
// seeded with before apply. Deciding early prunes the dependent's existing
// child (first spec) or never creates it (second spec).
var _ = Describe("IncludeWhen readiness gating order", func() {
	var namespace string

	BeforeEach(func(ctx SpecContext) {
		namespace = fmt.Sprintf("test-%s", rand.String(5))
		Expect(env.Client.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	AfterEach(func(ctx SpecContext) {
		Expect(env.Client.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	// patchConfigMapData merge-patches one data key under a field manager distinct
	// from kro's, standing in for an external controller populating a field kro
	// does not template.
	patchConfigMapData := func(ctx SpecContext, name, key, value string) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
		patch := []byte(fmt.Sprintf(`{"data":{%q:%q}}`, key, value))
		Expect(env.Client.Patch(ctx, cm, client.RawPatch(types.MergePatchType, patch))).To(Succeed())
	}

	instanceState := func(ctx SpecContext, g Gomega, kind, name string) string {
		inst := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
			"kind":       kind,
		}}
		g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, inst)).To(Succeed())
		state, found, err := unstructured.NestedString(inst.Object, "status", "state")
		g.Expect(err).ToNot(HaveOccurred())
		g.Expect(found).To(BeTrue())
		return state
	}

	// app's includeWhen reads the db field that also drives db's readyWhen. Once
	// app exists, db flipping to an interim (not ready) value must hold app in
	// place rather than decide its includeWhen against that value and prune it.
	It("keeps an includeWhen-gated dependent while its dependency reports an interim value", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("include-when-gating-order",
			generator.WithSchema(
				"GatingOrder", "v1alpha1",
				map[string]any{"name": "string"},
				nil,
			),
			// db templates only `owner`; `phase` is populated externally (the
			// test stands in for the owning controller) and drives readiness.
			generator.WithResource("db", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-db",
				},
				"data": map[string]any{
					"owner": "${schema.spec.name}",
				},
			}, []string{`${db.data.phase == "Running"}`}, nil),
			// app is included only while db reports Running, and reads db too.
			generator.WithResource("app", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-app",
				},
				"data": map[string]any{
					"dbOwner": "${db.data.owner}",
				},
			}, nil, []string{`${schema.spec.name != "" && db.data.phase == "Running"}`}),
		)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		name := "gating-order"
		instance := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       "GatingOrder",
				"metadata": map[string]any{
					"name":      name,
					"namespace": namespace,
				},
				"spec": map[string]any{
					"name": name,
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			_ = env.Client.Delete(ctx, instance)
		})

		dbKey := types.NamespacedName{Name: name + "-db", Namespace: namespace}
		appKey := types.NamespacedName{Name: name + "-app", Namespace: namespace}

		By("applying db and holding app back while db has no phase yet")
		Eventually(func(g Gomega, ctx SpecContext) {
			db := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, dbKey, db)).To(Succeed())
			g.Expect(db.Data).To(HaveKeyWithValue("owner", name))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(instanceState(ctx, g, "GatingOrder", name)).To(Equal("IN_PROGRESS"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		Consistently(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, appKey, &corev1.ConfigMap{})
			g.Expect(err).To(MatchError(errors.IsNotFound, "app must not be created before db is ready"))
		}, 5*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("creating app once db reports Running")
		patchConfigMapData(ctx, dbKey.Name, "phase", "Running")
		Eventually(func(g Gomega, ctx SpecContext) {
			app := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, appKey, app)).To(Succeed())
			g.Expect(app.Data).To(HaveKeyWithValue("dbOwner", name))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		waitForInstanceActive(ctx, namespace, name, instance)

		By("flipping db to an interim value: the instance leaves ACTIVE but app must survive")
		patchConfigMapData(ctx, dbKey.Name, "phase", "Starting")
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(instanceState(ctx, g, "GatingOrder", name)).To(Equal("IN_PROGRESS"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		// The controller has observed the interim value (state left ACTIVE); keep
		// polling so a delayed prune is caught too.
		Consistently(func(g Gomega, ctx SpecContext) {
			app := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, appKey, app)).To(Succeed(),
				"app must not be pruned while db is applied but not ready")
			g.Expect(app.Data).To(HaveKeyWithValue("dbOwner", name))
		}, 15*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("converging again once db reports Running")
		patchConfigMapData(ctx, dbKey.Name, "phase", "Running")
		waitForInstanceActive(ctx, namespace, name, instance)
		app := &corev1.ConfigMap{}
		Expect(env.Client.Get(ctx, appKey, app)).To(Succeed())
		Expect(app.Data).To(HaveKeyWithValue("dbOwner", name))
	}, SpecTimeout(180*time.Second))

	// A status field referencing db makes db a soft-dependency `{}` placeholder
	// before apply. A presence-tolerant includeWhen (`.?`/orValue, has()) evaluated
	// against it is a definite false; if that verdict were memoized before apply,
	// app would never be created even though db is Running from its first apply.
	It("creates a dependent whose presence-tolerant includeWhen was projected against a soft-dependency placeholder", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("include-when-placeholder-verdict",
			generator.WithSchema(
				"PlaceholderVerdict", "v1alpha1",
				map[string]any{"name": "string"},
				// The status reference makes db a soft-dependency `{}` placeholder.
				map[string]any{"dbOwner": "${db.data.owner}"},
			),
			generator.WithResource("db", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-db",
				},
				"data": map[string]any{
					"owner": "${schema.spec.name}",
					"phase": "Running",
				},
			}, nil, nil),
			generator.WithResource("app", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-app",
				},
				"data": map[string]any{
					"key": "value",
				},
			}, nil, []string{`${db.?data.phase.orValue("missing") == "Running"}`}),
		)

		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			Expect(env.Client.Delete(ctx, rgd)).To(Succeed())
		})
		waitForRGDActive(ctx, rgd.Name)

		name := "placeholder-verdict"
		instance := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       "PlaceholderVerdict",
				"metadata": map[string]any{
					"name":      name,
					"namespace": namespace,
				},
				"spec": map[string]any{
					"name": name,
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())
		DeferCleanup(func(ctx SpecContext) {
			_ = env.Client.Delete(ctx, instance)
		})

		Eventually(func(g Gomega, ctx SpecContext) {
			db := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name + "-db", Namespace: namespace}, db)).To(Succeed())
			g.Expect(db.Data).To(HaveKeyWithValue("phase", "Running"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		Eventually(func(g Gomega, ctx SpecContext) {
			app := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name + "-app", Namespace: namespace}, app)).To(Succeed(),
				"app must be created: its includeWhen is true against the applied db")
			g.Expect(app.Data).To(HaveKeyWithValue("key", "value"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		waitForInstanceActive(ctx, namespace, name, instance)
		dbOwner, found, err := unstructured.NestedString(instance.Object, "status", "dbOwner")
		Expect(err).ToNot(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(dbOwner).To(Equal(name))
	}, SpecTimeout(120*time.Second))
})
