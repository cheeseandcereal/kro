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

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	krov1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/testutil/generator"
)

// A collection member whose UPDATE fails transiently (here: a failurePolicy=Fail
// admission webhook with an unreachable backend, so the apiserver answers 500
// "failed calling webhook") must hold the instance not-ready and be retried, not
// be tolerated as converged. envtest's kube-apiserver runs the
// ValidatingAdmissionWebhook plugin, so a ValidatingWebhookConfiguration pointing
// at an unreachable loopback URL reproduces this end to end.
var _ = Describe("Collection transient update rejection", func() {
	var namespace string
	// nsLabelKey/value scope the (cluster-wide) webhook configuration to this
	// spec's namespace, so parallel specs on the shared apiserver are unaffected.
	const nsLabelKey = "kro-test-transient-webhook"

	BeforeEach(func(ctx SpecContext) {
		namespace = fmt.Sprintf("test-%s", rand.String(5))
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:   namespace,
				Labels: map[string]string{nsLabelKey: namespace},
			},
		}
		Expect(env.Client.Create(ctx, ns)).To(Succeed())
	})

	AfterEach(func(ctx SpecContext) {
		Expect(env.Client.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: namespace},
		})).To(Succeed())
	})

	It("holds the instance not-ready while one member's update fails transiently and converges once the failure clears", func(ctx SpecContext) {
		rgd := generator.NewResourceGraphDefinition("test-transient-rejection",
			generator.WithSchema(
				"TransientRejectionCollection", "v1alpha1",
				map[string]any{
					"name":  "string",
					"slots": "[]string",
					"value": "string",
				},
				nil,
			),
			generator.WithResourceCollection("configmaps", map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name": "${schema.spec.name}-${slot}",
					"labels": map[string]any{
						"slot": "${slot}",
					},
				},
				"data": map[string]any{
					"value": "${schema.spec.value}",
				},
			},
				[]krov1alpha1.ForEachDimension{
					{"slot": "${schema.spec.slots}"},
				},
				nil, nil),
		)
		Expect(env.Client.Create(ctx, rgd)).To(Succeed())
		defer func() { _ = env.Client.Delete(ctx, rgd) }()

		Eventually(func(g Gomega, ctx SpecContext) {
			createdRGD := &krov1alpha1.ResourceGraphDefinition{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: rgd.Name}, createdRGD)).To(Succeed())
			g.Expect(createdRGD.Status.State).To(Equal(krov1alpha1.ResourceGraphDefinitionStateActive))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		name := "coll"
		instance := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": fmt.Sprintf("%s/%s", krov1alpha1.KRODomainName, "v1alpha1"),
				"kind":       "TransientRejectionCollection",
				"metadata":   map[string]any{"name": name, "namespace": namespace},
				"spec": map[string]any{
					"name":  name,
					"slots": []any{"a", "b"},
					"value": "v1",
				},
			},
		}
		Expect(env.Client.Create(ctx, instance)).To(Succeed())

		By("converging both members at v1")
		waitForInstanceActive(ctx, namespace, name, instance)
		for _, slot := range []string{"a", "b"} {
			Eventually(func(g Gomega, ctx SpecContext) {
				cm := &corev1.ConfigMap{}
				g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name + "-" + slot, Namespace: namespace}, cm)).To(Succeed())
				g.Expect(cm.Data["value"]).To(Equal("v1"))
			}, 20*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
		}

		By("installing a failurePolicy=Fail webhook with an unreachable backend that intercepts UPDATEs of the slot=a member only")
		webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("block-slot-a-%s", namespace)},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{
				Name:                    "block-slot-a.kro-test.invalid",
				AdmissionReviewVersions: []string{"v1"},
				SideEffects:             new(admissionregistrationv1.SideEffectClassNone),
				FailurePolicy:           new(admissionregistrationv1.Fail),
				TimeoutSeconds:          new(int32(1)),
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
					Rule: admissionregistrationv1.Rule{
						APIGroups:   []string{""},
						APIVersions: []string{"v1"},
						Resources:   []string{"configmaps"},
					},
				}},
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{nsLabelKey: namespace}},
				ObjectSelector:    &metav1.LabelSelector{MatchLabels: map[string]string{"slot": "a"}},
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					// Nothing listens on port 1; under failurePolicy=Fail the apiserver
					// rejects the UPDATE with 500 InternalError "failed calling webhook".
					URL: new("https://127.0.0.1:1/validate"),
				},
			}},
		}
		Expect(env.Client.Create(ctx, webhook)).To(Succeed())
		defer func() { _ = env.Client.Delete(ctx, webhook) }()

		// The apiserver picks the configuration up asynchronously: probe with a
		// throwaway ConfigMap until an UPDATE is rejected, so the change below
		// cannot race ahead of the webhook becoming active.
		probe := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "webhook-probe", Namespace: namespace, Labels: map[string]string{"slot": "a"}},
			Data:       map[string]string{"n": "0"},
		}
		Expect(env.Client.Create(ctx, probe)).To(Succeed())
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: probe.Name, Namespace: namespace}, probe)).To(Succeed())
			probe.Data["n"] = rand.String(4)
			err := env.Client.Update(ctx, probe)
			g.Expect(err).To(HaveOccurred(), "the webhook must intercept UPDATEs of slot=a ConfigMaps")
			g.Expect(apierrors.IsInternalError(err)).To(BeTrue(), "expected 500 InternalError, got %v", err)
			g.Expect(err.Error()).To(ContainSubstring("failed calling webhook"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("changing the desired value so every member needs an UPDATE")
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, instance)).To(Succeed())
			g.Expect(unstructured.SetNestedField(instance.Object, "v2", "spec", "value")).To(Succeed())
			g.Expect(env.Client.Update(ctx, instance)).To(Succeed())
		}, 10*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		By("reporting the instance not-ready with the webhook failure while the unaffected member converges")
		Eventually(func(g Gomega, ctx SpecContext) {
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, instance)).To(Succeed())

			state, _, _ := unstructured.NestedString(instance.Object, "status", "state")
			g.Expect(state).To(Equal("IN_PROGRESS"),
				"a transient update failure is a soft not-ready, not converged (ACTIVE) and not degraded (ERROR)")

			cond := instanceConditionByType(instance, "ResourcesReady")
			g.Expect(cond).ToNot(BeNil())
			g.Expect(cond["status"]).To(Equal("False"))
			g.Expect(cond["message"]).To(ContainSubstring("failed calling webhook"))
			g.Expect(cond["message"]).To(ContainSubstring(name + "-a"))

			cmB := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name + "-b", Namespace: namespace}, cmB)).To(Succeed())
			g.Expect(cmB.Data["value"]).To(Equal("v2"), "the sibling not covered by the webhook must still be updated")
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())

		// The blocked member is still present (never pruned) and still stale.
		cmA := &corev1.ConfigMap{}
		Expect(env.Client.Get(ctx, types.NamespacedName{Name: name + "-a", Namespace: namespace}, cmA)).To(Succeed())
		Expect(cmA.Data["value"]).To(Equal("v1"), "the webhook must have blocked the update of the slot=a member")

		By("removing the webhook and letting the requeue retry the update — without touching the instance")
		Expect(env.Client.Delete(ctx, webhook)).To(Succeed())

		// Nothing on the instance or its children changes when the webhook is
		// removed, so only the not-ready requeue can drive this convergence.
		Eventually(func(g Gomega, ctx SpecContext) {
			cmA := &corev1.ConfigMap{}
			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name + "-a", Namespace: namespace}, cmA)).To(Succeed())
			g.Expect(cmA.Data["value"]).To(Equal("v2"), "the retried update must land once the webhook is gone")

			g.Expect(env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, instance)).To(Succeed())
			state, _, _ := unstructured.NestedString(instance.Object, "status", "state")
			g.Expect(state).To(Equal("ACTIVE"))
			cond := instanceConditionByType(instance, "ResourcesReady")
			g.Expect(cond).ToNot(BeNil())
			g.Expect(cond["status"]).To(Equal("True"))
		}, 2*time.Minute, 500*time.Millisecond).WithContext(ctx).Should(Succeed())

		Expect(env.Client.Delete(ctx, instance)).To(Succeed())
		Eventually(func(g Gomega, ctx SpecContext) {
			err := env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, instance)
			g.Expect(err).To(MatchError(apierrors.IsNotFound, "instance should be deleted"))
		}, 30*time.Second, 250*time.Millisecond).WithContext(ctx).Should(Succeed())
	})
})

// instanceConditionByType returns the status.conditions entry of the given type
// from an unstructured instance, or nil when absent.
func instanceConditionByType(instance *unstructured.Unstructured, condType string) map[string]any {
	conditions, found, err := unstructured.NestedSlice(instance.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	for _, cond := range conditions {
		if c, ok := cond.(map[string]any); ok && c["type"] == condType {
			return c
		}
	}
	return nil
}
