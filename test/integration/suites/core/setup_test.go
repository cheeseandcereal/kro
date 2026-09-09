// Copyright 2025 The Kubernetes Authors.
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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-base/featuregate"

	ctrlinstance "github.com/kubernetes-sigs/kro/pkg/controller/instance"
	"github.com/kubernetes-sigs/kro/pkg/features"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

var env *environment.Environment

// fullFeatureGates is enabled when environment.FeatureGatesEnv is unset: the
// alpha gates this suite has dedicated specs for.
const fullFeatureGates = "CELOmitFunction=true,GraphKind=true"

const featureGateLabelPrefix = "feature-gate:"

// requiresFeatureGate labels a container whose specs need the given alpha gate;
// the top-level BeforeEach below skips them when the gate is off.
func requiresFeatureGate(gate featuregate.Feature) Labels {
	return Label(featureGateLabelPrefix + string(gate))
}

// Skips gate-labelled specs whose gate is off. It must stay at the top level:
// Ginkgo then runs neither the container's BeforeEach nor its AfterEach for a
// skipped spec, so no fixtures are created or torn down.
var _ = BeforeEach(func() {
	for _, label := range CurrentSpecReport().Labels() {
		name, ok := strings.CutPrefix(label, featureGateLabelPrefix)
		if !ok {
			continue
		}
		gate := featuregate.Feature(name)
		if _, known := features.FeatureGate.GetAll()[gate]; !known {
			Fail(fmt.Sprintf("spec is labelled with unknown feature gate %q", name))
		}
		if !features.FeatureGate.Enabled(gate) {
			Skip(fmt.Sprintf("requires feature gate %s=true, which is off in this lane (%s=%q)",
				name, environment.FeatureGatesEnv, os.Getenv(environment.FeatureGatesEnv)))
		}
	}
})

func TestCore(t *testing.T) {
	// Gates are process-global and consulted while the manager is wired, so
	// they must be set before the suite boots (see environment.FeatureGatesEnv).
	if gates := environment.FeatureGatesFromEnv(fullFeatureGates); gates != "" {
		if err := features.FeatureGate.Set(gates); err != nil {
			t.Fatalf("failed to enable feature gates %q: %v", gates, err)
		}
	}

	RegisterFailHandler(Fail)

	// SynchronizedBeforeSuite starts a SINGLE envtest control plane + controller
	// manager on Ginkgo parallel process #1 and shares its apiserver connection
	// with every other parallel process. This replaces the previous per-process
	// BeforeSuite, which started one apiserver+etcd (and manager) per parallel
	// process and caused envtest to stall under load.
	SynchronizedBeforeSuite(func() []byte {
		var err error
		env, err = environment.New(t.Context(),
			environment.ControllerConfig{
				AllowCRDDeletion: true,
				ReconcileConfig: ctrlinstance.ReconcileConfig{
					DefaultRequeueDuration: 5 * time.Second,
				},
				LogWriter: GinkgoWriter,
			},
		)
		Expect(err).NotTo(HaveOccurred())

		data, err := environment.EncodeRESTConfig(env.ClientSet.RESTConfig())
		Expect(err).NotTo(HaveOccurred())
		return data
	}, func(data []byte) {
		// Runs on every process. Process #1 already holds the full environment
		// (control plane + manager); only secondary processes need a thin client
		// bound to the shared apiserver.
		if env == nil {
			cfg, err := environment.DecodeRESTConfig(data)
			Expect(err).NotTo(HaveOccurred())
			env, err = environment.NewShared(t.Context(), cfg)
			Expect(err).NotTo(HaveOccurred())
		}
		// Give each parallel process its own virtual kro.run API group so that
		// specs sharing a schema kind never collide on the shared apiserver's
		// cluster-scoped CRDs.
		env.Client = environment.NewGroupIsolatingClient(env.Client,
			fmt.Sprintf("p%d", GinkgoParallelProcess()))
	})

	SynchronizedAfterSuite(func() {
		// Runs on every process. Secondary processes hold only thin clients and
		// tear them down here. Process #1 must keep the shared control plane
		// alive until all other processes finish, so it defers teardown to the
		// second (process #1 only) function below.
		if GinkgoParallelProcess() != 1 {
			Expect(env.Stop()).To(Succeed())
		}
	}, func() {
		// Runs on process #1 only, after all other processes have completed.
		err := (func() (err error) {
			// Need to sleep if the first stop fails due to a bug:
			// https://github.com/kubernetes-sigs/controller-runtime/issues/1571
			sleepTime := 1 * time.Millisecond
			for range 12 { // Exponentially sleep up to ~4s
				if err = env.Stop(); err == nil {
					return
				}
				sleepTime *= 2
				time.Sleep(sleepTime)
			}
			return
		})()
		Expect(err).NotTo(HaveOccurred())
	})

	RunSpecs(t, "Core Suite")
}

// Helper function to convert map to runtime.RawExtension
func toRawExtension(v any) runtime.RawExtension {
	rawJSON, err := json.Marshal(v)
	Expect(err).NotTo(HaveOccurred())
	return runtime.RawExtension{Raw: rawJSON}
}
