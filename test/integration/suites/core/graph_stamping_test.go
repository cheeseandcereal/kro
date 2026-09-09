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
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/test/integration/environment"
)

var graphGVK = schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "Graph"}

// Graph Stamping covers the second form of nesting: a `template:` node whose
// payload is `kind: Graph`. The child is an independent object reconciled on
// its own loop, so the child's CEL must survive the parent's evaluation. The
// `$${...}` deferral syntax is what makes that possible; these specs check it
// end to end — each evaluation level peels exactly one dollar sign — using the
// shipped example verbatim and a Go fixture for a three-level chain.
var _ = Describe("Graph Stamping", func() {
	// expectData returns an AwaitObject matcher that checks ConfigMap data.
	expectData := func(want map[string]string) func(*unstructured.Unstructured) error {
		return func(u *unstructured.Unstructured) error {
			data, _, _ := unstructured.NestedStringMap(u.Object, "data")
			for k, v := range want {
				if data[k] != v {
					return errMismatch("data."+k, v, data[k])
				}
			}
			return nil
		}
	}

	// nodeByID returns the spec.nodes entry with the given id from a Graph
	// object, so assertions do not depend on node order in the fixture.
	nodeByID := func(u *unstructured.Unstructured, id string) (map[string]any, error) {
		nodes, _, _ := unstructured.NestedSlice(u.Object, "spec", "nodes")
		for _, n := range nodes {
			if node, ok := n.(map[string]any); ok && node["id"] == id {
				return node, nil
			}
		}
		return nil, fmt.Errorf("graph %s has no node %q among %d nodes", u.GetName(), id, len(nodes))
	}

	It("applies examples/graph/stamped.yaml and peels one dollar per level", func() {
		t := GinkgoT()
		const ns = "stamped-demo" // fixed by the example

		env.ApplyYAMLFile(t, environment.RepoPath(t, "examples", "graph", "stamped.yaml"))

		parent := types.NamespacedName{Namespace: ns, Name: "team-stamper"}
		env.AwaitCondition(t, parent, expv1alpha1.GraphConditionTypeReady, metav1.ConditionTrue, 30*time.Second)

		for _, team := range []string{"alpha", "beta"} {
			// L1: the stamped child carries the deferred text verbatim and the
			// parent-evaluated values baked in.
			env.AwaitObject(t, graphGVK, types.NamespacedName{Namespace: ns, Name: "team-" + team},
				func(u *unstructured.Unstructured) error {
					cfgNode, err := nodeByID(u, "cfg")
					if err != nil {
						return err
					}
					cfg, _, _ := unstructured.NestedString(cfgNode, "def", "team")
					if cfg != team {
						return errMismatch("cfg.def.team", team, cfg)
					}
					cm, err := nodeByID(u, "cm")
					if err != nil {
						return err
					}
					name, _, _ := unstructured.NestedString(cm, "template", "metadata", "name")
					if name != "${cfg.team}" {
						return errMismatch("cm.template.metadata.name", "${cfg.team}", name)
					}
					label, _, _ := unstructured.NestedString(cm, "template", "data", "label")
					if label != team+"-${cfg.team}" {
						return errMismatch("cm.template.data.label", team+"-${cfg.team}", label)
					}
					upper, _, _ := unstructured.NestedString(cm, "template", "data", "upper")
					if upper != "${cfg.team.upperAscii()}" {
						return errMismatch("cm.template.data.upper", "${cfg.team.upperAscii()}", upper)
					}
					return nil
				}, 30*time.Second)

			// L1 evaluates its expressions and produces the team ConfigMap.
			env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: team},
				expectData(map[string]string{
					"region": "eu-west-1",
					"label":  team + "-" + team,
					"upper":  map[string]string{"alpha": "ALPHA", "beta": "BETA"}[team],
				}), 30*time.Second)

			// L2: stamped by L1; the $$${...} span has been peeled twice.
			env.AwaitObject(t, graphGVK, types.NamespacedName{Namespace: ns, Name: "team-" + team + "-audit"},
				func(u *unstructured.Unstructured) error {
					cm, err := nodeByID(u, "cm")
					if err != nil {
						return err
					}
					data, _, _ := unstructured.NestedStringMap(cm, "template", "data")
					want := map[string]string{
						"fromL0": team,             // evaluated by L0
						"fromL1": team + "-l1",     // evaluated by L1
						"fromL2": "${audit.stamp}", // still an expression for L2
					}
					for k, v := range want {
						if data[k] != v {
							return errMismatch("audit.template.data."+k, v, data[k])
						}
					}
					return nil
				}, 30*time.Second)

			env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: team + "-audit"},
				expectData(map[string]string{
					"fromL0": team,
					"fromL1": team + "-l1",
					"fromL2": "audited",
				}), 30*time.Second)
		}

		// Deleting the root cascades through every level.
		ctx := env.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		if err := env.Client.Delete(ctx, env.GetGraph(t, parent)); err != nil {
			t.Fatalf("delete parent graph: %v", err)
		}
		env.AwaitGraphGone(t, parent, 30*time.Second)
		for _, team := range []string{"alpha", "beta"} {
			env.AwaitGraphGone(t, types.NamespacedName{Namespace: ns, Name: "team-" + team}, 30*time.Second)
			env.AwaitGraphGone(t, types.NamespacedName{Namespace: ns, Name: "team-" + team + "-audit"}, 30*time.Second)
			env.AwaitDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: team}, 30*time.Second)
			env.AwaitDeleted(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: team + "-audit"}, 30*time.Second)
		}
	})

	It("composes deferred spans inside parent CEL and across a three-level chain", func() {
		t := GinkgoT()
		ns := env.CreateNamespace(t)

		// L0 -> L1 -> L2, each level contributing one value to the leaf
		// ConfigMap. Also exercises in-expression composition (a deferred
		// span as a CEL operand) and a body with quotes that need no escaping.
		leaf := map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "chain-leaf"},
			"data": map[string]any{
				"a":        "${l0.v}",
				"b":        "$${l1.v}",
				"c":        "$$${l2.v}",
				"composed": "${'l0=' + l0.v + ' l1=' + $${l1.v}}",
				"quoted":   `$$${l2.v == "from-l2" ? 'match' : 'miss'}`,
			},
		}
		l2 := map[string]any{
			"apiVersion": "kro.run/v1alpha1", "kind": "Graph",
			"metadata": map[string]any{"name": "chain-l2"},
			"spec": map[string]any{"nodes": []any{
				map[string]any{"id": "l2", "def": map[string]any{"v": "from-l2"}},
				map[string]any{"id": "cm", "template": leaf},
			}},
		}
		l1 := map[string]any{
			"apiVersion": "kro.run/v1alpha1", "kind": "Graph",
			"metadata": map[string]any{"name": "chain-l1"},
			"spec": map[string]any{"nodes": []any{
				map[string]any{"id": "l1", "def": map[string]any{"v": "from-l1"}},
				map[string]any{"id": "next", "template": l2},
			}},
		}
		g := &expv1alpha1.Graph{
			ObjectMeta: metav1.ObjectMeta{Name: "chain-l0", Namespace: ns},
			Spec: expv1alpha1.GraphSpec{Nodes: []expv1alpha1.Node{
				{ID: "l0", Def: environment.RawExt(t, map[string]any{"v": "from-l0"})},
				{ID: "next", Template: environment.RawExt(t, l1)},
			}},
		}
		env.CreateGraph(t, g)

		// Each hop peels exactly one dollar sign.
		env.AwaitObject(t, graphGVK, types.NamespacedName{Namespace: ns, Name: "chain-l1"},
			func(u *unstructured.Unstructured) error {
				next, err := nodeByID(u, "next")
				if err != nil {
					return err
				}
				// The L2 Graph is still a template here; find its "cm" node.
				l2 := &unstructured.Unstructured{}
				l2.Object, _, _ = unstructured.NestedMap(next, "template")
				cm, err := nodeByID(l2, "cm")
				if err != nil {
					return err
				}
				data, _, _ := unstructured.NestedStringMap(cm, "template", "data")
				want := map[string]string{
					"a":        "from-l0",
					"b":        "${l1.v}",
					"c":        "$${l2.v}",
					"composed": "l0=from-l0 l1=${l1.v}",
					"quoted":   `$${l2.v == "from-l2" ? 'match' : 'miss'}`,
				}
				for k, v := range want {
					if data[k] != v {
						return errMismatch("L1 leaf data."+k, v, data[k])
					}
				}
				return nil
			}, 30*time.Second)

		env.AwaitObject(t, graphGVK, types.NamespacedName{Namespace: ns, Name: "chain-l2"},
			func(u *unstructured.Unstructured) error {
				cm, err := nodeByID(u, "cm")
				if err != nil {
					return err
				}
				data, _, _ := unstructured.NestedStringMap(cm, "template", "data")
				want := map[string]string{
					"a":        "from-l0",
					"b":        "from-l1",
					"c":        "${l2.v}",
					"composed": "l0=from-l0 l1=from-l1",
					"quoted":   `${l2.v == "from-l2" ? 'match' : 'miss'}`,
				}
				for k, v := range want {
					if data[k] != v {
						return errMismatch("L2 leaf data."+k, v, data[k])
					}
				}
				return nil
			}, 30*time.Second)

		env.AwaitObject(t, configMapGVK, types.NamespacedName{Namespace: ns, Name: "chain-leaf"},
			expectData(map[string]string{
				"a":        "from-l0",
				"b":        "from-l1",
				"c":        "from-l2",
				"composed": "l0=from-l0 l1=from-l1",
				"quoted":   "match",
			}), 30*time.Second)
	})
})
