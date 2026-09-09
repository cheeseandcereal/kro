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

package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
)

// stampedChildGraph is a parent Graph that fans out child Graph objects with
// a template whose CEL is a mix of parent-evaluated and deferred expressions.
// It mirrors examples/graph/stamped.yaml.
func stampedChildGraph() *expv1alpha1.Graph {
	return generator.NewGraph("parent",
		generator.WithNamespace("default"),
		generator.WithDef("teams", map[string]any{"names": []any{"alpha", "beta"}}),
		generator.WithTemplate("children", map[string]any{
			"apiVersion": "kro.run/v1alpha1",
			"kind":       "Graph",
			"metadata":   map[string]any{"name": "team-${team}"},
			"spec": map[string]any{
				"nodes": []any{
					map[string]any{
						"id":  "cfg",
						"def": map[string]any{"team": "${team}"}, // evaluated by the parent
					},
					map[string]any{
						"id": "cm",
						"template": map[string]any{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"metadata":   map[string]any{"name": "$${cfg.team}"}, // deferred to the child
							"data": map[string]any{
								"label": "${team}-$${cfg.team}",                    // mixed
								"upper": "${'${' + 'cfg.team.upperAscii()' + '}'}", // built dynamically
								"deep":  "$$${leaf.value}",                         // deferred two levels
							},
						},
					},
				},
			},
		}, expv1alpha1.ForEachDimension{"team": "${teams.names}"}),
	)
}

// TestCompile_DeferredExpressions checks that $${...} spans in a stamped
// child Graph template compile at the parent: each deferred span becomes a
// string literal in the compiled expression (so the child receives the text
// verbatim), the parent's own references still create dependencies, and no
// dependency is created on the child-scope names inside deferred spans.
func TestCompile_DeferredExpressions(t *testing.T) {
	t.Parallel()

	prog, err := newExampleTestCompiler(t).Compile(stampedChildGraph())
	require.NoError(t, err)

	children := prog.Nodes["children"]
	require.NotNil(t, children)
	assert.Contains(t, children.HardDepIDs(), "teams", "forEach source is a real dependency")
	assert.NotContains(t, children.HardDepIDs(), "cfg", "child-scope names inside deferred spans are opaque text, not references")

	byPath := map[string]string{}
	userText := map[string]string{}
	for _, v := range children.Variables {
		byPath[v.Path] = v.Expression.Original
		userText[v.Path] = v.Expression.UserExpression()
	}

	assert.Equal(t, `"team-" + (team)`, byPath["metadata.name"])
	assert.Equal(t, "team", byPath["spec.nodes[0].def.team"])
	assert.Equal(t, `"${cfg.team}"`, byPath["spec.nodes[1].template.metadata.name"])
	assert.Equal(t, `(team) + "-" + ("${cfg.team}")`, byPath["spec.nodes[1].template.data.label"])
	assert.Equal(t, `'${' + 'cfg.team.upperAscii()' + '}'`, byPath["spec.nodes[1].template.data.upper"])
	assert.Equal(t, `"$${leaf.value}"`, byPath["spec.nodes[1].template.data.deep"])

	// Diagnostics show what the author wrote, not the rewritten literal.
	assert.Equal(t, "$${cfg.team}", userText["spec.nodes[1].template.metadata.name"])
	assert.Equal(t, "$$${leaf.value}", userText["spec.nodes[1].template.data.deep"])
}

// TestCompile_DeferredExpressionIsAString pins the one limitation of
// deferral: the parent sees a deferred span as a string, so it can only land
// on a field of the parent's own object that accepts a string. Here the
// target field is a typed boolean, and the compiler's type check rejects it.
func TestCompile_DeferredExpressionIsAString(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("cfg", map[string]any{"dns": true}),
		generator.WithTemplate("vpc", map[string]any{
			"apiVersion": "ec2.services.k8s.aws/v1alpha1",
			"kind":       "VPC",
			"metadata":   map[string]any{"name": "vpc"},
			"spec": map[string]any{
				"cidrBlocks":       []any{"10.0.0.0/16"},
				"enableDNSSupport": "$${cfg.dns}",
			},
		}),
	)
	_, err := newTestCompiler(t).Compile(g)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
	assert.Contains(t, err.Error(), `"$${cfg.dns}"`, "the message names the author's text")
	assert.Contains(t, err.Error(), `returns "string" but expected "bool"`)
}

// TestCompile_BareNestingStillRejected guards the guardrail: introducing
// $${...} must not make a forgotten-dollar nesting silently parse.
func TestCompile_BareNestingStillRejected(t *testing.T) {
	t.Parallel()

	g := generator.NewGraph("g",
		generator.WithNamespace("default"),
		generator.WithDef("cfg", map[string]any{"name": "x"}),
		generator.WithTemplate("cm", map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": "${'a-' + ${cfg.name}}"},
		}),
	)
	_, err := newTestCompiler(t).Compile(g)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nested expressions are not allowed")
	assert.Contains(t, err.Error(), "$${...}", "the error points at the deferral syntax")
}
