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

package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubernetes-sigs/kro/pkg/graphengine/compiler"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
)

// The IsIgnored memo is refreshed only by ResetIgnoredCache, which clears
// every node — including a dependent whose verdict was reached contagiously.
func TestResetIgnoredCache_ClearsMemoizedVerdicts(t *testing.T) {
	t.Parallel()
	g := generator.NewGraph("g",
		generator.WithDef("flag", map[string]any{"enabled": false}),
		generator.WithDef("guarded", map[string]any{"x": "y"}),
		generator.WithIncludeWhen("${flag.enabled}"),
		// leaf has no includeWhen of its own; it is ignored only contagiously.
		generator.WithDef("leaf", map[string]any{"y": "${guarded.x}"}),
	)
	prog := compileGraph(t, g)
	rt := New(prog, g)
	setFirst(rt, "flag")

	ignored, err := rt.Node("guarded").IsIgnored()
	require.NoError(t, err)
	assert.True(t, ignored, "flag.enabled=false must ignore guarded")
	ignored, err = rt.Node("leaf").IsIgnored()
	require.NoError(t, err)
	assert.True(t, ignored, "leaf must be contagiously ignored through guarded")

	// The memo is sticky within a reconcile: a scope change alone changes nothing.
	rt.Set("flag", map[string]any{"enabled": true})
	ignored, err = rt.Node("guarded").IsIgnored()
	require.NoError(t, err)
	assert.True(t, ignored, "IsIgnored is memoized: a scope change alone must not change the verdict")

	rt.ResetIgnoredCache()

	ignored, err = rt.Node("guarded").IsIgnored()
	require.NoError(t, err)
	assert.False(t, ignored, "after the reset the verdict is recomputed against the current scope")
	ignored, err = rt.Node("leaf").IsIgnored()
	require.NoError(t, err)
	assert.False(t, ignored, "the reset must also clear the contagious verdict cached on the dependent")
}

// A presence-tolerant includeWhen evaluated against a soft-dependency `{}`
// placeholder returns a definite false with a nil error, so IsIgnored memoizes
// it; the memo must be reset before the target's published value is consulted.
func TestResetIgnoredCache_PlaceholderVerdictIsNotReused(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		includeWhen string
	}{
		{name: "optional chain with orValue", includeWhen: `${db.?data.phase.orValue("missing") == "Running"}`},
		{name: "has() on a top-level field", includeWhen: `${has(db.data)}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := generator.NewGraph("g",
				generator.WithNamespace("default"),
				generator.WithTemplate("db", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": "db"},
					"data":     map[string]any{"owner": "team", "phase": "Running"},
				}),
				generator.WithTemplate("app", map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": map[string]any{"name": "app"},
					"data":     map[string]any{"k": "v"},
				}),
				generator.WithIncludeWhen(tc.includeWhen),
				// A soft-deps consumer reading db makes db a soft target seeded with `{}`.
				generator.WithDef("writeback", map[string]any{"dbOwner": "${db.data.owner}"}),
			)
			prog, err := mustCompiler(t).CompileWithOptions(g, compiler.WithSoftDependencies("writeback"))
			require.NoError(t, err)
			require.Contains(t, prog.Nodes["writeback"].SoftDepIDs(), "db", "db must be a soft dep of writeback")

			rt := New(prog, g)
			require.Equal(t, map[string]any{}, rt.Scope()["db"],
				"a singleton soft target is seeded with an empty-object placeholder before apply")

			ignored, err := rt.Node("app").IsIgnored()
			require.NoError(t, err, "a presence-tolerant includeWhen evaluates cleanly against the placeholder")
			assert.True(t, ignored, "against the `{}` placeholder the includeWhen is (wrongly) false")

			setFirst(rt, "db")
			ignored, err = rt.Node("app").IsIgnored()
			require.NoError(t, err)
			assert.True(t, ignored, "without a reset the placeholder verdict is reused against the published value")

			rt.ResetIgnoredCache()
			ignored, err = rt.Node("app").IsIgnored()
			require.NoError(t, err)
			assert.False(t, ignored, "after the reset the includeWhen is decided against the published db and app is included")
		})
	}
}
