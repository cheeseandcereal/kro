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

package library

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/kubernetes-sigs/kro/pkg/graph/crd"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
)

// newStringsEnv builds an environment with both kro's Strings() library and
// cel-go's ext.Strings(). Both register functions in the `strings.` namespace,
// so every test in this file also exercises their coexistence.
func newStringsEnv(t *testing.T, opts ...cel.EnvOption) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(append([]cel.EnvOption{Strings(), ext.Strings()}, opts...)...)
	require.NoError(t, err)
	return env
}

func evalString(t *testing.T, env *cel.Env, expr string, vars map[string]any) string {
	t.Helper()
	ast, issues := env.Compile(expr)
	require.NoError(t, issues.Err(), "compile %q", expr)
	require.Equal(t, cel.StringType, ast.OutputType(), "output type of %q", expr)
	prg, err := env.Program(ast)
	require.NoError(t, err)
	out, _, err := prg.Eval(vars)
	require.NoError(t, err, "eval %q", expr)
	s, ok := out.Value().(string)
	require.True(t, ok, "expected string result for %q, got %T", expr, out.Value())
	return s
}

func TestStringsPlural(t *testing.T) {
	env := newStringsEnv(t)

	testCases := []struct {
		name string
		expr string
		want string
	}{
		{name: "simple kind", expr: `strings.plural('Pod')`, want: "Pods"},
		{name: "multi-word kind", expr: `strings.plural('ClusterPolicy')`, want: "ClusterPolicies"},
		{name: "y to ies", expr: `strings.plural('NetworkPolicy')`, want: "NetworkPolicies"},
		{name: "vowel y stays", expr: `strings.plural('Gateway')`, want: "Gateways"},
		{name: "ss to sses", expr: `strings.plural('Ingress')`, want: "Ingresses"},
		{name: "compound ss to sses", expr: `strings.plural('IngressClass')`, want: "IngressClasses"},
		{name: "x to xes", expr: `strings.plural('Box')`, want: "Boxes"},
		{name: "leading acronym preserved", expr: `strings.plural('HTTPRoute')`, want: "HTTPRoutes"},
		{name: "irregular noun", expr: `strings.plural('Person')`, want: "People"},
		{name: "lowercase input stays lowercase", expr: `strings.plural('deployment')`, want: "deployments"},
		{name: "already plural (trailing s)", expr: `strings.plural('Endpoints')`, want: "Endpoints"},
		{name: "already plural (rule form)", expr: `strings.plural('Pods')`, want: "Pods"},
		{name: "empty string", expr: `strings.plural('')`, want: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, evalString(t, env, tc.expr, nil))
		})
	}
}

// TestStringsPluralInflectorBehaviour pins the edges of the underlying
// inflector that the docs describe (or deliberately leave undefined): input is
// not trimmed or normalised, only the last word is inflected, all-caps input
// is not case-preserving, and already-plural detection is heuristic. RGD
// authors now see these directly, and a change in any of them would also
// rename the CRDs kro derives from kinds, so a dependency bump that alters
// them must be a conscious decision.
func TestStringsPluralInflectorBehaviour(t *testing.T) {
	env := newStringsEnv(t, cel.Variable("x", cel.StringType))

	testCases := map[string]string{
		// Whitespace is neither trimmed nor treated as part of the word.
		" ":     "",
		"   ":   "",
		" Pod":  " Pods",
		"Pod ":  "Pod s",
		" Pod ": " Pod s",
		// Symbol-only input has no word to inflect.
		"🚀": "",
		// Separators are kept; only the final segment is inflected.
		"cluster-policy": "cluster-policies",
		"cluster_policy": "cluster_policies",
		"cluster.policy": "cluster.policies",
		"cluster policy": "cluster policies",
		"Cluster-Policy": "Cluster-Policies",
		// Digits are part of the word.
		"pod1":        "pod1s",
		"v1":          "v1s",
		"EC2Instance": "EC2Instances",
		// Trailing upper-case acronyms take the acronym's suffix rule.
		"ServiceDNS":  "ServiceDNSes",
		"servicedns":  "servicedns",
		"ServiceCIDR": "ServiceCIDRs",
		"API":         "APIs",
		// All-caps input is not case-preserving.
		"POLICY":    "POLICYs",
		"BOX":       "BOXs",
		"INGRESS":   "INGRESSes",
		"PERSON":    "People",
		"INDEX":     "Indices",
		"PODS":      "PODSes",
		"ENDPOINTS": "ENDPOINTSes",
		// Irregular and Latin-derived nouns.
		"Child":  "Children",
		"Datum":  "Data",
		"Index":  "Indices",
		"Status": "Statuses",
		"Schema": "Schemas",
		"Radius": "Radii",
		"Medium": "Media",
		"Repo":   "Repoes",
		"Kro":    "Kroes",
		// Words ending in -s are treated as already plural.
		"Kubernetes": "Kubernetes",
		"Analytics":  "Analytics",
		"Redis":      "Redis",
		"Series":     "Series",
		// Already-plural forms are returned unchanged.
		"Policies":        "Policies",
		"Ingresses":       "Ingresses",
		"People":          "People",
		"Data":            "Data",
		"clusterpolicies": "clusterpolicies",
		// Non-ASCII input just gets an 's'.
		"Pöd": "Pöds",
		"ポッド": "ポッドs",
	}

	for in, want := range testCases {
		t.Run(fmt.Sprintf("%q", in), func(t *testing.T) {
			assert.Equal(t, want, evalString(t, env, `strings.plural(x)`, map[string]any{"x": in}))
		})
	}

	t.Run("long input", func(t *testing.T) {
		long := strings.Repeat("a", 10000)
		assert.Equal(t, long+"s", evalString(t, env, `strings.plural(x)`, map[string]any{"x": long}))
	})
}

// TestStringsPluralCRDNameContract pins the documented recipe for deriving a
// CRD name from a kind,
//
//	strings.plural(kind.lowerAscii()) + '.' + group
//
// to the Go code kro actually uses to name the CRD for a
// ResourceGraphDefinition (crd.SynthesizeCRD) and to address its instances
// (metadata.GetResourceGraphDefinitionInstanceGVR). If the CEL recipe and
// either call site ever disagree, this test fails.
func TestStringsPluralCRDNameContract(t *testing.T) {
	env := newStringsEnv(t,
		cel.Variable("kind", cel.StringType),
		cel.Variable("group", cel.StringType),
	)

	const expr = `strings.plural(kind.lowerAscii()) + '.' + group`
	const group = "kro.run"
	const version = "v1alpha1"

	kinds := []string{
		"Pod", "Deployment", "ClusterPolicy", "NetworkPolicy", "Gateway",
		"Ingress", "IngressClass", "HTTPRoute", "Endpoints", "WebApplication",
		"ComponentStatus", "FlowSchema", "StorageClass", "PodDisruptionBudget",
		"CustomResourceDefinition", "ResourceGraphDefinition", "EndpointSlice",
		"IPAddress", "ServiceCIDR", "CSIStorageCapacity", "ValidatingAdmissionPolicy",
		"EC2Instance", "S3Bucket", "IAMRole",
		// Kinds ending in an upper-case acronym: pluralizing before lowercasing
		// would give "servicednses", but kro's CRD name is "servicedns".
		"ServiceDNS",
		// All-caps kinds: pluralizing before lowercasing would give "policys".
		"POLICY",
	}

	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			got := evalString(t, env, expr, map[string]any{"kind": kind, "group": group})

			synthesized := crd.SynthesizeCRD(group, version, kind,
				extv1.JSONSchemaProps{}, extv1.JSONSchemaProps{}, false, extv1.NamespaceScoped, nil)
			assert.Equal(t, synthesized.Name, got, "CRD metadata.name")
			assert.Equal(t, synthesized.Spec.Names.Plural+"."+group, got, "CRD spec.names.plural")

			gvr := metadata.GetResourceGraphDefinitionInstanceGVR(group, version, kind)
			assert.Equal(t, gvr.Resource+"."+group, got, "instance GVR resource")
		})
	}

	// Sanity-check the two documented divergences so the guidance in the docs
	// stays honest: lowercase-first and pluralize-first are not interchangeable.
	t.Run("operand order matters for ServiceDNS", func(t *testing.T) {
		vars := map[string]any{"kind": "ServiceDNS", "group": group}
		assert.Equal(t, "servicedns.kro.run", evalString(t, env, expr, vars))
		assert.Equal(t, "servicednses.kro.run",
			evalString(t, env, `strings.plural(kind).lowerAscii() + '.' + group`, vars))
	})
	t.Run("operand order matters for POLICY", func(t *testing.T) {
		vars := map[string]any{"kind": "POLICY", "group": group}
		assert.Equal(t, "policies.kro.run", evalString(t, env, expr, vars))
		assert.Equal(t, "policys.kro.run",
			evalString(t, env, `strings.plural(kind).lowerAscii() + '.' + group`, vars))
	})
}

// TestStringsPluralCoexistsWithCelGoStrings verifies that kro's `strings.`
// functions and cel-go's ext.Strings() (strings.quote and the <string>
// member helpers) work side by side in one environment, including when they
// are chained in a single expression.
func TestStringsPluralCoexistsWithCelGoStrings(t *testing.T) {
	env := newStringsEnv(t)

	assert.Equal(t, "clusterpolicies", evalString(t, env, `strings.plural('ClusterPolicy').lowerAscii()`, nil))
	assert.Equal(t, `"Pods"`, evalString(t, env, `strings.quote(strings.plural('Pod'))`, nil))
	assert.Equal(t, "Pods, Deployments",
		evalString(t, env, `['Pod', 'Deployment'].map(k, strings.plural(k)).join(', ')`, nil))
}

func TestStringsPluralCompileErrors(t *testing.T) {
	env := newStringsEnv(t)

	testCases := []struct {
		name    string
		expr    string
		wantErr string
	}{
		{name: "int argument", expr: `strings.plural(1)`, wantErr: "found no matching overload"},
		{name: "null argument", expr: `strings.plural(null)`, wantErr: "found no matching overload"},
		{name: "bytes argument", expr: `strings.plural(b'Pod')`, wantErr: "found no matching overload"},
		{name: "list argument", expr: `strings.plural(['Pod'])`, wantErr: "found no matching overload"},
		{name: "no arguments", expr: `strings.plural()`, wantErr: "found no matching overload"},
		{name: "too many arguments", expr: `strings.plural('Pod', 'Deployment')`, wantErr: "found no matching overload"},
		{name: "not a member function", expr: `'Pod'.plural()`, wantErr: "undeclared reference to 'plural'"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, issues := env.Compile(tc.expr)
			require.Error(t, issues.Err())
			assert.Contains(t, issues.String(), tc.wantErr)
		})
	}
}

// TestStringsPluralRuntimeTypeGuard covers arguments whose static type is dyn
// (as every field of an untyped resource is). cel-go's runtime overload guard
// rejects a non-string before the binding runs, so the author still sees an
// error that names the function.
func TestStringsPluralRuntimeTypeGuard(t *testing.T) {
	env := newStringsEnv(t, cel.Variable("v", cel.DynType))
	ast, issues := env.Compile(`strings.plural(v)`)
	require.NoError(t, issues.Err())
	prg, err := env.Program(ast)
	require.NoError(t, err)

	out, _, err := prg.Eval(map[string]any{"v": "Pod"})
	require.NoError(t, err)
	assert.Equal(t, "Pods", out.Value())

	for name, v := range map[string]any{
		"int":   int64(3),
		"bytes": []byte("Pod"),
		"map":   map[string]any{"kind": "Pod"},
		"list":  []any{"Pod"},
		"bool":  true,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := prg.Eval(map[string]any{"v": v})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "no such overload: strings.plural(")
		})
	}
}

// The inflector guards its dictionaries with a package-level lock, and one
// compiled program is shared by every reconcile that evaluates it.
func TestStringsPluralConcurrentEvaluation(t *testing.T) {
	env := newStringsEnv(t, cel.Variable("x", cel.StringType))
	ast, issues := env.Compile(`strings.plural(x)`)
	require.NoError(t, issues.Err())
	prg, err := env.Program(ast)
	require.NoError(t, err)

	inputs := map[string]string{
		"Pod": "Pods", "ClusterPolicy": "ClusterPolicies", "Ingress": "Ingresses",
		"Person": "People", "ServiceDNS": "ServiceDNSes", "": "",
	}

	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 16 {
				for in, want := range inputs {
					out, _, err := prg.Eval(map[string]any{"x": in})
					if err != nil || out.Value() != want {
						t.Errorf("strings.plural(%q) = %v, %v; want %q", in, out, err, want)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestStringsLibraryName(t *testing.T) {
	lib := &stringsLibrary{}
	assert.Equal(t, "kro.strings", lib.LibraryName())
}
