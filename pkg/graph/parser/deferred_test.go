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

package parser

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/kube-openapi/pkg/validation/spec"

	krocel "github.com/kubernetes-sigs/kro/pkg/cel"
	"github.com/kubernetes-sigs/kro/pkg/graph/schema"
	"github.com/kubernetes-sigs/kro/pkg/graph/variable"
)

// TestDeferredExpressions_ParseEntryPoints checks that the $${...} deferral
// syntax flows through every public parse entry point with the same result:
// the standalone form yields a string-literal expression whose user-facing
// text is the author's original, the interpolated form is folded into the
// usual concatenation, and a deferred span at a typed field is passed
// through as a string (type enforcement is the compiler's job, not the
// parser's).
func TestDeferredExpressions_ParseEntryPoints(t *testing.T) {
	stringField := spec.Schema{SchemaProps: spec.SchemaProps{Type: []string{"string"}}}
	sch := &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"name":     stringField,
				"label":    stringField,
				"replicas": {SchemaProps: spec.SchemaProps{Type: []string{"integer"}}},
				"opaque": {
					VendorExtensible: spec.VendorExtensible{
						Extensions: spec.Extensions{xKubernetesPreserveUnknownFields: true},
					},
					SchemaProps: spec.SchemaProps{Type: []string{"object"}},
				},
			},
		},
	}
	resource := map[string]any{
		"name":     "$${cfg.team}",
		"label":    "${team}-$${cfg.team}",
		"replicas": "$${cfg.replicas}",
		"opaque": map[string]any{
			"deep": "$$${leaf.value}",
		},
	}

	want := map[string]struct {
		original string
		template string
	}{
		"name":        {original: `"${cfg.team}"`, template: "$${cfg.team}"},
		"label":       {original: `(team) + "-" + ("${cfg.team}")`, template: "${team}-$${cfg.team}"},
		"replicas":    {original: `"${cfg.replicas}"`, template: "$${cfg.replicas}"},
		"opaque.deep": {original: `"$${leaf.value}"`, template: "$$${leaf.value}"},
	}

	check := func(t *testing.T, got []variable.FieldDescriptor) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %d descriptors, want %d: %+v", len(got), len(want), got)
		}
		for _, fd := range got {
			w, ok := want[fd.Path]
			if !ok {
				t.Errorf("unexpected descriptor at path %q", fd.Path)
				continue
			}
			if fd.Expression.Original != w.original {
				t.Errorf("%s: Original = %q, want %q", fd.Path, fd.Expression.Original, w.original)
			}
			if fd.Expression.OriginalTemplate != w.template {
				t.Errorf("%s: OriginalTemplate = %q, want %q", fd.Path, fd.Expression.OriginalTemplate, w.template)
			}
			if fd.Expression.UserExpression() != w.template {
				t.Errorf("%s: UserExpression() = %q, want the author's text %q", fd.Path, fd.Expression.UserExpression(), w.template)
			}
		}
	}

	t.Run("schema-aware", func(t *testing.T) {
		got, err := New(schema.NewCache()).ParseResource(resource, sch)
		if err != nil {
			t.Fatalf("ParseResource: %v", err)
		}
		check(t, got)
	})

	t.Run("schemaless", func(t *testing.T) {
		got, plain, err := ParseSchemalessResource(resource)
		if err != nil {
			t.Fatalf("ParseSchemalessResource: %v", err)
		}
		if len(plain) != 0 {
			t.Errorf("expected no plain fields, got %v", plain)
		}
		check(t, got)
	})
}

// TestDeferredExpressions_NoTemplateWithoutRewrite guards the error-message
// contract: an ordinary standalone expression must not gain an
// OriginalTemplate, so existing diagnostics keep printing the bare expression.
func TestDeferredExpressions_NoTemplateWithoutRewrite(t *testing.T) {
	got, _, err := ParseSchemalessResource(map[string]any{
		"plain":  "${cfg.team}",
		"legacy": `${"${cfg.team}"}`,
	})
	if err != nil {
		t.Fatalf("ParseSchemalessResource: %v", err)
	}
	for _, fd := range got {
		if fd.Expression.OriginalTemplate != "" {
			t.Errorf("%s: OriginalTemplate = %q, want empty (no rewrite happened)", fd.Path, fd.Expression.OriginalTemplate)
		}
	}
}

// TestUnwrapExpressions_Deferred checks that condition-style entries
// (readyWhen, includeWhen, forEach) unwrap a deferred span to its literal
// rather than string-trimming the delimiters, which would leave "$${x"
// behind.
func TestUnwrapExpressions_Deferred(t *testing.T) {
	exprs, err := UnwrapExpressions([]string{"$${x}", "${y}", "$$${z}"})
	if err != nil {
		t.Fatalf("UnwrapExpressions: %v", err)
	}
	want := []struct{ original, template string }{
		{`"${x}"`, "$${x}"},
		{"y", ""},
		{`"$${z}"`, "$$${z}"},
	}
	for i, w := range want {
		if exprs[i].Original != w.original {
			t.Errorf("[%d] Original = %q, want %q", i, exprs[i].Original, w.original)
		}
		if exprs[i].OriginalTemplate != w.template {
			t.Errorf("[%d] OriginalTemplate = %q, want %q", i, exprs[i].OriginalTemplate, w.template)
		}
	}

	if _, err := UnwrapExpressions([]string{"a-$${x}"}); err == nil {
		t.Error("expected a deferred span with a prefix to be rejected as non-standalone")
	}
}

// TestBuildStringTemplate_DeferredMatch checks the concatenation builder
// treats a rewritten (string-literal) match like any other expression
// operand — parenthesized, not re-quoted.
func TestBuildStringTemplate_DeferredMatch(t *testing.T) {
	input := "a-$${x}-b"
	matches, err := extractExpressions(input)
	if err != nil {
		t.Fatal(err)
	}
	got := buildStringTemplate(input, matches)
	want := `"a-" + ("${x}") + "-b"`
	if got != want {
		t.Errorf("buildStringTemplate = %q, want %q", got, want)
	}
}

// TestDeferredLiteral_CELEvaluation evaluates the rewritten literals with the
// real CEL environment rather than strconv.Unquote: the escapes deferredLiteral
// emits must mean the same thing to CEL as to Go, so the next level receives
// the author's body byte for byte — including non-ASCII text, control
// characters, raw newlines from YAML block scalars, and backslash escapes
// meant for the next level's CEL.
func TestDeferredLiteral_CELEvaluation(t *testing.T) {
	env, err := krocel.DefaultEnvironment()
	if err != nil {
		t.Fatalf("DefaultEnvironment: %v", err)
	}
	tests := []struct {
		name  string
		input string
		want  string // the text the next level receives
	}{
		{"plain", "$${cfg.team}", "${cfg.team}"},
		{"two levels", "$$${x}", "$${x}"},
		{"double quotes in body", `$${x == "a"}`, `${x == "a"}`},
		{"escaped quote in body", `$${x == 'it\'s'}`, `${x == 'it\'s'}`},
		{"backslash in body", `$${x.matches('\\d+')}`, `${x.matches('\\d+')}`},
		{"non-ASCII", "$${'héllo 日本 😀'}", "${'héllo 日本 😀'}"},
		{"raw newline and tab", "$${a\n\t+ b}", "${a\n\t+ b}"},
		{"unicode line separator", "$${'\u2028'}", "${'\u2028'}"},
		{"control character", "$${'\x01'}", "${'\x01'}"},
		{"composed inside CEL", "${'team-' + $${x} + '!'}", "team-${x}!"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := standaloneMatch(tt.input)
			if err != nil || m == nil {
				t.Fatalf("standaloneMatch(%q) = %v, %v", tt.input, m, err)
			}
			ast, iss := env.Compile(m.expr)
			if iss != nil && iss.Err() != nil {
				t.Fatalf("CEL compile %q: %v", m.expr, iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("CEL program: %v", err)
			}
			out, _, err := prg.Eval(map[string]any{})
			if err != nil {
				t.Fatalf("CEL eval %q: %v", m.expr, err)
			}
			if got := out.Value(); got != tt.want {
				t.Errorf("CEL value = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestParseErrors_NameTheField checks that scanner errors from a template
// field are reported with the field's path, on both parse entry points. An
// offset alone is meaningless once the error is wrapped by the node/resource
// name, and $${...} makes such errors easier to hit (an unbalanced quote in
// shell text swallows the closing brace).
func TestParseErrors_NameTheField(t *testing.T) {
	resource := map[string]any{
		"metadata": map[string]any{"name": "ok"},
		"data": map[string]any{
			"cmd": "echo $${VAR:-don't}",
		},
	}
	sch := &spec.Schema{
		VendorExtensible: spec.VendorExtensible{
			Extensions: spec.Extensions{xKubernetesPreserveUnknownFields: true},
		},
		SchemaProps: spec.SchemaProps{Type: []string{"object"}},
	}

	check := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, ErrUnterminatedExpression) {
			t.Fatalf("err = %v, want ErrUnterminatedExpression", err)
		}
		if !strings.Contains(err.Error(), "path data.cmd") {
			t.Errorf("error %q does not name the field path", err)
		}
	}

	t.Run("schema-aware", func(t *testing.T) {
		_, err := New(schema.NewCache()).ParseResource(resource, sch)
		check(t, err)
	})
	t.Run("schemaless", func(t *testing.T) {
		_, _, err := ParseSchemalessResource(resource)
		check(t, err)
	})
}
