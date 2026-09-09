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

package parser

import (
	"errors"
	"strconv"
	"testing"
)

func TestExtractExpressions(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []exprMatch
		wantErr bool
	}{
		{
			name:  "Simple expression",
			input: "${resource.field}",
			want:  []exprMatch{{expr: "resource.field", start: 0, end: 17}},
		},
		{
			name:  "Expression with function",
			input: "${length(resource.list)}",
			want:  []exprMatch{{expr: "length(resource.list)", start: 0, end: 24}},
		},
		{
			name:  "Expression with prefix",
			input: "prefix-${resource.field}",
			want:  []exprMatch{{expr: "resource.field", start: 7, end: 24}},
		},
		{
			name:  "Expression with suffix",
			input: "${resource.field}-suffix",
			want:  []exprMatch{{expr: "resource.field", start: 0, end: 17}},
		},
		{
			name:  "Multiple expressions",
			input: "${resource1.field}-middle-${resource2.field}",
			want: []exprMatch{
				{expr: "resource1.field", start: 0, end: 18},
				{expr: "resource2.field", start: 26, end: 44},
			},
		},
		{
			name:  "Expression with map",
			input: "${resource.map['key']}",
			want:  []exprMatch{{expr: "resource.map['key']", start: 0, end: 22}},
		},
		{
			name:  "Expression with list index",
			input: "${resource.list[0]}",
			want:  []exprMatch{{expr: "resource.list[0]", start: 0, end: 19}},
		},
		{
			name:  "Complex expression",
			input: "${resource.field == 'value' && resource.number > 5}",
			want:  []exprMatch{{expr: "resource.field == 'value' && resource.number > 5", start: 0, end: 51}},
		},
		{
			name:  "No expressions",
			input: "plain string",
			want:  nil,
		},
		{
			name:  "Empty string",
			input: "",
			want:  nil,
		},
		{
			name:    "Incomplete expression",
			input:   "${incomplete",
			want:    nil,
			wantErr: true,
		},
		{
			name:  "Expression with escaped quotes",
			input: "${resource.field == \"escaped\\\"quote\"}",
			want:  []exprMatch{{expr: "resource.field == \"escaped\\\"quote\"", start: 0, end: 37}},
		},
		{
			name:  "Multiple expressions with whitespace",
			input: "  ${resource1.field}  ${resource2.field}  ",
			want: []exprMatch{
				{expr: "resource1.field", start: 2, end: 20},
				{expr: "resource2.field", start: 22, end: 40},
			},
		},
		{
			name:  "Expression with newlines",
			input: "${resource.list.map(\n  x,\n  x * 2\n)}",
			want:  []exprMatch{{expr: "resource.list.map(\n  x,\n  x * 2\n)", start: 0, end: 36}},
		},
		{
			name:    "Nested expression (should error)",
			input:   "${outer(${inner})} ${outer}",
			want:    nil,
			wantErr: true,
		},
		{
			name:  "Nested expression but with quotes",
			input: "${outer(\"${inner}\")}",
			want:  []exprMatch{{expr: "outer(\"${inner}\")", start: 0, end: 20}},
		},
		{
			name:  "Nested closing brace without opening one",
			input: "${\"text with }} inside\"}",
			want:  []exprMatch{{expr: "\"text with }} inside\"", start: 0, end: 24}},
		},
		{
			name:  "Nested open brace without closing one",
			input: "${\"text with { inside\"}",
			want:  []exprMatch{{expr: "\"text with { inside\"", start: 0, end: 23}},
		},
		{
			name:  "Expressions with dictionary building",
			input: "${true ? {'key': 'value'} : {'key': 'value2'}}",
			want:  []exprMatch{{expr: "true ? {'key': 'value'} : {'key': 'value2'}", start: 0, end: 46}},
		},
		{
			name:  "Multiple expressions with dictionary building",
			input: "${true ? {'key': 'value'} : {'key': 'value2'}} somewhat ${resource.field} then ${false ? {'key': {'nestedKey':'value'}} : {'key': 'value2'}}",
			want: []exprMatch{
				{expr: "true ? {'key': 'value'} : {'key': 'value2'}", start: 0, end: 46},
				{expr: "resource.field", start: 56, end: 73},
				{expr: "false ? {'key': {'nestedKey':'value'}} : {'key': 'value2'}", start: 79, end: 140},
			},
		},
		{
			name:    "Multiple incomplete expressions",
			input:   "${incomplete1 ${incomplete2",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Mixed complete and incomplete",
			input:   "${complete} ${complete2} ${incomplete",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Mixed incomplete and complete",
			input:   "${incomplete ${complete}",
			want:    nil,
			wantErr: true,
		},
		{
			name:  "Ternary with empty map literal",
			input: "${condition ? value : {}}",
			want:  []exprMatch{{expr: "condition ? value : {}", start: 0, end: 25}},
		},
		{
			name:  "Ternary with empty map on both sides",
			input: "${condition ? {} : {}}",
			want:  []exprMatch{{expr: "condition ? {} : {}", start: 0, end: 22}},
		},
		{
			name:  "Complex ternary with empty map (real world example)",
			input: "${schema.spec.deployment.includeAnnotations ? schema.spec.deployment.annotations : {}}",
			want:  []exprMatch{{expr: "schema.spec.deployment.includeAnnotations ? schema.spec.deployment.annotations : {}", start: 0, end: 86}},
		},
		{
			name:  "Ternary with has() and empty map",
			input: "${has(schema.annotations) && includeAnnotations ? schema.annotations : {}}",
			want:  []exprMatch{{expr: "has(schema.annotations) && includeAnnotations ? schema.annotations : {}", start: 0, end: 74}},
		},
		{
			// Finding 6a: a "${" inside a single-quoted CEL string literal is
			// ordinary text, not a nested expression start, and a "}" inside
			// one must not close the outer expression.
			name:  "Single-quoted literal containing ${ is not a nested expression",
			input: "${resource.field == '${literal}'}",
			want:  []exprMatch{{expr: "resource.field == '${literal}'", start: 0, end: 33}},
		},
		{
			name:  "Single-quoted literal with a closing brace inside",
			input: "${'text with } inside'}",
			want:  []exprMatch{{expr: "'text with } inside'", start: 0, end: 23}},
		},
		{
			name:  "Single-quoted literal with an escaped quote",
			input: "${resource.field == 'escaped\\'quote'}",
			want:  []exprMatch{{expr: "resource.field == 'escaped\\'quote'", start: 0, end: 37}},
		},
		{
			name:  "Double quote inside a single-quoted literal is ordinary text",
			input: "${'a \" b'}",
			want:  []exprMatch{{expr: "'a \" b'", start: 0, end: 10}},
		},

		// ─── Deferred expressions: $${...} ──────────────────────────────────
		{
			// Two dollars defer one level: the parent emits the literal text
			// "${cfg.team}" for a downstream evaluator.
			name:  "Deferred standalone expression",
			input: "$${cfg.team}",
			want:  []exprMatch{{expr: `"${cfg.team}"`, start: 0, end: 12, rewritten: true}},
		},
		{
			// Each extra dollar adds one more level; exactly one is peeled here.
			name:  "Doubly deferred expression peels one dollar",
			input: "$$${x}",
			want:  []exprMatch{{expr: `"$${x}"`, start: 0, end: 6, rewritten: true}},
		},
		{
			name:  "Deferred with prefix and suffix",
			input: "pre-$${x}-post",
			want:  []exprMatch{{expr: `"${x}"`, start: 4, end: 9, rewritten: true}},
		},
		{
			// A parent-evaluated and a deferred expression mixed in one string.
			name:  "Mixed evaluated and deferred",
			input: "${team}-$${cfg.team}",
			want: []exprMatch{
				{expr: "team", start: 0, end: 7},
				{expr: `"${cfg.team}"`, start: 8, end: 20, rewritten: true},
			},
		},
		{
			// A deferred span directly after an evaluated expression: the
			// dollar run is counted from the end of the previous match.
			name:  "Deferred immediately after an evaluated expression",
			input: "${a}$${b}",
			want: []exprMatch{
				{expr: "a", start: 0, end: 4},
				{expr: `"${b}"`, start: 4, end: 9, rewritten: true},
			},
		},
		{
			name:  "Doubly deferred immediately after an evaluated expression",
			input: "${a}$$${b}",
			want: []exprMatch{
				{expr: "a", start: 0, end: 4},
				{expr: `"$${b}"`, start: 4, end: 10, rewritten: true},
			},
		},
		{
			// Every extra dollar is one more level; five defer four.
			name:  "Long dollar run",
			input: "$$$$${x}",
			want:  []exprMatch{{expr: `"$$$${x}"`, start: 0, end: 8, rewritten: true}},
		},
		{
			// A deferred span may be the very first thing inside an expression.
			name:  "Deferred span at the start of an expression body",
			input: "${$${x}}",
			want:  []exprMatch{{expr: `"${x}"`, start: 0, end: 8, rewritten: true}},
		},
		{
			// YAML block scalars hand the scanner raw newlines; they are
			// escaped in the literal and restored by CEL.
			name:  "Deferred body with a raw newline",
			input: "$${a\n+ b}",
			want:  []exprMatch{{expr: `"${a\n+ b}"`, start: 0, end: 9, rewritten: true}},
		},
		{
			// Quotes inside the deferred body are escaped for CEL by the
			// parser, so the author never has to.
			name:  "Deferred body containing quotes is escaped",
			input: `$${x == "a" && y == 'b'}`,
			want:  []exprMatch{{expr: `"${x == \"a\" && y == 'b'}"`, start: 0, end: 24, rewritten: true}},
		},
		{
			// The body is scanned with the same literal rules the child will
			// use, so a brace inside a quoted string does not end the span.
			name:  "Deferred body with a brace inside a literal",
			input: "$${x == '}'}",
			want:  []exprMatch{{expr: `"${x == '}'}"`, start: 0, end: 12, rewritten: true}},
		},
		{
			// Backslashes in the body survive verbatim (doubled in the CEL
			// literal, which CEL undoes), so the child sees the same escape.
			name:  "Deferred body with an escaped quote",
			input: `$${x == 'it\'s'}`,
			want:  []exprMatch{{expr: `"${x == 'it\\'s'}"`, start: 0, end: 16, rewritten: true}},
		},
		{
			// A deferred span nested inside a deferred span is opaque text;
			// the child peels it on its own turn.
			name:  "Deferred span inside a deferred span is opaque",
			input: "$${a + $${b}}",
			want:  []exprMatch{{expr: `"${a + $${b}}"`, start: 0, end: 13, rewritten: true}},
		},
		{
			// In-expression composition: the deferred span becomes a string
			// literal operand of the parent's CEL.
			name:  "Deferred span inside an evaluated expression",
			input: "${'team-' + $${x}}",
			want:  []exprMatch{{expr: `'team-' + "${x}"`, start: 0, end: 18, rewritten: true}},
		},
		{
			name:  "Deferred span as a function argument",
			input: `${f($${x == "a"})}`,
			want:  []exprMatch{{expr: `f("${x == \"a\"}")`, start: 0, end: 18, rewritten: true}},
		},
		{
			name:  "Multiple deferred spans inside one expression",
			input: "${$${a} + ' ' + $$${b}}",
			want:  []exprMatch{{expr: `"${a}" + ' ' + "$${b}"`, start: 0, end: 23, rewritten: true}},
		},
		{
			// A "$${" inside a CEL string literal is ordinary text.
			name:  "Deferred marker inside a string literal is not rewritten",
			input: "${'$${x}'}",
			want:  []exprMatch{{expr: "'$${x}'", start: 0, end: 10}},
		},
		{
			// Whitespace breaks the dollar run: "$ ${x}" is a literal "$ "
			// followed by an ordinary expression.
			name:  "Space between dollar and expression is not a deferral",
			input: "$ ${x}",
			want:  []exprMatch{{expr: "x", start: 2, end: 6}},
		},
		{
			name:    "Unterminated deferred expression",
			input:   "$${x",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Unterminated deferred span inside an expression",
			input:   "${f($${x)}",
			want:    nil,
			wantErr: true,
		},
		{
			// Quotes in the body are honored when looking for the closing
			// brace, so an unbalanced apostrophe (common in shell text)
			// swallows the "}". Such text needs the ${"..."} spelling.
			name:    "Unbalanced quote in a deferred body",
			input:   "$${VAR:-don't}",
			want:    nil,
			wantErr: true,
		},
		{
			// The guardrail is unchanged: bare nesting is still an error.
			name:    "Bare nested expression is still rejected",
			input:   "${outer(${inner})}",
			want:    nil,
			wantErr: true,
		},
		{
			// ... including directly after a closing quote, where the old
			// scanner let it through for CEL to reject with a less helpful
			// message.
			name:    "Bare nested expression after a string literal is rejected",
			input:   `${"a"${b}}`,
			want:    nil,
			wantErr: true,
		},

		// ─── Legacy deferral: a string literal whose text is an expression ──
		{
			name:  "Legacy deferral with double quotes",
			input: `${"${x}"}`,
			want:  []exprMatch{{expr: `"${x}"`, start: 0, end: 9}},
		},
		{
			name:  "Legacy deferral with single quotes",
			input: "${'${x}'}",
			want:  []exprMatch{{expr: "'${x}'", start: 0, end: 9}},
		},
		{
			name:  "Legacy two-level deferral alternates quotes",
			input: `${"${'${x}'}"}`,
			want:  []exprMatch{{expr: `"${'${x}'}"`, start: 0, end: 14}},
		},
		{
			name:  "Legacy deferral mixed with evaluated expression",
			input: `${"${plural(kind)}"}.${"${group}"}`,
			want: []exprMatch{
				{expr: `"${plural(kind)}"`, start: 0, end: 20},
				{expr: `"${group}"`, start: 21, end: 34},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractExpressions(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractExpressions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("extractExpressions() returned %d matches, want %d", len(got), len(tt.want))
				return
			}
			for i, g := range got {
				w := tt.want[i]
				if g.expr != w.expr || g.start != w.start || g.end != w.end || g.rewritten != w.rewritten {
					t.Errorf("match[%d] = {expr:%q, start:%d, end:%d, rewritten:%v}, want {expr:%q, start:%d, end:%d, rewritten:%v}",
						i, g.expr, g.start, g.end, g.rewritten, w.expr, w.start, w.end, w.rewritten)
				}
			}
		})
	}
}

// TestDeferredLiteralRoundTrip pins the contract between evaluation levels:
// the text a deferred span produces, when scanned again by the next level,
// yields the original body with exactly one dollar sign removed.
func TestDeferredLiteralRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input string // what L0 is given
		l1    string // the text L1 receives (the literal's value)
		l1Exp string // what L1 extracts from that text
	}{
		{"one level", "$${cfg.team}", "${cfg.team}", "cfg.team"},
		{"two levels", "$$${x}", "$${x}", `"${x}"`},
		{"quotes survive", `$${a == "b"}`, `${a == "b"}`, `a == "b"`},
		{"escapes survive", `$${a == 'it\'s'}`, `${a == 'it\'s'}`, `a == 'it\'s'`},
		{"nested deferral survives", "$${a + $${b}}", "${a + $${b}}", `a + "${b}"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := standaloneMatch(tt.input)
			if err != nil || m == nil {
				t.Fatalf("standaloneMatch(%q) = %v, %v", tt.input, m, err)
			}
			// The rewritten expr is a Go-quoted literal; its value is what L1 sees.
			l1, err := strconv.Unquote(m.expr)
			if err != nil {
				t.Fatalf("unquote %q: %v", m.expr, err)
			}
			if l1 != tt.l1 {
				t.Fatalf("L1 receives %q, want %q", l1, tt.l1)
			}
			next, err := standaloneMatch(l1)
			if err != nil || next == nil {
				t.Fatalf("L1 scan of %q = %v, %v", l1, next, err)
			}
			if next.expr != tt.l1Exp {
				t.Errorf("L1 extracts %q, want %q", next.expr, tt.l1Exp)
			}
		})
	}
}

// TestDeferredBodyIsOpaque pins that a deferred body is not validated by the
// level that peels it: a bare nested "${" inside it is accepted here and
// rejected only when the next level scans the text it receives. Validating
// the body here would break deferring text meant for a non-kro evaluator,
// such as a shell's nested "${VAR:-${OTHER}}".
func TestDeferredBodyIsOpaque(t *testing.T) {
	m, err := standaloneMatch("$${a(${b})}")
	if err != nil || m == nil {
		t.Fatalf("L0 should accept an opaque body: match=%v err=%v", m, err)
	}
	l1, err := strconv.Unquote(m.expr)
	if err != nil {
		t.Fatalf("unquote %q: %v", m.expr, err)
	}
	if l1 != "${a(${b})}" {
		t.Fatalf("L1 receives %q, want %q", l1, "${a(${b})}")
	}
	if _, err := standaloneMatch(l1); !errors.Is(err, ErrNestedExpression) {
		t.Errorf("L1 scan of %q: err = %v, want ErrNestedExpression", l1, err)
	}
}

// TestExtractExpressions_ErrorOffsets checks that the reported offset is that
// of the delimiter which never closed, including a deferred span nested in an
// otherwise well-formed expression, and that the sentinel errors are
// preserved for errors.Is.
func TestExtractExpressions_ErrorOffsets(t *testing.T) {
	tests := []struct {
		input   string
		wantErr error
		wantMsg string
	}{
		{"${x", ErrUnterminatedExpression, "unterminated expression starting at offset 0"},
		{"ab ${x", ErrUnterminatedExpression, "unterminated expression starting at offset 3"},
		{"$${x", ErrUnterminatedExpression, "unterminated expression starting at offset 0"},
		{"ab $${x", ErrUnterminatedExpression, "unterminated expression starting at offset 3"},
		// The inner span "$${x)}" is closed by the only "}", so the outer
		// "${" is the one left open.
		{"${f($${x)}", ErrUnterminatedExpression, "unterminated expression starting at offset 0"},
		{"${f($${x", ErrUnterminatedExpression, "unterminated expression starting at offset 4"},
		{"${a}${x", ErrUnterminatedExpression, "unterminated expression starting at offset 4"},
		{"${outer(${inner})}", ErrNestedExpression, ErrNestedExpression.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := extractExpressions(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("extractExpressions(%q) err = %v, want %v", tt.input, err, tt.wantErr)
			}
			if err.Error() != tt.wantMsg {
				t.Errorf("message = %q, want %q", err.Error(), tt.wantMsg)
			}
		})
	}
}

func TestIsOneShotExpression(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    bool
		wantErr bool
	}{
		{"Simple one-shot", "${resource.field}", true, false},
		{"One-shot with function", "${length(resource.list)}", true, false},
		{"Not one-shot prefix", "prefix-${resource.field}", false, false},
		{"Not one-shot suffix", "${resource.field}-suffix", false, false},
		{"Not one-shot multiple", "${resource1.field}${resource2.field}", false, false},
		{"Not expression", "plain string", false, false},
		{"Empty string", "", false, false},
		{"Incomplete expression", "${incomplete", false, true},
		{"With map access", "${resource.map['key']}", true, false},
		{"With list index", "${resource.list[0]}", true, false},
		{"With escaped quotes", "${resource.field == \"escaped\\\"quote\"}", true, false},
		{"With newlines", "${resource.list.map(\n  x,\n  x * 2\n)}", true, false},
		{"Complex expression", "${resource.list.map(x, x.field).filter(y, y > 5)}", true, false},
		{"Nested expression (should error)", "${outer(${inner})}", false, true},
		{"Nested expression but with quotes", "${outer(\"${inner}\")}", true, false},
		{"Nested expression after a closing quote (should error)", "${\"a\"${b}}", false, true},
		{"Nested closing brace without opening one", "${\"text with }} inside\"}", true, false},
		{"Nested open brace without closing one", "${\"text with { inside\"}", true, false},
		{"Deferred expression is standalone", "$${resource.field}", true, false},
		{"Doubly deferred expression is standalone", "$$${resource.field}", true, false},
		{"Deferred inside evaluated is standalone", "${f($${x})}", true, false},
		{"Deferred with prefix is not standalone", "a-$${x}", false, false},
		{"Unterminated deferred", "$${x", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IsStandaloneExpression(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("isOneShotExpression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isOneShotExpression() = %v, want %v", got, tt.want)
			}
		})
	}
}
