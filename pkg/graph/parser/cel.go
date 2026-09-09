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

// Package parser extracts and validates CEL expressions embedded in resource
// templates.
package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// In kro, CEL expressions are enclosed between "${" and "}"
	exprStart = "${"
	exprEnd   = "}"
)

// ErrNestedExpression is returned for a bare "${" inside an expression:
// ${outer(${inner})} is rejected. To embed the literal text of an expression
// for a downstream evaluator, defer it with an extra dollar sign —
// ${outer($${inner})} — or place it inside a CEL string literal,
// ${outer("${inner}")}.
var ErrNestedExpression = errors.New("nested expressions are not allowed unless deferred with $${...} or inside string literals")

// ErrUnterminatedExpression is returned when a "${" has no matching closing
// "}" before the end of the input. Such input was previously swallowed and
// treated as literal string data, silently discarding the author's intent.
var ErrUnterminatedExpression = errors.New("unterminated expression")

// exprMatch holds a parsed CEL expression and its position in the original string.
type exprMatch struct {
	expr  string // Expression content (without ${})
	start int    // Position of the first '$' (of "${", or of a "$${" run)
	end   int    // Position after closing }
	// rewritten is true when expr is not the verbatim source between the
	// delimiters — a deferred "$${...}" span was substituted with a CEL
	// string literal. Callers use it to keep the author's text for messages.
	rewritten bool
}

// extractExpressions extracts all CEL expressions from a string, returning
// each expression along with its start/end position.
//
// An expression is delimited by "${" and its matching "}"; braces and "${"
// inside a CEL string literal ('...' or "...") are ordinary text, and a bare
// nested "${" is rejected (ErrNestedExpression). A delimiter prefixed with
// extra dollar signs — "$${...}", "$$${...}", ... — is a deferred expression:
// its body is not parsed here but emitted as a CEL string literal holding the
// same text with one dollar sign removed, so each evaluation level peels
// exactly one layer ("$${cfg.team}" yields the string "${cfg.team}").
func extractExpressions(str string) ([]exprMatch, error) {
	var matches []exprMatch

	start := 0
	// Iterate over the string and find all expressions
	for start < len(str) {
		// Find the start of the next expression. If none is found, break
		startIdx := strings.Index(str[start:], exprStart)
		if startIdx == -1 {
			break
		}
		// Adjust the start index to the actual position in the string
		startIdx += start

		// Count the dollar signs immediately preceding "${" within the
		// current segment. Two or more make this a deferred expression whose
		// body is emitted as a string literal.
		runStart := startIdx
		for runStart > start && str[runStart-1] == '$' {
			runStart--
		}
		dollars := startIdx - runStart + 1

		if dollars >= 2 {
			literal, closeIdx, err := deferredSpan(str, runStart, dollars)
			if err != nil {
				return nil, err
			}
			matches = append(matches, exprMatch{
				expr:      literal,
				start:     runStart,
				end:       closeIdx + 1,
				rewritten: true,
			})
			start = closeIdx + 1
			continue
		}

		expr, closeIdx, rewritten, err := scanExpression(str, startIdx+len(exprStart))
		if err != nil {
			return nil, err
		}
		matches = append(matches, exprMatch{
			expr:      expr,
			start:     startIdx,
			end:       closeIdx + 1,
			rewritten: rewritten,
		})
		start = closeIdx + 1
	}
	return matches, nil
}

// literalState tracks whether the scanner is inside a CEL string literal.
// CEL allows both single- and double-quoted literals; a quote of the other
// kind inside a literal is ordinary text, so a literal only closes on the
// same quote character that opened it. Backslash escapes the next byte.
type literalState struct {
	inLiteral bool
	quote     byte
	escape    bool
}

// step consumes byte c and reports whether it was absorbed by string-literal
// handling (so the caller must not interpret it as structure).
func (l *literalState) step(c byte) bool {
	if l.escape {
		l.escape = false
		return true
	}
	if l.inLiteral {
		switch c {
		case '\\':
			l.escape = true
		case l.quote:
			l.inLiteral = false
			l.quote = 0
		}
		return true
	}
	if c == '"' || c == '\'' {
		l.inLiteral = true
		l.quote = c
		return true
	}
	return false
}

// scanExpression scans one CEL expression whose body starts at bodyStart. It
// returns the expression text, the index of its closing "}", and whether the
// text differs from the source because deferred "$${...}" spans inside the
// expression were replaced with CEL string literals. A bare nested "${" is
// rejected.
func scanExpression(str string, bodyStart int) (expr string, closeIdx int, rewritten bool, err error) {
	var (
		lit      literalState
		sb       strings.Builder
		segStart = bodyStart // start of the source segment not yet copied to sb
		depth    = 1
	)
	for i := bodyStart; i < len(str); {
		c := str[i]
		if lit.step(c) {
			i++
			continue
		}
		if c == '$' {
			if run := dollarRun(str, i); i+run < len(str) && str[i+run] == '{' {
				if run == 1 {
					return "", 0, false, ErrNestedExpression
				}
				literal, spanEnd, err := deferredSpan(str, i, run)
				if err != nil {
					return "", 0, false, err
				}
				sb.WriteString(str[segStart:i])
				sb.WriteString(literal)
				i = spanEnd + 1
				segStart = i
				continue
			}
			// A stray '$' (not followed by '{') is left for CEL to report.
		}
		switch c {
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				if segStart == bodyStart {
					return str[bodyStart:i], i, false, nil
				}
				sb.WriteString(str[segStart:i])
				return sb.String(), i, true, nil
			}
		}
		i++
	}
	return "", 0, false, unterminated(bodyStart - len(exprStart))
}

// deferredSpan handles a deferred expression: a run of dollars (>= 2) '$'
// bytes at offset open, followed by "{". It locates the span's closing brace
// and renders the body as a CEL string literal with one dollar sign removed.
func deferredSpan(str string, open, dollars int) (literal string, closeIdx int, err error) {
	bodyStart := open + dollars + 1 // past the dollars and the "{"
	closeIdx, err = scanOpaque(str, bodyStart)
	if err != nil {
		return "", 0, unterminated(open)
	}
	return deferredLiteral(dollars, str[bodyStart:closeIdx]), closeIdx, nil
}

// scanOpaque finds the "}" matching an opening "${" whose body starts at
// bodyStart, honoring string literals and escapes but attaching no meaning to
// anything else. It returns the index of that closing brace. Used for
// deferred spans, whose text belongs to a downstream evaluator and is not
// validated here.
func scanOpaque(str string, bodyStart int) (int, error) {
	var lit literalState
	depth := 1
	for i := bodyStart; i < len(str); i++ {
		c := str[i]
		if lit.step(c) {
			continue
		}
		switch c {
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				return i, nil
			}
		}
	}
	return 0, ErrUnterminatedExpression
}

// dollarRun returns the number of consecutive '$' bytes starting at offset i.
func dollarRun(str string, i int) int {
	n := 0
	for i+n < len(str) && str[i+n] == '$' {
		n++
	}
	return n
}

// unterminated reports an expression opened at offset open that never closes.
func unterminated(open int) error {
	return fmt.Errorf("%w starting at offset %d", ErrUnterminatedExpression, open)
}

// deferredLiteral renders the text a deferred span hands to the next
// evaluator — the same span with one dollar sign removed — as a CEL string
// literal. strconv.Quote's escapes (\" \\ \n \uHHHH ...) are all valid CEL
// escape sequences, and for valid UTF-8 input (which every Kubernetes string
// field is) the literal evaluates to exactly that text.
func deferredLiteral(dollars int, body string) string {
	return strconv.Quote(strings.Repeat("$", dollars-2) + exprStart + body + exprEnd)
}

// isStandalone reports whether matches is exactly one expression (or deferred
// span) covering all of str.
func isStandalone(str string, matches []exprMatch) bool {
	return len(matches) == 1 && matches[0].start == 0 && matches[0].end == len(str)
}

// IsStandaloneExpression returns true if the string is a single, complete non-nested expression.
// It returns an error if it encounters a nested expression.
func IsStandaloneExpression(str string) (bool, error) {
	m, err := standaloneMatch(str)
	return m != nil, err
}

// standaloneMatch returns the single match when str is exactly one complete
// expression (or deferred span) and nothing else, or nil otherwise.
func standaloneMatch(str string) (*exprMatch, error) {
	matches, err := extractExpressions(str)
	if err != nil {
		return nil, err
	}
	if isStandalone(str, matches) {
		return &matches[0], nil
	}
	return nil, nil
}
