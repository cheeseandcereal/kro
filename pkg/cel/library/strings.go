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
	"reflect"

	"github.com/gobuffalo/flect"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// Strings returns a CEL library that provides kro-specific string functions.
//
// The functions live in the `strings.` namespace alongside cel-go's
// ext.Strings() (which provides strings.quote and the <string>.method
// helpers). CEL has no real namespaces: both libraries simply register
// fully-qualified function names, and cel-go de-duplicates libraries by
// LibraryName(), so the two can coexist in one environment.
//
// Library functions:
//
// strings.plural(word: string) -> string
//
// Returns the English plural form of word, using the same inflection rules
// kro uses to derive CRD resource names from kinds. The input is not trimmed
// or otherwise normalised: only the final word is inflected, so separators
// are kept ('cluster-policy' -> 'cluster-policies') and whitespace-only or
// symbol-only input yields an empty string. Input that already looks plural
// is usually returned unchanged, but that detection is heuristic.
//
//	strings.plural('Pod')            // 'Pods'
//	strings.plural('ClusterPolicy')  // 'ClusterPolicies'
//	strings.plural('Ingress')        // 'Ingresses'
//	strings.plural('deployment')     // 'deployments'
//	strings.plural('Endpoints')      // 'Endpoints' (already plural)
//	strings.plural('')               // ''
//
// Case handling: lowercase and CamelCase words keep their case
// ('ClusterPolicy' -> 'ClusterPolicies'). All-caps words are not handled
// well ('POLICY' -> 'POLICYs') and irregular nouns come back in dictionary
// case ('PERSON' -> 'People'), so lowercase first when the case of the input
// is not under your control.
//
// To build a CRD name or plural resource name, lowercase the kind first and
// then pluralize. That is the exact operation kro performs when it names the
// CRD for a ResourceGraphDefinition, so the result always matches:
//
//	strings.plural(schema.spec.kind.lowerAscii()) + '.' + group
//
// Pluralizing before lowercasing gives a different result for kinds ending
// in an upper-case acronym or written in all caps: 'ServiceDNS' ->
// 'ServiceDNSes' -> 'servicednses' and 'POLICY' -> 'POLICYs' -> 'policys',
// whereas kro's CRD names are 'servicedns' and 'policies'.
func Strings() cel.EnvOption {
	return cel.Lib(&stringsLibrary{})
}

type stringsLibrary struct{}

// LibraryName implements the cel.SingletonLibrary interface method.
//
// cel-go de-duplicates singleton libraries by this name, so it must differ
// from every other registered library, including cel-go's ext.Strings()
// ("cel.lib.ext.strings") which shares the `strings.` function namespace.
// The "kro." prefix follows kro.lists, kro.omit and kro.runtime.
func (l *stringsLibrary) LibraryName() string {
	return "kro.strings"
}

func (l *stringsLibrary) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.Function("strings.plural",
			cel.Overload("strings.plural_string",
				[]*cel.Type{cel.StringType},
				cel.StringType,
				cel.UnaryBinding(stringsPlural),
			),
		),
	}
}

func (l *stringsLibrary) ProgramOptions() []cel.ProgramOption {
	return nil
}

// stringsPlural implements strings.plural(string) -> string.
func stringsPlural(value ref.Val) ref.Val {
	native, err := value.ConvertToNative(reflect.TypeFor[string]())
	if err != nil {
		return types.NewErr("strings.plural argument must be a string")
	}

	word, ok := native.(string)
	if !ok {
		return types.NewErr("strings.plural argument must be a string")
	}

	return types.String(flect.Pluralize(word))
}
