// Copyright 2025 The Kube Resource Orchestrator Authors
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

package metadata

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/validation"
)

// TestNodeIDToken pins the kro.run/node-id label-value format: bare id at the
// root, '.'-joined frames when nested, stable hash for an over-long path.
func TestNodeIDToken(t *testing.T) {
	longSeg := strings.Repeat("Xy", 34) // 68 chars > 63 on its own

	cases := []struct {
		name string
		path string
		want string
	}{
		{name: "top-level node is the bare id", path: "res", want: "res"},
		{name: "one frame joins with a dot", path: "subA/res", want: "subA.res"},
		{name: "nested frames join with dots", path: "subA/subB/res", want: "subA.subB.res"},
		{name: "empty path stays empty", path: "", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NodeIDToken(tc.path))
		})
	}

	t.Run("over-long path hashes to a valid, stable, marked label value", func(t *testing.T) {
		got := NodeIDToken("outer/" + longSeg)
		assert.Empty(t, validation.IsValidLabelValue(got), "hashed token %q must be a valid label value", got)
		assert.LessOrEqual(t, len(got), validation.LabelValueMaxLength)
		assert.True(t, IsHashedNodeIDToken(got), "an over-long path must produce the hashed form, got %q", got)
		assert.Equal(t, got, NodeIDToken("outer/"+longSeg), "the hash must be deterministic for a given path")
		assert.NotEqual(t, got, NodeIDToken("other/"+longSeg), "different paths must hash differently")
	})

	t.Run("a path exactly at the limit is not hashed", func(t *testing.T) {
		exact := strings.Repeat("a", validation.LabelValueMaxLength)
		require.Len(t, exact, validation.LabelValueMaxLength)
		assert.Equal(t, exact, NodeIDToken(exact))
		assert.False(t, IsHashedNodeIDToken(exact))
	})

	t.Run("plain tokens are never mistaken for hashed ones", func(t *testing.T) {
		// Node ids are alphanumeric, so no plain token can contain "-".
		assert.False(t, IsHashedNodeIDToken("h"))
		assert.False(t, IsHashedNodeIDToken("hx.res"))
		assert.False(t, IsHashedNodeIDToken(""))
	})
}

// TestNodeIDTokenWithin pins the subtree test used to attribute a child object
// to the subgraph node that (transitively) owns it.
func TestNodeIDTokenWithin(t *testing.T) {
	cases := []struct {
		name     string
		token    string
		ancestor string
		want     bool
	}{
		{name: "same node", token: "sub", ancestor: "sub", want: true},
		{name: "direct child", token: "sub.res", ancestor: "sub", want: true},
		{name: "nested descendant", token: "sub.inner.res", ancestor: "sub", want: true},
		{name: "descendant of a nested ancestor", token: "sub.inner.res", ancestor: "sub.inner", want: true},
		{name: "sibling sharing a prefix is not within", token: "subx.res", ancestor: "sub", want: false},
		{name: "unrelated node", token: "other", ancestor: "sub", want: false},
		{name: "ancestor is not within its own child", token: "sub", ancestor: "sub.res", want: false},
		{name: "hashed token matches only by equality", token: "h-abc", ancestor: "h-abc", want: true},
		{name: "hashed token never matches by prefix", token: "h-abc", ancestor: "sub", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, NodeIDTokenWithin(tc.token, tc.ancestor))
		})
	}
}
