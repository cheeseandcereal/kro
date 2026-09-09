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

package environment

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFeatureGatesFromEnv(t *testing.T) {
	const fallback = "CELOmitFunction=true,GraphKind=true"

	tests := []struct {
		name  string
		unset bool
		value string
		want  string
	}{
		{
			name:  "unset keeps the suite's own gate set",
			unset: true,
			want:  fallback,
		},
		{
			name: "exported empty enables nothing",
			want: "",
		},
		{
			name:  "whitespace-only is treated as empty",
			value: "  ",
			want:  "",
		},
		{
			name:  "the literal none enables nothing",
			value: "none",
			want:  "",
		},
		{
			name:  "none is matched case-insensitively",
			value: "None",
			want:  "",
		},
		{
			name:  "an explicit spec is passed through verbatim",
			value: "GraphKind=true",
			want:  "GraphKind=true",
		},
		{
			name:  "surrounding whitespace is trimmed from an explicit spec",
			value: " CELOmitFunction=false,GraphKind=true ",
			want:  "CELOmitFunction=false,GraphKind=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// t.Setenv registers the restore; Unsetenv then makes the variable absent.
			t.Setenv(FeatureGatesEnv, tt.value)
			if tt.unset {
				require.NoError(t, os.Unsetenv(FeatureGatesEnv))
			}
			assert.Equal(t, tt.want, FeatureGatesFromEnv(fallback))
		})
	}
}
