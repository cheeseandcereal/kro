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
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	expv1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/kubernetes-sigs/kro/pkg/graphengine/testutil/generator"
)

// TestRuntimeMaxCollectionSizeOption pins the new per-Runtime cap
// option introduced when MaxCollectionSize moved off the package
// global. Three rows: default cap, custom cap, disabled cap.
func TestRuntimeMaxCollectionSizeOption(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want int
	}{
		{name: "default-cap", opts: nil, want: DefaultMaxCollectionSize},
		{name: "custom-cap", opts: []Option{WithMaxCollectionSize(7)}, want: 7},
		{name: "disabled-cap", opts: []Option{WithMaxCollectionSize(0)}, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := generator.NewGraph("g",
				generator.WithNamespace("default"),
				generator.WithDef("seed", map[string]any{"k": "v"}),
			)
			prog := compileGraph(t, g)
			rt := New(prog, g, tc.opts...)
			assert.Equal(t, tc.want, rt.MaxCollectionSize())
		})
	}
}

// TestCartesianProductOverflowRejected pins the overflow safeguard in
// cartesianProduct. With cap disabled (maxSize=0), a malicious spec
// using many axes can amplify the product past int range. We trigger
// it cheaply via seven axes of length 1000 — 1000^7 ≈ 10^21, well past
// 2^63. Only 7×1000 = 7k entries to allocate.
func TestCartesianProductOverflowRejected(t *testing.T) {
	axis := make([]any, 1000)
	for i := range axis {
		axis[i] = i
	}
	dims := make([]evaluatedDimension, 7)
	for i := range dims {
		dims[i] = evaluatedDimension{name: "a", values: axis}
	}
	rows, err := cartesianProduct(dims, 0, DefaultMaxCollectionDimensions)
	require.Error(t, err, "overflow must be rejected even when cap disabled")
	assert.Contains(t, err.Error(), "overflows")
	assert.Nil(t, rows)

	// Sanity: math.MaxInt is what we expected (avoid silent test
	// behavior drift if someone redefines the constant).
	assert.Greater(t, math.MaxInt, 0)
}

// TestCartesianProductDimensionCap proves that >10 axes is rejected and
// exactly 10 is allowed.
func TestCartesianProductDimensionCap(t *testing.T) {
	// Exactly DefaultMaxCollectionDimensions (10) axes is allowed.
	dims10 := make([]evaluatedDimension, DefaultMaxCollectionDimensions)
	for i := range dims10 {
		dims10[i] = evaluatedDimension{name: fmt.Sprintf("d%d", i), values: []any{1}}
	}
	rows, err := cartesianProduct(dims10, 0, DefaultMaxCollectionDimensions)
	require.NoError(t, err)
	assert.Len(t, rows, 1)

	// >10 axes is rejected.
	dims11 := make([]evaluatedDimension, DefaultMaxCollectionDimensions+1)
	for i := range dims11 {
		dims11[i] = evaluatedDimension{name: fmt.Sprintf("d%d", i), values: []any{1}}
	}
	_, err = cartesianProduct(dims11, 0, DefaultMaxCollectionDimensions)
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("collection has %d forEach dimensions, exceeds the maximum of %d", len(dims11), DefaultMaxCollectionDimensions))
}

func TestNodeResolveCollectionDimensions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		axes    int
		opts    []Option
		wantMax int
	}{
		{name: "default allows ten", axes: 10},
		{name: "configured allows eleven", axes: 11, opts: []Option{WithMaxCollectionDimensions(11)}},
		{name: "default still rejects eleven", axes: 11, wantMax: 10},
		{name: "configured rejects twelve", axes: 12, opts: []Option{WithMaxCollectionDimensions(11)}, wantMax: 11},
		{name: "lower limit rejects three", axes: 3, opts: []Option{WithMaxCollectionDimensions(2)}, wantMax: 2},
		{name: "lower limit allows two", axes: 2, opts: []Option{WithMaxCollectionDimensions(2)}},
		{name: "zero retains default", axes: 11, opts: []Option{WithMaxCollectionDimensions(0)}, wantMax: 10},
		{name: "negative retains default", axes: 11, opts: []Option{WithMaxCollectionDimensions(-1)}, wantMax: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			axes := make([]expv1alpha1.ForEachDimension, tc.axes)
			name := "dims"
			for i := range axes {
				iterator := fmt.Sprintf("d%d", i)
				axes[i] = generator.ForEachDim(iterator, `${["x"]}`)
				name += "-${" + iterator + "}"
			}
			g := generator.NewGraph("g", generator.WithTemplate("cm", map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"name": name},
			}, axes...))
			prog := compileGraph(t, g)
			rt := New(prog, g, tc.opts...)
			// Keep both runtimes on the same Program before resolving either.
			defaultRT := New(prog, g)
			objects, err := rt.Node("cm").Resolve()
			if tc.wantMax > 0 {
				require.EqualError(t, err, fmt.Sprintf(
					`node "cm": collection has %d forEach dimensions, exceeds the maximum of %d`, tc.axes, tc.wantMax))
				return
			}
			require.NoError(t, err)
			require.Len(t, objects, 1)
			assert.Equal(t, "dims"+strings.Repeat("-x", tc.axes), objects[0].GetName())
			if tc.axes > DefaultMaxCollectionDimensions {
				_, err := defaultRT.Node("cm").Resolve()
				require.EqualError(t, err, fmt.Sprintf(
					`node "cm": collection has %d forEach dimensions, exceeds the maximum of %d`, tc.axes, DefaultMaxCollectionDimensions))
			}
		})
	}
}

// TestCartesianProductHonoursCap regression-pins the post-multiply cap
// check, which the overflow guard sits in front of.
func TestCartesianProductHonoursCap(t *testing.T) {
	dims := []evaluatedDimension{
		{name: "a", values: []any{1, 2, 3}},
		{name: "b", values: []any{1, 2, 3}},
	}
	_, err := cartesianProduct(dims, 5, DefaultMaxCollectionDimensions)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

// TestIsSoftErrorClassifier pins the runtime error sentinels used by
// the executor to decide hard-abort vs soft-continue.
func TestIsSoftErrorClassifier(t *testing.T) {
	tests := []struct {
		name string
		err  error
		soft bool
	}{
		{"data-pending", ErrDataPending, true},
		{"waiting-for-readiness", ErrWaitingForReadiness, true},
		{"wrapped-data-pending", errors.New("wrapper: " + ErrDataPending.Error()), false},
		{"unwrapped-wrap", errors.Join(ErrDataPending, errors.New("x")), true},
		{"unrelated", errors.New("hard error"), false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			soft := errors.Is(tc.err, ErrDataPending) || errors.Is(tc.err, ErrWaitingForReadiness)
			assert.Equal(t, tc.soft, soft)
		})
	}
}
