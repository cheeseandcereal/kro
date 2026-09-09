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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIsConflictErr(t *testing.T) {
	graphs := schema.GroupResource{Group: "kro.run", Resource: "graphs"}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil is not a conflict",
		},
		{
			name: "optimistic concurrency 409 from Update",
			err:  apierrors.NewConflict(graphs, "g", errors.New("the object has been modified; please apply your changes to the latest version and try again")),
			want: true,
		},
		{
			name: "server-side apply field-manager 409",
			err: apierrors.NewApplyConflict([]metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldManagerConflict,
				Message: "conflict with \"other\"",
				Field:   ".spec.nodes",
			}}, "Apply failed with 1 conflict: conflict with \"other\": .spec.nodes"),
			want: true,
		},
		{
			name: "a non-409 API error is not retried",
			err:  apierrors.NewNotFound(graphs, "g"),
		},
		{
			name: "a plain error carrying the 409 message text is not an API conflict",
			err:  errors.New("the object has been modified"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isConflictErr(tt.err))
		})
	}
}
