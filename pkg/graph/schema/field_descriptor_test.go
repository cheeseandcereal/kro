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

package schema

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func TestGenerateSchemaFromCELTypes_NestedFields(t *testing.T) {
	result, err := GenerateSchemaFromCELTypes(map[string]*cel.Type{
		"status.parent.child": cel.StringType,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	status, ok := result.Properties["status"]
	require.True(t, ok)
	assert.Equal(t, "object", status.Type)

	parent, ok := status.Properties["parent"]
	require.True(t, ok)
	assert.Equal(t, "object", parent.Type)

	child, ok := parent.Properties["child"]
	require.True(t, ok)
	assert.Equal(t, "string", child.Type)
}

func TestGenerateSchemaFromCELTypes_NestedArrayFields(t *testing.T) {
	result, err := GenerateSchemaFromCELTypes(map[string]*cel.Type{
		"status.parents[0].child": cel.StringType,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	status, ok := result.Properties["status"]
	require.True(t, ok)

	parents, ok := status.Properties["parents"]
	require.True(t, ok)
	require.Equal(t, "array", parents.Type)
	require.NotNil(t, parents.Items)
	require.NotNil(t, parents.Items.Schema)
	assert.Equal(t, "object", parents.Items.Schema.Type)

	child, ok := parents.Items.Schema.Properties["child"]
	require.True(t, ok)
	assert.Equal(t, "string", child.Type)
}

// A trailing index makes the leaf the array's items schema, not a property.
func TestGenerateSchemaFromCELTypes_ArrayLeaf(t *testing.T) {
	result, err := GenerateSchemaFromCELTypes(map[string]*cel.Type{
		"status.names[0]": cel.StringType,
	}, nil)
	require.NoError(t, err)

	names, ok := result.Properties["status"].Properties["names"]
	require.True(t, ok)
	require.Equal(t, "array", names.Type)
	require.NotNil(t, names.Items)
	require.NotNil(t, names.Items.Schema)
	assert.Equal(t, &extv1.JSONSchemaProps{Type: "string"}, names.Items.Schema)
	assert.Empty(t, names.Properties, "an array leaf must not also grow object properties")
}

func TestGenerateSchemaFromCELTypes_ComplexPath(t *testing.T) {
	result, err := GenerateSchemaFromCELTypes(map[string]*cel.Type{
		"status.parents[0].children[0].metadata.labels[0].key": cel.StringType,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	status, ok := result.Properties["status"]
	require.True(t, ok)
	assert.Equal(t, "object", status.Type)

	parents, ok := status.Properties["parents"]
	require.True(t, ok)
	require.Equal(t, "array", parents.Type)
	require.NotNil(t, parents.Items)
	require.NotNil(t, parents.Items.Schema)

	children, ok := parents.Items.Schema.Properties["children"]
	require.True(t, ok)
	require.Equal(t, "array", children.Type)
	require.NotNil(t, children.Items)
	require.NotNil(t, children.Items.Schema)

	metadata, ok := children.Items.Schema.Properties["metadata"]
	require.True(t, ok)
	assert.Equal(t, "object", metadata.Type)

	labels, ok := metadata.Properties["labels"]
	require.True(t, ok)
	require.Equal(t, "array", labels.Type)
	require.NotNil(t, labels.Items)
	require.NotNil(t, labels.Items.Schema)

	key, ok := labels.Items.Schema.Properties["key"]
	require.True(t, ok)
	assert.Equal(t, "string", key.Type)
}

// Sibling fields under the same array index share one items schema.
func TestGenerateSchemaFromCELTypes_SiblingArrayFieldsMerge(t *testing.T) {
	result, err := GenerateSchemaFromCELTypes(map[string]*cel.Type{
		"status.parents[0].name":     cel.StringType,
		"status.parents[0].ready":    cel.BoolType,
		"status.parents[0].replicas": cel.IntType,
	}, nil)
	require.NoError(t, err)

	parents, ok := result.Properties["status"].Properties["parents"]
	require.True(t, ok)
	require.Equal(t, "array", parents.Type)
	require.NotNil(t, parents.Items)
	require.NotNil(t, parents.Items.Schema)

	item := parents.Items.Schema
	assert.Equal(t, "object", item.Type)
	assert.Equal(t, map[string]extv1.JSONSchemaProps{
		"name":     {Type: "string"},
		"ready":    {Type: "boolean"},
		"replicas": {Type: "integer"},
	}, item.Properties)
}
