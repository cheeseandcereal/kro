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

package v1alpha1

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// serviceAccountNamePattern must stay in sync with the
// +kubebuilder:validation:Pattern marker on GraphSpec.ServiceAccountName in
// graph_types.go. The pattern is only enforced by the apiserver via the
// generated CRD OpenAPI schema, so this test guards the regex contract that
// the marker encodes: a Kubernetes ServiceAccount name is an RFC 1123
// subdomain (dots allowed), not a bare RFC 1123 label — or the empty string,
// which means the namespace default. TestGraphCRD_MarkersMatchConstants asserts
// the generated CRD carries this exact pattern.
const serviceAccountNamePattern = `^$|^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`

func TestGraphSpec_ServiceAccountNamePattern(t *testing.T) {
	re := regexp.MustCompile(serviceAccountNamePattern)

	tests := []struct {
		name    string
		value   string
		allowed bool
	}{
		{name: "simple label", value: "my-sa", allowed: true},
		{name: "single character", value: "a", allowed: true},
		{name: "numeric", value: "123", allowed: true},
		// Regression: RFC 1123 subdomains allow dots. These were wrongly
		// rejected by the old bare-label pattern.
		{name: "dotted subdomain", value: "my.service.account", allowed: true},
		{name: "dotted with hyphens", value: "my-app.team-a.example", allowed: true},
		// Helm/Kustomize render an unset value as ""; it means the namespace default.
		{name: "empty means namespace default", value: "", allowed: true},
		{name: "leading uppercase rejected", value: "Bad_Name", allowed: false},
		{name: "underscore rejected", value: "bad_name", allowed: false},
		{name: "leading dot rejected", value: ".invalid", allowed: false},
		{name: "trailing dot rejected", value: "invalid.", allowed: false},
		{name: "double dot rejected", value: "a..b", allowed: false},
		{name: "leading hyphen rejected", value: "-invalid", allowed: false},
		// The empty alternative must not loosen anything else.
		{name: "single space rejected", value: " ", allowed: false},
		{name: "lone dot rejected", value: ".", allowed: false},
		{name: "lone hyphen rejected", value: "-", allowed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := re.MatchString(tt.value)
			if got != tt.allowed {
				t.Errorf("pattern.MatchString(%q) = %v, want %v", tt.value, got, tt.allowed)
			}
		})
	}
}

// TestGraphCRD_MarkersMatchConstants pins the generated CRD
// (helm/crds/kro.run_graphs.yaml) to this package: the two MaxItems markers must
// equal GraphInventoryMaxItems (markers cannot reference constants) and the
// serviceAccountName pattern must be the one the regex test exercises. Run with
// `go test ./api/...`; `make test WHAT=unit` covers ./pkg/... only.
func TestGraphCRD_MarkersMatchConstants(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "helm", "crds", "kro.run_graphs.yaml"))
	if err != nil {
		t.Fatalf("read generated Graph CRD: %v", err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("unmarshal Graph CRD: %v", err)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Schema == nil || crd.Spec.Versions[0].Schema.OpenAPIV3Schema == nil {
		t.Fatalf("Graph CRD: want exactly one served version with an OpenAPI v3 schema, got %d versions", len(crd.Spec.Versions))
	}
	root := crd.Spec.Versions[0].Schema.OpenAPIV3Schema

	status, ok := root.Properties["status"]
	if !ok {
		t.Fatalf("Graph CRD schema has no status property")
	}
	for _, field := range []string{"managedResources", "contributions"} {
		prop, ok := status.Properties[field]
		if !ok {
			t.Errorf("status.%s: missing from the CRD schema", field)
			continue
		}
		if prop.MaxItems == nil {
			t.Errorf("status.%s: no maxItems in the CRD; the +kubebuilder:validation:MaxItems marker is missing", field)
			continue
		}
		if *prop.MaxItems != GraphInventoryMaxItems {
			t.Errorf("status.%s: CRD maxItems=%d but GraphInventoryMaxItems=%d; the marker and the constant must agree",
				field, *prop.MaxItems, GraphInventoryMaxItems)
		}
	}

	spec, ok := root.Properties["spec"]
	if !ok {
		t.Fatalf("Graph CRD schema has no spec property")
	}
	sa, ok := spec.Properties["serviceAccountName"]
	if !ok {
		t.Fatalf("spec.serviceAccountName: missing from the CRD schema")
	}
	if sa.Pattern != serviceAccountNamePattern {
		t.Errorf("spec.serviceAccountName: CRD pattern %q differs from the pattern this test exercises %q",
			sa.Pattern, serviceAccountNamePattern)
	}
}
