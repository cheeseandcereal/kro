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
	"strings"
)

// FeatureGatesEnv selects the kro feature gates an integration suite enables
// before it starts the controller manager, in --feature-gates syntax
// ("Name=bool,Name=bool"). Unset keeps the suite's own gate set; empty or
// "none" enables nothing (a stock install's configuration); anything else is
// passed verbatim to features.FeatureGate.Set.
const FeatureGatesEnv = "KRO_INTEGRATION_FEATURE_GATES"

// featureGatesNone spells out an empty FeatureGatesEnv for shells where an
// empty exported variable is indistinguishable from an unset one.
const featureGatesNone = "none"

// FeatureGatesFromEnv resolves FeatureGatesEnv, returning fallback when the
// variable is unset and "" when nothing should be enabled.
func FeatureGatesFromEnv(fallback string) string {
	value, set := os.LookupEnv(FeatureGatesEnv)
	if !set {
		return fallback
	}
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, featureGatesNone) {
		return ""
	}
	return value
}
