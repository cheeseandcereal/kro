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
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// nodeIDTokenHashPrefix marks the hashed fallback form of an over-long path;
// node IDs are alphanumeric, so no plain token contains '-'.
const nodeIDTokenHashPrefix = "h-"

// nodeIDTokenSeparator joins the frames of a qualified path ("subA.res").
const nodeIDTokenSeparator = "."

// NodeIDToken returns the label-safe rendering of a node's qualified path
// ("subA/res" -> "subA.res"; a top-level node is its bare id) that is stamped
// on every child as the NodeIDLabel value. Paths longer than the 63-char label
// limit fall back to a stable "h-<40 hex>" hash; the readable path is always in
// the NodePathAnnotation. Every writer and reader of the label must use it.
func NodeIDToken(qualifiedPath string) string {
	dotted := strings.ReplaceAll(qualifiedPath, "/", nodeIDTokenSeparator)
	if len(dotted) <= validation.LabelValueMaxLength {
		return dotted
	}
	// The leading letter keeps the value a valid label and marks it as hashed.
	sum := sha256.Sum256([]byte(qualifiedPath))
	return nodeIDTokenHashPrefix + hex.EncodeToString(sum[:20])
}

// IsHashedNodeIDToken reports whether token is NodeIDToken's hashed fallback,
// which identifies one node but carries no nesting structure.
func IsHashedNodeIDToken(token string) bool {
	return strings.HasPrefix(token, nodeIDTokenHashPrefix)
}

// NodeIDTokenWithin reports whether token is ancestor's own token or that of a
// node nested beneath it ("subA.res" is within "subA", "subAx.res" is not).
func NodeIDTokenWithin(token, ancestor string) bool {
	return token == ancestor || strings.HasPrefix(token, ancestor+nodeIDTokenSeparator)
}
