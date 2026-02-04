// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

package component

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryRepoData_GetBranches(t *testing.T) {
	data := &BinaryRepoData{
		Branches: map[string]BinaryDetail{
			"main": {Path: "/path/to/main", BuildTime: "2023-01-01", Size: "10MB"},
			"dev":  {Path: "/path/to/dev", BuildTime: "2023-01-02", Size: "8MB"},
		},
	}

	branches := data.GetBranches()
	assert.Equal(t, data.Branches, branches)

	// Verify it's the same map (not a copy)
	branches["new"] = BinaryDetail{Path: "/new/path"}
	assert.Contains(t, data.Branches, "new")
}

func TestBinaryRepoData_GetTags(t *testing.T) {
	data := &BinaryRepoData{
		Tags: map[string]BinaryDetail{
			"v1.0.0": {Path: "/path/to/v1.0.0", BuildTime: "2023-01-01", Size: "10MB"},
			"v1.1.0": {Path: "/path/to/v1.1.0", BuildTime: "2023-01-02", Size: "12MB"},
		},
	}

	tags := data.GetTags()
	assert.Equal(t, data.Tags, tags)

	// Verify it's the same map (not a copy)
	tags["v2.0.0"] = BinaryDetail{Path: "/path/to/v2.0.0"}
	assert.Contains(t, data.Tags, "v2.0.0")
}

func TestBinaryRepoData_GetCommits(t *testing.T) {
	data := &BinaryRepoData{
		Commits: map[string]BinaryDetail{
			"abc123": {Path: "/path/to/abc123", BuildTime: "2023-01-01", Size: "10MB", Commit: "abc123"},
			"def456": {Path: "/path/to/def456", BuildTime: "2023-01-02", Size: "8MB", Commit: "def456"},
		},
	}

	commits := data.GetCommits()
	assert.Equal(t, data.Commits, commits)

	// Verify it's the same map (not a copy)
	commits["ghi789"] = BinaryDetail{Path: "/path/to/ghi789", Commit: "ghi789"}
	assert.Contains(t, data.Commits, "ghi789")
}

func TestBinaryRepoData_GetLatest(t *testing.T) {
	tests := []struct {
		name          string
		tags          map[string]BinaryDetail
		expectedTag   string
		expectedFound bool
	}{
		{
			name: "multiple versions, highest returned",
			tags: map[string]BinaryDetail{
				"v1.0.0": {Path: "/path/to/v1.0.0"},
				"v2.0.0": {Path: "/path/to/v2.0.0"},
				"v1.5.0": {Path: "/path/to/v1.5.0"},
			},
			expectedTag:   "v2.0.0",
			expectedFound: true,
		},
		{
			name: "semantic versioning with pre-release",
			tags: map[string]BinaryDetail{
				"v1.0.0-alpha": {Path: "/path/to/v1.0.0-alpha"},
				"v1.0.0":       {Path: "/path/to/v1.0.0"},
				"v1.0.0-beta":  {Path: "/path/to/v1.0.0-beta"},
			},
			expectedTag:   "v1.0.0-beta", // String comparison, v1.0.0-beta > v1.0.0-alpha > v1.0.0
			expectedFound: true,
		},
		{
			name: "single version",
			tags: map[string]BinaryDetail{
				"v1.0.0": {Path: "/path/to/v1.0.0"},
			},
			expectedTag:   "v1.0.0",
			expectedFound: true,
		},
		{
			name:          "empty tags",
			tags:          map[string]BinaryDetail{},
			expectedTag:   "",
			expectedFound: false,
		},
		{
			name:          "nil tags",
			tags:          nil,
			expectedTag:   "",
			expectedFound: false,
		},
		{
			name: "version comparison with different formats",
			tags: map[string]BinaryDetail{
				"v0.9.9":  {Path: "/path/to/v0.9.9"},
				"v1.0.0":  {Path: "/path/to/v1.0.0"},
				"v10.0.0": {Path: "/path/to/v10.0.0"}, // v10.0.0 > v1.0.0 in string comparison
			},
			expectedTag:   "v10.0.0",
			expectedFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := &BinaryRepoData{
				Tags: tt.tags,
			}

			tag, detail, found := data.GetLatest()

			assert.Equal(t, tt.expectedFound, found)
			assert.Equal(t, tt.expectedTag, tag)

			if tt.expectedFound {
				require.NotNil(t, detail)
				assert.Equal(t, tt.tags[tt.expectedTag], *detail)
			} else {
				assert.Nil(t, detail)
			}
		})
	}
}

func TestBinaryRepoData_GetMain(t *testing.T) {
	tests := []struct {
		name           string
		branches       map[string]BinaryDetail
		expectedFound  bool
		expectedDetail *BinaryDetail
	}{
		{
			name: "main branch exists",
			branches: map[string]BinaryDetail{
				"main": {Path: "/path/to/main", BuildTime: "2023-01-01", Size: "10MB"},
				"dev":  {Path: "/path/to/dev", BuildTime: "2023-01-02", Size: "8MB"},
			},
			expectedFound: true,
			expectedDetail: &BinaryDetail{
				Path:      "/path/to/main",
				BuildTime: "2023-01-01",
				Size:      "10MB",
			},
		},
		{
			name: "main branch does not exist",
			branches: map[string]BinaryDetail{
				"dev":  {Path: "/path/to/dev", BuildTime: "2023-01-02", Size: "8MB"},
				"test": {Path: "/path/to/test", BuildTime: "2023-01-03", Size: "6MB"},
			},
			expectedFound:  false,
			expectedDetail: nil,
		},
		{
			name:           "empty branches",
			branches:       map[string]BinaryDetail{},
			expectedFound:  false,
			expectedDetail: nil,
		},
		{
			name:           "nil branches",
			branches:       nil,
			expectedFound:  false,
			expectedDetail: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := &BinaryRepoData{
				Branches: tt.branches,
			}

			detail, found := data.GetMain()

			assert.Equal(t, tt.expectedFound, found)
			if tt.expectedFound {
				require.NotNil(t, detail)
				assert.Equal(t, tt.expectedDetail, detail)
			} else {
				assert.Nil(t, detail)
			}
		})
	}
}

func TestBinaryRepoData_FindVersion(t *testing.T) {
	tags := map[string]BinaryDetail{
		"v1.0.0": {Path: "/path/to/v1.0.0", BuildTime: "2023-01-01", Size: "10MB"},
		"v1.1.0": {Path: "/path/to/v1.1.0", BuildTime: "2023-01-02", Size: "12MB"},
		"v2.0.0": {Path: "/path/to/v2.0.0", BuildTime: "2023-01-03", Size: "15MB"},
	}

	data := &BinaryRepoData{
		Tags: tags,
	}

	tests := []struct {
		name           string
		version        string
		expectedFound  bool
		expectedDetail *BinaryDetail
	}{
		{
			name:          "existing version",
			version:       "v1.1.0",
			expectedFound: true,
			expectedDetail: &BinaryDetail{
				Path:      "/path/to/v1.1.0",
				BuildTime: "2023-01-02",
				Size:      "12MB",
			},
		},
		{
			name:           "non-existing version",
			version:        "v3.0.0",
			expectedFound:  false,
			expectedDetail: nil,
		},
		{
			name:           "empty version",
			version:        "",
			expectedFound:  false,
			expectedDetail: nil,
		},
		{
			name:           "case sensitive check",
			version:        "V1.0.0",
			expectedFound:  false,
			expectedDetail: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail, found := data.FindVersion(tt.version)

			assert.Equal(t, tt.expectedFound, found)
			if tt.expectedFound {
				require.NotNil(t, detail)
				assert.Equal(t, tt.expectedDetail, detail)
			} else {
				assert.Nil(t, detail)
			}
		})
	}
}

func TestBinaryRepoData_GetName(t *testing.T) {
	tests := []struct {
		name     string
		binary   string
		expected string
	}{
		{
			name:     "normal binary name",
			binary:   "dingo-mds",
			expected: "dingo-mds",
		},
		{
			name:     "empty binary name",
			binary:   "",
			expected: "",
		},
		{
			name:     "binary name with special characters",
			binary:   "dingo-client-v2",
			expected: "dingo-client-v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := &BinaryRepoData{
				Binary: tt.binary,
			}

			assert.Equal(t, tt.expected, data.GetName())
		})
	}
}

func TestNewBinaryRepoData(t *testing.T) {
	// Test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dingo-mds.version":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"binary": "dingo-mds",
				"generated_at": "2023-01-01T00:00:00Z",
				"branches": {
					"main": {
						"path": "/path/to/main",
						"build_time": "2023-01-01T00:00:00Z",
						"size": "10MB"
					}
				},
				"tags": {
					"v1.0.0": {
						"path": "/path/to/v1.0.0",
						"build_time": "2023-01-01T00:00:00Z",
						"size": "8MB"
					}
				},
				"commits": {}
			}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tests := []struct {
		name      string
		baseURL   string
		component string
		expectErr bool
	}{
		{
			name:      "successful fetch",
			baseURL:   server.URL,
			component: "dingo-mds",
			expectErr: false,
		},
		{
			name:      "nonexistent component",
			baseURL:   server.URL,
			component: "nonexistent",
			expectErr: true,
		},
		{
			name:      "invalid base URL",
			baseURL:   "invalid-url",
			component: "dingo-mds",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := NewBinaryRepoData(tt.baseURL, tt.component)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, data)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, data)
				assert.Equal(t, tt.component, data.Binary)
				assert.NotEmpty(t, data.GeneratedAt)
			}
		})
	}
}

func TestBinaryRepoData_Integration(t *testing.T) {
	// Create a complete BinaryRepoData with all fields
	data := &BinaryRepoData{
		Binary:      "test-component",
		GeneratedAt: "2023-01-01T00:00:00Z",
		Branches: map[string]BinaryDetail{
			"main": {
				Path:      "/branches/main",
				BuildTime: "2023-01-01T00:00:00Z",
				Size:      "10MB",
			},
		},
		Tags: map[string]BinaryDetail{
			"v1.0.0": {
				Path:      "/tags/v1.0.0",
				BuildTime: "2023-01-01T00:00:00Z",
				Size:      "8MB",
			},
			"v2.0.0": {
				Path:      "/tags/v2.0.0",
				BuildTime: "2023-01-02T00:00:00Z",
				Size:      "12MB",
			},
		},
		Commits: map[string]BinaryDetail{
			"abc123": {
				Path:      "/commits/abc123",
				BuildTime: "2023-01-01T00:00:00Z",
				Size:      "9MB",
				Commit:    "abc123",
			},
		},
	}

	// Test all methods work together correctly
	assert.Equal(t, "test-component", data.GetName())

	// Test GetLatest
	latest, detail, found := data.GetLatest()
	assert.True(t, found)
	assert.Equal(t, "v2.0.0", latest)
	assert.Equal(t, "/tags/v2.0.0", detail.Path)

	// Test GetMain
	mainDetail, mainFound := data.GetMain()
	assert.True(t, mainFound)
	assert.Equal(t, "/branches/main", mainDetail.Path)

	// Test FindVersion
	versionDetail, versionFound := data.FindVersion("v1.0.0")
	assert.True(t, versionFound)
	assert.Equal(t, "/tags/v1.0.0", versionDetail.Path)

	// Test getters return the maps
	assert.Len(t, data.GetBranches(), 1)
	assert.Len(t, data.GetTags(), 2)
	assert.Len(t, data.GetCommits(), 1)
}

// Benchmark tests
func BenchmarkBinaryRepoData_GetLatest(b *testing.B) {
	data := &BinaryRepoData{
		Tags: map[string]BinaryDetail{
			"v1.0.0": {Path: "/path/to/v1.0.0"},
			"v2.0.0": {Path: "/path/to/v2.0.0"},
			"v1.5.0": {Path: "/path/to/v1.5.0"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.GetLatest()
	}
}

func BenchmarkBinaryRepoData_FindVersion(b *testing.B) {
	data := &BinaryRepoData{
		Tags: map[string]BinaryDetail{
			"v1.0.0": {Path: "/path/to/v1.0.0"},
			"v2.0.0": {Path: "/path/to/v2.0.0"},
			"v1.5.0": {Path: "/path/to/v1.5.0"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.FindVersion("v1.5.0")
	}
}
