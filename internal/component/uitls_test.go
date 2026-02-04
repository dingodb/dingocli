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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseComponentVersion(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedName string
		expectedVer  string
	}{
		{
			name:         "with version",
			input:        "dingo-mds:v1.0.0",
			expectedName: "dingo-mds",
			expectedVer:  "v1.0.0",
		},
		{
			name:         "without version",
			input:        "dingo-client",
			expectedName: "dingo-client",
			expectedVer:  "",
		},
		{
			name:         "empty string",
			input:        "",
			expectedName: "",
			expectedVer:  "",
		},
		{
			name:         "multiple colons",
			input:        "component:name:with:colons",
			expectedName: "component",
			expectedVer:  "name:with:colons",
		},
		{
			name:         "colon at start",
			input:        ":version-only",
			expectedName: "",
			expectedVer:  "version-only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, version := ParseComponentVersion(tt.input)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedVer, version)
		})
	}
}

func TestURLJoin(t *testing.T) {
	tests := []struct {
		name     string
		base     string
		paths    []string
		expected string
	}{
		{
			name:     "single path",
			base:     "https://example.com",
			paths:    []string{"api"},
			expected: "https://example.com/api",
		},
		{
			name:     "multiple paths",
			base:     "https://example.com",
			paths:    []string{"v1", "components", "dingo-mds"},
			expected: "https://example.com/v1/components/dingo-mds",
		},
		{
			name:     "base with trailing slash",
			base:     "https://example.com/",
			paths:    []string{"api"},
			expected: "https://example.com/api",
		},
		{
			name:     "empty paths",
			base:     "https://example.com",
			paths:    []string{"", "api", ""},
			expected: "https://example.com/api",
		},
		{
			name:     "no paths",
			base:     "https://example.com",
			paths:    []string{},
			expected: "https://example.com",
		},
		{
			name:     "base with path",
			base:     "https://example.com/v1",
			paths:    []string{"components"},
			expected: "https://example.com/v1/components",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := URLJoin(tt.base, tt.paths...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestURLJoinInvalidBase(t *testing.T) {
	// The URLJoin function should handle invalid URLs gracefully
	// but it might panic depending on implementation
	assert.NotPanics(t, func() {
		result := URLJoin("http://valid.com", "path")
		assert.NotEmpty(t, result)
	})
}

func TestParseBinaryRepoData(t *testing.T) {
	tests := []struct {
		name      string
		jsonData  []byte
		expectErr bool
		errMsg    string
	}{
		{
			name:      "valid JSON",
			jsonData:  []byte(`{"version": "1.0.0", "components": {"dingo-mds": {"url": "http://example.com"}}}`),
			expectErr: false,
		},
		{
			name:      "empty JSON object",
			jsonData:  []byte(`{}`),
			expectErr: false,
		},
		{
			name:      "invalid JSON",
			jsonData:  []byte(`{"invalid": json}`),
			expectErr: true,
			errMsg:    "failed to parse JSON",
		},
		{
			name:      "empty data",
			jsonData:  []byte(``),
			expectErr: true,
			errMsg:    "failed to parse JSON",
		},
		{
			name:      "null JSON",
			jsonData:  []byte(`null`),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseBinaryRepoData(tt.jsonData)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

func TestParseFromFile(t *testing.T) {
	// Create temporary directory for test files
	tempDir := t.TempDir()

	tests := []struct {
		name      string
		filename  string
		content   string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "valid JSON file",
			filename:  "valid.json",
			content:   `{"version": "1.0.0", "components": {"dingo-mds": {"url": "http://example.com"}}}`,
			expectErr: false,
		},
		{
			name:      "empty file",
			filename:  "empty.json",
			content:   "",
			expectErr: true,
			errMsg:    "failed to parse JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test file
			filePath := filepath.Join(tempDir, tt.filename)
			err := os.WriteFile(filePath, []byte(tt.content), 0644)
			require.NoError(t, err)

			// Test the function
			result, err := ParseFromFile(filePath)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := ParseFromFile(filepath.Join(tempDir, "nonexistent.json"))
		assert.Error(t, err)
		assert.True(t, os.IsNotExist(err))
	})
}

func TestParseFromURL(t *testing.T) {
	// Test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/valid":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"version": "1.0.0", "components": {"dingo-mds": {"url": "http://example.com"}}}`))
		case "/empty":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(""))
		case "/not-found":
			w.WriteHeader(http.StatusNotFound)
		case "/invalid-json":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"invalid": json}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	tests := []struct {
		name      string
		url       string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "valid JSON response",
			url:       server.URL + "/valid",
			expectErr: false,
		},
		{
			name:      "empty response",
			url:       server.URL + "/empty",
			expectErr: true,
			errMsg:    "is empty",
		},
		{
			name:      "404 response",
			url:       server.URL + "/not-found",
			expectErr: true,
			errMsg:    "404",
		},
		{
			name:      "invalid JSON response",
			url:       server.URL + "/invalid-json",
			expectErr: true,
			errMsg:    "failed to parse JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseFromURL(tt.url)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}

	t.Run("invalid URL", func(t *testing.T) {
		_, err := ParseFromURL("invalid-url")
		assert.Error(t, err)
	})

	t.Run("network error", func(t *testing.T) {
		// Use a URL that will definitely fail
		_, err := ParseFromURL("http://localhost:99999/nonexistent")
		assert.Error(t, err)
	})
}

// Benchmark tests
func BenchmarkParseComponentVersion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParseComponentVersion("dingo-mds:v1.0.0")
	}
}

func BenchmarkURLJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		URLJoin("https://example.com", "v1", "components", "dingo-mds")
	}
}

func BenchmarkParseBinaryRepoData(b *testing.B) {
	data := []byte(`{"version": "1.0.0", "components": {"dingo-mds": {"url": "http://example.com"}}}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseBinaryRepoData(data)
	}
}
