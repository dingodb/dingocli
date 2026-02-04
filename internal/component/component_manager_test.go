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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock implementations for testing
type MockFileSystem struct {
	files map[string][]byte
	dirs  map[string]bool
}

func NewMockFileSystem() *MockFileSystem {
	return &MockFileSystem{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (m *MockFileSystem) WriteFile(path string, data []byte) error {
	m.files[path] = data
	return nil
}

func (m *MockFileSystem) ReadFile(path string) ([]byte, error) {
	if data, exists := m.files[path]; exists {
		return data, nil
	}
	return nil, os.ErrNotExist
}

func (m *MockFileSystem) Stat(path string) (os.FileInfo, error) {
	if _, exists := m.files[path]; exists {
		return &MockFileInfo{isDir: false}, nil
	}
	if _, exists := m.dirs[path]; exists {
		return &MockFileInfo{isDir: true}, nil
	}
	return nil, os.ErrNotExist
}

func (m *MockFileSystem) MkdirAll(path string, perm os.FileMode) error {
	m.dirs[path] = true
	return nil
}

func (m *MockFileSystem) Remove(path string) error {
	delete(m.files, path)
	return nil
}

type MockFileInfo struct {
	isDir bool
}

func (m *MockFileInfo) Name() string       { return "" }
func (m *MockFileInfo) Size() int64        { return 0 }
func (m *MockFileInfo) Mode() os.FileMode  { return 0 }
func (m *MockFileInfo) ModTime() time.Time { return time.Now() }
func (m *MockFileInfo) IsDir() bool        { return m.isDir }
func (m *MockFileInfo) Sys() interface{}   { return nil }

// Test component manager with mocked dependencies
func TestComponentManager_LoadInstalledComponents(t *testing.T) {
	tests := []struct {
		name               string
		fileExists         bool
		fileContent        string
		expectErr          bool
		expectedLen        int
		expectedComponents []*Component
	}{
		{
			name:        "file does not exist",
			fileExists:  false,
			expectErr:   false,
			expectedLen: 0,
		},
		{
			name:       "valid JSON file",
			fileExists: true,
			fileContent: `[
				{
					"name": "dingo-mds",
					"version": "v1.0.0",
					"installed": true,
					"active": true
				},
				{
					"name": "dingo-client",
					"version": "v1.1.0",
					"installed": true,
					"active": false
				}
			]`,
			expectErr:   false,
			expectedLen: 2,
		},
		{
			name:        "invalid JSON file",
			fileExists:  true,
			fileContent: `[{"invalid": json}]`,
			expectErr:   true,
		},
		{
			name:        "empty JSON array",
			fileExists:  true,
			fileContent: `[]`,
			expectErr:   false,
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			installedFile := filepath.Join(tempDir, "installed.json")

			// Setup mock file system
			if tt.fileExists {
				err := os.WriteFile(installedFile, []byte(tt.fileContent), 0644)
				require.NoError(t, err)
			}

			cm := &ComponentManager{
				installedFile: installedFile,
			}

			components, err := cm.LoadInstalledComponents()

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, components, tt.expectedLen)
			}
		})
	}
}

func TestComponentManager_SaveInstalledComponents(t *testing.T) {
	tempDir := t.TempDir()
	installedFile := filepath.Join(tempDir, "installed.json")

	cm := &ComponentManager{
		installedFile: installedFile,
		installed: []*Component{
			{
				Name:        "dingo-mds",
				Version:     "v1.0.0",
				IsInstalled: true,
				IsActive:    true,
			},
		},
	}

	err := cm.SaveInstalledComponents()
	assert.NoError(t, err)

	// Verify file was created and contains correct data
	data, err := os.ReadFile(installedFile)
	assert.NoError(t, err)

	var savedComponents []*Component
	err = json.Unmarshal(data, &savedComponents)
	assert.NoError(t, err)
	assert.Len(t, savedComponents, 1)
	assert.Equal(t, "dingo-mds", savedComponents[0].Name)
}

func TestComponentManager_FindVersion(t *testing.T) {
	// Create mock repo data
	repoData := &BinaryRepoData{
		Tags: map[string]BinaryDetail{
			"v1.0.0": {
				Path:      "/tags/v1.0.0",
				BuildTime: "2023-01-01T00:00:00Z",
				Commit:    "abc123",
			},
			"v2.0.0": {
				Path:      "/tags/v2.0.0",
				BuildTime: "2023-02-01T00:00:00Z",
				Commit:    "def456",
			},
		},
		Branches: map[string]BinaryDetail{
			"main": {
				Path:      "/branches/main",
				BuildTime: "2023-03-01T00:00:00Z",
				Commit:    "main-commit",
			},
		},
	}

	cm := &ComponentManager{
		repodata: map[string]*BinaryRepoData{
			"dingo-mds": repoData,
		},
	}

	tests := []struct {
		name          string
		componentName string
		version       string
		expectErr     bool
		expectedErr   string
		expectedVer   string
	}{
		{
			name:          "specific version exists",
			componentName: "dingo-mds",
			version:       "v1.0.0",
			expectErr:     false,
			expectedVer:   "v1.0.0",
		},
		{
			name:          "latest version",
			componentName: "dingo-mds",
			version:       "latest",
			expectErr:     false,
			expectedVer:   "v2.0.0", // Should return highest version
		},
		{
			name:          "main version",
			componentName: "dingo-mds",
			version:       "main",
			expectErr:     false,
		},
		{
			name:          "version not found",
			componentName: "dingo-mds",
			version:       "v999.0.0",
			expectErr:     true,
			expectedErr:   "version 'v999.0.0' not found",
		},
		{
			name:          "component not found",
			componentName: "nonexistent",
			version:       "v1.0.0",
			expectErr:     true,
			expectedErr:   "not found in repository",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, detail, err := cm.FindVersion(tt.componentName, tt.version)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				assert.Empty(t, version)
				assert.Nil(t, detail)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, version)
				assert.NotNil(t, detail)
				if tt.expectedVer != "" {
					assert.Equal(t, tt.expectedVer, version)
				}
			}
		})
	}
}

func TestComponentManager_FindInstallComponent(t *testing.T) {
	components := []*Component{
		{
			Name:    "dingo-mds",
			Version: "v1.0.0",
		},
		{
			Name:    "dingo-client",
			Version: "v1.1.0",
		},
	}

	cm := &ComponentManager{
		installed: components,
	}

	tests := []struct {
		name          string
		componentName string
		version       string
		expectFound   bool
	}{
		{
			name:          "existing component",
			componentName: "dingo-mds",
			version:       "v1.0.0",
			expectFound:   true,
		},
		{
			name:          "non-existing component",
			componentName: "nonexistent",
			version:       "v1.0.0",
			expectFound:   false,
		},
		{
			name:          "wrong version",
			componentName: "dingo-mds",
			version:       "v999.0.0",
			expectFound:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := cm.FindInstallComponent(tt.componentName, tt.version)

			if tt.expectFound {
				assert.NoError(t, err)
				assert.NotNil(t, component)
				assert.Equal(t, tt.componentName, component.Name)
				assert.Equal(t, tt.version, component.Version)
			} else {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, ErrNotFound))
				assert.Nil(t, component)
			}
		})
	}
}

func TestComponentManager_IsInstalled(t *testing.T) {
	components := []*Component{
		{
			Name:    "dingo-mds",
			Version: "v1.0.0",
		},
	}

	cm := &ComponentManager{
		installed: components,
	}

	tests := []struct {
		name          string
		componentName string
		version       string
		expected      bool
	}{
		{
			name:          "installed component",
			componentName: "dingo-mds",
			version:       "v1.0.0",
			expected:      true,
		},
		{
			name:          "not installed component",
			componentName: "nonexistent",
			version:       "v1.0.0",
			expected:      false,
		},
		{
			name:          "different version",
			componentName: "dingo-mds",
			version:       "v2.0.0",
			expected:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.IsInstalled(tt.componentName, tt.version)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComponentManager_SetDefaultVersion(t *testing.T) {
	components := []*Component{
		{
			Name:     "dingo-mds",
			Version:  "v1.0.0",
			IsActive: true,
		},
		{
			Name:     "dingo-mds",
			Version:  "v1.1.0",
			IsActive: false,
		},
		{
			Name:     "dingo-client",
			Version:  "v1.0.0",
			IsActive: true,
		},
	}

	cm := &ComponentManager{
		installed: components,
	}

	tests := []struct {
		name          string
		componentName string
		version       string
		expectErr     bool
		expectedErr   string
	}{
		{
			name:          "set existing version as default",
			componentName: "dingo-mds",
			version:       "v1.1.0",
			expectErr:     false,
		},
		{
			name:          "version not installed",
			componentName: "dingo-mds",
			version:       "v999.0.0",
			expectErr:     true,
			expectedErr:   "not installed",
		},
		{
			name:          "component not found",
			componentName: "nonexistent",
			version:       "v1.0.0",
			expectErr:     true,
			expectedErr:   "not installed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cm.SetDefaultVersion(tt.componentName, tt.version)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)

				// Verify the change
				for _, comp := range cm.installed {
					if comp.Name == tt.componentName {
						if comp.Version == tt.version {
							assert.True(t, comp.IsActive)
						} else {
							assert.False(t, comp.IsActive)
						}
					}
				}
			}
		})
	}
}

func TestComponentManager_GetActiveComponent(t *testing.T) {
	components := []*Component{
		{
			Name:     "dingo-mds",
			Version:  "v1.0.0",
			IsActive: true,
		},
		{
			Name:     "dingo-client",
			Version:  "v1.0.0",
			IsActive: false,
		},
	}

	cm := &ComponentManager{
		installed: components,
	}

	tests := []struct {
		name          string
		componentName string
		expectFound   bool
		expectedVer   string
	}{
		{
			name:          "active component exists",
			componentName: "dingo-mds",
			expectFound:   true,
			expectedVer:   "v1.0.0",
		},
		{
			name:          "no active component",
			componentName: "dingo-client",
			expectFound:   false,
		},
		{
			name:          "component not installed",
			componentName: "nonexistent",
			expectFound:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			component, err := cm.GetActiveComponent(tt.componentName)

			if tt.expectFound {
				assert.NoError(t, err)
				assert.NotNil(t, component)
				assert.Equal(t, tt.componentName, component.Name)
				assert.Equal(t, tt.expectedVer, component.Version)
				assert.True(t, component.IsActive)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "no active version")
				assert.Nil(t, component)
			}
		})
	}
}

func TestComponentManager_UpdateState(t *testing.T) {
	components := []*Component{
		{
			Name:    "dingo-mds",
			Version: "v1.0.0",
			Release: "2023-01-01T00:00:00Z",
		},
		{
			Name:    "dingo-mds",
			Version: "v1.1.0",
			Release: "2023-02-01T00:00:00Z",
		},
	}

	cm := &ComponentManager{
		installed: components,
	}

	tests := []struct {
		name          string
		componentName string
		version       string
		release       string
		expected      bool
	}{
		{
			name:          "newer release available",
			componentName: "dingo-mds",
			version:       "v1.0.0",
			release:       "2023-03-01T00:00:00Z",
			expected:      true,
		},
		{
			name:          "same release",
			componentName: "dingo-mds",
			version:       "v1.0.0",
			release:       "2023-01-01T00:00:00Z",
			expected:      false,
		},
		{
			name:          "older release",
			componentName: "dingo-mds",
			version:       "v1.0.0",
			release:       "2022-01-01T00:00:00Z",
			expected:      false,
		},
		{
			name:          "component not found",
			componentName: "nonexistent",
			version:       "v1.0.0",
			release:       "2023-01-01T00:00:00Z",
			expected:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.UpdateState(tt.componentName, tt.version, tt.release)
			assert.Equal(t, tt.expected, result)

			if tt.expected {
				// Verify the component is marked as updatable
				comp, err := cm.FindInstallComponent(tt.componentName, tt.version)
				require.NoError(t, err)
				assert.True(t, comp.Updatable)
			}
		})
	}
}

func TestComponentManager_RemoveComponent(t *testing.T) {
	components := []*Component{
		{
			Name:     "dingo-mds",
			Version:  "v1.0.0",
			IsActive: true,
			Path:     "/some/path",
		},
		{
			Name:     "dingo-mds",
			Version:  "v1.1.0",
			IsActive: false,
			Path:     "/some/other/path",
		},
	}

	cm := &ComponentManager{
		installed: components,
	}

	tests := []struct {
		name           string
		componentName  string
		version        string
		force          bool
		expectErr      bool
		expectedErr    string
		remainingComps int
	}{
		{
			name:           "remove inactive component",
			componentName:  "dingo-mds",
			version:        "v1.1.0",
			force:          false,
			expectErr:      false,
			remainingComps: 1,
		},
		{
			name:           "try to remove active component without force",
			componentName:  "dingo-mds",
			version:        "v1.0.0",
			force:          false,
			expectErr:      true,
			expectedErr:    "cannot remove active component",
			remainingComps: 2,
		},
		{
			name:           "remove active component with force",
			componentName:  "dingo-mds",
			version:        "v1.0.0",
			force:          true,
			expectErr:      false,
			remainingComps: 1,
		},
		{
			name:           "remove non-existent component",
			componentName:  "nonexistent",
			version:        "v1.0.0",
			force:          false,
			expectErr:      true,
			expectedErr:    "not installed",
			remainingComps: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset components for each test
			cm.installed = make([]*Component, len(components))
			copy(cm.installed, components)

			err := cm.RemoveComponent(tt.componentName, tt.version, tt.force, false)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}

			assert.Len(t, cm.installed, tt.remainingComps)
		})
	}
}

func TestComponentManager_LoadAvailableComponentVersions(t *testing.T) {
	repoData := &BinaryRepoData{
		Tags: map[string]BinaryDetail{
			"v1.0.0": {
				Path:      "/tags/v1.0.0",
				BuildTime: "2023-01-01T00:00:00Z",
				Commit:    "abc123",
			},
		},
		Branches: map[string]BinaryDetail{
			"main": {
				Path:      "/branches/main",
				BuildTime: "2023-02-01T00:00:00Z",
				Commit:    "main-commit",
			},
		},
	}

	cm := &ComponentManager{
		repodata: map[string]*BinaryRepoData{
			"dingo-mds": repoData,
		},
		mirror: "https://example.com",
	}

	tests := []struct {
		name          string
		componentName string
		expectErr     bool
		expectedCount int
	}{
		{
			name:          "load versions for existing component",
			componentName: "dingo-mds",
			expectErr:     false,
			expectedCount: 2, // 1 tag + 1 branch
		},
		{
			name:          "load versions for non-existing component",
			componentName: "nonexistent",
			expectErr:     true,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			components, err := cm.LoadAvailableComponentVersions(tt.componentName)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Nil(t, components)
			} else {
				assert.NoError(t, err)
				assert.Len(t, components, tt.expectedCount)

				// Verify component properties
				for _, comp := range components {
					assert.Equal(t, tt.componentName, comp.Name)
					assert.False(t, comp.IsActive)
					assert.NotEmpty(t, comp.URL)
					assert.Contains(t, comp.URL, cm.mirror)
				}
			}
		})
	}
}

// Integration test with mock HTTP server
func TestNewBinaryRepoData_Integration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dingo-mds.version" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"binary": "dingo-mds",
				"generated_at": "2023-01-01T00:00:00Z",
				"branches": {},
				"tags": {},
				"commits": {}
			}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Temporarily set environment variable
	originalMirror := Mirror_URL
	Mirror_URL = server.URL
	defer func() { Mirror_URL = originalMirror }()

	// This test would require mocking the entire initialization process
	// For now, we'll test the individual components
	t.Run("test repo data creation", func(t *testing.T) {
		repoData, err := NewBinaryRepoData(server.URL, "dingo-mds")
		assert.NoError(t, err)
		assert.NotNil(t, repoData)
		assert.Equal(t, "dingo-mds", repoData.Binary)
	})
}

// Benchmark tests
func BenchmarkComponentManager_FindInstallComponent(b *testing.B) {
	components := make([]*Component, 1000)
	for i := 0; i < 1000; i++ {
		components[i] = &Component{
			Name:    fmt.Sprintf("component-%d", i%10),
			Version: fmt.Sprintf("v%d.0.0", i),
		}
	}

	cm := &ComponentManager{
		installed: components,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cm.FindInstallComponent("component-5", "v505.0.0")
	}
}

func BenchmarkComponentManager_IsInstalled(b *testing.B) {
	components := make([]*Component, 1000)
	for i := 0; i < 1000; i++ {
		components[i] = &Component{
			Name:    fmt.Sprintf("component-%d", i%10),
			Version: fmt.Sprintf("v%d.0.0", i),
		}
	}

	cm := &ComponentManager{
		installed: components,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cm.IsInstalled("component-5", "v505.0.0")
	}
}
