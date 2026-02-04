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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstants(t *testing.T) {
	assert.Equal(t, "dingo-client", DINGO_CLIENT)
	assert.Equal(t, "dingo-cache", DINGO_DACHE)
	assert.Equal(t, "dingo-mds", DINGO_MDS)
	assert.Equal(t, "dingo-mds-client", DINGO_MDS_CLIENT)
	assert.Equal(t, "installed.json", INSTALLED_FILE)
	assert.Equal(t, "latest", LASTEST_VERSION)
	assert.Equal(t, "main", MAIN_VERSION)
}

func TestErrors(t *testing.T) {
	assert.Equal(t, "already with latest build", ErrAlreadyLatest.Error())
	assert.Equal(t, "already exist", ErrAlreadyExist.Error())
	assert.Equal(t, "not found", ErrNotFound.Error())
}

func TestRepostoryDir(t *testing.T) {
	// Get the expected home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("Cannot get home directory: %v", err)
	}

	expectedDir := homeDir + "/.dingo/components"
	assert.Equal(t, expectedDir, RepostoryDir)
}

func TestAllComponents(t *testing.T) {
	expected := []string{
		DINGO_CLIENT,
		DINGO_DACHE,
		DINGO_MDS,
		DINGO_MDS_CLIENT,
	}

	assert.Equal(t, expected, ALL_COMPONENTS)
	assert.Len(t, ALL_COMPONENTS, 4)
}

func TestComponentStruct(t *testing.T) {
	// Test zero value
	var zeroComponent Component
	assert.Empty(t, zeroComponent.Name)
	assert.Empty(t, zeroComponent.Version)
	assert.Empty(t, zeroComponent.Commit)
	assert.False(t, zeroComponent.IsInstalled)
	assert.False(t, zeroComponent.IsActive)
	assert.Empty(t, zeroComponent.Release)
	assert.Empty(t, zeroComponent.Path)
	assert.Empty(t, zeroComponent.URL)
	assert.False(t, zeroComponent.Updatable)
}

func TestComponentJSONSerialization(t *testing.T) {
	// Test JSON marshaling
	component := Component{
		Name:        "dingo-mds",
		Version:     "v1.0.0",
		Commit:      "abc123",
		IsInstalled: true,
		IsActive:    true,
		Release:     "stable",
		Path:        "/path/to/component",
		URL:         "http://example.com/component",
	}

	// Marshal to JSON
	data, err := json.Marshal(component)
	assert.NoError(t, err)

	// Check that the JSON contains expected fields (but not Updatable since it has `-` tag)
	jsonStr := string(data)
	assert.Contains(t, jsonStr, `"name"`)
	assert.Contains(t, jsonStr, `"version"`)
	assert.Contains(t, jsonStr, `"commit"`)
	assert.Contains(t, jsonStr, `"installed"`)
	assert.Contains(t, jsonStr, `"active"`)
	assert.Contains(t, jsonStr, `"release"`)
	assert.Contains(t, jsonStr, `"path"`)
	assert.Contains(t, jsonStr, `"url"`)
	assert.NotContains(t, jsonStr, `"Updatable"`)

	// Unmarshal from JSON
	var unmarshaled Component
	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)

	assert.Equal(t, component.Name, unmarshaled.Name)
	assert.Equal(t, component.Version, unmarshaled.Version)
	assert.Equal(t, component.Commit, unmarshaled.Commit)
	assert.Equal(t, component.IsInstalled, unmarshaled.IsInstalled)
	assert.Equal(t, component.IsActive, unmarshaled.IsActive)
	assert.Equal(t, component.Release, unmarshaled.Release)
	assert.Equal(t, component.Path, unmarshaled.Path)
	assert.Equal(t, component.URL, unmarshaled.URL)
	// Updatable should be false since it's not in JSON
	assert.False(t, unmarshaled.Updatable)
}

func TestComponentWithPartialJSON(t *testing.T) {
	// Test with minimal JSON
	jsonData := `{"name": "test-component", "version": "v1.0.0"}`

	var component Component
	err := json.Unmarshal([]byte(jsonData), &component)
	assert.NoError(t, err)

	assert.Equal(t, "test-component", component.Name)
	assert.Equal(t, "v1.0.0", component.Version)
	assert.Empty(t, component.Commit)
	assert.False(t, component.IsInstalled)
	assert.False(t, component.IsActive)
	assert.Empty(t, component.Release)
	assert.Empty(t, component.Path)
	assert.Empty(t, component.URL)
}

func TestComponentWithEmptyJSON(t *testing.T) {
	// Test with empty JSON object
	jsonData := `{}`

	var component Component
	err := json.Unmarshal([]byte(jsonData), &component)
	assert.NoError(t, err)

	assert.Empty(t, component.Name)
	assert.Empty(t, component.Version)
	assert.Empty(t, component.Commit)
	assert.False(t, component.IsInstalled)
	assert.False(t, component.IsActive)
	assert.Empty(t, component.Release)
	assert.Empty(t, component.Path)
	assert.Empty(t, component.URL)
}

func TestComponentWithInvalidJSON(t *testing.T) {
	// Test with invalid JSON
	jsonData := `{"name": "test", invalid}`

	var component Component
	err := json.Unmarshal([]byte(jsonData), &component)
	assert.Error(t, err)
}

func TestConstantsIntegration(t *testing.T) {
	// Test that ALL_COMPONENTS contains all defined component constants
	componentMap := make(map[string]bool)
	for _, comp := range ALL_COMPONENTS {
		componentMap[comp] = true
	}

	assert.True(t, componentMap[DINGO_CLIENT], "ALL_COMPONENTS should contain DINGO_CLIENT")
	assert.True(t, componentMap[DINGO_DACHE], "ALL_COMPONENTS should contain DINGO_DACHE")
	assert.True(t, componentMap[DINGO_MDS], "ALL_COMPONENTS should contain DINGO_MDS")
	assert.True(t, componentMap[DINGO_MDS_CLIENT], "ALL_COMPONENTS should contain DINGO_MDS_CLIENT")
}

func TestComponentFactoryMethods(t *testing.T) {
	// While there are no factory methods, we can test creating components with different states

	tests := []struct {
		name      string
		component Component
	}{
		{
			name: "installed active component",
			component: Component{
				Name:        DINGO_MDS,
				Version:     "v1.0.0",
				Commit:      "abc123",
				IsInstalled: true,
				IsActive:    true,
				Release:     "stable",
				Path:        "/some/path",
				URL:         "http://example.com",
				Updatable:   true,
			},
		},
		{
			name: "not installed component",
			component: Component{
				Name:        DINGO_CLIENT,
				Version:     LASTEST_VERSION,
				IsInstalled: false,
				IsActive:    false,
				Updatable:   false,
			},
		},
		{
			name: "installed but inactive component",
			component: Component{
				Name:        DINGO_DACHE,
				Version:     MAIN_VERSION,
				Commit:      "def456",
				IsInstalled: true,
				IsActive:    false,
				Updatable:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify the component has the expected properties
			assert.NotEmpty(t, tt.component.Name)

			// Test JSON serialization works for all states
			data, err := json.Marshal(tt.component)
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			// Test deserialization
			var unmarshaled Component
			err = json.Unmarshal(data, &unmarshaled)
			assert.NoError(t, err)

			// Important fields should match
			assert.Equal(t, tt.component.Name, unmarshaled.Name)
			assert.Equal(t, tt.component.Version, unmarshaled.Version)
			assert.Equal(t, tt.component.IsInstalled, unmarshaled.IsInstalled)
			assert.Equal(t, tt.component.IsActive, unmarshaled.IsActive)
		})
	}
}
