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
	"os"
	"path/filepath"

	"github.com/dingodb/dingocli/internal/utils"
)

var (
	Mirror_URL = "https://www.dingodb.com/dingofs"
)

func init() {
	if val, ok := os.LookupEnv("DINGOFS_MIRROR"); ok {
		Mirror_URL = val
	}
}

type ComponentManager struct {
	rootDir       string
	installedFile string
	installed     []*Component
	avaliable     []*Component
	repodata      map[string]*BinaryRepoData
	mirror        string
}

func NewComponentManager() (*ComponentManager, error) {
	if err := os.MkdirAll(RepostoryDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create config directory: %v", err))
	}

	ComponentManager := &ComponentManager{
		rootDir:       RepostoryDir,
		installedFile: filepath.Join(RepostoryDir, INSTALLED_FILE),
		repodata:      make(map[string]*BinaryRepoData),
		mirror:        Mirror_URL,
	}

	//load remote repostory
	for _, name := range ALL_COMPONENTS {
		repodata, err := NewBinaryRepoData(Mirror_URL, name)
		if err != nil {
			return nil, err
		}
		ComponentManager.repodata[name] = repodata
	}

	if _, err := ComponentManager.LoadInstalledComponents(); err != nil {
		return nil, err
	}
	if _, err := ComponentManager.LoadAvailableComponents(); err != nil {
		return nil, err
	}

	return ComponentManager, nil
}

func (cm *ComponentManager) LoadInstalledComponents() ([]*Component, error) {
	var components []*Component
	if _, err := os.Stat(cm.installedFile); os.IsNotExist(err) {
		return components, nil
	}

	data, err := os.ReadFile(cm.installedFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read installed file: %w", err)
	}

	if err := json.Unmarshal(data, &components); err != nil {
		return nil, fmt.Errorf("failed to unmarshal components: %w", err)
	}

	cm.installed = components
	return cm.installed, nil
}

func (cm *ComponentManager) LoadAvailableComponentVersions(name string) ([]*Component, error) {
	var components []*Component

	repodata, exists := cm.repodata[name]
	if !exists {
		return nil, fmt.Errorf("component %s not found in repository", name)
	}

	for tagname, branch := range repodata.GetTags() {
		components = append(components, &Component{
			Name:     name,
			Version:  tagname,
			Commit:   branch.Commit,
			IsActive: false,
			Release:  branch.BuildTime,
			Path:     "",
			URL:      URLJoin(cm.mirror, branch.Path),
		})
	}

	main, ok := repodata.GetMain()
	if ok {
		components = append(components, &Component{
			Name:     name,
			Version:  MAIN_VERSION,
			Commit:   main.Commit,
			Release:  main.BuildTime,
			IsActive: false,
			Path:     "",
			URL:      URLJoin(cm.mirror, main.Path),
		})
	}

	return components, nil
}

func (cm *ComponentManager) LoadAvailableComponents() ([]*Component, error) {
	var components []*Component

	for _, name := range ALL_COMPONENTS {
		comps, err := cm.LoadAvailableComponentVersions(name)
		if err != nil {
			return nil, err
		}
		components = append(components, comps...)
	}

	cm.avaliable = components

	return cm.avaliable, nil
}

func (cm *ComponentManager) SaveInstalledComponents() error {
	data, err := json.MarshalIndent(cm.installed, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal components: %w", err)
	}

	return os.WriteFile(cm.installedFile, data, 0644)
}

func (cm *ComponentManager) FindVersion(name, version string) (string, *BinaryDetail, error) {
	var binaryDetail *BinaryDetail
	var ok bool

	repodata, exists := cm.repodata[name]
	if !exists {
		return "", nil, fmt.Errorf("component %s not found in repository", name)
	}

	var foundVersion = version // save real version, latest->v5.0.0 maybe.

	switch version {
	case LASTEST_VERSION:
		foundVersion, binaryDetail, ok = repodata.GetLatest()
		if !ok {
			return "", nil, fmt.Errorf("%s: No stable version available", name)
		}

	case MAIN_VERSION:
		binaryDetail, ok = repodata.GetMain()
		if !ok {
			return "", nil, fmt.Errorf("%s: main version not found", name)
		}

	default:
		binaryDetail, ok = repodata.FindVersion(version)
		if !ok {
			return "", nil, fmt.Errorf("%s: version '%s' not found", name, version)
		}
	}

	return foundVersion, binaryDetail, nil
}

func (cm *ComponentManager) InstallComponent(name, version string) (*Component, error) {
	return cm.installOrUpdateComponent(name, version, false)
}

func (cm *ComponentManager) UpdateComponent(name, version string) (*Component, error) {
	return cm.installOrUpdateComponent(name, version, true)
}

func (cm *ComponentManager) installOrUpdateComponent(name, version string, isUpdate bool) (*Component, error) {
	foundVersion, binaryDetail, err := cm.FindVersion(name, version)
	if err != nil {
		return nil, err
	}

	// check if is installed
	existingComp, err := cm.FindInstallComponent(name, foundVersion)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	// for install , return error if exists
	if !isUpdate && existingComp != nil {
		return nil, fmt.Errorf("%s:%s already installed", name, foundVersion)
	}

	// for update, return if already latest build
	if isUpdate && existingComp != nil {
		if version == LASTEST_VERSION {
			return existingComp, ErrAlreadyExist
		}
		if existingComp.Release >= binaryDetail.BuildTime {
			return existingComp, ErrAlreadyLatest
		}
	}

	newComponent := &Component{
		Name:        name,
		Version:     foundVersion,
		Commit:      binaryDetail.Commit,
		Release:     binaryDetail.BuildTime,
		IsInstalled: true,
		Path:        filepath.Join(cm.rootDir, name, foundVersion),
		URL:         URLJoin(cm.mirror, binaryDetail.Path),
	}

	fmt.Printf("Download %s from %s\n", name, newComponent.URL)

	_, err = utils.DownloadFileWithProgress(newComponent.URL, newComponent.Path, newComponent.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to download %s: %v", name, err)
	}

	// for update, if already exists, replace old
	if isUpdate && existingComp != nil {
		for i, comp := range cm.installed {
			if comp.Name == name && comp.Version == foundVersion {
				cm.installed[i] = newComponent
				break
			}
		}
	} else {
		cm.installed = append(cm.installed, newComponent)
	}

	// set as default version
	if err := cm.SetDefaultVersion(name, foundVersion); err != nil {
		return nil, err
	}

	return newComponent, cm.SaveInstalledComponents()
}

func (cm *ComponentManager) SetDefaultVersion(name, version string) error {
	found := false

	for i := range cm.installed {
		if cm.installed[i].Name == name {
			if cm.installed[i].Version == version {
				cm.installed[i].IsActive = true
				found = true
			} else {
				cm.installed[i].IsActive = false
			}
		}
	}

	if !found {
		return fmt.Errorf("component %s:%s not installed", name, version)
	}

	return nil
}

func (cm *ComponentManager) RemoveComponent(name, version string, force bool, saveToFile bool) error {
	var newComponents []*Component
	var filename string

	for _, comp := range cm.installed {
		if (comp.Name == name && comp.Version == version) && comp.IsActive && !force {
			return fmt.Errorf("cannot remove active component %s, please set another version as default or use --force to remove", name)
		}

		if !(comp.Name == name && comp.Version == version) {
			newComponents = append(newComponents, comp)
		} else {
			filename = filepath.Join(comp.Path, name)
			os.Remove(filename)
		}
	}

	if len(newComponents) == len(cm.installed) {
		return fmt.Errorf("component %s:%s not installed", name, version)
	}

	cm.installed = newComponents

	if saveToFile {
		return cm.SaveInstalledComponents()
	}

	return nil
}

func (cm *ComponentManager) RemoveComponents(name string, saveToFile bool) ([]*Component, error) {
	var newComponents []*Component
	var removedComponents []*Component

	for _, comp := range cm.installed {
		if !(comp.Name == name) {
			newComponents = append(newComponents, comp)
		} else {
			removedComponents = append(removedComponents, comp)
		}
	}

	if len(removedComponents) == 0 {
		return nil, fmt.Errorf("component %s not installed", name)
	} else {
		for _, comp := range removedComponents {
			os.Remove(filepath.Join(comp.Path, comp.Name))
		}
	}

	cm.installed = newComponents

	if saveToFile {
		return removedComponents, cm.SaveInstalledComponents()
	}

	return removedComponents, nil
}

func (cm *ComponentManager) GetActiveComponent(name string) (*Component, error) {
	for _, comp := range cm.installed {
		if comp.Name == name && comp.IsActive {
			return comp, nil
		}
	}

	return nil, fmt.Errorf("no active version for component %s", name)
}

func (cm *ComponentManager) ListComponents() ([]*Component, error) {
	allComponents := make([]*Component, 0)
	for _, availableComp := range cm.avaliable {
		if cm.IsInstalled(availableComp.Name, availableComp.Version) {
			cm.UpdateState(availableComp.Name, availableComp.Version, availableComp.Release)
			continue
		}

		allComponents = append(allComponents, availableComp)
	}

	allComponents = append(allComponents, cm.installed...)

	return allComponents, nil
}

func (cm *ComponentManager) FindInstallComponent(name string, version string) (*Component, error) {
	for _, comp := range cm.installed {
		if comp.Name == name && comp.Version == version {
			return comp, nil
		}
	}

	return nil, ErrNotFound
}

func (cm *ComponentManager) IsInstalled(name, version string) bool {
	for _, comp := range cm.installed {
		if comp.Name == name && comp.Version == version {
			return true
		}
	}
	return false
}

// update component whether is updatable
func (cm *ComponentManager) UpdateState(name, version, release string) bool {
	for _, comp := range cm.installed {
		if comp.Name == name && comp.Version == version {
			comp.Updatable = release > comp.Release
			return comp.Updatable
		}
	}

	return false
}
