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

import "fmt"

type BinaryRepoData struct {
	Binary      string                  `json:"binary"`
	GeneratedAt string                  `json:"generated_at"`
	Branches    map[string]BinaryDetail `json:"branches"`
	Commits     map[string]BinaryDetail `json:"commits"`
	Tags        map[string]BinaryDetail `json:"tags"`
}

type BinaryDetail struct {
	Path      string `json:"path"`
	BuildTime string `json:"build_time"`
	Size      string `json:"size"`
	Commit    string `json:"commit,omitempty"`
}

func (b *BinaryRepoData) GetBranches() map[string]BinaryDetail {
	return b.Branches
}
func (b *BinaryRepoData) GetTags() map[string]BinaryDetail {
	return b.Tags
}

func (b *BinaryRepoData) GetCommits() map[string]BinaryDetail {
	return b.Commits
}

func (b *BinaryRepoData) GetLatest() (string, *BinaryDetail, bool) {
	latest := "v0.0.0"
	for version := range b.Tags {
		if version > latest {
			latest = version
		}
	}

	tag, ok := b.Tags[latest]
	if ok {
		return latest, &tag, true
	}

	return "", nil, false
}

func (b *BinaryRepoData) GetMain() (*BinaryDetail, bool) {
	if branch, exists := b.Branches[MAIN_VERSION]; exists {
		return &branch, true
	}

	return nil, false
}

func (b *BinaryRepoData) FindVersion(tag string) (*BinaryDetail, bool) {
	if tag, exists := b.Tags[tag]; exists {
		return &tag, true
	}

	return nil, false
}

func (b *BinaryRepoData) GetName() string {
	return b.Binary
}

func NewBinaryRepoData(url string, name string) (*BinaryRepoData, error) {
	requestURL := URLJoin(url, fmt.Sprintf("%s.version", name))
	metadata, err := ParseFromURL(requestURL)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}
