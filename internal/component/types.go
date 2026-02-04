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
	"errors"
	"fmt"
	"os"
)

const (
	DINGO_CLIENT     = "dingo-client"
	DINGO_DACHE      = "dingo-cache"
	DINGO_MDS        = "dingo-mds"
	DINGO_MDS_CLIENT = "dingo-mds-client"
	INSTALLED_FILE   = "installed.json"
	LASTEST_VERSION  = "latest"
	MAIN_VERSION     = "main"
)

var (
	ErrAlreadyLatest = errors.New("already with latest build")
	ErrAlreadyExist  = errors.New("already exist")
	ErrNotFound      = errors.New("not found")

	RepostoryDir = fmt.Sprintf("%s/.dingo/components", func() string {
		homeDir, _ := os.UserHomeDir()
		return homeDir
	}())
)

var ALL_COMPONENTS = []string{
	DINGO_CLIENT,
	DINGO_DACHE,
	DINGO_MDS,
	DINGO_MDS_CLIENT,
}

type Component struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Commit      string `json:"commit"`
	IsInstalled bool   `json:"installed"`
	IsActive    bool   `json:"active"`
	Release     string `json:"release"`
	Path        string `json:"path"`
	URL         string `json:"url"`
	Updatable   bool   `json:"-"`
}
