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
