/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package upgrade

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	tui "github.com/dingodb/dingocli/internal/tui/common"
	"github.com/go-resty/resty/v2"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
)

const (
	URL_LATEST_VERSION_TEMPLATE = "https://github.com/dingodb/dingocli/releases/download/%s/commit_id"
	URL_INSTALL_SCRIPT_TEMPLATE = "https://raw.githubusercontent.com/dingodb/dingocli/%s/scripts/install_dingo.sh"
	ENV_DINGO_UPGRADE           = "DINGO_UPGRADE"
	ENV_DINGO_VERSION           = "DINGO_VERSION"
	ENV_DINGO_BRANCH            = "DINGO_BRANCH"
	DEFAULT_BRANCH              = "main"
)

func calcVersion(v string) int {
	num := 0
	base := 1000
	items := strings.Split(v, ".")
	for _, item := range items {
		n, err := strconv.Atoi(item)
		if err != nil {
			return -1
		}
		num = num*base + n
	}
	return num
}

func IsLatest(currentVersion, remoteVersion string) (error, bool) {
	v1 := calcVersion(currentVersion)
	v2 := calcVersion(remoteVersion)
	if v1 == -1 || v2 == -1 {
		return fmt.Errorf("invalid version format: %s, %s", currentVersion, remoteVersion), false
	}

	return nil, v1 >= v2
}

func GetLatestCommitId(currentCommit string, branch string) (string, error) {
	// Default to main branch if not specified
	if branch == "" {
		branch = DEFAULT_BRANCH
	}

	url := fmt.Sprintf(URL_LATEST_VERSION_TEMPLATE, branch)

	// get latest commit id from remote
	client := resty.New()
	client.SetTimeout(time.Duration(10 * time.Second)) // 10 seconds
	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		Get(url)

	if err != nil {
		fmt.Println("request error:", err)
		return "", err
	}

	// check response content
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("failed to get latest commit for branch '%s', status code: %d (check if branch exists)", branch, resp.StatusCode())
	}
	// trim newline character
	latestCommitId := strings.TrimSuffix(string(resp.Body()), "\n")
	if len(latestCommitId) == 0 {
		return "", fmt.Errorf("failed to get latest commit, response is empty")
	}

	if currentCommit == latestCommitId {
		return "", nil // already up to date
	}

	return latestCommitId, nil
}

func Upgrade2Latest(currentCommit string, branch string) error {
	// Default to main branch if not specified
	if branch == "" {
		branch = DEFAULT_BRANCH
	}

	// Create a progress bar with actual file size
	wg := sync.WaitGroup{}
	p := mpb.New(mpb.WithWaitGroup(&wg), mpb.WithOutput(os.Stdout))
	checkBar := p.New(1,
		mpb.BarStyle().Lbound("").Filler("").Tip("").Padding("").Rbound(""),
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("Checking for update (branch: %s): ", branch), decor.WC{W: 40}),
			decor.OnComplete(decor.Spinner([]string{}), ""),
			//decor.Spinner([]string{"-", "\\", "|", "/"}, decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO, decor.WC{W: 4}),
		),
	)

	version, err := GetLatestCommitId(currentCommit, branch)
	if err != nil {
		checkBar.Abort(true)
		p.Wait()
		return fmt.Errorf("failed to check for updates: %w", err)
	}

	if len(version) == 0 {
		checkBar.Abort(true)
		p.Wait()
		fmt.Printf("The current version is up-to-date with branch '%s'\n", branch)
		return nil
	}
	checkBar.Abort(true)
	p.Wait()

	if pass := tui.ConfirmYes("Upgrade dingocli to %s (branch: %s)?", version, branch); !pass {
		return nil
	}

	// Step 2: Download new version
	installScriptURL := fmt.Sprintf(URL_INSTALL_SCRIPT_TEMPLATE, branch)

	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("curl -fsSL %s | bash -s -- --branch=%s", installScriptURL, branch))
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=true", ENV_DINGO_UPGRADE))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", ENV_DINGO_VERSION, version))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", ENV_DINGO_BRANCH, branch))
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}

func Upgrade(version string, branch string) error {
	// Default to main branch if not specified
	if branch == "" {
		branch = DEFAULT_BRANCH
	}

	installScriptURL := fmt.Sprintf(URL_INSTALL_SCRIPT_TEMPLATE, branch)

	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("curl -fsSL %s | bash -s -- --branch=%s", installScriptURL, branch))
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=true", ENV_DINGO_UPGRADE))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", ENV_DINGO_VERSION, version))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", ENV_DINGO_BRANCH, branch))
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	return cmd.Run()
}
