/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package warmup

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dingodb/dingocli/pkg/logger"
	"github.com/fatih/color"
	"github.com/pkg/xattr"
	"github.com/schollz/progressbar/v3"

	"github.com/spf13/cobra"
)

const (
	WARMUP_QUERY_EXAMPLE = `Examples:
   $ dingo fs warmup query /mnt/dir1`
)

type queryOptions struct {
	path string
}

func NewWarmupQueryCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options queryOptions

	cmd := &cobra.Command{
		Use:     "query [PATH] [OPTIONS]",
		Short:   "query the warmup progress",
		Args:    utils.ExactArgs(1),
		Example: WARMUP_QUERY_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			output.SetShow(true)

			options.path = args[0]

			return runQuery(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	return cmd
}

func runQuery(cmd *cobra.Command, dingocli *cli.DingoCli, options queryOptions) error {

	var warmErrors int64 = 0
	var finished int64 = 0
	var total int64 = 0
	var err error

	logger.Infof("query warmup progress, file: %s", options.path)
	filename := filepath.Base(options.path)

	total, _, _, err = getWarmupProgress(options.path)
	if err != nil {
		return err
	}

	if total == 0 {
		fmt.Println("warmup not started or just finished")
		return nil
	}

	var bar *progressbar.ProgressBar = progressbar.NewOptions64(total,
		progressbar.OptionSetDescription("[cyan]Warmup[reset] "+filename+"..."),
		progressbar.OptionShowCount(),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	for {
		total, finished, warmErrors, err = getWarmupProgress(options.path)
		if err != nil {
			return err
		}

		logger.Infof("warmup result: total[%d], finished[%d], errors[%d]", total, finished, warmErrors)
		if total == 0 { //finished
			break
		}

		bar.Set64(finished + warmErrors)

		time.Sleep(200 * time.Millisecond)
	}

	if warmErrors > 0 { //warmup failed
		fmt.Println(color.RedString("\nwarmup finished,%d errors\n", warmErrors))
	}

	bar.Finish()

	return nil
}

func getWarmupProgress(path string) (int64, int64, int64, error) {
	// result data format [finished/total/errors]
	logger.Infof("get warmup xattr")
	result, err := xattr.Get(path, DINGOFS_WARMUP_OP_XATTR)
	if err != nil {
		return 0, 0, 0, err
	}
	resultStr := string(result)

	logger.Infof("warmup xattr: [%s],[total/finished/errors]", resultStr)
	strs := strings.Split(resultStr, "/")
	if len(strs) != 3 {
		return 0, 0, 0, fmt.Errorf("response data format error, should be [finished/total/errors]")
	}
	total, err := strconv.ParseInt(strs[0], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}

	finished, err := strconv.ParseInt(strs[1], 10, 64)
	if err != nil {
		return 0, 0, 0, err

	}
	warmErrors, err := strconv.ParseInt(strs[2], 10, 64)
	if err != nil {
		return 0, 0, 0, err

	}

	return total, finished, warmErrors, nil
}
