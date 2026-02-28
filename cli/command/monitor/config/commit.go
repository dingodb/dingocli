/*
 * 	Copyright (c) 2025 dingodb.com Inc.
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

package config

import (
	"errors"
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/configure"
	"github.com/dingodb/dingocli/internal/configure/topology"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/storage"
	tui "github.com/dingodb/dingocli/internal/tui/common"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

const (
	COMMIT_EXAMPLE = `Examples:
  $ dingo monitor config commit -c /path/to/monitor.yaml  # Commit monitor topology`
)

type commitOptions struct {
	filename string
	slient   bool
	force    bool
}

func NewCommitCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options commitOptions

	cmd := &cobra.Command{
		Use:   "commit TOPOLOGY [OPTIONS]",
		Short: "Commit monitor topology",
		// Args:    utils.ExactArgs(1),
		Example: COMMIT_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			// options.filename = args[0]
			return runCommit(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.StringVarP(&options.filename, "conf", "c", "monitor.yaml", "Specify monitor configuration file")
	flags.BoolVarP(&options.slient, "slient", "s", false, "Slient output for config commit")
	flags.BoolVarP(&options.force, "force", "f", false, "Commit cluster topology by force")

	return cmd
}

func skipError(err error) bool {
	if errors.Is(err, errno.ERR_EMPTY_CLUSTER_TOPOLOGY) ||
		errors.Is(err, errno.ERR_NO_SERVICES_IN_TOPOLOGY) {
		return true
	}
	return false
}

func checkDiff(dingocli *cli.DingoCli, newData string) error {
	diffs, err := configure.DiffMonitor(dingocli, dingocli.Monitor().Monitor, newData)
	if err != nil && !skipError(err) {
		return err
	}

	for _, diff := range diffs {
		mc := diff.MonitorConfig
		switch diff.DiffType {
		case topology.DIFF_DELETE:
			fmt.Printf("Warning: delete service: %s.host[%s]\n", mc.GetRole(), mc.GetHost())
		case topology.DIFF_ADD:
			fmt.Printf("Warning: added service: %s.host[%s]\n", mc.GetRole(), mc.GetHost())
		}
	}
	return nil
}

func checkMonitor(dingocli *cli.DingoCli, data string, options commitOptions) error {
	if options.force {
		return nil
	}

	// 1) parse monitor configure
	_, err := configure.ParseMonitorInfo(dingocli, options.filename, configure.INFO_TYPE_FILE)
	if err != nil {
		return err
	}

	// 2) check wether add/delete service
	if len(dingocli.Monitor().Monitor) > 0 {
		err = checkDiff(dingocli, data)
		if err != nil {
			return err
		}
	}

	return nil
}

func readMonitor(dingocli *cli.DingoCli, options commitOptions) (string, error) {
	filename := options.filename
	if len(filename) == 0 {
		return "", nil
	} else if !utils.PathExist(filename) {
		return "", errno.ERR_TOPOLOGY_FILE_NOT_FOUND.
			F("%s: no such file", utils.AbsPath(filename))
	}

	data, err := utils.ReadFile(filename)
	if err != nil {
		return "", errno.ERR_READ_TOPOLOGY_FILE_FAILED.E(err)
	}

	oldData := dingocli.Monitor().Monitor
	if !options.slient {
		diff := utils.Diff(oldData, data)
		dingocli.WriteOutln("%s", diff)
	}
	return data, nil
}

func runCommit(dingocli *cli.DingoCli, options commitOptions) error {
	// 1) parse cluster topology
	_, err := configure.ParseMonitorInfo(dingocli, options.filename, configure.INFO_TYPE_FILE)
	if err != nil {
		return err
	}

	// 2) read monitor data, and print diff content
	data, err := readMonitor(dingocli, options)
	if err != nil {
		return err
	}

	// 3) check topology
	err = checkMonitor(dingocli, data, options)
	if err != nil {
		return err
	}

	if !options.force {
		// 4) confirm by user
		if pass := tui.ConfirmYes("Do you want to continue?"); !pass {
			dingocli.WriteOutln(tui.PromptCancelOpetation("commit monitor"))
			return errno.ERR_CANCEL_OPERATION
		}
	}

	// 5) update monitor in database
	err = dingocli.Storage().ReplaceMonitor(storage.Monitor{
		ClusterId: dingocli.ClusterId(),
		Monitor:   data,
	})

	// 6) print success prompt
	dingocli.WriteOutln("'%s' Monitor updated", dingocli.ClusterName())
	return err
}
