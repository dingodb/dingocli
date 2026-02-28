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

package monitor

import (
	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/configure"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/playbook"
	"github.com/dingodb/dingocli/internal/storage"
	"github.com/dingodb/dingocli/internal/tasks"
	"github.com/dingodb/dingocli/internal/utils"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	DEPLOY_EXAMPLE = `Examples:
	$ dingo monitor deploy -c monitor.yaml    # deploy monitor for current cluster`
)

var (
	MONITOR_DEPLOY_STEPS = []int{
		playbook.PULL_MONITOR_IMAGE,
		playbook.CREATE_MONITOR_CONTAINER,
		playbook.SYNC_MONITOR_ORIGIN_CONFIG,
		playbook.SYNC_MONITOR_ALT_CONFIG,
		playbook.CLEAN_CONFIG_CONTAINER,
		playbook.START_MONITOR_SERVICE,
		playbook.SYNC_GRAFANA_DASHBOARD,
		playbook.SYNC_HOSTS_MAPPING,
	}
)

type deployOptions struct {
	filename      string
	useLocalImage bool
}

/*
 * Deploy Steps:
 *   1) pull images(dingofs, node_exporter, prometheus, grafana)
 *   2) create container
 *   3) sync config
 *   4) start container
 *     4.1) start node_exporter container
 *     4.2) start prometheus container
 *     4.3) start grafana container
 */
func NewDeployCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options deployOptions

	cmd := &cobra.Command{
		Use:     "deploy [OPTIONS]",
		Short:   "Deploy monitor for current cluster",
		Args:    cliutil.NoArgs,
		Example: DEPLOY_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDeploy(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.StringVarP(&options.filename, "conf", "c", "monitor.yaml", "Specify monitor configuration file")
	flags.BoolVar(&options.useLocalImage, "local", false, "Use local image")
	return cmd
}

func genDeployPlaybook(dingocli *cli.DingoCli,
	mcs []*configure.MonitorConfig, options deployOptions) (*playbook.Playbook, error) {
	steps := MONITOR_DEPLOY_STEPS
	if options.useLocalImage {
		// remove PULL_MONITOR_IMAGE step
		for i, item := range steps {
			if item == playbook.PULL_MONITOR_IMAGE {
				steps = append(steps[:i], steps[i+1:]...)
				break
			}
		}
	}
	pb := playbook.NewPlaybook(dingocli)
	for _, step := range steps {
		if step == playbook.CLEAN_CONFIG_CONTAINER {
			pb.AddStep(&playbook.PlaybookStep{
				Type:    step,
				Configs: mcs,
				ExecOptions: tasks.ExecOptions{
					SilentMainBar: true,
					SilentSubBar:  true,
				},
			})
			continue
		}
		pb.AddStep(&playbook.PlaybookStep{
			Type:    step,
			Configs: mcs,
		})
	}
	return pb, nil
}

func runDeploy(dingocli *cli.DingoCli, options deployOptions) error {
	// 1) parse cluster topology and get services' hosts
	mcs, err := configure.ParseMonitorInfo(dingocli, options.filename, configure.INFO_TYPE_FILE)
	if err != nil {
		return err
	}

	// 2) save monitor data
	data, err := utils.ReadFile(options.filename)
	if err != nil {
		return errno.ERR_READ_MONITOR_FILE_FAILED.E(err)
	}
	err = dingocli.Storage().ReplaceMonitor(storage.Monitor{
		ClusterId: dingocli.ClusterId(),
		Monitor:   data,
	})
	if err != nil {
		return errno.ERR_REPLACE_MONITOR_FAILED.E(err)
	}

	// 4) generate deploy playbook
	pb, err := genDeployPlaybook(dingocli, mcs, options)
	if err != nil {
		return err
	}

	// 5) run playground
	err = pb.Run()
	if err != nil {
		return err
	}

	// 6) print success prompt
	dingocli.WriteOutln("")
	dingocli.WriteOutln(color.GreenString("Deploy monitor success ^_^"))
	return nil
}
