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

package hosts

import (
	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/configure/hosts"
	"github.com/dingodb/dingocli/internal/tui"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

type listOptions struct {
	verbose bool
	labels  []string
}

func NewListCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options listOptions

	cmd := &cobra.Command{
		Use:     "ls [OPTIONS]",
		Aliases: []string{"list"},
		Short:   "List hosts",
		Args:    cliutil.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.BoolVarP(&options.verbose, "verbose", "v", false, "Verbose output for hosts")
	hosts.AddLabelFlag(flags, &options.labels, ":")

	return cmd
}

func runList(dingocli *cli.DingoCli, options listOptions) error {
	var hcs []*hosts.HostConfig
	var err error
	data := dingocli.Hosts()
	if len(data) > 0 {
		hcs, err = hosts.Filter(data, options.labels) // filter hosts
		if err != nil {
			return err
		}
	}

	output := tui.FormatHosts(hcs, options.verbose)
	dingocli.WriteOut(output)
	return nil
}
