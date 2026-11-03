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

package cluster

import (
	"github.com/dingodb/dingocli/cli/cli"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

func NewClusterCommand(dingocli *cli.DingoCli) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "cluster",
		Short:   "Manage clusters",
		GroupID: "DEPLOY",
		Args:    cliutil.NoArgs,
		RunE:    cliutil.ShowHelp(dingocli.Err()),
	}

	cmd.AddCommand(
		NewAddCommand(dingocli),
		NewCheckoutCommand(dingocli),
		NewListCommand(dingocli),
		NewRemoveCommand(dingocli),
		NewRenameCommand(dingocli),
		NewStatusCommand(dingocli),
		NewStartCommand(dingocli),
		NewStopCommand(dingocli),
		NewRestartCommand(dingocli),
		NewDeployCommand(dingocli),
		NewUpgradeCommand(dingocli),
		NewScaleOutCommand(dingocli),
		NewScaleInCommand(dingocli),
		NewCleanCommand(dingocli),
		NewPrecheckCommand(dingocli),
	)
	return cmd
}
