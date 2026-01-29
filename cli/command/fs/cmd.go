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

package fs

import (
	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/cli/command/fs/config"
	"github.com/dingodb/dingocli/cli/command/fs/quota"
	"github.com/dingodb/dingocli/cli/command/fs/subpath"
	"github.com/dingodb/dingocli/cli/command/fs/warmup"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

func NewFSCommand(dingocli *cli.DingoCli) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "fs",
		Short:   "Manage filesystem",
		GroupID: "ADMIN",
		Args:    cliutil.NoArgs,
	}

	cmd.AddCommand(
		NewFsCreateCommand(dingocli),
		NewFsDeleteCommand(dingocli),
		NewFsListCommand(dingocli),
		NewFsQueryCommand(dingocli),
		NewFsMountpointCommand(dingocli),
		NewFsUsageCommand(dingocli),
		NewFsUmountCommand(dingocli),
		NewFsMountCommand(dingocli),
		config.NewFsCommand(dingocli),
		quota.NewQuotaCommand(dingocli),
		warmup.NewWarmupCommand(dingocli),
		subpath.NewSubpathCommand(dingocli),
		NewStatsCommand(dingocli),
	)

	return cmd
}
