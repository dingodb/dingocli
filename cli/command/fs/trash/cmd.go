/*
 * Copyright (c) 2026 dingofs org, Inc. All Rights Reserved
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

package trash

import (
	"github.com/dingodb/dingocli/cli/cli"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

// NewTrashCommand builds the `dingo fs trash` parent subcommand that
// groups the trash management commands (restore / retention).
func NewTrashCommand(dingocli *cli.DingoCli) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trash",
		Short: "Trash management",
		Args:  cliutil.NoArgs,
	}

	cmd.AddCommand(
		NewTrashRestoreCommand(dingocli),
		NewTrashRetentionCommand(dingocli),
	)

	return cmd
}
