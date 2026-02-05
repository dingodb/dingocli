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

package nfs

import (
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dingodb/dingocli/pkg/logger"
	"github.com/dingodb/dingocli/pkg/module"

	"github.com/spf13/cobra"
)

const (
	NFS_REMOVE_EXAMPLE = `Examples:
   $ dingo nfs remove /mnt/dingofs/export`
)

type removeOptions struct {
	shell       *module.Shell
	execOptions module.ExecOptions
	exportPath  string
}

func NewNFSRemoveCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options removeOptions

	cmd := &cobra.Command{
		Use:     "remove PATH",
		Short:   "remove nfs-ganesha export",
		Args:    utils.ExactArgs(1),
		Example: NFS_REMOVE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.exportPath = args[0]

			return runRemove(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	return cmd
}

func runRemove(cmd *cobra.Command, dingocli *cli.DingoCli, options removeOptions) error {

	options.shell = module.NewShell(nil)
	options.execOptions = module.ExecOptions{ExecWithSudo: true, ExecInLocal: true, ExecTimeoutSec: 10}

	// step 1: get export path inodeid
	inodeId, err := utils.GetInodeId(options.shell, options.execOptions, options.exportPath)
	if err != nil {
		return err
	}

	// step2: remove export config file
	configFileName := utils.GenerateFileName(inodeId, options.exportPath)
	options.shell.Remove(configFileName)
	options.shell.ClearOption().AddOption("-f")
	_, err = options.shell.Execute(options.execOptions)
	if err != nil {
		return fmt.Errorf("remove export config %s failed, err: %v", configFileName, err)
	}
	logger.Infof("remove export config %s ok", configFileName)

	// step 3: notify nfs-ganesha reload new config
	ganeshaPid, err := utils.GetGaneshaPID(options.shell, options.execOptions)
	if err != nil {
		return err
	}
	err = utils.NotifyGaneshaReLoadConfig(options.shell, options.execOptions, ganeshaPid)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully remove %s\n", options.exportPath)

	return nil
}
