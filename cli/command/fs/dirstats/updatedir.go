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

package dirstats

import (
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

const (
	FS_UPDATEDIR_EXAMPLE = `Examples:
$ dingo fs dirstats updatedir --fsname dingofs1 --enabledirstats=true`
)

type updateDirOptions struct {
	fsname         string
	enabledirstats bool
	format         string
}

func NewDirstatsUpdateDirCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options updateDirOptions

	cmd := &cobra.Command{
		Use:     "updatedir [OPTIONS]",
		Short:   "Enable or disable per-directory usage statistics for a filesystem",
		Args:    utils.NoArgs,
		Example: FS_UPDATEDIR_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsname, err := rpc.GetFsName(cmd)
			if err != nil {
				return err
			}
			options.fsname = fsname
			options.enabledirstats = utils.GetBoolFlag(cmd, utils.DINGOFS_ENABLE_DIR_STATS)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runUpdateDir(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddBoolFlag(cmd, utils.DINGOFS_ENABLE_DIR_STATS, "Target value of enable_dir_stats")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runUpdateDir(cmd *cobra.Command, dingocli *cli.DingoCli, options updateDirOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// read current fs info by name (bypass the meta cache is not needed: this is
	// a one-shot read-modify-write)
	fsInfo, err := rpc.GetFsInfo(cmd, 0, options.fsname)
	if err != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
		return outputErr(options.format, outputResult)
	}
	if fsInfo.GetFsId() == 0 {
		outputResult.Error = errno.ERR_RPC_FAILED.S(fmt.Sprintf("not found fs %s", options.fsname))
		return outputErr(options.format, outputResult)
	}

	// flip only enable_dir_stats and send the full FsInfo back
	fsInfo.EnableDirStats = options.enabledirstats
	if updErr := rpc.UpdateFsInfo(cmd, options.fsname, fsInfo); updErr != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(updErr.Error())
		return outputErr(options.format, outputResult)
	}

	outputResult.Result = map[string]interface{}{
		common.ROW_FS_NAME:             options.fsname,
		utils.DINGOFS_ENABLE_DIR_STATS: options.enabledirstats,
	}
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	fmt.Printf("Successfully update filesystem %s enable_dir_stats to %v\n", options.fsname, options.enabledirstats)

	return nil
}
