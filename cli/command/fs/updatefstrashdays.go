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

package fs

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
	FS_UPDATE_TRASH_DAYS_EXAMPLE = `Examples:
# set trash retention to 7 days
$ dingo fs updatefstrashdays --fsname dingofs1 --trashdays 7

# disable the trash (0 empties the existing trash)
$ dingo fs updatefstrashdays --fsname dingofs1 --trashdays 0`
)

type updateTrashDaysOptions struct {
	fsname    string
	trashdays uint32
	format    string
}

func NewFsUpdateFsTrashDaysCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options updateTrashDaysOptions

	cmd := &cobra.Command{
		Use:     "updatefstrashdays [OPTIONS]",
		Short:   "update the trash retention days of a filesystem (0 = disabled)",
		Args:    utils.NoArgs,
		Example: FS_UPDATE_TRASH_DAYS_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsname, err := rpc.GetFsName(cmd)
			if err != nil {
				return err
			}
			options.fsname = fsname
			options.trashdays = utils.GetUint32Flag(cmd, utils.DINGOFS_TRASH_DAYS)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runUpdateTrashDays(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddUint32Flag(cmd, utils.DINGOFS_TRASH_DAYS, "Trash retention days, 0 = disabled")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runUpdateTrashDays(cmd *cobra.Command, dingocli *cli.DingoCli, options updateTrashDaysOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// read current fs info by name
	fsInfo, err := rpc.GetFsInfo(cmd, 0, options.fsname)
	if err != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
		return outputErr(options.format, outputResult)
	}
	if fsInfo.GetFsId() == 0 {
		outputResult.Error = errno.ERR_RPC_FAILED.S(fmt.Sprintf("not found fs %s", options.fsname))
		return outputErr(options.format, outputResult)
	}

	// flip only trash_days and send the full FsInfo back
	fsInfo.TrashDays = options.trashdays
	if updErr := rpc.UpdateFsInfo(cmd, options.fsname, fsInfo); updErr != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(updErr.Error())
		return outputErr(options.format, outputResult)
	}

	outputResult.Result = map[string]interface{}{
		common.ROW_FS_NAME:       options.fsname,
		utils.DINGOFS_TRASH_DAYS: options.trashdays,
	}
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	fmt.Printf("Successfully update filesystem %s trash_days to %d\n", options.fsname, options.trashdays)

	return nil
}
