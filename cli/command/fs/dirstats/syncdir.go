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
	"github.com/dingodb/dingocli/internal/table"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

const (
	FS_SYNCDIR_EXAMPLE = `Examples:
# report mismatches between stored dir-stat counters and a fresh scan
$ dingo fs dirstats syncdir --fsname dingofs1 --path /dir1

# repair the mismatches
$ dingo fs dirstats syncdir --fsname dingofs1 --path /dir1 --repair`
)

type syncDirOptions struct {
	fsid   uint32
	path   string
	repair bool
	format string
}

func NewDirstatsSyncDirCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options syncDirOptions

	cmd := &cobra.Command{
		Use:     "syncdir [OPTIONS]",
		Short:   "reconcile a directory's stored usage counters with a fresh scan",
		Args:    utils.NoArgs,
		Example: FS_SYNCDIR_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid
			options.path = utils.GetStringFlag(cmd, utils.DINGOFS_PATH)
			options.repair = utils.GetBoolFlag(cmd, utils.DINGOFS_REPAIR)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runSyncDir(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddStringFlag(cmd, utils.DINGOFS_PATH, "Full path of the directory within the volume")
	utils.AddBoolFlag(cmd, utils.DINGOFS_REPAIR, "Repair mismatches found during the scan")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runSyncDir(cmd *cobra.Command, dingocli *cli.DingoCli, options syncDirOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// epoch + router
	epoch, epochErr := rpc.GetFsEpochByFsId(cmd, options.fsid)
	if epochErr != nil {
		return epochErr
	}
	if routerErr := rpc.InitFsMDSRouter(cmd, options.fsid); routerErr != nil {
		return routerErr
	}

	// resolve dir inode
	dirInodeId, inodeErr := rpc.GetDirPathInodeId(cmd, options.fsid, options.path, epoch)
	if inodeErr != nil {
		return inodeErr
	}

	mismatches, err := rpc.SyncDirStat(cmd, options.fsid, dirInodeId, options.repair, epoch)
	if err != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
		return outputErr(options.format, outputResult)
	}
	outputResult.Result = mismatches

	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	if len(mismatches) == 0 {
		fmt.Println("all dir-stats are consistent")
		return nil
	}

	header := []string{common.ROW_INODE_ID, common.ROW_WANT_INODES, common.ROW_WANT_LENGTH, common.ROW_GOT_INODES, common.ROW_GOT_LENGTH, common.ROW_RESULT}
	table.SetHeader(header)
	rows := make([]map[string]string, 0, len(mismatches))
	for _, m := range mismatches {
		result := "should be synced, re-run with --repair to fix it"
		if options.repair {
			result = "successfully synced"
		}
		if !m.GetFound() {
			result = result + " (no stored record)"
		}
		rows = append(rows, map[string]string{
			common.ROW_INODE_ID:    fmt.Sprintf("%d", m.GetIno()),
			common.ROW_WANT_INODES: fmt.Sprintf("%d", m.GetWantInodes()),
			common.ROW_WANT_LENGTH: fmt.Sprintf("%d", m.GetWantLength()),
			common.ROW_GOT_INODES:  fmt.Sprintf("%d", m.GetGotInodes()),
			common.ROW_GOT_LENGTH:  fmt.Sprintf("%d", m.GetGotLength()),
			common.ROW_RESULT:      result,
		})
	}
	table.AppendBulk(table.ListMap2ListSortByKeys(rows, header, []string{common.ROW_INODE_ID}))
	table.RenderWithNoData("all dir-stats are consistent")

	return nil
}
