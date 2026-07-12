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
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/table"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

const (
	FS_USAGE_EXAMPLE = `Examples:
   $ dingo fs usage`
)

type usageOptions struct {
	fsid     uint32
	fsname   string
	humanize bool
	threads  uint32
	format   string
}

func NewFsUsageCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options usageOptions

	cmd := &cobra.Command{
		Use:     "usage [OPTIONS]",
		Short:   "Get the filesystem usage",
		Args:    utils.NoArgs,
		Example: FS_USAGE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)

			options.fsid = utils.GetUint32Flag(cmd, utils.DINGOFS_FSID)
			options.fsname = utils.GetStringFlag(cmd, utils.DINGOFS_FSNAME)
			options.humanize = utils.GetBoolFlag(cmd, utils.DINGOFS_HUMANIZE)
			options.threads = utils.GetUint32Flag(cmd, utils.DINGOFS_THREADS)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			return runUsage(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddUint32Flag(cmd, utils.DINGOFS_FSID, "Filesystem id")
	utils.AddStringFlag(cmd, utils.DINGOFS_FSNAME, "Filesystem name")

	utils.AddUint32Flag(cmd, utils.DINGOFS_THREADS, "Number of threads")
	utils.AddBoolFlag(cmd, utils.DINGOFS_HUMANIZE, "Humanize display")
	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddFormatFlag(cmd)
	utils.AddConfigFileFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runUsage(cmd *cobra.Command, dingocli *cli.DingoCli, options usageOptions) error {

	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// filesystem info
	fsids := make([]uint32, 0)
	fsnames := make([]string, 0)
	epochs := make([]uint64, 0)

	if options.fsid == 0 && len(options.fsname) == 0 { // get all filesystem info
		fsInfos, err := rpc.ListFsInfo(cmd)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return err
		}

		for _, fsInfo := range fsInfos {
			if fsInfo.GetStatus() != mds.FsStatus_NORMAL {
				continue
			}
			fsids = append(fsids, fsInfo.GetFsId())
			fsnames = append(fsnames, fsInfo.GetFsName())
			epochs = append(epochs, rpc.GetFsEpochByFsInfo(fsInfo))
		}

	} else { // one filesystem
		fsid, err := rpc.GetFsId(cmd)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return err
		}

		fsname, err := rpc.GetFsName(cmd)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return err
		}

		epoch, err := rpc.GetFsEpochByFsId(cmd, fsid)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return err
		}

		fsids = append(fsids, fsid)
		fsnames = append(fsnames, fsname)
		epochs = append(epochs, epoch)
	}

	if len(fsids) == 0 {
		return fmt.Errorf("no fsid is set")
	}

	// get every fs usage
	rows := make([]map[string]string, 0)
	for idx, fsid := range fsids {
		// create router
		routerErr := rpc.InitFsMDSRouter(cmd, fsid)
		if routerErr != nil {
			return routerErr
		}
		row := make(map[string]string)
		//get real used space
		realUsedBytes, realUsedInodes, err := rpc.GetDirectorySizeAndInodes(cmd, fsid, common.ROOTINODEID, true, epochs[idx], options.threads)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.E(err)
			break
		}

		row[common.ROW_FS_ID] = fmt.Sprintf("%d", fsid)
		row[common.ROW_FS_NAME] = fsnames[idx]
		if options.humanize {
			row[common.ROW_USED] = humanize.IBytes(uint64(realUsedBytes))
			row[common.ROW_INODES_IUSED] = humanize.Comma(int64(realUsedInodes))
		} else {
			row[common.ROW_USED] = fmt.Sprintf("%d", realUsedBytes)
			row[common.ROW_INODES_IUSED] = fmt.Sprintf("%d", realUsedInodes)
		}

		rows = append(rows, row)
	}
	outputResult.Result = rows

	// print result
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	// set table header
	header := []string{common.ROW_FS_ID, common.ROW_FS_NAME, common.ROW_USED, common.ROW_INODES_IUSED}
	table.SetHeader(header)
	// fill table
	list := table.ListMap2ListSortByKeys(rows, header, []string{common.ROW_FS_ID})
	table.AppendBulk(list)
	table.RenderWithNoData("no fs in the cluster")

	return nil
}
