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

package quota

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/table"
	"github.com/dingodb/dingocli/internal/utils"
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

const (
	QUOTA_LIST_EXAMPLE = `Examples:
   $ dingo fs quota list --fsname fs1`
)

type listOptions struct {
	fsid   uint32
	format string
}

func NewQuotaListCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options listOptions

	cmd := &cobra.Command{
		Use:     "list [OPTIONS]",
		Short:   "list all directory quota",
		Args:    utils.NoArgs,
		Example: QUOTA_LIST_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid

			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runList(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runList(cmd *cobra.Command, dingocli *cli.DingoCli, options listOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}
	// new prc
	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "LoadDirQuotas")
	if err != nil {
		return err
	}
	// get epoch id
	epoch, epochErr := rpc.GetFsEpochByFsId(cmd, options.fsid)
	if epochErr != nil {
		return epochErr
	}
	// create router
	routerErr := rpc.InitFsMDSRouter(cmd, options.fsid)
	if routerErr != nil {
		return routerErr
	}
	// set request info
	listQuotaRpc := &rpc.ListDirQuotaRpc{
		Info: mdsRpc,
		Request: &mds.LoadDirQuotasRequest{
			Context: &mds.Context{Epoch: epoch},
			FsId:    options.fsid},
	}
	// get rpc result
	var result *mds.LoadDirQuotasResponse
	response, rpcError := rpc.GetRpcResponse(listQuotaRpc.Info, listQuotaRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		outputResult.Error = rpcError
	} else {
		result = response.(*mds.LoadDirQuotasResponse)
		if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
			outputResult.Error = errno.ERR_RPC_FAILED.S(mdsErr.String())
		}
		outputResult.Result = result
	}

	// print result
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}
	if outputResult.Error != nil && outputResult.Error.GetCode() != errno.ERR_OK.GetCode() {
		return outputResult.Error
	}

	// set table header
	header := []string{common.ROW_INODE_ID, common.ROW_PATH, common.ROW_CAPACITY, common.ROW_USED, common.ROW_USED_PERCNET, common.ROW_INODES, common.ROW_INODES_IUSED, common.ROW_INODES_PERCENT}
	table.SetHeader(header)

	dirQuotas := result.GetQuotas()
	// fill table
	rows := make([]map[string]string, 0)
	for dirInode, quota := range dirQuotas {
		row := make(map[string]string)
		quotaValueSlice := utils.ConvertQuotaToHumanizeValue(uint64(quota.GetMaxBytes()), quota.GetUsedBytes(), uint64(quota.GetMaxInodes()), quota.GetUsedInodes())

		dirPath, _, dirErr := rpc.GetInodePath(cmd, options.fsid, dirInode, epoch)
		if errors.Is(dirErr, syscall.ENOENT) {
			continue
		}
		if dirErr != nil {
			return dirErr
		}
		if dirPath == "" { // directory may be deleted,not show
			continue
		}
		row[common.ROW_INODE_ID] = fmt.Sprintf("%d", dirInode)
		row[common.ROW_PATH] = dirPath
		row[common.ROW_CAPACITY] = quotaValueSlice[0]
		row[common.ROW_USED] = quotaValueSlice[1]
		row[common.ROW_USED_PERCNET] = quotaValueSlice[2]
		row[common.ROW_INODES] = quotaValueSlice[3]
		row[common.ROW_INODES_IUSED] = quotaValueSlice[4]
		row[common.ROW_INODES_PERCENT] = quotaValueSlice[5]
		rows = append(rows, row)
	}
	list := table.ListMap2ListSortByKeys(rows, header, []string{common.ROW_PATH})
	table.AppendBulk(list)
	table.RenderWithNoData("no directory quota found")

	return nil
}
