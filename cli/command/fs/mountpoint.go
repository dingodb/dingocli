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
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

const (
	FS_MOUNTPOINT_EXAMPLE = `Examples:
   $ dingo fs mountpoint`
)

type mountpointOptions struct {
	format string
}

func NewFsMountpointCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options deleteOptions

	cmd := &cobra.Command{
		Use:     "mountpoint [OPTIONS] ",
		Short:   "list all mountpoints in the cluster",
		Args:    utils.NoArgs,
		Example: FS_MOUNTPOINT_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)

			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			return runMountpoint(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddFormatFlag(cmd)
	utils.AddConfigFileFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runMountpoint(cmd *cobra.Command, dingocli *cli.DingoCli, options deleteOptions) error {
	// new rpc
	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "ListFsInfo")
	if err != nil {
		return err
	}

	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// set request info
	listRpc := &rpc.ListFsRpc{
		Info:    mdsRpc,
		Request: &mds.ListFsInfoRequest{},
	}
	// get rpc result
	var result *mds.ListFsInfoResponse
	response, rpcError := rpc.GetRpcResponse(listRpc.Info, listRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		outputResult.Error = rpcError
	} else {
		result = response.(*mds.ListFsInfoResponse)
		if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
			outputResult.Error = errno.ERR_RPC_FAILED.S(mdsErr.String())
		}
		outputResult.Result = result
	}

	// print result
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	if outputResult.Error.GetCode() != errno.ERR_OK.GetCode() {
		return outputResult.Error
	}

	// set table header
	header := []string{common.ROW_FS_ID, common.ROW_FS_NAME, common.ROW_FS_CLIENTID, common.ROW_MOUNTPOINT, common.ROW_FS_CTO}
	table.SetHeader(header)
	// fill table
	var number_mountpoints int = 0
	rows := make([]map[string]string, 0)
	for _, fsInfo := range result.GetFsInfos() {
		if len(fsInfo.GetMountPoints()) == 0 {
			continue
		}

		for _, mountPoint := range fsInfo.GetMountPoints() {
			number_mountpoints++

			row := make(map[string]string)

			row[common.ROW_FS_ID] = fmt.Sprintf("%d", fsInfo.GetFsId())
			row[common.ROW_FS_NAME] = fsInfo.GetFsName()
			row[common.ROW_FS_CLIENTID] = mountPoint.GetClientId()
			row[common.ROW_MOUNTPOINT] = fmt.Sprintf("%s:%d:%s", mountPoint.GetIp(), mountPoint.GetPort(), mountPoint.GetPath())
			row[common.ROW_FS_CTO] = fmt.Sprintf("%v", mountPoint.GetCto())

			rows = append(rows, row)

		}

	}

	list := table.ListMap2ListSortByKeys(rows, header, []string{common.ROW_FS_ID})
	table.AppendBulk(list)
	table.RenderWithNoData("no mountpoint in the cluster")

	return nil
}
