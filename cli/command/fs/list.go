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
	FS_LIST_EXAMPLE = `Examples:
   $ dingo fs list`
)

type listOptions struct {
	format string
}

func NewFsListCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options listOptions

	cmd := &cobra.Command{
		Use:     "list [OPTIONS]",
		Short:   "List all fs info",
		Args:    utils.NoArgs,
		Example: FS_LIST_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)

			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			return runList(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
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
	header := []string{common.ROW_FS_ID, common.ROW_FS_NAME, common.ROW_STATUS, common.ROW_BLOCKSIZE, common.ROW_CHUNK_SIZE, common.ROW_MDS_NUM, common.ROW_STORAGE_TYPE, common.ROW_STORAGE, common.ROW_MOUNT_NUM, common.ROW_UUID}
	table.SetHeader(header)
	// fill table
	rows := make([]map[string]string, 0)
	for _, fsInfo := range result.GetFsInfos() {
		row := make(map[string]string)
		row[common.ROW_FS_ID] = fmt.Sprintf("%d", fsInfo.GetFsId())
		row[common.ROW_FS_NAME] = fsInfo.GetFsName()
		row[common.ROW_STATUS] = fsInfo.GetStatus().String()
		row[common.ROW_BLOCKSIZE] = fmt.Sprintf("%d", fsInfo.GetBlockSize())
		row[common.ROW_CHUNK_SIZE] = fmt.Sprintf("%d", fsInfo.GetChunkSize())

		partitionType := fsInfo.GetPartitionPolicy().GetType()
		if partitionType == mds.PartitionType_PARENT_ID_HASH_PARTITION {
			row[common.ROW_STORAGE_TYPE] = fmt.Sprintf("%s(%s %d)", fsInfo.GetFsType().String(),
				utils.ConvertPbPartitionTypeToString(partitionType), fsInfo.GetPartitionPolicy().GetParentHash().GetBucketNum())
			row[common.ROW_MDS_NUM] = fmt.Sprintf("%d", len(fsInfo.GetPartitionPolicy().GetParentHash().GetDistributions()))
		} else {
			row[common.ROW_STORAGE_TYPE] = fmt.Sprintf("%s(%s)", fsInfo.GetFsType().String(), utils.ConvertPbPartitionTypeToString(partitionType))
			row[common.ROW_MDS_NUM] = "1"
		}

		row[common.ROW_STORAGE] = utils.ConvertFsExtraToString(fsInfo.GetExtra())
		row[common.ROW_MOUNT_NUM] = fmt.Sprintf("%d", len(fsInfo.GetMountPoints()))
		row[common.ROW_UUID] = fsInfo.GetUuid()

		rows = append(rows, row)
	}

	list := table.ListMap2ListSortByKeys(rows, header, []string{common.ROW_FS_ID})
	table.AppendBulk(list)
	table.RenderWithNoData("no fs in cluster")

	return nil
}
