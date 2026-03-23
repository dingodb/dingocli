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
	QUOTA_GET_EXAMPLE = `Examples:
   $ dingo fs quota get --fsname fs1 --path /dir1`
)

type getOptions struct {
	fsid   uint32
	path   string
	format string
}

func NewQuotaGetCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options getOptions

	cmd := &cobra.Command{
		Use:     "get [OPTIONS]",
		Short:   "get directory quota",
		Args:    utils.NoArgs,
		Example: QUOTA_GET_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid

			options.path = utils.GetStringFlag(cmd, "path")
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runGet(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddStringRequiredFlag(cmd, "path", "full path of the directory within the volume")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runGet(cmd *cobra.Command, dingocli *cli.DingoCli, options getOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
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
	//get inodeid
	dirInodeId, inodeErr := rpc.GetDirPathInodeId(cmd, options.fsid, options.path, epoch)
	if inodeErr != nil {
		return inodeErr
	}

	// get quota
	_, result, err := GetDirQuotaData(cmd, options.fsid, dirInodeId, epoch)
	if err != nil {
		outputResult.Error = err
	}
	outputResult.Result = result

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

	dirQuota := result.GetQuota()
	quotaValueSlice := utils.ConvertQuotaToHumanizeValue(uint64(dirQuota.GetMaxBytes()), dirQuota.GetUsedBytes(), uint64(dirQuota.GetMaxInodes()), dirQuota.GetUsedInodes())
	// fill table
	row := map[string]string{
		common.ROW_INODE_ID:       fmt.Sprintf("%d", dirInodeId),
		common.ROW_PATH:           options.path,
		common.ROW_CAPACITY:       quotaValueSlice[0],
		common.ROW_USED:           quotaValueSlice[1],
		common.ROW_USED_PERCNET:   quotaValueSlice[2],
		common.ROW_INODES:         quotaValueSlice[3],
		common.ROW_INODES_IUSED:   quotaValueSlice[4],
		common.ROW_INODES_PERCENT: quotaValueSlice[5],
	}

	list := table.Map2List(row, header)
	table.Append(list)
	table.RenderWithNoData("no directory quota set")

	return nil
}

func GetDirQuotaData(cmd *cobra.Command, fsId uint32, dirInodeId uint64, epoch uint64) (*mds.GetDirQuotaRequest, *mds.GetDirQuotaResponse, *errno.ErrorCode) {
	endpoint := rpc.GetEndPoint(dirInodeId)
	// new prc
	mdsRpc := rpc.CreateNewMdsRpcWithEndPoint(cmd, endpoint, "GetDirQuota")

	// set request info
	request := &mds.GetDirQuotaRequest{
		Context:       &mds.Context{Epoch: epoch},
		FsId:          fsId,
		Ino:           dirInodeId,
		NotUseFsQuota: true,
	}
	getQuotaRpc := &rpc.GetDirQuotaRpc{
		Info:    mdsRpc,
		Request: request,
	}

	// get rpc result
	response, rpcError := rpc.GetRpcResponse(getQuotaRpc.Info, getQuotaRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, nil, rpcError
	} else {
		result := response.(*mds.GetDirQuotaResponse)
		if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
			return nil, nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
		}
		return request, result, nil
	}
}
