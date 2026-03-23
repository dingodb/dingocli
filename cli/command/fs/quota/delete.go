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
	"github.com/dingodb/dingocli/internal/utils"
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

const (
	QUOTA_DELETE_EXAMPLE = `Examples:
   $ dingo fs quota delete --fsname dingofs --path /dir1`
)

type deleteOptions struct {
	fsid   uint32
	path   string
	format string
}

func NewQuotaDeleteCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options deleteOptions

	cmd := &cobra.Command{
		Use:     "delete [OPTIONS]",
		Short:   "delete directory quota",
		Args:    utils.NoArgs,
		Example: QUOTA_DELETE_EXAMPLE,
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

			return runDelete(cmd, dingocli, options)
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

func runDelete(cmd *cobra.Command, dingocli *cli.DingoCli, options deleteOptions) error {
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
	endpoint := rpc.GetEndPoint(dirInodeId)
	mdsRpc := rpc.CreateNewMdsRpcWithEndPoint(cmd, endpoint, "DeleteDirQuota")

	// set request info
	deleteRpc := &rpc.DeleteDirQuotaRpc{
		Info: mdsRpc,
		Request: &mds.DeleteDirQuotaRequest{
			Context: &mds.Context{Epoch: epoch},
			FsId:    options.fsid,
			Ino:     dirInodeId,
		},
	}

	// get rpc result
	response, rpcError := rpc.GetRpcResponse(deleteRpc.Info, deleteRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		outputResult.Error = rpcError
	} else {
		result := response.(*mds.DeleteDirQuotaResponse)
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
	fmt.Printf("Successfully delete directory[%s] quota\n", options.path)

	return nil
}
