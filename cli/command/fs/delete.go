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
	"github.com/dingodb/dingocli/internal/utils"
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

const (
	FS_DELETE_EXAMPLE = `Examples:
   $ dingo fs delete dingofs1`
)

type deleteOptions struct {
	fsname    string
	format    string
	noConfirm bool
}

func NewFsDeleteCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options deleteOptions

	cmd := &cobra.Command{
		Use:     "delete FSNAME [OPTIONS]",
		Short:   "Delete fs from cluster",
		Args:    utils.ExactArgs(1),
		Example: FS_DELETE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)

			options.fsname = args[0]
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)
			options.noConfirm = utils.GetBoolFlag(cmd, utils.DINGOFS_NOCONFIRM)

			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			return runDelete(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddBoolFlag(cmd, utils.DINGOFS_NOCONFIRM, "Do not confirm the command")
	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddFormatFlag(cmd)
	utils.AddConfigFileFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runDelete(cmd *cobra.Command, dingocli *cli.DingoCli, options deleteOptions) error {
	// new rpc
	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "DeleteFs")
	if err != nil {
		return err
	}

	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}
	// set request info
	deleteRpc := &rpc.DeleteFsRpc{
		Info: mdsRpc,
		Request: &mds.DeleteFsRequest{
			FsName: options.fsname,
		},
	}

	if !options.noConfirm && !utils.AskConfirmation(fmt.Sprintf("Are you sure to delete fs %s?", options.fsname), options.fsname) {
		return fmt.Errorf("abort delete fs")
	}

	// get rpc result
	response, rpcError := rpc.GetRpcResponse(deleteRpc.Info, deleteRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		outputResult.Error = rpcError
	} else {
		result := response.(*mds.DeleteFsResponse)
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
	fmt.Printf("Successfully delete filesystem %s\n", options.fsname)

	return nil
}
