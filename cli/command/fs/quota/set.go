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
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

const (
	QUOTA_SET_EXAMPLE = `Examples:
   $ dingo fs quota set --fsname dingofs --capacity 10 --inodes 1000000`
)

type setOptions struct {
	fsid     uint32
	path     string
	capacity int64
	inodes   int64
	threads  uint32
	format   string
}

func NewQuotaSetCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options setOptions

	cmd := &cobra.Command{
		Use:     "set [OPTIONS]",
		Short:   "set directory quota",
		Args:    utils.NoArgs,
		Example: QUOTA_SET_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid

			options.threads, err = cmd.Flags().GetUint32("threads")
			if err != nil {
				return err
			}

			options.path = utils.GetStringFlag(cmd, "path")

			options.capacity, options.inodes, err = utils.GetQuotaValue(cmd)
			if err != nil {
				return err
			}

			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runSet(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	cmd.Flags().Uint64("capacity", 0, "Hard quota for usage space in GiB")
	cmd.Flags().Uint64("inodes", 0, "Hard quota for inodes")
	cmd.Flags().Uint32("threads", 8, "Number of threads calculate directory usage")
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

func runSet(cmd *cobra.Command, dingocli *cli.DingoCli, options setOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}
	// check flags values
	maxBytes, maxInodes, quotaErr := utils.GetQuotaValue(cmd)
	if quotaErr != nil {
		return quotaErr
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
	mdsRpc := rpc.CreateNewMdsRpcWithEndPoint(cmd, endpoint, "SetDirQuota")

	// get real used space
	dirUsedBytes, dirUsedInodes, getErr := rpc.GetDirectorySizeAndInodes(cmd, options.fsid, dirInodeId, false, epoch, options.threads)
	if getErr != nil {
		return getErr
	}

	// set request info
	request := &mds.SetDirQuotaRequest{
		Context: &mds.Context{Epoch: epoch},
		FsId:    options.fsid,
		Ino:     dirInodeId,
		Quota:   &mds.Quota{MaxBytes: maxBytes, MaxInodes: maxInodes, UsedBytes: dirUsedBytes, UsedInodes: dirUsedInodes},
	}
	setRpc := &rpc.SetDirQuotaRpc{
		Info:    mdsRpc,
		Request: request,
	}

	// get rpc result
	response, rpcError := rpc.GetRpcResponse(setRpc.Info, setRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		outputResult.Error = rpcError
	} else {
		result := response.(*mds.SetDirQuotaResponse)
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
	fmt.Printf("Successfully set directory[%s] quota, capacity: %s, inodes: %s\n", options.path, humanize.IBytes(uint64(options.capacity)), humanize.Comma(options.inodes))

	return nil
}
