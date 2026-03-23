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
	QUOTA_CHECK_EXAMPLE = `Examples:
   $ dingo fs quota check--fsname fs1 --path /dir1`
)

type checkOptions struct {
	fsid    uint32
	path    string
	threads uint32
	format  string
	repair  bool
}

func NewQuotaCheckCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options checkOptions

	cmd := &cobra.Command{
		Use:     "check [OPTIONS]",
		Short:   "check directory quota",
		Args:    utils.NoArgs,
		Example: QUOTA_CHECK_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid

			options.path = utils.GetStringFlag(cmd, "path")

			options.threads, err = cmd.Flags().GetUint32("threads")
			if err != nil {
				return err
			}

			options.repair, err = cmd.Flags().GetBool("repair")
			if err != nil {
				return err
			}

			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runCheck(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddStringRequiredFlag(cmd, "path", "full path of the directory within the volume")
	cmd.Flags().Uint32("threads", 8, "Number of check threads")
	cmd.Flags().Bool("repair", false, "Repair inconsistent quota")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runCheck(cmd *cobra.Command, dingocli *cli.DingoCli, options checkOptions) error {
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

	// get filesystem usage
	dirUsedBytes, dirUsedInodes, getErr := rpc.GetDirectorySizeAndInodes(cmd, options.fsid, dirInodeId, false, epoch, options.threads)
	if getErr != nil {
		return getErr
	}

	fsQuota := result.GetQuota()
	checkResult, ok := utils.CheckQuota(fsQuota.GetMaxBytes(), fsQuota.GetUsedBytes(), fsQuota.GetMaxInodes(), fsQuota.GetUsedInodes(), dirUsedBytes, dirUsedInodes)

	if options.repair && !ok { // inconsistent and need to repair
		// new prc
		mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "SetDirQuota")
		if err != nil {
			return err
		}
		// set request info
		request := &mds.SetDirQuotaRequest{
			Context: &mds.Context{Epoch: epoch, IsBypassCache: true},
			FsId:    options.fsid,
			Ino:     dirInodeId,
			Quota:   &mds.Quota{UsedBytes: dirUsedBytes, UsedInodes: dirUsedInodes},
		}

		setDirQuotaRpc := &rpc.SetDirQuotaRpc{
			Info:    mdsRpc,
			Request: request,
		}

		// get rpc result
		response, rpcError := rpc.GetRpcResponse(setDirQuotaRpc.Info, setDirQuotaRpc)
		if rpcError.GetCode() != errno.ERR_OK.GetCode() {
			return rpcError
		} else {
			result := response.(*mds.SetDirQuotaResponse)
			if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
				return errno.ERR_RPC_FAILED.S(mdsErr.String())
			}
		}

		fmt.Println("Successfully repair dir inconsistent quota")
	} else {
		header := []string{common.ROW_INODE_ID, common.ROW_NAME, common.ROW_CAPACITY, common.ROW_USED, common.ROW_REAL_USED, common.ROW_INODES, common.ROW_INODES_IUSED, common.ROW_INODES_REAL_IUSED, common.ROW_STATUS}
		table.SetHeader(header)

		row := map[string]string{
			common.ROW_INODE_ID:          fmt.Sprintf("%d", dirInodeId),
			common.ROW_NAME:              options.path,
			common.ROW_CAPACITY:          checkResult[0],
			common.ROW_USED:              checkResult[1],
			common.ROW_REAL_USED:         checkResult[2],
			common.ROW_INODES:            checkResult[3],
			common.ROW_INODES_IUSED:      checkResult[4],
			common.ROW_INODES_REAL_IUSED: checkResult[5],
			common.ROW_STATUS:            checkResult[6],
		}
		table.Append(table.Map2List(row, header))
		table.RenderWithNoData("no dir quota found")
	}

	return nil
}
