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

package subpath

import (
	"fmt"
	"os"
	"path/filepath"

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
	SUBPATH_CREATE_EXAMPLE = `Examples:
   $ dingo subpath create --fsid 1 --path /path1
   $ dingo subpath create --fsname dingofs1 --path /path1`
)

const (
	DirectoryLength = 4096
	Mode            = 16877 // os.ModeDir | 0755
)

type InodeParam struct {
	fsId   uint32
	parent uint64
	length uint64
	uid    uint32
	gid    uint32
	mode   uint32
	rdev   uint64
	name   string
	epoch  uint64
}

type createOptions struct {
	fsid   uint32
	path   string
	name   string
	parent string
	uid    uint32
	gid    uint32
	format string
}

func NewSubpathCreateCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options createOptions

	cmd := &cobra.Command{
		Use:     "create [OPTIONS]",
		Short:   "create sub directory in filesystem",
		Args:    utils.ExactArgs(0),
		Example: SUBPATH_CREATE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			utils.ReadCommandConfig(cmd)

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid

			options.path = utils.GetStringFlag(cmd, "path")
			options.path = filepath.Clean(options.path)
			options.parent = filepath.Dir(options.path)
			options.name = filepath.Base(options.path)

			options.uid = utils.GetUint32Flag(cmd, utils.DINGOFS_SUBPATH_UID)
			options.gid = utils.GetUint32Flag(cmd, utils.DINGOFS_SUBPATH_GID)

			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runCreate(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddUint32Flag(cmd, utils.DINGOFS_FSID, "Filesystem id")
	utils.AddStringFlag(cmd, utils.DINGOFS_FSNAME, "Filesystem name")
	utils.AddStringRequiredFlag(cmd, "path", "Full path in filesystem")
	utils.AddUint32Flag(cmd, utils.DINGOFS_SUBPATH_UID, "Uid")
	utils.AddUint32Flag(cmd, utils.DINGOFS_SUBPATH_GID, "Gid")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runCreate(cmd *cobra.Command, dingocli *cli.DingoCli, options createOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}
	// get epoch id
	epoch, err := rpc.GetFsEpochByFsId(cmd, options.fsid)
	if err != nil {
		return err
	}
	// create router
	routerErr := rpc.InitFsMDSRouter(cmd, options.fsid)
	if routerErr != nil {
		return routerErr
	}
	// get path parent inodeid
	parentInodeId, inodeErr := rpc.GetDirPathInodeId(cmd, options.fsid, options.parent, epoch)
	if inodeErr != nil {
		return inodeErr
	}

	inodeParam := InodeParam{
		fsId:   options.fsid,
		parent: parentInodeId,
		length: DirectoryLength,
		uid:    options.uid,
		gid:    options.gid,
		mode:   Mode,
		rdev:   0,
		name:   options.name,
		epoch:  epoch,
	}

	exists, _ := checkPathIsExist(cmd, options, parentInodeId, epoch)
	if !exists {
		outputResult.Error, outputResult.Result = mkDir(cmd, inodeParam)
	}
	// print result
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	if outputResult.Error.GetCode() != errno.ERR_OK.GetCode() {
		return outputResult.Error
	}

	fmt.Printf("Successfully create directory: %s\n", options.path)

	return nil
}

func checkPathIsExist(cmd *cobra.Command, options createOptions, parentId uint64, epoch uint64) (bool, error) {
	entries, entErr := rpc.ListDentry(cmd, options.fsid, parentId, epoch)
	if entErr != nil {
		return false, entErr
	}
	for _, entry := range entries {
		if entry.GetName() == options.name {
			return true, nil
		}
	}

	return false, os.ErrNotExist
}

func mkDir(cmd *cobra.Command, inodeParam InodeParam) (*errno.ErrorCode, interface{}) {
	// new prc request
	endpoint := rpc.GetEndPoint(inodeParam.parent)
	mdsRpc := rpc.CreateNewMdsRpcWithEndPoint(cmd, endpoint, "MkDir")

	mkDirRpc := &rpc.MkDirRpc{
		Info: mdsRpc,
		Request: &mds.MkDirRequest{
			Context: &mds.Context{Epoch: inodeParam.epoch},
			FsId:    inodeParam.fsId,
			Name:    inodeParam.name,
			Length:  inodeParam.length,
			Uid:     inodeParam.uid,
			Gid:     inodeParam.gid,
			Mode:    inodeParam.mode,
			Parent:  inodeParam.parent,
		},
	}

	// get rpc result
	response, rpcError := rpc.GetRpcResponse(mkDirRpc.Info, mkDirRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return rpcError, response
	}
	result := response.(*mds.MkDirResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return errno.ERR_RPC_FAILED.S(mdsErr.String()), result
	}

	return errno.ERR_OK, result
}
