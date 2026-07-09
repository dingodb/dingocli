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
	"math"
	"strings"

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
	FS_CREATE_EXAMPLE = `Examples:
# store in s3
$ dingo fs create dingofs1 --storagetype s3 --s3.ak AK --s3.sk SK --s3.endpoint http://localhost:9000 --s3.bucketname dingofs-bucket

# store in rados
$ dingo fs create dingofs1 --storagetype rados --rados.username admin --rados.key AQDg3Y2h --rados.mon 10.220.32.1:3300,10.220.32.2:3300,10.220.32.3:3300 --rados.poolname pool1 --rados.clustername ceph
`
)

type createOptions struct {
	// basic
	fsid        uint32
	fsname      string
	blocksize   uint64
	chunksize   uint64
	storagetype string
	fstype      mds.FsType
	fsextra     mds.FsExtra

	// s3 options
	ak         string
	sk         string
	endpoint   string
	bucketname string

	// rados options
	key         string
	mon         string
	username    string
	poolname    string
	clustername string

	mdsnum        uint32
	partitiontype mds.PartitionType

	// extra options
	trashdays           uint32
	immediatetrashquota bool
	enableuidgidmap     bool
	enabledirstats      bool

	format string
}

func NewFsCreateCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options createOptions

	cmd := &cobra.Command{
		Use:     "create FSNAME [OPTIONS]",
		Short:   "create fs in cluster",
		Args:    utils.ExactArgs(1),
		Example: FS_CREATE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			// fsname
			options.fsname = args[0]
			//fsid
			options.fsid = utils.GetUint32Flag(cmd, utils.DINGOFS_FSID)
			// block size
			blocksizeStr := utils.GetStringFlag(cmd, utils.DINGOFS_BLOCKSIZE)
			blocksize, err := humanize.ParseBytes(blocksizeStr)
			if err != nil {
				return fmt.Errorf("invalid blocksize: %s", blocksizeStr)
			}
			options.blocksize = blocksize
			// chunk size
			chunksizeStr := utils.GetStringFlag(cmd, utils.DINGOFS_CHUNKSIZE)
			chunksize, err := humanize.ParseBytes(chunksizeStr)
			if err != nil {
				return fmt.Errorf("invalid chunksize: %s", chunksizeStr)
			}
			options.chunksize = chunksize
			//storage type
			storagetypeStr := strings.ToUpper(utils.GetStringFlag(cmd, utils.DINGOFS_STORAGETYPE))
			switch storagetypeStr {
			case "S3":
				options.ak = utils.GetStringFlag(cmd, utils.DINGOFS_S3_AK)
				options.sk = utils.GetStringFlag(cmd, utils.DINGOFS_S3_SK)
				options.endpoint = utils.GetStringFlag(cmd, utils.DINGOFS_S3_ENDPOINT)
				options.bucketname = utils.GetStringFlag(cmd, utils.DINGOFS_S3_BUCKETNAME)
				err := SetS3Info(&options)
				if err != nil {
					return err
				}
			case "RADOS":
				options.username = utils.GetStringFlag(cmd, utils.DINGOFS_RADOS_USERNAME)
				options.key = utils.GetStringFlag(cmd, utils.DINGOFS_RADOS_KEY)
				options.mon = utils.GetStringFlag(cmd, utils.DINGOFS_RADOS_MON)
				options.poolname = utils.GetStringFlag(cmd, utils.DINGOFS_RADOS_POOLNAME)
				options.clustername = utils.GetStringFlag(cmd, utils.DINGOFS_RADOS_CLUSTERNAME)
				err := SetRadosInfo(&options)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid storage type: %s", storagetypeStr)
			}
			// partition type
			partitionTypeStr := strings.ToUpper(utils.GetStringFlag(cmd, utils.DINGOFS_PARTITION_TYPE))
			switch partitionTypeStr {
			case "HASH":
				options.partitiontype = mds.PartitionType_PARENT_ID_HASH_PARTITION
			case "MONOLITHIC":
				options.partitiontype = mds.PartitionType_MONOLITHIC_PARTITION
			default:
				return fmt.Errorf("invalid partition type: %s", partitionTypeStr)
			}
			// mdsnum
			options.mdsnum = utils.GetUint32Flag(cmd, utils.DINGOFS_MDS_NUM)
			// extra options
			options.trashdays = utils.GetUint32Flag(cmd, utils.DINGOFS_TRASH_DAYS)
			options.immediatetrashquota = utils.GetBoolFlag(cmd, utils.DINGOFS_IMMEDIATE_TRASH_QUOTA)
			options.enableuidgidmap = utils.GetBoolFlag(cmd, utils.DINGOFS_ENABLE_UID_GID_MAP)
			options.enabledirstats = utils.GetBoolFlag(cmd, utils.DINGOFS_ENABLE_DIR_STATS)
			//format
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			return runCreate(cmd, dingocli, &options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddUint32Flag(cmd, utils.DINGOFS_FSID, "Specify filesystem id")
	utils.AddStringFlag(cmd, utils.DINGOFS_BLOCKSIZE, "Filesystem block size")
	utils.AddStringFlag(cmd, utils.DINGOFS_CHUNKSIZE, "Filesystem chunk size")
	utils.AddStringFlag(cmd, utils.DINGOFS_STORAGETYPE, "Filesystem storage type, should be: s3, rados")
	utils.AddStringFlag(cmd, utils.DINGOFS_PARTITION_TYPE, "Filesystem partition type, should be: hash, monolithic")
	utils.AddUint32Flag(cmd, utils.DINGOFS_MDS_NUM, "Specify filesystem expect mds numbers, only used for hash partition")
	utils.AddUint32Flag(cmd, utils.DINGOFS_TRASH_DAYS, "Trash retention days, 0 = disabled")
	utils.AddBoolFlag(cmd, utils.DINGOFS_IMMEDIATE_TRASH_QUOTA, "Debit per-dir quota immediately at trash-move time")
	utils.AddBoolFlag(cmd, utils.DINGOFS_ENABLE_UID_GID_MAP, "Enable uid/gid map for the filesystem")
	utils.AddBoolFlag(cmd, utils.DINGOFS_ENABLE_DIR_STATS, "Enable per-directory usage statistics")

	utils.AddStringFlag(cmd, utils.DINGOFS_S3_AK, "S3 access key")
	utils.AddStringFlag(cmd, utils.DINGOFS_S3_SK, "S3 secret key")
	utils.AddStringFlag(cmd, utils.DINGOFS_S3_ENDPOINT, "S3 endpoint")
	utils.AddStringFlag(cmd, utils.DINGOFS_S3_BUCKETNAME, "S3 bucketname")

	utils.AddStringFlag(cmd, utils.DINGOFS_RADOS_KEY, "Rados user secret key")
	utils.AddStringFlag(cmd, utils.DINGOFS_RADOS_USERNAME, "Rados user name")
	utils.AddStringFlag(cmd, utils.DINGOFS_RADOS_MON, "Rados monitor host, should be like 10.220.32.1:3300,10.220.32.2:3300,10.220.32.3:3300")
	utils.AddStringFlag(cmd, utils.DINGOFS_RADOS_POOLNAME, "Rados pool name")
	utils.AddStringFlag(cmd, utils.DINGOFS_RADOS_CLUSTERNAME, "Rados cluster name")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddFormatFlag(cmd)
	utils.AddConfigFileFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runCreate(cmd *cobra.Command, dingocli *cli.DingoCli, options *createOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}
	// new rpc
	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "CreateFs")
	if err != nil {
		return err
	}
	request := mds.CreateFsRequest{
		FsName:              options.fsname,
		BlockSize:           options.blocksize,
		ChunkSize:           options.chunksize,
		FsType:              options.fstype,
		Owner:               "anonymous",
		Capacity:            math.MaxInt32,
		FsExtra:             &options.fsextra,
		PartitionType:       options.partitiontype,
		TrashDays:           options.trashdays,
		ImmediateTrashQuota: options.immediatetrashquota,
		EnableUidGidMap:     options.enableuidgidmap,
		EnableDirStats:      options.enabledirstats,
	}
	if options.fsid > 0 {
		request.FsId = options.fsid
	}
	if options.mdsnum > 0 {
		request.ExpectMdsNum = options.mdsnum
	}
	// set request info
	deleteRpc := &rpc.CreateFsRpc{
		Info:    mdsRpc,
		Request: &request,
	}

	// get rpc result
	var result *mds.CreateFsResponse
	response, rpcError := rpc.GetRpcResponse(deleteRpc.Info, deleteRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		outputResult.Error = rpcError
	} else {
		result = response.(*mds.CreateFsResponse)
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

	fmt.Printf("Successfully create filesystem %s, uuid: %s\n", options.fsname, result.GetFsInfo().GetUuid())

	return nil
}

func SetS3Info(options *createOptions) error {
	if len(options.ak) == 0 || len(options.sk) == 0 || len(options.endpoint) == 0 || len(options.bucketname) == 0 {
		return fmt.Errorf("s3 info is incomplete, please check s3.ak, s3.sk, s3.endpoint, s3.bucketname")
	}

	s3Info := &mds.S3Info{
		Ak:         options.ak,
		Sk:         options.sk,
		Endpoint:   options.endpoint,
		Bucketname: options.bucketname,
	}
	options.fsextra.S3Info = s3Info
	options.fstype = mds.FsType_S3

	return nil
}

func SetRadosInfo(options *createOptions) error {
	if len(options.username) == 0 || len(options.key) == 0 || len(options.mon) == 0 || len(options.poolname) == 0 {
		return fmt.Errorf("rados info is incomplete, please check rados.username, rados.key, rados.mon, rados.poolname")
	}
	if len(options.clustername) == 0 {
		options.clustername = "ceph"
	}

	radosInfo := &mds.RadosInfo{
		UserName:    options.username,
		Key:         options.key,
		MonHost:     options.mon,
		PoolName:    options.poolname,
		ClusterName: options.clustername,
	}
	options.fsextra.RadosInfo = radosInfo
	options.fstype = mds.FsType_RADOS

	return nil
}
