/*
 * Copyright (c) 2026 dingofs org, Inc. All Rights Reserved
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

package dirstats

import (
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/table"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

const (
	FS_INFO_EXAMPLE = `Examples:
# directory usage (fast, dir-stats counters)
$ dingo fs dirstats info --fsname dingofs1 --path /dir1

# directory usage, authoritative single-level dentry scan
$ dingo fs dirstats info --fsname dingofs1 --path /dir1 --strict

# directory usage, recursive subtree aggregation
$ dingo fs dirstats info --fsname dingofs1 --path /dir1 --recursive

# file object layout (default) or raw slices (--raw)
$ dingo fs dirstats info --fsname dingofs1 --path /dir1/file.bin
$ dingo fs dirstats info --fsname dingofs1 --path /dir1/file.bin --raw`
)

type infoOptions struct {
	fsid      uint32
	path      string
	recursive bool
	strict    bool
	raw       bool
	format    string
}

func NewDirstatsInfoCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options infoOptions

	cmd := &cobra.Command{
		Use:     "info [OPTIONS]",
		Short:   "show usage of a directory or object layout of a file",
		Args:    utils.NoArgs,
		Example: FS_INFO_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid
			options.path = utils.GetStringFlag(cmd, utils.DINGOFS_PATH)
			options.recursive = utils.GetBoolFlag(cmd, utils.DINGOFS_RECURSIVE)
			options.strict = utils.GetBoolFlag(cmd, utils.DINGOFS_STRICT)
			options.raw = utils.GetBoolFlag(cmd, utils.DINGOFS_RAW)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runInfo(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddStringFlag(cmd, utils.DINGOFS_PATH, "Full path of the directory or file within the volume")
	utils.AddBoolFlag(cmd, utils.DINGOFS_RECURSIVE, "Recursively aggregate the whole subtree (directory only)")
	utils.AddBoolFlag(cmd, utils.DINGOFS_STRICT, "Use an authoritative dentry scan instead of maintained counters")
	utils.AddBoolFlag(cmd, utils.DINGOFS_RAW, "Show raw slices instead of objects (file only)")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runInfo(cmd *cobra.Command, dingocli *cli.DingoCli, options infoOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// epoch + router
	epoch, epochErr := rpc.GetFsEpochByFsId(cmd, options.fsid)
	if epochErr != nil {
		return epochErr
	}
	if routerErr := rpc.InitFsMDSRouter(cmd, options.fsid); routerErr != nil {
		return routerErr
	}

	// resolve target inode (path may end in a file)
	ino, parent, fileType, resolveErr := rpc.ResolvePathInode(cmd, options.fsid, options.path, epoch)
	if resolveErr != nil {
		return resolveErr
	}

	if fileType == mds.FileType_DIRECTORY {
		return runInfoDir(cmd, options, ino, epoch, outputResult)
	}

	return runInfoFile(cmd, options, ino, parent, epoch, outputResult)
}

func runInfoDir(cmd *cobra.Command, options infoOptions, ino uint64, epoch uint64, outputResult *common.OutputResult) error {
	var files, dirs, length uint64

	switch {
	case options.recursive:
		// recursive subtree aggregation done client-side (the mds has no
		// server-side tree summary). fast path uses maintained counters when the
		// fs has dir-stats enabled and --strict was not requested.
		useFast, fastErr := useFastPath(cmd, options.fsid, options.strict)
		if fastErr != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(fastErr.Error())
			return outputErr(options.format, outputResult)
		}
		tree, err := rpc.WalkDirTree(cmd, options.fsid, ino, options.path, useFast, 0, epoch)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return outputErr(options.format, outputResult)
		}
		files = tree.Files
		dirs = tree.Dirs
		length = tree.Length
	case options.strict:
		// authoritative single-level dentry scan
		entries, err := rpc.ListDentry(cmd, options.fsid, ino, epoch)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return outputErr(options.format, outputResult)
		}
		for _, entry := range entries {
			if entry.GetType() == mds.FileType_DIRECTORY {
				dirs++
			} else {
				files++
				inodeAttr, iErr := rpc.GetInode(cmd, options.fsid, entry.GetIno(), entry.GetParent(), epoch)
				if iErr != nil {
					outputResult.Error = errno.ERR_RPC_FAILED.S(iErr.Error())
					return outputErr(options.format, outputResult)
				}
				length += inodeAttr.GetLength()
			}
		}
	default:
		// fast path: maintained per-directory counters
		dirStat, err := rpc.GetDirStat(cmd, options.fsid, ino, epoch)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
			return outputErr(options.format, outputResult)
		}
		inodes := uint64(dirStat.GetInodes())
		dirs = uint64(dirStat.GetDirs())
		if inodes >= dirs {
			files = inodes - dirs
		}
		length = uint64(dirStat.GetLength())
	}

	row := map[string]string{
		common.ROW_INODE_ID: fmt.Sprintf("%d", ino),
		common.ROW_PATH:     options.path,
		common.ROW_TYPE:     "directory",
		common.ROW_FILES:    fmt.Sprintf("%d", files),
		common.ROW_DIRS:     fmt.Sprintf("%d", dirs),
		common.ROW_LENGTH:   fmt.Sprintf("%d", length),
	}
	outputResult.Result = row

	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	// human-readable length for the table (json keeps raw bytes)
	row[common.ROW_LENGTH] = humanize.IBytes(length)
	header := []string{common.ROW_INODE_ID, common.ROW_PATH, common.ROW_TYPE, common.ROW_FILES, common.ROW_DIRS, common.ROW_LENGTH}
	table.SetHeader(header)
	table.Append(table.Map2List(row, header))
	table.RenderWithNoData("no data")

	return nil
}

func runInfoFile(cmd *cobra.Command, options infoOptions, ino uint64, parent uint64, epoch uint64, outputResult *common.OutputResult) error {
	// file attributes (bypass cache for a consistent length)
	inodeAttr, err := rpc.GetInode(cmd, options.fsid, ino, parent, epoch)
	if err != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
		return outputErr(options.format, outputResult)
	}
	length := inodeAttr.GetLength()

	typeName := "file"
	if inodeAttr.GetType() == mds.FileType_SYM_LINK {
		typeName = "symlink"
	}

	var chunks []*mds.Chunk
	var chunkSize, blockSize uint64
	if inodeAttr.GetType() == mds.FileType_FILE && length > 0 {
		fsInfo, fsErr := rpc.GetFsInfo(cmd, options.fsid, "")
		if fsErr != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(fsErr.Error())
			return outputErr(options.format, outputResult)
		}
		chunkSize = fsInfo.GetChunkSize()
		blockSize = fsInfo.GetBlockSize()
		if chunkSize == 0 {
			outputResult.Error = errno.ERR_RPC_FAILED.S("invalid chunk size")
			return outputErr(options.format, outputResult)
		}
		chunkNum := uint32((length + chunkSize - 1) / chunkSize)
		sliceChunks, rsErr := rpc.ReadSliceAll(cmd, options.fsid, ino, parent, chunkNum, epoch)
		if rsErr != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.S(rsErr.Error())
			return outputErr(options.format, outputResult)
		}
		chunks = sliceChunks
	}

	header := map[string]string{
		common.ROW_INODE_ID: fmt.Sprintf("%d", ino),
		common.ROW_PATH:     options.path,
		common.ROW_TYPE:     typeName,
		common.ROW_FILES:    "1",
		common.ROW_DIRS:     "0",
		common.ROW_LENGTH:   fmt.Sprintf("%d", length),
	}

	if options.format == "json" {
		result := map[string]interface{}{"info": header}
		if options.raw {
			result["chunks"] = buildSliceRows(chunks)
		} else {
			result["objects"] = buildObjectRows(chunks, chunkSize, blockSize)
		}
		outputResult.Result = result
		return output.OutputJson(outputResult)
	}

	// info block
	fmt.Printf("inode: %d\npath: %s\ntype: %s\nlength: %s\n", ino, options.path, typeName, humanize.IBytes(length))

	if len(chunks) == 0 {
		return nil
	}

	if options.raw {
		fmt.Println("slices:")
		header := []string{common.ROW_CHUNK_INDEX, common.ROW_SLICE_ID, common.ROW_SIZE, common.ROW_OFFSET, common.ROW_LENGTH}
		table.SetHeader(header)
		rows := buildSliceRows(chunks)
		table.AppendBulk(table.ListMap2ListSortByKeys(rows, header, []string{}))
		table.RenderWithNoData("no slices")
	} else {
		fmt.Println("objects:")
		header := []string{common.ROW_CHUNK_INDEX, common.ROW_OBJECT_NAME, common.ROW_SIZE, common.ROW_POS}
		table.SetHeader(header)
		rows := buildObjectRows(chunks, chunkSize, blockSize)
		table.AppendBulk(table.ListMap2ListSortByKeys(rows, header, []string{}))
		table.RenderWithNoData("no objects")
	}

	return nil
}

func buildSliceRows(chunks []*mds.Chunk) []map[string]string {
	rows := make([]map[string]string, 0)
	for _, chunk := range chunks {
		for _, slice := range chunk.GetSlices() {
			rows = append(rows, map[string]string{
				common.ROW_CHUNK_INDEX: fmt.Sprintf("%d", chunk.GetIndex()),
				common.ROW_SLICE_ID:    fmt.Sprintf("%d", slice.GetId()),
				common.ROW_SIZE:        fmt.Sprintf("%d", slice.GetSize()),
				common.ROW_OFFSET:      fmt.Sprintf("%d", slice.GetPos()),
				common.ROW_LENGTH:      fmt.Sprintf("%d", slice.GetLen()),
			})
		}
	}
	return rows
}

func buildObjectRows(chunks []*mds.Chunk, chunkSize uint64, blockSize uint64) []map[string]string {
	rows := make([]map[string]string, 0)
	for _, chunk := range chunks {
		for _, slice := range chunk.GetSlices() {
			objects := utils.EnumerateBlockKeys(slice.GetId(), slice.GetPos(), slice.GetSize(), chunk.GetIndex(), chunkSize, blockSize)
			for _, obj := range objects {
				rows = append(rows, map[string]string{
					common.ROW_CHUNK_INDEX: fmt.Sprintf("%d", chunk.GetIndex()),
					common.ROW_OBJECT_NAME: obj.Name,
					common.ROW_SIZE:        fmt.Sprintf("%d", obj.Size),
					common.ROW_POS:         fmt.Sprintf("%d", obj.Pos),
				})
			}
		}
	}
	return rows
}

// useFastPath decides whether to read maintained dir-stat counters (fast) or
// do an authoritative dentry scan (strict). The fast path requires the fs to
// have dir-stats enabled and --strict not requested, mirroring the C++ client.
func useFastPath(cmd *cobra.Command, fsId uint32, strict bool) (bool, error) {
	if strict {
		return false, nil
	}
	fsInfo, err := rpc.GetFsInfo(cmd, fsId, "")
	if err != nil {
		return false, err
	}
	return fsInfo.GetEnableDirStats(), nil
}

// outputErr renders an error either as json or by returning the error code.
func outputErr(format string, outputResult *common.OutputResult) error {
	if format == "json" {
		return output.OutputJson(outputResult)
	}
	return outputResult.Error
}
