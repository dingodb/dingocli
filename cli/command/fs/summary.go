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

package fs

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/table"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

const (
	FS_SUMMARY_EXAMPLE = `Examples:
$ dingo fs summary --fsname dingofs1 --path /dir1 --depth 3 --entries 20`

	SUMMARY_MAX_DEPTH   = uint32(10)
	SUMMARY_MAX_ENTRIES = uint32(100)
)

type summaryOptions struct {
	fsid    uint32
	path    string
	depth   uint32
	entries uint32
	strict  bool
	format  string
}

func NewFsSummaryCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options summaryOptions

	cmd := &cobra.Command{
		Use:     "summary [OPTIONS]",
		Short:   "show a hierarchical usage summary of a directory tree",
		Args:    utils.NoArgs,
		Example: FS_SUMMARY_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid
			options.path = utils.GetStringFlag(cmd, utils.DINGOFS_PATH)
			options.depth = utils.GetUint32Flag(cmd, utils.DINGOFS_DEPTH)
			options.entries = utils.GetUint32Flag(cmd, utils.DINGOFS_ENTRIES)
			options.strict = utils.GetBoolFlag(cmd, utils.DINGOFS_STRICT)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runSummary(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddStringFlag(cmd, utils.DINGOFS_PATH, "Full path of the directory within the volume")
	utils.AddUint32Flag(cmd, utils.DINGOFS_DEPTH, "Tree depth to expand (0-10)")
	utils.AddUint32Flag(cmd, utils.DINGOFS_ENTRIES, "Top-N entries per level (0-100)")
	utils.AddBoolFlag(cmd, utils.DINGOFS_STRICT, "Use an authoritative dentry scan instead of maintained counters")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runSummary(cmd *cobra.Command, dingocli *cli.DingoCli, options summaryOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// clamp depth/entries
	depth := options.depth
	if depth > SUMMARY_MAX_DEPTH {
		fmt.Printf("depth %d exceeds max %d, clamped\n", depth, SUMMARY_MAX_DEPTH)
		depth = SUMMARY_MAX_DEPTH
	}
	entries := options.entries
	if entries > SUMMARY_MAX_ENTRIES {
		fmt.Printf("entries %d exceeds max %d, clamped\n", entries, SUMMARY_MAX_ENTRIES)
		entries = SUMMARY_MAX_ENTRIES
	}

	// epoch + router
	epoch, epochErr := rpc.GetFsEpochByFsId(cmd, options.fsid)
	if epochErr != nil {
		return epochErr
	}
	if routerErr := rpc.InitFsMDSRouter(cmd, options.fsid); routerErr != nil {
		return routerErr
	}

	// resolve dir inode
	dirInodeId, inodeErr := rpc.GetDirPathInodeId(cmd, options.fsid, options.path, epoch)
	if inodeErr != nil {
		return inodeErr
	}

	// the mds has no server-side tree summary; aggregate the subtree client-side
	// (mirrors dingo-mds-client). fast path uses maintained counters unless
	// --strict was requested or the fs has dir-stats disabled.
	useFast, fastErr := useFastPath(cmd, options.fsid, options.strict)
	if fastErr != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(fastErr.Error())
		return outputErr(options.format, outputResult)
	}
	rootName := options.path
	if rootName == "" {
		rootName = "/"
	}
	tree, err := rpc.WalkDirTree(cmd, options.fsid, dirInodeId, rootName, useFast, int(depth), epoch)
	if err != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.S(err.Error())
		return outputErr(options.format, outputResult)
	}

	// collapse each expanded level to its top-N children by length
	collapseTopN(tree, int(entries))

	if options.format == "json" {
		outputResult.Result = tree
		return output.OutputJson(outputResult)
	}

	header := []string{common.ROW_PATH, common.ROW_LENGTH, common.ROW_DIRS, common.ROW_FILES}
	table.SetHeader(header)
	rows := make([][]string, 0)
	flattenDirTree(tree, 0, &rows)
	table.AppendBulk(rows)
	table.RenderWithNoData("no data")

	return nil
}

// collapseTopN keeps only the top-N children (by length, descending) at each
// level, merging the remainder into a synthetic "..." node. topN <= 0 keeps all.
func collapseTopN(node *rpc.DirTreeNode, topN int) {
	if node == nil {
		return
	}
	for _, child := range node.Children {
		collapseTopN(child, topN)
	}
	if topN <= 0 || len(node.Children) <= topN {
		return
	}
	sort.SliceStable(node.Children, func(i, j int) bool {
		return node.Children[i].Length > node.Children[j].Length
	})
	rest := node.Children[topN:]
	var restFiles, restDirs, restLength uint64
	for _, r := range rest {
		restFiles += r.Files
		restDirs += r.Dirs
		restLength += r.Length
	}
	node.Children = append(node.Children[:topN:topN], &rpc.DirTreeNode{
		Name:   "...",
		Files:  restFiles,
		Dirs:   restDirs,
		Length: restLength,
	})
}

// flattenDirTree renders a DirTreeNode into indented rows depth-first.
func flattenDirTree(node *rpc.DirTreeNode, indent int, rows *[][]string) {
	if node == nil {
		return
	}
	name := node.Name
	if name == "" {
		name = "/"
	}
	*rows = append(*rows, []string{
		strings.Repeat("  ", indent) + name,
		humanize.IBytes(node.Length),
		fmt.Sprintf("%d", node.Dirs),
		fmt.Sprintf("%d", node.Files),
	})
	for _, child := range node.Children {
		flattenDirTree(child, indent+1, rows)
	}
}
