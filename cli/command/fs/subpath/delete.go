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
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

const (
	FS_DELETE_EXAMPLE = `Examples:
   $ dingo subpath delete --fsid 1 --path /path1
   $ dingo subpath delete --fsname dingofs1 --path /path1
   `
)

type deleteOptions struct {
	fsid    uint32
	path    string
	name    string
	parent  string
	threads uint32
	format  string
}

func NewSubpathDeleteCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options deleteOptions

	cmd := &cobra.Command{
		Use:     "delete [OPTIONS]",
		Short:   "delete sub directory in filesystem",
		Args:    utils.ExactArgs(0),
		Example: FS_DELETE_EXAMPLE,
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

			options.threads = utils.GetUint32Flag(cmd, utils.DINGOFS_THREADS)
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runDelete(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddUint32Flag(cmd, utils.DINGOFS_FSID, "Filesystem id")
	utils.AddStringFlag(cmd, utils.DINGOFS_FSNAME, "Filesystem name")
	utils.AddStringRequiredFlag(cmd, "path", "Full path in filesystem")
	utils.AddUint32Flag(cmd, utils.DINGOFS_THREADS, "Number of threads")

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
	var deleteInodes uint64 = 0
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	if strings.TrimSpace(options.name) == "/" {
		return fmt.Errorf("root directory can not be deleted")
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

	err, inodeId := GetInodeId(cmd, options, parentInodeId, epoch)
	if err != nil {
		outputResult.Error = errno.ERR_RPC_FAILED.E(err)
	} else {
		summary, err := deleteDirectory(cmd, options.fsid, epoch, parentInodeId, inodeId, options.name, options.threads)
		if err != nil {
			outputResult.Error = errno.ERR_RPC_FAILED.E(err)
		}
		deleteInodes = summary.Inodes
	}

	// print result
	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	if outputResult.Error.GetCode() != errno.ERR_OK.GetCode() {
		return outputResult.Error
	}

	fmt.Printf("Successfully delete directory: %s, deleteInodes: %d\n", options.path, deleteInodes)

	return nil
}

func GetInodeId(cmd *cobra.Command, options deleteOptions, parentId uint64, epoch uint64) (error, uint64) {
	entries, entErr := rpc.ListDentry(cmd, options.fsid, parentId, epoch)
	if entErr != nil {
		return entErr, 0
	}
	for _, entry := range entries {
		if entry.GetName() == options.name {
			return nil, entry.GetIno()
		}
	}

	return os.ErrNotExist, 0
}

func deleteDirectoryAndData(cmd *cobra.Command, fsId uint32, epoch uint64, parentInodeId uint64, dirInodeId uint64, name string, summary *common.Summary, concurrent chan struct{},
	ctx context.Context, cancel context.CancelFunc) error {
	var err error
	entries, entErr := rpc.ListDentry(cmd, fsId, dirInodeId, epoch)
	if entErr != nil {
		return entErr
	}

	var wg sync.WaitGroup
	var errCh = make(chan error, 1)
	for _, entry := range entries {
		if entry.GetType() != mds.FileType_DIRECTORY {
			err := rpc.DeleteFile(cmd, fsId, entry.GetParent(), entry.GetName(), epoch)
			if err != nil {
				return err
			}
			log.Printf("success delete file:[%d,%s]\n", entry.GetIno(), entry.GetName())

			atomic.AddUint64(&summary.Inodes, 1)
			continue
		}

		select {
		case err := <-errCh:
			cancel()
			return err
		case <-ctx.Done():
			return fmt.Errorf("cancel delete directory for other goroutine error")
		case concurrent <- struct{}{}:
			wg.Add(1)
			go func(e *mds.Dentry) {
				defer wg.Done()
				deleteErr := deleteDirectoryAndData(cmd, fsId, epoch, e.GetParent(), e.GetIno(), e.GetName(), summary, concurrent, ctx, cancel)
				<-concurrent
				if deleteErr != nil {
					select {
					case errCh <- deleteErr:
					default:
					}
				}
			}(entry)
		default:
			if deleteErr := deleteDirectoryAndData(cmd, fsId, epoch, entry.GetParent(), entry.GetIno(), entry.GetName(), summary, concurrent, ctx, cancel); deleteErr != nil {
				return deleteErr
			}
		}
	}
	// wait all subdirectory deleted
	wg.Wait()

	select {
	case err = <-errCh:
	default:
		// delete self
		err := rpc.DeleteDirectory(cmd, fsId, parentInodeId, name, epoch)
		if err != nil {
			return err
		}
		log.Printf("success delete directory:[%d,%s]\n", dirInodeId, name)
		atomic.AddUint64(&summary.Inodes, 1)
	}

	return err
}

func deleteDirectory(cmd *cobra.Command, fsId uint32, epoch uint64, parentInodeId uint64, dirInodeId uint64, name string, threads uint32) (*common.Summary, error) {
	log.Printf("start to delete directory[%s], inode[%d]\n", name, dirInodeId)
	summary := &common.Summary{Length: 0, Inodes: 0}
	concurrent := make(chan struct{}, threads)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	deleteErr := deleteDirectoryAndData(cmd, fsId, epoch, parentInodeId, dirInodeId, name, summary, concurrent, ctx, cancel)
	log.Printf("success delete directory:[%d,%s], TotalInodes[%d]\n", dirInodeId, name, summary.Inodes)
	if deleteErr != nil {
		return nil, deleteErr
	}

	return summary, nil
}
