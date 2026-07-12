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

package trash

import (
	"fmt"
	"os"
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
	TRASH_RESTORE_EXAMPLE = `Examples:
# tree-rebuild restore of two hour buckets (UTC)
$ dingo fs trash restore --fsname dingofs1 --hours 2026-04-05-14,2026-04-05-15

# put every entry back to its live original parent
$ dingo fs trash restore --fsname dingofs1 --hours 2026-04-05-14 --putback --restorethreads 10`
)

type restoreTrashOptions struct {
	fsid           uint32
	hours          []string
	putback        bool
	restorethreads uint32
	format         string
}

// hourResult holds the per-hour restore counters.
type hourResult struct {
	Hour     string `json:"hour"`
	Restored uint64 `json:"restored"`
	Skipped  uint64 `json:"skipped"`
	Failed   uint64 `json:"failed"`
}

func NewTrashRestoreCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options restoreTrashOptions

	cmd := &cobra.Command{
		Use:     "restore [OPTIONS]",
		Short:   "Restore entries from the trash (requires root)",
		Args:    utils.NoArgs,
		Example: TRASH_RESTORE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)
			output.SetShow(utils.GetBoolFlag(cmd, utils.VERBOSE))

			fsid, err := rpc.GetFsId(cmd)
			if err != nil {
				return err
			}
			options.fsid = fsid

			hoursStr := utils.GetStringFlag(cmd, utils.DINGOFS_HOURS)
			for _, h := range strings.Split(hoursStr, ",") {
				h = strings.TrimSpace(h)
				if h != "" {
					options.hours = append(options.hours, h)
				}
			}
			options.putback = utils.GetBoolFlag(cmd, utils.DINGOFS_PUT_BACK)
			options.restorethreads = utils.GetUint32Flag(cmd, utils.DINGOFS_RESTORE_THREADS)
			if options.restorethreads == 0 {
				options.restorethreads = 1
			}
			options.format = utils.GetStringFlag(cmd, utils.FORMAT)

			return runRestoreTrash(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().Uint32("fsid", 0, "Filesystem id")
	cmd.Flags().String("fsname", "", "Filesystem name")
	utils.AddStringFlag(cmd, utils.DINGOFS_HOURS, "Trash hour buckets to restore (UTC, YYYY-MM-DD-HH), comma-separated")
	utils.AddBoolFlag(cmd, utils.DINGOFS_PUT_BACK, "Put every entry back to its live original parent (default: tree-rebuild)")
	utils.AddUint32Flag(cmd, utils.DINGOFS_RESTORE_THREADS, "Number of file restore worker threads")

	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)
	utils.AddFormatFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	return cmd
}

func runRestoreTrash(cmd *cobra.Command, dingocli *cli.DingoCli, options restoreTrashOptions) error {
	outputResult := &common.OutputResult{
		Error: errno.ERR_OK,
	}

	// only root can restore from trash (server enforces uid == 0)
	if os.Geteuid() != 0 {
		return fmt.Errorf("only root can restore files from trash")
	}
	if len(options.hours) == 0 {
		return fmt.Errorf("at least one hour bucket (YYYY-MM-DD-HH, UTC) is required via --hours")
	}

	// epoch + router
	epoch, epochErr := rpc.GetFsEpochByFsId(cmd, options.fsid)
	if epochErr != nil {
		return epochErr
	}
	if routerErr := rpc.InitFsMDSRouter(cmd, options.fsid); routerErr != nil {
		return routerErr
	}

	results := make([]hourResult, 0, len(options.hours))
	for _, hour := range options.hours {
		res := restoreHour(cmd, options, hour, epoch)
		results = append(results, res)
	}
	outputResult.Result = results

	if options.format == "json" {
		return output.OutputJson(outputResult)
	}

	for _, r := range results {
		fmt.Printf("restore trash %s: restored=%d skipped=%d failed=%d\n", r.Hour, r.Restored, r.Skipped, r.Failed)
	}

	return nil
}

// restoreHour restores a single hour bucket, mirroring DoRestoreHour: directories
// are restored serially in topological order, then files in parallel.
func restoreHour(cmd *cobra.Command, options restoreTrashOptions, hour string, epoch uint64) hourResult {
	res := hourResult{Hour: hour}

	// validate hour format; warn and skip rather than abort the whole run
	if utils.ParseTrashBucketName(hour) == 0 {
		fmt.Printf("invalid hour format '%s', expected YYYY-MM-DD-HH (UTC), skipped\n", hour)
		return res
	}

	// .trash is synthesized client-side, so look up the hour bucket under the
	// trash root inode directly.
	bucketInode, err := rpc.Lookup(cmd, options.fsid, common.TRASHINODEID, hour, epoch)
	if err != nil {
		fmt.Printf("lookup .trash/%s fail: %s\n", hour, err.Error())
		return res
	}
	bucketIno := bucketInode.GetIno()

	entries, listErr := rpc.ListDentry(cmd, options.fsid, bucketIno, epoch)
	if listErr != nil {
		fmt.Printf("list .trash/%s fail: %s\n", hour, listErr.Error())
		return res
	}
	if len(entries) == 0 {
		return res
	}

	// partition into directories vs files
	var dirs, files []*mds.Dentry
	for _, d := range entries {
		if d.GetType() == mds.FileType_DIRECTORY {
			dirs = append(dirs, d)
		} else {
			files = append(files, d)
		}
	}

	// topologically order dirs so a parent is restored before its children
	// (a numeric ino sort is not a valid topo order under hash partitioning).
	dirs = topoOrderTrashDirs(dirs)

	// in tree-rebuild mode, only entries whose original parent was also trashed
	// (i.e. appears here as a directory) are restored.
	trashedDirInos := make(map[uint64]struct{})
	if !options.putback {
		for _, d := range dirs {
			trashedDirInos[d.GetIno()] = struct{}{}
		}
	}

	var restored, skipped, failed uint64

	// phase 1: restore directories serially
	for _, d := range dirs {
		restoreOne(cmd, options, bucketIno, d, trashedDirInos, epoch, &restored, &skipped, &failed)
	}

	// phase 2: restore files in parallel
	var wg sync.WaitGroup
	work := make(chan *mds.Dentry)
	for i := uint32(0); i < options.restorethreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range work {
				restoreOne(cmd, options, bucketIno, d, trashedDirInos, epoch, &restored, &skipped, &failed)
			}
		}()
	}
	for _, d := range files {
		work <- d
	}
	close(work)
	wg.Wait()

	res.Restored = atomic.LoadUint64(&restored)
	res.Skipped = atomic.LoadUint64(&skipped)
	res.Failed = atomic.LoadUint64(&failed)
	return res
}

// topoOrderTrashDirs orders trashed directories so each dir's original parent
// (parsed from its trash name) is restored before it. Mirrors the Kahn-style
// ordering in trash_restore.cc.
func topoOrderTrashDirs(dirs []*mds.Dentry) []*mds.Dentry {
	pending := make(map[uint64]struct{}, len(dirs))
	for _, d := range dirs {
		pending[d.GetIno()] = struct{}{}
	}

	remaining := append([]*mds.Dentry(nil), dirs...)
	ordered := make([]*mds.Dentry, 0, len(dirs))
	progress := true
	for len(remaining) > 0 && progress {
		progress = false
		next := remaining[:0]
		for _, d := range remaining {
			origParent := utils.ParseTrashEntryParent(d.GetName())
			if _, parentPending := pending[origParent]; !parentPending {
				delete(pending, d.GetIno())
				ordered = append(ordered, d)
				progress = true
			} else {
				next = append(next, d)
			}
		}
		remaining = next
	}
	// leftovers (unparseable names or a cycle): append as-is
	ordered = append(ordered, remaining...)

	return ordered
}

// restoreOne restores a single trash entry, updating the counters atomically.
func restoreOne(cmd *cobra.Command, options restoreTrashOptions, bucketIno uint64, dentry *mds.Dentry,
	trashedDirInos map[uint64]struct{}, epoch uint64, restored, skipped, failed *uint64) {
	origParent, _, _, ok := utils.ParseTrashEntryName(dentry.GetName())
	if !ok || origParent == 0 {
		fmt.Printf("skip unparseable trash entry '%s'\n", dentry.GetName())
		atomic.AddUint64(failed, 1)
		return
	}

	// tree-rebuild: only restore entries whose original parent was also trashed
	if !options.putback {
		if _, isTrashed := trashedDirInos[origParent]; !isTrashed {
			atomic.AddUint64(skipped, 1)
			return
		}
	}

	allowTrashParent := !options.putback

	// put_back of a directory carries whatever subtree was assembled inside it;
	// compute the carried totals so the server credits the live ancestor chain.
	var carriedBytes, carriedInodes uint64
	if options.putback && dentry.GetType() == mds.FileType_DIRECTORY {
		b, i, cErr := computeCarried(cmd, options.fsid, dentry.GetIno(), epoch)
		if cErr != nil {
			fmt.Printf("compute carried totals for '%s' fail: %s, skip restore\n", dentry.GetName(), cErr.Error())
			atomic.AddUint64(failed, 1)
			return
		}
		carriedBytes, carriedInodes = b, i
	}

	err := rpc.RestoreFromTrash(cmd, options.fsid, bucketIno, dentry.GetName(), origParent, allowTrashParent, carriedBytes, carriedInodes, epoch)
	if err != nil {
		fmt.Printf("restore '%s' fail: %s\n", dentry.GetName(), err.Error())
		atomic.AddUint64(failed, 1)
		return
	}
	atomic.AddUint64(restored, 1)
}

// computeCarried sums the dir-stat totals of a directory subtree, reading each
// level from its owner MDS. Mirrors ComputeCarried in trash_restore.cc.
func computeCarried(cmd *cobra.Command, fsId uint32, dirIno uint64, epoch uint64) (bytes uint64, inodes uint64, err error) {
	pending := []uint64{dirIno}
	for len(pending) > 0 {
		dir := pending[len(pending)-1]
		pending = pending[:len(pending)-1]

		dirStat, statErr := rpc.GetDirStat(cmd, fsId, dir, epoch)
		if statErr != nil {
			return 0, 0, statErr
		}
		bytes += uint64(dirStat.GetLength())
		inodes += uint64(dirStat.GetInodes())

		// descend the directory skeleton only; files are counted by dir stats
		entries, listErr := rpc.ListDentry(cmd, fsId, dir, epoch)
		if listErr != nil {
			return 0, 0, listErr
		}
		for _, d := range entries {
			if d.GetType() == mds.FileType_DIRECTORY {
				pending = append(pending, d.GetIno())
			}
		}
	}

	return bytes, inodes, nil
}
