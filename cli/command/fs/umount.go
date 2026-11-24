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
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/utils"
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

const (
	FS_UMOUNT_EXAMPLE = `Examples:
   $ dingo fs umount /mnt/dingofs
   $ dingo fs umount /mnt/dingofs --mdsaddr 10.220.69.6:7400`
	FS_UMOUNT_CLIENTID = "client-id"
)

type umountOptions struct {
	mountpoint string
	lazy       bool
	fsName     string
	clientId   string
}

type mdsMountPoint struct {
	fsName   string
	clientId string
	hostname string
	path     string
}

func NewFsUmountCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options umountOptions

	cmd := &cobra.Command{
		Use:     "umount MOUNTPOINT [OPTIONS]",
		Short:   "Umount filesystem",
		Args:    utils.ExactArgs(1),
		Example: FS_UMOUNT_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)

			options.mountpoint = args[0]
			options.fsName = utils.GetStringFlag(cmd, utils.DINGOFS_FSNAME)
			options.clientId, _ = cmd.Flags().GetString(FS_UMOUNT_CLIENTID)

			return runUmuont(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	cmd.Flags().BoolVarP(&options.lazy, "lazy", "l", false, "Lazy umount")
	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddStringFlag(cmd, utils.DINGOFS_FSNAME, "Specify filesystem name of the mountpoint")
	cmd.Flags().String(FS_UMOUNT_CLIENTID, "", "Specify client id of the mountpoint")
	utils.AddConfigFileFlag(cmd)
	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	return cmd
}

func runUmuont(cmd *cobra.Command, dingocli *cli.DingoCli, options umountOptions) error {
	flags := 0

	if options.lazy {
		flags = syscall.MNT_DETACH
	}

	if _, err := os.Stat(options.mountpoint); os.IsNotExist(err) {
		return reconcileStaleMountPoint(cmd, dingocli, options,
			fmt.Errorf("mountpoint does not exist: %s", options.mountpoint))
	}

	err := syscall.Unmount(options.mountpoint, flags)
	if err != nil {
		switch {
		case err == syscall.EINVAL:
			// The path is not a mountpoint: the local mount is already gone
			// (e.g. the client died before it could call umountfs, or a
			// previous umount already detached it). Still reconcile mds so a
			// retry can remove the stale mountpoint record.
			return reconcileStaleMountPoint(cmd, dingocli, options,
				fmt.Errorf("invalid mountpoint: %s", options.mountpoint))
		case err == syscall.EPERM:
			// use fusermount3  to umount
			umountErr := runFuseumount(options)
			if umountErr != nil {
				return fmt.Errorf("error unmounting: %v", umountErr)
			}
		case err == syscall.EBUSY:
			return fmt.Errorf("mountpoint %s is busy, try umount with lazy option", options.mountpoint)
		case err == syscall.ENOENT:
			return reconcileStaleMountPoint(cmd, dingocli, options,
				fmt.Errorf("mountpoint %s does not exist", options.mountpoint))
		default:
			return fmt.Errorf("system error: %v", err)
		}
	}

	dingocli.WriteOutln("Successfully unmounted %s", options.mountpoint)

	// Remove the mountpoint from mds metadata so that "dingo fs mountpoint"
	// stops showing it immediately. Without this, the mountpoint lingers until
	// the mds monitor reaps the stale client (mds_monitor_client_clean_period_time_ms,
	// 800s by default). The local umount already succeeded, so a failure here
	// must not fail the command.
	if utils.IsMDSAddrSet(cmd) {
		if _, err := removeMountPointFromMds(cmd, options); err != nil {
			fmt.Fprintf(dingocli.Err(), "warning: unmounted %s locally but failed to remove it from mds: %v\n",
				options.mountpoint, err)
		}
	}

	return nil
}

// reconcileStaleMountPoint handles the case where the local mountpoint is
// already gone but mds still lists it. When the stale record is removed the
// command succeeds; otherwise the original "not mounted" error is returned.
func reconcileStaleMountPoint(cmd *cobra.Command, dingocli *cli.DingoCli, options umountOptions,
	notMountedErr error) error {
	// Never wait for the default mds address if the user did not configure one.
	if !utils.IsMDSAddrSet(cmd) {
		return notMountedErr
	}

	removed, err := removeMountPointFromMds(cmd, options)
	if err != nil {
		return fmt.Errorf("%v (failed to remove the stale mountpoint from mds: %v)", notMountedErr, err)
	}
	if !removed {
		return notMountedErr
	}

	dingocli.WriteOutln("%s is not mounted locally, removed its stale mountpoint from mds", options.mountpoint)

	return nil
}

// removeMountPointFromMds resolves the (fs_name, client_id) pair of the
// mountpoint and sends UmountFs to mds. It reports whether a matching record
// was found and removed. If the pair cannot be found, it is a no-op: the client
// may have already removed it, or the entry was already reaped by the mds
// monitor.
func removeMountPointFromMds(cmd *cobra.Command, options umountOptions) (bool, error) {
	fsName, clientId := options.fsName, options.clientId

	if fsName == "" || clientId == "" {
		mountpoint, err := findMdsMountPoint(cmd, options)
		if err != nil {
			return false, err
		}
		if mountpoint == nil {
			return false, nil
		}

		if fsName == "" {
			fsName = mountpoint.fsName
		}
		if clientId == "" {
			clientId = mountpoint.clientId
		}
	}

	if fsName == "" || clientId == "" {
		return false, nil
	}

	if err := umountFsFromMds(cmd, fsName, clientId); err != nil {
		return false, err
	}

	return true, nil
}

// findMdsMountPoint looks up the mountpoint record stored by mds. When
// --client-id is given it is matched directly, otherwise the mountpoint path
// must match and the record must belong to the local host (mountpoints on
// different hosts may share the same path).
func findMdsMountPoint(cmd *cobra.Command, options umountOptions) (*mdsMountPoint, error) {
	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "ListFsInfo")
	if err != nil {
		return nil, err
	}

	listRpc := &rpc.ListFsRpc{
		Info:    mdsRpc,
		Request: &mds.ListFsInfoRequest{},
	}

	response, rpcError := rpc.GetRpcResponse(listRpc.Info, listRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}

	result, ok := response.(*mds.ListFsInfoResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected listfsinfo response")
	}
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, fmt.Errorf("mds listfsinfo failed: %s", mdsErr.String())
	}

	hostname, _ := os.Hostname()
	matched := matchMdsMountPoints(result.GetFsInfos(), mountPointPathCandidates(options.mountpoint),
		utils.GetLocalIPSet(), hostname, options.fsName, options.clientId)

	if len(matched) == 0 {
		return nil, nil
	}
	if len(matched) > 1 {
		return nil, fmt.Errorf("found %d mountpoints for %s in mds, specify --fsname and --client-id to choose one",
			len(matched), options.mountpoint)
	}

	return &matched[0], nil
}

func matchMdsMountPoints(fsInfos []*mds.FsInfo, candidates map[string]struct{},
	localIPs map[string]bool, hostname, fsName, clientId string) []mdsMountPoint {
	matched := make([]mdsMountPoint, 0)

	for _, fsInfo := range fsInfos {
		if fsName != "" && fsInfo.GetFsName() != fsName {
			continue
		}

		for _, mountPoint := range fsInfo.GetMountPoints() {
			if clientId != "" {
				if mountPoint.GetClientId() != clientId {
					continue
				}
			} else {
				if _, ok := candidates[filepath.Clean(mountPoint.GetPath())]; !ok {
					continue
				}
				if !isLocalMountPoint(mountPoint, localIPs, hostname) {
					continue
				}
			}

			matched = append(matched, mdsMountPoint{
				fsName:   fsInfo.GetFsName(),
				clientId: mountPoint.GetClientId(),
				hostname: mountPoint.GetHostname(),
				path:     mountPoint.GetPath(),
			})
		}
	}

	return matched
}

// isLocalMountPoint tells whether the mountpoint record was created by a
// client running on this host. The client records both the hostname and the
// IP it resolved from that hostname; the IP is the reliable discriminator
// when several hosts mount a filesystem on the same path.
func isLocalMountPoint(mountPoint *mds.MountPoint, localIPs map[string]bool, hostname string) bool {
	if ip := mountPoint.GetIp(); ip != "" {
		return localIPs[ip]
	}

	return hostname != "" && mountPoint.GetHostname() == hostname
}

// mountPointPathCandidates returns the path forms a mountpoint may be stored
// under. dingo-client stores weakly_canonical(mountpoint), so both the absolute
// path and the symlink-resolved path are accepted.
func mountPointPathCandidates(mountpoint string) map[string]struct{} {
	candidates := make(map[string]struct{})

	add := func(path string) {
		if path != "" {
			candidates[filepath.Clean(path)] = struct{}{}
		}
	}

	add(utils.AbsPath(mountpoint))
	add(weaklyCanonicalPath(mountpoint))

	return candidates
}

// weaklyCanonicalPath canonicalizes the existing prefix of path and appends the
// non-existing suffix. It mirrors std::filesystem::weakly_canonical, which
// dingo-client uses when it records the mountpoint, so a mountpoint whose
// directory was removed can still be matched against the recorded path.
func weaklyCanonicalPath(path string) string {
	abs := utils.AbsPath(path)

	prefix := abs
	suffix := ""
	for {
		if resolved, err := filepath.EvalSymlinks(prefix); err == nil {
			return filepath.Clean(filepath.Join(resolved, suffix))
		}

		parent := filepath.Dir(prefix)
		if parent == prefix {
			return abs
		}

		suffix = filepath.Join(filepath.Base(prefix), suffix)
		prefix = parent
	}
}

func umountFsFromMds(cmd *cobra.Command, fsName, clientId string) error {
	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "UmountFs")
	if err != nil {
		return err
	}

	umountRpc := &rpc.UmountFsRpc{
		Info: mdsRpc,
		Request: &mds.UmountFsRequest{
			FsName:   fsName,
			ClientId: clientId,
		},
	}

	response, rpcError := rpc.GetRpcResponse(umountRpc.Info, umountRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return rpcError
	}

	result, ok := response.(*mds.UmountFsResponse)
	if !ok {
		return fmt.Errorf("unexpected umountfs response")
	}
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return fmt.Errorf("mds umountfs failed: %s", mdsErr.String())
	}

	return nil
}

func runFuseumount(options umountOptions) error {

	var oscmd *exec.Cmd

	args := []string{"-u", options.mountpoint}
	if options.lazy {
		args = append(args, "-z")
	}
	oscmd = exec.Command("fusermount3", args...)
	oscmd.Stderr = os.Stderr
	oscmd.Stdout = os.Stdout

	return oscmd.Run()

}
