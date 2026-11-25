/*
 * 	Copyright (c) 2024 dingodb.com Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package utils

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cilium/cilium/pkg/mountinfo"
)

const (
	DINGOFS_MOUNTPOINT_FSTYPE  = "fuse.dingofs"
	DINGOFS_MOUNTPOINT_FSTYPE2 = "fuse" //for backward compatibility
)

func GetDingoFSMountPoints() ([]*mountinfo.MountInfo, error) {
	mountpoints, err := mountinfo.GetMountInfo()
	if err != nil {
		return nil, fmt.Errorf("get mountpoint failed.")
	}

	dingofs_mountpoints := make([]*mountinfo.MountInfo, 0)
	for _, m := range mountpoints {
		if m.FilesystemType == DINGOFS_MOUNTPOINT_FSTYPE || m.FilesystemType == DINGOFS_MOUNTPOINT_FSTYPE2 {
			// check if the mountpoint is a dingofs mountpoint
			dingofs_mountpoints = append(dingofs_mountpoints, m)
		}
	}
	return dingofs_mountpoints, nil
}

// IsMountPoint reports whether path is currently a mountpoint on this host,
// according to /proc/self/mountinfo. It is used to tell "already unmounted"
// apart from "failed to unmount" before cleaning up stale metadata.
func IsMountPoint(path string) bool {
	mountPoints, err := mountinfo.GetMountInfo()
	if err != nil {
		return false
	}

	candidates := map[string]struct{}{}
	add := func(p string) {
		if p != "" {
			candidates[filepath.Clean(p)] = struct{}{}
		}
	}
	add(AbsPath(path))
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		add(AbsPath(resolved))
	}

	for _, mountPoint := range mountPoints {
		if _, ok := candidates[filepath.Clean(mountPoint.MountPoint)]; ok {
			return true
		}
	}

	return false
}

// make sure path' abs path start with mountpoint.MountPoint
func Path2DingofsPath(path string, mountpoint *mountinfo.MountInfo) string {
	path, _ = filepath.Abs(path)
	mountPoint := mountpoint.MountPoint
	root := mountpoint.Root
	dingofsPath, _ := filepath.Abs(strings.Replace(path, mountPoint, root, 1))
	return dingofsPath
}
