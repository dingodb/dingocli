/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
	"net"
	"os"
	"path"
	"strings"
	"syscall"
)

func CheckMountPoint(mountPoint string) error {
	if !PathExist(mountPoint) {
		return fmt.Errorf("%s: path not exist", mountPoint)
	} else if !path.IsAbs(mountPoint) {
		return fmt.Errorf("%s: is not an absolute path", mountPoint)
	}
	return nil
}

// GetLocalIPSet returns the set of IP addresses assigned to this host.
// It is used to tell which mds mountpoint records were created by clients
// running on this host.
func GetLocalIPSet() map[string]bool {
	ips := map[string]bool{"127.0.0.1": true}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok {
			ips[ipNet.IP.String()] = true
		}
	}
	return ips
}

// get mountPoint inode
func GetFileInode(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return sst.Ino, nil
	}
	return 0, nil
}

func GetInodesAsString(listFilePath string) (string, error) {
	content, err := os.ReadFile(listFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file list: %v", err)
	}

	lines := strings.Split(string(content), "\n")
	var inodeStrings []string

	for _, line := range lines {
		filePath := strings.TrimSpace(line)
		if filePath == "" {
			continue
		}

		if !strings.HasPrefix(filePath, "/") {
			return "", fmt.Errorf("filelist[%s] content error, each line requires a full path name", listFilePath)
		}

		inodeId, err2 := GetFileInode(filePath)
		if err2 != nil {
			return "", fmt.Errorf("%s not exist", filePath)
		}
		if inodeId == 0 {
			continue
		}
		inodeStrings = append(inodeStrings, fmt.Sprintf("%d", inodeId))
	}

	inodeStrings = RemoveDuplicates(inodeStrings)

	return strings.Join(inodeStrings, ","), nil
}

func GetHomeDir() string {
	homedir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	return homedir
}
