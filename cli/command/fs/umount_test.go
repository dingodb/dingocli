/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
	"os"
	"path/filepath"
	"testing"

	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
)

func TestNewFsUmountCommand(t *testing.T) {
	cmd := NewFsUmountCommand(nil)

	for _, name := range []string{
		"lazy", "verbose", "fsname", FS_UMOUNT_CLIENTID, "conf", "mdsaddr",
		"rpctimeout", "rpcretrydelay", "rpcretrytimes",
	} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("expected flag --%s to be registered", name)
		}
	}
}

func TestMountPointPathCandidates(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	candidates := mountPointPathCandidates(link)
	if _, ok := candidates[dir]; !ok {
		t.Errorf("expected resolved path %q in candidates %v", dir, candidates)
	}
	if _, ok := candidates[link]; !ok {
		t.Errorf("expected symlink path %q in candidates %v", link, candidates)
	}

	// a trailing slash must be normalized away
	candidates = mountPointPathCandidates(dir + "/")
	if _, ok := candidates[dir]; !ok {
		t.Errorf("expected cleaned path %q in candidates %v", dir, candidates)
	}

	// a path below a symlink is resolved even when its tail does not exist
	// (the mountpoint directory may have been removed with the mount).
	candidates = mountPointPathCandidates(filepath.Join(link, "gone"))
	if _, ok := candidates[filepath.Join(dir, "gone")]; !ok {
		t.Errorf("expected resolved nonexistent path %q in candidates %v", filepath.Join(dir, "gone"), candidates)
	}
}

func TestMatchMdsMountPoints(t *testing.T) {
	localIPs := map[string]bool{"10.0.0.1": true}
	fsInfos := []*mds.FsInfo{
		{
			FsName: "fs1",
			MountPoints: []*mds.MountPoint{
				{ClientId: "client-a", Hostname: "host-a", Ip: "10.0.0.1", Path: "/mnt/dingofs"},
				{ClientId: "client-b", Hostname: "host-b", Ip: "10.0.0.2", Path: "/mnt/dingofs"},
			},
		},
		{
			FsName: "fs2",
			MountPoints: []*mds.MountPoint{
				{ClientId: "client-c", Hostname: "host-a", Ip: "10.0.0.1", Path: "/mnt/other"},
			},
		},
	}

	// path + local ip selects only the local host's record, even though the
	// path is shared with another host.
	matched := matchMdsMountPoints(fsInfos, mountPointPathCandidates("/mnt/dingofs"),
		localIPs, "host-a", "", "")
	if len(matched) != 1 || matched[0].clientId != "client-a" || matched[0].fsName != "fs1" {
		t.Fatalf("unexpected match: %+v", matched)
	}

	// a mountpoint owned by another host must not be selected.
	matched = matchMdsMountPoints(fsInfos, mountPointPathCandidates("/mnt/dingofs"),
		map[string]bool{"10.0.0.3": true}, "host-c", "", "")
	if len(matched) != 0 {
		t.Fatalf("expected no match for unknown host, got: %+v", matched)
	}

	// an explicit client id takes precedence over path and host.
	matched = matchMdsMountPoints(fsInfos, nil, localIPs, "host-a", "", "client-b")
	if len(matched) != 1 || matched[0].hostname != "host-b" {
		t.Fatalf("unexpected client id match: %+v", matched)
	}

	// fs name narrows the match.
	matched = matchMdsMountPoints(fsInfos, mountPointPathCandidates("/mnt/dingofs"),
		localIPs, "", "fs2", "")
	if len(matched) != 0 {
		t.Fatalf("expected no match for fs2, got: %+v", matched)
	}

	// fall back to hostname when the client did not record an ip.
	fsInfos = append(fsInfos, &mds.FsInfo{
		FsName: "fs3",
		MountPoints: []*mds.MountPoint{
			{ClientId: "client-d", Hostname: "host-a", Path: "/mnt/legacy"},
		},
	})
	matched = matchMdsMountPoints(fsInfos, mountPointPathCandidates("/mnt/legacy"),
		localIPs, "host-a", "", "")
	if len(matched) != 1 || matched[0].clientId != "client-d" {
		t.Fatalf("unexpected hostname fallback match: %+v", matched)
	}
}
