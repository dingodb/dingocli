/*
 * 	Copyright (c) 2026 dingofs org, Inc. All Rights Reserved
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
	"strconv"
	"strings"
	"time"
)

// trashBucketFormat is the UTC hour-bucket name layout "YYYY-MM-DD-HH".
// Mirrors kBucketFormat in src/mds/common/trash.cc.
const trashBucketFormat = "2006-01-02-15"

// ParseTrashBucketName validates a trash hour-bucket name "YYYY-MM-DD-HH" (UTC)
// and returns the unix timestamp in seconds. Returns 0 on failure.
func ParseTrashBucketName(name string) uint64 {
	t, err := time.ParseInLocation(trashBucketFormat, name, time.UTC)
	if err != nil {
		return 0
	}
	return uint64(t.Unix())
}

// ParseTrashEntryName parses a trash entry name "{parent_ino}-{file_ino}-{name}"
// into its components. Mirrors ParseTrashEntryName in src/mds/common/trash.cc.
func ParseTrashEntryName(trashName string) (parentIno uint64, fileIno uint64, originalName string, ok bool) {
	pos1 := strings.IndexByte(trashName, '-')
	if pos1 < 0 {
		return 0, 0, "", false
	}
	pos2 := strings.IndexByte(trashName[pos1+1:], '-')
	if pos2 < 0 {
		return 0, 0, "", false
	}
	pos2 += pos1 + 1

	p, err := strconv.ParseUint(trashName[:pos1], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}
	f, err := strconv.ParseUint(trashName[pos1+1:pos2], 10, 64)
	if err != nil {
		return 0, 0, "", false
	}

	return p, f, trashName[pos2+1:], true
}

// ParseTrashEntryParent returns just the original parent inode from a trash
// entry name, or 0 if it cannot be parsed.
func ParseTrashEntryParent(trashName string) uint64 {
	parent, _, _, ok := ParseTrashEntryName(trashName)
	if !ok {
		return 0
	}
	return parent
}
