// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

const (
	ROW_ADDR           = "addr"
	ROW_ALLOC          = "alloc"
	ROW_ALLOC_SIZE     = "allocatedSize"
	ROW_BLOCKSIZE      = "blocksize"
	ROW_CAPACITY       = "capacity"
	ROW_CHILD_LIST     = "childList"
	ROW_CHILD_TYPE     = "childType"
	ROW_CHUNK          = "chunk"
	ROW_CHUNK_SIZE     = "chunkSize"
	ROW_CREATE_TIME    = "create time"
	ROW_CREATED        = "created"
	ROW_LASTONLINETIME = "last online time"
	ROW_FS_ID          = "fsId"
	ROW_FS_NAME        = "fsName"
	ROW_FS_CLIENTID    = "clientId"
	ROW_FS_CTO         = "cto"
	ROW_STORAGE_TYPE   = "storageType"
	ROW_STORAGE        = "storage"
	ROW_PARTITION_TYPE = "partitionType"
	ROW_GROUP          = "group"
	ROW_HOSTNAME       = "hostname"
	ROW_ID             = "id"
	ROW_INODE_ID       = "inodeId"
	ROW_IP             = "ip"
	ROW_PORT           = "port"
	ROW_MEMBERID       = "memberid"
	ROW_WEIGHT         = "weight"
	ROW_KEY            = "key"
	ROW_LOCKED         = "locked"
	ROW_LEFT           = "left"
	ROW_LENGTH         = "length"
	ROW_LOCATION       = "location"
	ROW_MOUNT_NUM      = "mountNum"
	ROW_MOUNTPOINT     = "mountpoint"
	ROW_UUID           = "uuid"
	ROW_NAME           = "name"
	ROW_NLINK          = "nlink"
	ROW_NUM            = "num"
	ROW_ONLINE_STATE   = "onlineState"
	ROW_OPERATION      = "operation"
	ROW_OPNAME         = "opname"
	ROW_ORIGINAL_PATH  = "originalPath"
	ROW_OWNER          = "owner"
	ROW_PARENT         = "parent"
	ROW_PARENT_ID      = "parentId"
	ROW_READONLY       = "readonly"
	ROW_REASON         = "reason"
	ROW_RECYCLE        = "recycle"
	ROW_RESULT         = "result"
	ROW_SEQ            = "seq"
	ROW_SIZE           = "size"
	ROW_START          = "start"
	ROW_STATE          = "state"
	ROW_STATUS         = "status"
	ROW_TERM           = "term"
	ROW_TOTAL          = "total"
	ROW_TYPE           = "type"
	ROW_USED           = "used"
	ROW_VERSION        = "version"

	// s3
	ROW_S3CHUNKINFO_CHUNKID = "s3ChunkId"
	ROW_S3CHUNKINFO_LENGTH  = "s3Length"
	ROW_S3CHUNKINFO_OFFSET  = "s3Offset"
	ROW_S3CHUNKINFO_SIZE    = "s3Size"

	// vale
	ROW_VALUE_ADD           = "add"
	ROW_VALUE_DEL           = "del"
	ROW_VALUE_DNE           = "DNE"
	ROW_VALUE_FAILED        = "failed"
	ROW_VALUE_LOGICAL       = "logical"
	ROW_VALUE_NO_RECOVERING = ""
	ROW_VALUE_NO_VALUE      = "-"
	ROW_VALUE_NULL          = "null"
	ROW_VALUE_OFFLINE       = "offline"
	ROW_VALUE_ONLINE        = "online"
	ROW_VALUE_PHYSICAL      = "physical"
	ROW_VALUE_SUCCESS       = "success"
	ROW_VALUE_UNKNOWN       = "unknown"

	// quota
	ROW_PATH              = "path"
	ROW_USED_PERCNET      = "use%"
	ROW_REAL_USED         = "realused"
	ROW_INODES            = "inodes"
	ROW_INODES_IUSED      = "iused"
	ROW_INODES_PERCENT    = "iuse%"
	ROW_INODES_REAL_IUSED = "realiused"

	//fuse
	ROW_FUSE_CONNECTION = "CONNECTION"
	ROW_FUSE_WAITING    = "WAITING"

	//mds
	ROW_MDS_NUM = "mdsnum"

	// delete subdir
	ROW_DELETE_INODES = "delete inodes"

	// dir stats
	ROW_FILES       = "files"
	ROW_DIRS        = "dirs"
	ROW_CHUNK_INDEX = "chunkIndex"
	ROW_OBJECT_NAME = "objectName"
	ROW_SLICE_ID    = "sliceId"
	ROW_OFFSET      = "offset"
	ROW_POS         = "pos"
	ROW_WANT_INODES = "wantInodes"
	ROW_WANT_LENGTH = "wantLength"
	ROW_GOT_INODES  = "gotInodes"
	ROW_GOT_LENGTH  = "gotLength"
)
