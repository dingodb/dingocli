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

import "fmt"

// kBlockStoreDir is the top-level directory under which file blocks are stored.
// Mirrors BlockKey::kStoreDir in the C++ code (src/common/block/block_key.h).
const kBlockStoreDir = "blocks"

// BlockObject is a single object (block) in the backend object store, as
// computed client-side from a slice. The mds only stores slice ids; object
// keys are derived on the client.
type BlockObject struct {
	Name string // store key, e.g. blocks/{id/1000/1000}/{id/1000}/{id}_{index}_{size}
	Size uint32 // block size in bytes
	Pos  uint64 // start offset of this object within the whole file
}

// BlockStoreKey builds the two-level bucketed store key for a block.
// Matches BlockKey::StoreKey (src/common/block/block_key.h).
func BlockStoreKey(id uint64, index uint32, size uint32) string {
	return fmt.Sprintf("%s/%d/%d/%d_%d_%d", kBlockStoreDir, id/1000/1000, id/1000, id, index, size)
}

// EnumerateBlockKeys expands a slice's physical data into its constituent block
// objects. Each block is block_size bytes except the last, which may be smaller.
// chunkIndex/chunkSize/blockSize are used to compute the object's absolute
// position within the file. Mirrors EnumerateBlockKeys + PrintObjectsTable in
// the C++ mds-cli.
func EnumerateBlockKeys(sliceId uint64, slicePos uint32, sliceSize uint32, chunkIndex uint32, chunkSize uint64, blockSize uint64) []BlockObject {
	objects := make([]BlockObject, 0)
	if blockSize == 0 {
		return objects
	}
	base := uint64(chunkIndex)*chunkSize + uint64(slicePos)
	bs := uint32(blockSize)
	for offset := uint32(0); offset < sliceSize; offset += bs {
		index := offset / bs
		actualSize := bs
		if sliceSize-offset < bs {
			actualSize = sliceSize - offset
		}
		objects = append(objects, BlockObject{
			Name: BlockStoreKey(sliceId, index, actualSize),
			Size: actualSize,
			Pos:  base + uint64(index)*blockSize,
		})
	}

	return objects
}
