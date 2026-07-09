// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/utils"
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"github.com/spf13/cobra"
)

// get mds list
func GetMDSList(cmd *cobra.Command) ([]*mds.MDS, error) {
	// new prc
	mdsRpc, err := CreateNewMdsRpc(cmd, "GetMDSList")
	if err != nil {
		return nil, err
	}
	getMDSRpc := &GetMDSRpc{
		Info:    mdsRpc,
		Request: &mds.GetMDSListRequest{},
	}

	// get rpc result
	response, rpcError := GetRpcResponse(getMDSRpc.Info, getMDSRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}
	result := response.(*mds.GetMDSListResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	return result.GetMdses(), nil
}

// retrieve fsid from command-line parameters,if not set, get by GetFsInfo via fsname
func GetFsId(cmd *cobra.Command) (uint32, error) {
	fsId, fsName, fsErr := utils.GetFsInfoFlagValue(cmd)
	if fsErr != nil {
		return 0, fsErr
	}
	// fsId is not set,need to get fsId by fsName (fsName -> fsId)
	if fsId == 0 {
		fsInfo, fsErr := GetFsInfo(cmd, 0, fsName)
		if fsErr != nil {
			return 0, fsErr
		}
		fsId = fsInfo.GetFsId()
		if fsId == 0 {
			return 0, fmt.Errorf("fsid is invalid")
		}
	}

	return fsId, nil
}

// retrieve fsid from command-line parameters,if not set, get by GetFsInfo via fsid
func GetFsName(cmd *cobra.Command) (string, error) {
	fsId, fsName, fsErr := utils.GetFsInfoFlagValue(cmd)
	if fsErr != nil {
		return "", fsErr
	}
	if len(fsName) == 0 { // fsName is not set,need to get fsName by fsId (fsId->fsName)
		fsInfo, fsErr := GetFsInfo(cmd, fsId, "")
		if fsErr != nil {
			return "", fsErr
		}
		fsName = fsInfo.GetFsName()
		if len(fsName) == 0 {
			return "", fmt.Errorf("fsName is invalid")
		}
	}

	return fsName, nil
}

// list filesystem info
func ListFsInfo(cmd *cobra.Command) ([]*mds.FsInfo, error) {
	// new prc
	mdsRpc, err := CreateNewMdsRpc(cmd, "ListFsInfo")
	if err != nil {
		return nil, err
	}
	// set request info
	listFsRpc := &ListFsInfoRpc{Info: mdsRpc, Request: &mds.ListFsInfoRequest{}}
	// get rpc result

	response, rpcError := GetRpcResponse(listFsRpc.Info, listFsRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}

	result := response.(*mds.ListFsInfoResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	fsInfos := result.GetFsInfos()
	// fill fs meta cache
	for _, fsInfo := range fsInfos {
		fsMetaCache.SetFsInfo(fsInfo)
	}

	return fsInfos, nil
}

// get fsinfo by fsid or fsname
func GetFsInfo(cmd *cobra.Command, fsId uint32, fsName string) (*mds.FsInfo, error) {
	// first read from cache
	fsInfo, ok := fsMetaCache.GetFsInfo(fsId)
	if ok {
		return fsInfo, nil
	}
	// new prc
	mdsRpc, err := CreateNewMdsRpc(cmd, "GetFsInfo")
	if err != nil {
		return nil, err
	}
	// set request info
	var getFsRpc *GetFsRpc
	if fsId > 0 {
		getFsRpc = &GetFsRpc{Info: mdsRpc, Request: &mds.GetFsInfoRequest{FsId: fsId}}
	} else {
		getFsRpc = &GetFsRpc{Info: mdsRpc, Request: &mds.GetFsInfoRequest{FsName: fsName}}
	}

	// get rpc result
	response, rpcError := GetRpcResponse(getFsRpc.Info, getFsRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}
	result := response.(*mds.GetFsInfoResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	fsInfo = result.GetFsInfo()
	fsMetaCache.SetFsInfo(fsInfo)

	return fsInfo, nil
}

// GetDentry
func GetDentry(cmd *cobra.Command, fsId uint32, parentId uint64, name string, epoch uint64) (*mds.Dentry, error) {
	endpoint := GetEndPoint(parentId)
	if len(endpoint) == 0 {
		return nil, fmt.Errorf("endpoint is null")
	}
	// new prc
	mdsRpc := CreateNewMdsRpcWithEndPoint(cmd, endpoint, "GetDentry")
	// set request info
	getDentryRpc := &GetDentryRpc{
		Info: mdsRpc,
		Request: &mds.GetDentryRequest{
			Context: &mds.Context{Epoch: epoch},
			FsId:    fsId,
			Parent:  parentId,
			Name:    name,
		},
	}
	// get rpc result
	response, rpcError := GetRpcResponse(getDentryRpc.Info, getDentryRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}
	result := response.(*mds.GetDentryResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	return result.GetDentry(), nil
}

func DeleteFile(cmd *cobra.Command, fsId uint32, parentId uint64, name string, epoch uint64) error {
	endpoint := GetEndPoint(parentId)
	if len(endpoint) == 0 {
		return fmt.Errorf("endpoint is null")
	}
	// new prc
	mdsRpc := CreateNewMdsRpcWithEndPoint(cmd, endpoint, "UnLink")
	// set request info
	unlinkFileRpc := &UnlinkFileRpc{
		Info: mdsRpc,
		Request: &mds.UnLinkRequest{
			Context: &mds.Context{Epoch: epoch},
			FsId:    fsId,
			Parent:  parentId,
			Name:    name,
		},
	}
	// get rpc result
	response, rpcError := GetRpcResponse(unlinkFileRpc.Info, unlinkFileRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return rpcError
	}
	result := response.(*mds.UnLinkResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	return nil
}

func DeleteDirectory(cmd *cobra.Command, fsId uint32, parentId uint64, name string, epoch uint64) error {
	endpoint := GetEndPoint(parentId)
	if len(endpoint) == 0 {
		return fmt.Errorf("endpoint is null")
	}
	// new prc
	mdsRpc := CreateNewMdsRpcWithEndPoint(cmd, endpoint, "Rmdir")
	// set request info
	rmDirRpc := &RmDirRpc{
		Info: mdsRpc,
		Request: &mds.RmDirRequest{
			Context: &mds.Context{Epoch: epoch},
			FsId:    fsId,
			Parent:  parentId,
			Name:    name,
		},
	}
	// get rpc result
	response, rpcError := GetRpcResponse(rmDirRpc.Info, rmDirRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return rpcError
	}
	result := response.(*mds.RmDirResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	return nil
}

// parse directory path -> inodeId
func GetDirPathInodeId(cmd *cobra.Command, fsId uint32, path string, epoch uint64) (uint64, error) {
	if path == "/" {
		return common.ROOTINODEID, nil
	}
	inodeId := common.ROOTINODEID

	for path != "" {
		names := strings.SplitN(path, "/", 2)
		if names[0] != "" {
			dentry, err := GetDentry(cmd, fsId, inodeId, names[0], epoch)
			if err != nil {
				return 0, err
			}
			if dentry.GetType() != mds.FileType_DIRECTORY {
				return 0, syscall.ENOTDIR
			}
			inodeId = dentry.GetIno()
		}
		if len(names) == 1 {
			break
		}
		path = names[1]
	}
	return inodeId, nil
}

// get inode
func GetInode(cmd *cobra.Command, fsId uint32, inodeId uint64, parent uint64, epoch uint64) (*mds.Inode, error) {
	var endpoint []string
	requestContext := &mds.Context{Epoch: epoch, IsBypassCache: true}

	if IsFile(inodeId) && parent > 0 { // file: get endpoint by parent
		endpoint = GetEndPoint(parent)
	} else {
		endpoint = GetEndPoint(inodeId) // directory: get endpoint by self inodeid
	}
	if len(endpoint) == 0 {
		return nil, fmt.Errorf("endpoint is null")
	}
	if IsFile(inodeId) && parent == 0 { // file but parent is not set, bypass cache
		requestContext.IsBypassCache = true
	}
	// new prc
	mdsRpc := CreateNewMdsRpcWithEndPoint(cmd, endpoint, "GetInode")

	// set request info
	getInodeRpc := &GetInodeRpc{
		Info: mdsRpc,
		Request: &mds.GetInodeRequest{
			Context: requestContext,
			FsId:    fsId,
			Ino:     inodeId,
		},
	}
	// get rpc result
	response, rpcError := GetRpcResponse(getInodeRpc.Info, getInodeRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}
	result := response.(*mds.GetInodeResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	return result.GetInode(), nil
}

// list dentry
func ListDentry(cmd *cobra.Command, fsId uint32, inodeId uint64, epoch uint64) ([]*mds.Dentry, error) {
	endpoint := GetEndPoint(inodeId)
	if len(endpoint) == 0 {
		return nil, fmt.Errorf("endpoint is null")
	}
	// new prc
	mdsRpc := CreateNewMdsRpcWithEndPoint(cmd, endpoint, "ListDentry")
	// set request info
	listDentryRpc := &ListDentryRpc{
		Info: mdsRpc,
		Request: &mds.ListDentryRequest{
			Context: &mds.Context{Epoch: epoch},
			FsId:    fsId,
			Parent:  inodeId,
		},
	}
	// get rpc result
	response, rpcError := GetRpcResponse(listDentryRpc.Info, listDentryRpc)
	if rpcError.GetCode() != errno.ERR_OK.GetCode() {
		return nil, rpcError
	}
	result := response.(*mds.ListDentryResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return nil, errno.ERR_RPC_FAILED.S(mdsErr.String())
	}

	return result.GetDentries(), nil
}

// get dir path
func GetInodePath(cmd *cobra.Command, fsId uint32, inodeId uint64, epoch uint64) (string, string, error) {
	reverse := func(s []string) {
		for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
			s[i], s[j] = s[j], s[i]
		}
	}
	if inodeId == common.ROOTINODEID {
		return "/", fmt.Sprintf("%d", common.ROOTINODEID), nil
	}
	var names []string
	var inodes []string
	for inodeId != common.ROOTINODEID {
		inode, inodeErr := GetInode(cmd, fsId, inodeId, 0, epoch)
		if inodeErr != nil {
			return "", "", inodeErr
		}
		//do list entry rpc
		parentIds := inode.GetParents()
		parentId := parentIds[0]
		entries, entryErr := ListDentry(cmd, fsId, parentId, epoch)
		if entryErr != nil {
			return "", "", entryErr
		}
		for _, e := range entries {
			if e.GetIno() == inodeId {
				names = append(names, e.GetName())
				inodes = append(inodes, fmt.Sprintf("%d", inodeId))
				break
			}
		}
		inodeId = parentId
	}
	if len(names) == 0 { //directory may be deleted
		return "", "", nil
	}
	names = append(names, "/")                                     // add root
	inodes = append(inodes, fmt.Sprintf("%d", common.ROOTINODEID)) // add root
	reverse(names)
	reverse(inodes)

	return path.Join(names...), path.Join(inodes...), nil
}

// get directory size and inodes by inode
func GetDirSummarySize(cmd *cobra.Command, fsId uint32, inode uint64, summary *common.Summary, concurrent chan struct{},
	ctx context.Context, cancel context.CancelFunc, isFsCheck bool, inodeMap *sync.Map, epoch uint64) error {
	var err error
	entries, entErr := ListDentry(cmd, fsId, inode, epoch)
	if entErr != nil {
		return entErr
	}
	var wg sync.WaitGroup
	var errCh = make(chan error, 1)
	for _, entry := range entries {
		if entry.GetType() == mds.FileType_FILE {
			inodeAttr, err := GetInode(cmd, fsId, entry.GetIno(), entry.GetParent(), epoch)
			if err != nil {
				return err
			}
			if isFsCheck && inodeAttr.GetNlink() >= 2 { //filesystem check, hardlink is ignored
				if _, ok := inodeMap.LoadOrStore(inodeAttr.GetIno(), struct{}{}); ok {
					continue
				}
			}
			atomic.AddUint64(&summary.Length, inodeAttr.GetLength())
		}
		atomic.AddUint64(&summary.Inodes, 1)
		if entry.GetType() != mds.FileType_DIRECTORY {
			continue
		}
		select {
		case err := <-errCh:
			cancel()
			return err
		case <-ctx.Done():
			return fmt.Errorf("cancel scan directory for other goroutine error")
		case concurrent <- struct{}{}:
			wg.Add(1)
			go func(e *mds.Dentry) {
				defer wg.Done()
				sumErr := GetDirSummarySize(cmd, fsId, e.GetIno(), summary, concurrent, ctx, cancel, isFsCheck, inodeMap, epoch)
				<-concurrent
				if sumErr != nil {
					select {
					case errCh <- sumErr:
					default:
					}
				}
			}(entry)
		default:
			if sumErr := GetDirSummarySize(cmd, fsId, entry.GetIno(), summary, concurrent, ctx, cancel, isFsCheck, inodeMap, epoch); sumErr != nil {
				return sumErr
			}
		}
	}
	wg.Wait()
	select {
	case err = <-errCh:
	default:
	}

	return err
}

// get directory size and inodes by path name
func GetDirectorySizeAndInodes(cmd *cobra.Command, fsId uint32, dirInode uint64, isFsCheck bool, epoch uint64, threads uint32) (int64, int64, error) {
	log.Printf("start to summary directory statistics, inode[%d]", dirInode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	summary := &common.Summary{Length: 0, Inodes: 0}
	concurrent := make(chan struct{}, threads)
	var inodeMap *sync.Map = &sync.Map{}

	sumErr := GetDirSummarySize(cmd, fsId, dirInode, summary, concurrent, ctx, cancel, isFsCheck, inodeMap, epoch)
	if sumErr != nil {
		return 0, 0, sumErr
	}

	log.Printf("end summary directory statistics, inode[%d],inodes[%d],size[%d]", dirInode, summary.Inodes, summary.Length)

	// add root inode
	atomic.AddUint64(&summary.Inodes, 1)
	return int64(summary.Length), int64(summary.Inodes), nil
}
