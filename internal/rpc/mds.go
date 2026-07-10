package rpc

import (
	"context"

	"github.com/dingodb/dingocli/internal/output"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	"google.golang.org/grpc"
)

// rpc services
type GetMDSRpc struct {
	Info      *Rpc
	Request   *mds.GetMDSListRequest
	mdsClient mds.MDSServiceClient
}

type CreateFsRpc struct {
	Info      *Rpc
	Request   *mds.CreateFsRequest
	mdsClient mds.MDSServiceClient
}

type DeleteFsRpc struct {
	Info      *Rpc
	Request   *mds.DeleteFsRequest
	mdsClient mds.MDSServiceClient
}

type ListFsRpc struct {
	Info      *Rpc
	Request   *mds.ListFsInfoRequest
	mdsClient mds.MDSServiceClient
}

type GetFsRpc struct {
	Info      *Rpc
	Request   *mds.GetFsInfoRequest
	mdsClient mds.MDSServiceClient
}

type GetMdsRpc struct {
	Info      *Rpc
	Request   *mds.GetMDSListRequest
	mdsClient mds.MDSServiceClient
}

type SetFsQuotaRpc struct {
	Info      *Rpc
	Request   *mds.SetFsQuotaRequest
	mdsClient mds.MDSServiceClient
}

type GetFsQuotaRpc struct {
	Info      *Rpc
	Request   *mds.GetFsQuotaRequest
	mdsClient mds.MDSServiceClient
}

type GetInodeRpc struct {
	Info      *Rpc
	Request   *mds.GetInodeRequest
	mdsClient mds.MDSServiceClient
}

type MkDirRpc struct {
	Info      *Rpc
	Request   *mds.MkDirRequest
	mdsClient mds.MDSServiceClient
}

type GetDentryRpc struct {
	Info      *Rpc
	Request   *mds.GetDentryRequest
	mdsClient mds.MDSServiceClient
}

type ListDentryRpc struct {
	Info      *Rpc
	Request   *mds.ListDentryRequest
	mdsClient mds.MDSServiceClient
}

type GetFsStatsRpc struct {
	Info      *Rpc
	Request   *mds.GetFsStatsRequest
	mdsClient mds.MDSServiceClient
}

type UmountFsRpc struct {
	Info      *Rpc
	Request   *mds.UmountFsRequest
	mdsClient mds.MDSServiceClient
}

type SetDirQuotaRpc struct {
	Info      *Rpc
	Request   *mds.SetDirQuotaRequest
	mdsClient mds.MDSServiceClient
}

type GetDirQuotaRpc struct {
	Info      *Rpc
	Request   *mds.GetDirQuotaRequest
	mdsClient mds.MDSServiceClient
}

type ListDirQuotaRpc struct {
	Info      *Rpc
	Request   *mds.LoadDirQuotasRequest
	mdsClient mds.MDSServiceClient
}

type DeleteDirQuotaRpc struct {
	Info      *Rpc
	Request   *mds.DeleteDirQuotaRequest
	mdsClient mds.MDSServiceClient
}

type ListFsInfoRpc struct {
	Info      *Rpc
	Request   *mds.ListFsInfoRequest
	mdsClient mds.MDSServiceClient
}

type UnlinkFileRpc struct {
	Info      *Rpc
	Request   *mds.UnLinkRequest
	mdsClient mds.MDSServiceClient
}

type RmDirRpc struct {
	Info      *Rpc
	Request   *mds.RmDirRequest
	mdsClient mds.MDSServiceClient
}

type GetDirStatRpc struct {
	Info      *Rpc
	Request   *mds.GetDirStatRequest
	mdsClient mds.MDSServiceClient
}

type SyncDirStatRpc struct {
	Info      *Rpc
	Request   *mds.SyncDirStatRequest
	mdsClient mds.MDSServiceClient
}

type UpdateFsInfoRpc struct {
	Info      *Rpc
	Request   *mds.UpdateFsInfoRequest
	mdsClient mds.MDSServiceClient
}

type ReadSliceRpc struct {
	Info      *Rpc
	Request   *mds.ReadSliceRequest
	mdsClient mds.MDSServiceClient
}

// check interface
var _ RpcFunc = (*GetMdsRpc)(nil)         // check interface
var _ RpcFunc = (*CreateFsRpc)(nil)       // check interface
var _ RpcFunc = (*DeleteFsRpc)(nil)       // check interface
var _ RpcFunc = (*ListFsRpc)(nil)         // check interface
var _ RpcFunc = (*GetFsRpc)(nil)          // check interface
var _ RpcFunc = (*GetMdsRpc)(nil)         // check interface
var _ RpcFunc = (*SetFsQuotaRpc)(nil)     // check interface
var _ RpcFunc = (*GetFsQuotaRpc)(nil)     // check interface
var _ RpcFunc = (*GetInodeRpc)(nil)       // check interface
var _ RpcFunc = (*MkDirRpc)(nil)          // check interface
var _ RpcFunc = (*GetDentryRpc)(nil)      // check interface
var _ RpcFunc = (*ListDentryRpc)(nil)     // check interface
var _ RpcFunc = (*GetFsStatsRpc)(nil)     // check interface
var _ RpcFunc = (*UmountFsRpc)(nil)       // check interface
var _ RpcFunc = (*SetDirQuotaRpc)(nil)    // check interface
var _ RpcFunc = (*GetDirQuotaRpc)(nil)    // check interface
var _ RpcFunc = (*ListDirQuotaRpc)(nil)   // check interface
var _ RpcFunc = (*DeleteDirQuotaRpc)(nil) // check interface
var _ RpcFunc = (*UnlinkFileRpc)(nil)     // check interface
var _ RpcFunc = (*RmDirRpc)(nil)          // check interface
var _ RpcFunc = (*GetDirStatRpc)(nil)     // check interface
var _ RpcFunc = (*SyncDirStatRpc)(nil)    // check interface
var _ RpcFunc = (*UpdateFsInfoRpc)(nil)   // check interface
var _ RpcFunc = (*ReadSliceRpc)(nil)      // check interface

func (mdsFs *GetMDSRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	mdsFs.mdsClient = mds.NewMDSServiceClient(cc)
}

func (mdsFs *GetMDSRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := mdsFs.mdsClient.GetMDSList(ctx, mdsFs.Request)
	output.ShowRpcData(mdsFs.Request, response, mdsFs.Info.RpcDataShow)
	return response, err
}

func (createFs *CreateFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	createFs.mdsClient = mds.NewMDSServiceClient(cc)
}

func (createFs *CreateFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := createFs.mdsClient.CreateFs(ctx, createFs.Request)
	output.ShowRpcData(createFs.Request, response, createFs.Info.RpcDataShow)
	return response, err
}

func (deleteFs *DeleteFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	deleteFs.mdsClient = mds.NewMDSServiceClient(cc)
}

func (deleteFs *DeleteFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := deleteFs.mdsClient.DeleteFs(ctx, deleteFs.Request)
	output.ShowRpcData(deleteFs.Request, response, deleteFs.Info.RpcDataShow)
	return response, err
}

func (listFs *ListFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	listFs.mdsClient = mds.NewMDSServiceClient(cc)
}

func (listFs *ListFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := listFs.mdsClient.ListFsInfo(ctx, listFs.Request)
	output.ShowRpcData(listFs.Request, response, listFs.Info.RpcDataShow)
	return response, err
}

func (getFs *GetFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getFs.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getFs *GetFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getFs.mdsClient.GetFsInfo(ctx, getFs.Request)
	output.ShowRpcData(getFs.Request, response, getFs.Info.RpcDataShow)
	return response, err
}

func (getMds *GetMdsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getMds.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getMds *GetMdsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getMds.mdsClient.GetMDSList(ctx, getMds.Request)
	output.ShowRpcData(getMds.Request, response, getMds.Info.RpcDataShow)
	return response, err
}

func (setFsQuota *SetFsQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	setFsQuota.mdsClient = mds.NewMDSServiceClient(cc)
}

func (setFsQuota *SetFsQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := setFsQuota.mdsClient.SetFsQuota(ctx, setFsQuota.Request)
	output.ShowRpcData(setFsQuota.Request, response, setFsQuota.Info.RpcDataShow)
	return response, err
}

func (getFsQuota *GetFsQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getFsQuota.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getFsQuota *GetFsQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getFsQuota.mdsClient.GetFsQuota(ctx, getFsQuota.Request)
	output.ShowRpcData(getFsQuota.Request, response, getFsQuota.Info.RpcDataShow)
	return response, err
}

func (getInode *GetInodeRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getInode.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getInode *GetInodeRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getInode.mdsClient.GetInode(ctx, getInode.Request)
	output.ShowRpcData(getInode.Request, response, getInode.Info.RpcDataShow)
	return response, err
}

func (mkDir *MkDirRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	mkDir.mdsClient = mds.NewMDSServiceClient(cc)
}

func (mkDir *MkDirRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := mkDir.mdsClient.MkDir(ctx, mkDir.Request)
	output.ShowRpcData(mkDir.Request, response, mkDir.Info.RpcDataShow)
	return response, err
}

func (listDentry *ListDentryRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	listDentry.mdsClient = mds.NewMDSServiceClient(cc)
}

func (listDentry *ListDentryRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := listDentry.mdsClient.ListDentry(ctx, listDentry.Request)
	output.ShowRpcData(listDentry.Request, response, listDentry.Info.RpcDataShow)
	return response, err
}

func (getDentry *GetDentryRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getDentry.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getDentry *GetDentryRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getDentry.mdsClient.GetDentry(ctx, getDentry.Request)
	output.ShowRpcData(getDentry.Request, response, getDentry.Info.RpcDataShow)
	return response, err
}

func (getFsStats *GetFsStatsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getFsStats.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getFsStats *GetFsStatsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getFsStats.mdsClient.GetFsStats(ctx, getFsStats.Request)
	output.ShowRpcData(getFsStats.Request, response, getFsStats.Info.RpcDataShow)
	return response, err
}

func (umountFs *UmountFsRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	umountFs.mdsClient = mds.NewMDSServiceClient(cc)
}

func (umountFs *UmountFsRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := umountFs.mdsClient.UmountFs(ctx, umountFs.Request)
	output.ShowRpcData(umountFs.Request, response, umountFs.Info.RpcDataShow)
	return response, err
}

func (setDirQuota *SetDirQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	setDirQuota.mdsClient = mds.NewMDSServiceClient(cc)
}

func (setDirQuota *SetDirQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := setDirQuota.mdsClient.SetDirQuota(ctx, setDirQuota.Request)
	output.ShowRpcData(setDirQuota.Request, response, setDirQuota.Info.RpcDataShow)
	return response, err
}

func (getDirQuota *GetDirQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getDirQuota.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getDirQuota *GetDirQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getDirQuota.mdsClient.GetDirQuota(ctx, getDirQuota.Request)
	output.ShowRpcData(getDirQuota.Request, response, getDirQuota.Info.RpcDataShow)
	return response, err
}

func (listDirQuota *ListDirQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	listDirQuota.mdsClient = mds.NewMDSServiceClient(cc)
}

func (listDirQuota *ListDirQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := listDirQuota.mdsClient.LoadDirQuotas(ctx, listDirQuota.Request)
	output.ShowRpcData(listDirQuota.Request, response, listDirQuota.Info.RpcDataShow)
	return response, err
}

func (deleteDirQuota *DeleteDirQuotaRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	deleteDirQuota.mdsClient = mds.NewMDSServiceClient(cc)
}

func (deleteDirQuota *DeleteDirQuotaRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := deleteDirQuota.mdsClient.DeleteDirQuota(ctx, deleteDirQuota.Request)
	output.ShowRpcData(deleteDirQuota.Request, response, deleteDirQuota.Info.RpcDataShow)
	return response, err
}

func (listFsInfo *ListFsInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	listFsInfo.mdsClient = mds.NewMDSServiceClient(cc)
}

func (listFsInfo *ListFsInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := listFsInfo.mdsClient.ListFsInfo(ctx, listFsInfo.Request)
	output.ShowRpcData(listFsInfo.Request, response, listFsInfo.Info.RpcDataShow)
	return response, err
}

func (unlinkFile *UnlinkFileRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	unlinkFile.mdsClient = mds.NewMDSServiceClient(cc)
}

func (unlinkFile *UnlinkFileRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := unlinkFile.mdsClient.UnLink(ctx, unlinkFile.Request)
	output.ShowRpcData(unlinkFile.Request, response, unlinkFile.Info.RpcDataShow)
	return response, err
}

func (rmDir *RmDirRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	rmDir.mdsClient = mds.NewMDSServiceClient(cc)
}

func (rmDir *RmDirRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := rmDir.mdsClient.RmDir(ctx, rmDir.Request)
	output.ShowRpcData(rmDir.Request, response, rmDir.Info.RpcDataShow)
	return response, err
}

func (getDirStat *GetDirStatRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	getDirStat.mdsClient = mds.NewMDSServiceClient(cc)
}

func (getDirStat *GetDirStatRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := getDirStat.mdsClient.GetDirStat(ctx, getDirStat.Request)
	output.ShowRpcData(getDirStat.Request, response, getDirStat.Info.RpcDataShow)
	return response, err
}

func (syncDirStat *SyncDirStatRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	syncDirStat.mdsClient = mds.NewMDSServiceClient(cc)
}

func (syncDirStat *SyncDirStatRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := syncDirStat.mdsClient.SyncDirStat(ctx, syncDirStat.Request)
	output.ShowRpcData(syncDirStat.Request, response, syncDirStat.Info.RpcDataShow)
	return response, err
}

func (updateFsInfo *UpdateFsInfoRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	updateFsInfo.mdsClient = mds.NewMDSServiceClient(cc)
}

func (updateFsInfo *UpdateFsInfoRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := updateFsInfo.mdsClient.UpdateFsInfo(ctx, updateFsInfo.Request)
	output.ShowRpcData(updateFsInfo.Request, response, updateFsInfo.Info.RpcDataShow)
	return response, err
}

func (readSlice *ReadSliceRpc) NewRpcClient(cc grpc.ClientConnInterface) {
	readSlice.mdsClient = mds.NewMDSServiceClient(cc)
}

func (readSlice *ReadSliceRpc) Stub_Func(ctx context.Context) (interface{}, error) {
	response, err := readSlice.mdsClient.ReadSlice(ctx, readSlice.Request)
	output.ShowRpcData(readSlice.Request, response, readSlice.Info.RpcDataShow)
	return response, err
}
