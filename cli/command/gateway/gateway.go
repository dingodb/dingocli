/*
 * Copyright (c) 2026 dingofs.com, Inc. All Rights Reserved
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

package gateway

import (
	"fmt"
	"net"
	"os"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/rpc"
	"github.com/dingodb/dingocli/internal/utils"
	pbmdserror "github.com/dingodb/dingocli/proto/dingofs/proto/error"
	"github.com/dingodb/dingocli/proto/dingofs/proto/mds"
	mnas "github.com/jackblack369/minio/cmd/gateway/nas"
	mcli "github.com/minio/cli"
	"github.com/spf13/cobra"
)

const (
	GATEWAY_EXAMPLE = `Examples:
   $ export S3_GATEWAY_ROOT_USER=admin S3_GATEWAY_ROOT_PASSWORD=password
   $ dingo gateway --fsid=1 --listen-address=:19000 --console-address=:19001
   $ dingo gateway --fsname=myfs --mdsaddr=10.220.69.6:7400 --listen-address=:19000
   $ dingo gateway --mountpoint=/mnt/dingofs --listen-address=:19000`

	GATEWAY_LISTEN_ADDRESS  = "listen-address"
	GATEWAY_CONSOLE_ADDRESS = "console-address"
	GATEWAY_MOUNTPOINT      = "mountpoint"

	DEFAULT_CONSOLE_ADDRESS = ":19001"
)

type gatewayOptions struct {
	listenAddr  string
	consoleAddr string
	mountpoint  string
}

func NewGatewayCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options gatewayOptions

	cmd := &cobra.Command{
		Use:     "gateway [OPTIONS]",
		Short:   "Start an S3-compatible gateway on a mounted filesystem",
		GroupID: "ADMIN",
		Args:    utils.NoArgs,
		Example: GATEWAY_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			utils.ReadCommandConfig(cmd)

			options.listenAddr, _ = cmd.Flags().GetString(GATEWAY_LISTEN_ADDRESS)
			options.consoleAddr, _ = cmd.Flags().GetString(GATEWAY_CONSOLE_ADDRESS)
			options.mountpoint, _ = cmd.Flags().GetString(GATEWAY_MOUNTPOINT)

			return runGateway(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	// add flags
	utils.AddBoolFlag(cmd, utils.VERBOSE, "Show more debug info")
	utils.AddConfigFileFlag(cmd)

	utils.AddDurationFlag(cmd, utils.RPCTIMEOUT, "RPC timeout")
	utils.AddDurationFlag(cmd, utils.RPCRETRYDElAY, "RPC retry delay")
	utils.AddUint32Flag(cmd, utils.RPCRETRYTIMES, "RPC retry times")

	utils.AddStringFlag(cmd, utils.DINGOFS_MDSADDR, "Specify mds address")
	utils.AddUint32Flag(cmd, utils.DINGOFS_FSID, "Specify filesystem id, used to locate the mountpoint")
	utils.AddStringFlag(cmd, utils.DINGOFS_FSNAME, "Specify filesystem name, used to locate the mountpoint")

	cmd.Flags().String(GATEWAY_LISTEN_ADDRESS, "", "Bind the S3 API to ADDRESS:PORT, e.g. :19000 (required)")
	cmd.Flags().String(GATEWAY_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ADDRESS, "Bind the web console to ADDRESS:PORT")
	cmd.Flags().String(GATEWAY_MOUNTPOINT, "", "Local mountpoint to serve, skips the mds lookup if set")
	cmd.MarkFlagRequired(GATEWAY_LISTEN_ADDRESS)

	return cmd
}

func runGateway(cmd *cobra.Command, dingocli *cli.DingoCli, options gatewayOptions) error {
	// S3_GATEWAY_ROOT_* are the credential variables read by the gateway;
	// MINIO_ACCESS_KEY/MINIO_SECRET_KEY are the deprecated fallback it still accepts.
	ak := os.Getenv("S3_GATEWAY_ROOT_USER")
	if ak == "" {
		ak = os.Getenv("MINIO_ACCESS_KEY")
	}
	if len(ak) < 3 {
		return fmt.Errorf("S3_GATEWAY_ROOT_USER should be specified as an environment variable with at least 3 characters")
	}
	sk := os.Getenv("S3_GATEWAY_ROOT_PASSWORD")
	if sk == "" {
		sk = os.Getenv("MINIO_SECRET_KEY")
	}
	if len(sk) < 8 {
		return fmt.Errorf("S3_GATEWAY_ROOT_PASSWORD should be specified as an environment variable with at least 8 characters")
	}

	mountpoint := options.mountpoint
	if mountpoint == "" {
		var err error
		if mountpoint, err = findMountpoint(cmd); err != nil {
			return err
		}
	}
	if info, err := os.Stat(mountpoint); err != nil || !info.IsDir() {
		return fmt.Errorf("mountpoint %q is not an accessible directory, mount the filesystem first or specify --%s", mountpoint, GATEWAY_MOUNTPOINT)
	}

	args := []string{"gateway", "--address", options.listenAddr, "--console-address", options.consoleAddr, "--anonymous", mountpoint}

	app := &mcli.App{
		Action: func(ctx *mcli.Context) error {
			mnas.NasGatewayMain(ctx)
			return nil
		},
		Flags: []mcli.Flag{
			mcli.StringFlag{
				Name:  "address",
				Value: ":19000",
				Usage: "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname",
			},
			mcli.StringFlag{
				Name:  "console-address",
				Value: DEFAULT_CONSOLE_ADDRESS,
				Usage: "bind to a specific CONSOLE_ADDRESS:PORT, CONSOLE_ADDRESS can be an IP or hostname",
			},
			mcli.BoolFlag{
				Name:  "anonymous",
				Usage: "hide sensitive information from logging",
			},
			mcli.BoolFlag{
				Name:  "json",
				Usage: "output server logs and startup information in json format",
			},
			mcli.BoolFlag{
				Name:  "quiet",
				Usage: "disable MinIO startup information",
			},
		},
	}

	return app.Run(args)
}

// findMountpoint asks mds for the mountpoints of the given filesystem and
// returns the path of the one mounted on this host.
func findMountpoint(cmd *cobra.Command) (string, error) {
	fsId, fsName, err := utils.GetFsInfoFlagValue(cmd)
	if err != nil {
		return "", fmt.Errorf("%v, or specify --%s", err, GATEWAY_MOUNTPOINT)
	}

	mdsRpc, err := rpc.CreateNewMdsRpc(cmd, "ListFsInfo")
	if err != nil {
		return "", err
	}

	listRpc := &rpc.ListFsRpc{
		Info:    mdsRpc,
		Request: &mds.ListFsInfoRequest{},
	}
	response, rpcErr := rpc.GetRpcResponse(listRpc.Info, listRpc)
	if rpcErr != nil && rpcErr.GetCode() != 0 {
		return "", rpcErr
	}
	result := response.(*mds.ListFsInfoResponse)
	if mdsErr := result.GetError(); mdsErr.GetErrcode() != pbmdserror.Errno_OK {
		return "", fmt.Errorf("failed to list filesystems: %s", mdsErr.String())
	}

	localIPs := localIPSet()
	for _, fsInfo := range result.GetFsInfos() {
		if (fsId != 0 && fsInfo.GetFsId() != fsId) || (fsName != "" && fsInfo.GetFsName() != fsName) {
			continue
		}
		for _, mp := range fsInfo.GetMountPoints() {
			if localIPs[mp.GetIp()] {
				return mp.GetPath(), nil
			}
		}
		return "", fmt.Errorf("filesystem %q (id %d) is not mounted on this host, mount it first or specify --%s",
			fsInfo.GetFsName(), fsInfo.GetFsId(), GATEWAY_MOUNTPOINT)
	}

	return "", fmt.Errorf("filesystem not found")
}

func localIPSet() map[string]bool {
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
