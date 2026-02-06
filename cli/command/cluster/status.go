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

package cluster

import (
	"fmt"
	"strings"

	"github.com/dingodb/dingocli/cli/cli"
	comm "github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/configure/topology"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/playbook"
	task "github.com/dingodb/dingocli/internal/task/task/common"
	tui "github.com/dingodb/dingocli/internal/tui/service"
	"github.com/dingodb/dingocli/internal/utils"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	GET_STATUS_PLAYBOOK_STEPS = []int{
		playbook.INIT_SERVIE_STATUS,
		playbook.GET_SERVICE_STATUS,
	}
)

type statusOptions struct {
	id            string
	role          string
	host          string
	verbose       bool
	showInstances bool
	withCluster   string
	dir           string
}

func NewStatusCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options statusOptions

	cmd := &cobra.Command{
		Use:   "status [OPTIONS]",
		Short: "Display cluster status",
		Args:  cliutil.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.StringVar(&options.id, "id", "*", "Specify service id")
	flags.StringVar(&options.role, "role", "*", "Specify service role")
	flags.StringVar(&options.host, "host", "*", "Specify service host")
	flags.BoolVarP(&options.verbose, "verbose", "v", false, "Verbose output for status")
	flags.BoolVarP(&options.showInstances, "show-instances", "s", false, "Display service num")
	flags.StringVarP(&options.withCluster, "with-cluster", "w", "", "Display status of specified cluster with current default cluster")
	flags.StringVar(&options.dir, "dir", "", "Only display services which data/raft/doc/vector dirs contain specified string")

	return cmd
}

func getClusterMdsAddr(dcs []*topology.DeployConfig) string {
	value, err := dcs[0].GetVariables().Get("cluster_mds_addr")
	if err != nil {
		return "-"
	}
	return value
}

func getClusterCoorAddr(dcs []*topology.DeployConfig) string {
	value, err := dcs[0].GetVariables().Get("coordinator_addr")
	if err != nil {
		return "-"
	}
	return value
}

func getClusterMdsLeader(statuses []task.ServiceStatus) string {
	leaders := []string{}
	for _, status := range statuses {
		if !status.IsLeader {
			continue
		}
		dc := status.Config
		leader := fmt.Sprintf("%s:%d / %s",
			dc.GetListenIp(), dc.GetListenPort(), status.Id)
		leaders = append(leaders, leader)
	}
	if len(leaders) > 0 {
		return strings.Join(leaders, ", ")
	}
	return color.RedString("<no leader>")
}

func getClusterCoorServerAddr(dcs []*topology.DeployConfig) string {
	value, err := dcs[0].GetVariables().Get("cluster_coor_srv_peers")
	if err != nil {
		return "-"
	}
	return value
}

func getClusterCoorRaftAddr(dcs []*topology.DeployConfig) string {
	value, err := dcs[0].GetVariables().Get("cluster_coor_raft_peers")
	if err != nil {
		return "-"
	}
	return value
}

func displayStatus(dingocli *cli.DingoCli, dcs []*topology.DeployConfig, options statusOptions) int {
	statuses := []task.ServiceStatus{}
	value := dingocli.MemStorage().Get(comm.KEY_ALL_SERVICE_STATUS)
	if value != nil {
		m := value.(map[string]task.ServiceStatus)
		for _, status := range m {
			statuses = append(statuses, status)
		}
	}
	excludeCols := []string{}
	roles := dingocli.GetRoles(dcs)
	isMdsv2 := dcs[0].GetCtx().Lookup(topology.CTX_KEY_MDS_VERSION) == topology.CTX_VAL_MDS_V2
	isMdsv2Only := false
	if utils.ContainsList(roles, []string{topology.ROLE_FS_MDS, topology.ROLE_FS_MDS_CLI}) && len(roles) == 2 {
		isMdsv2Only = true
		excludeCols = append(excludeCols, "Data Dir")
	}

	output := ""
	width := 0
	if len(options.dir) == 0 {
		output, width = tui.FormatStatus(dcs[0].GetKind(), statuses, options.verbose, options.showInstances, excludeCols, isMdsv2, isMdsv2Only)
	} else {
		dirStrs := strings.Split(options.dir, ",")
		onlyDirs := []string{}
		if len(dirStrs) > 0 {
			if utils.Contains(dirStrs, "log") {
				onlyDirs = append(onlyDirs, "Log Dir")
			}
			if utils.Contains(dirStrs, "data") {
				onlyDirs = append(onlyDirs, "Data Dir")
			}
			if utils.Contains(dirStrs, "raft") {
				onlyDirs = append(onlyDirs, "Raft Dir")
			}
			if utils.Contains(dirStrs, "doc") {
				onlyDirs = append(onlyDirs, "Doc Dir")
			}
			if utils.Contains(dirStrs, "vector") {
				onlyDirs = append(onlyDirs, "Vector Dir")
			}
		}

		output, width = tui.FormatDirStatus(dcs[0].GetKind(), statuses, options.showInstances, onlyDirs)
	}

	dingocli.WriteOutln("")

	switch dcs[0].GetKind() {
	case topology.KIND_DINGOFS:
		if isMdsv2 {
			if utils.ContainsList(roles, []string{topology.ROLE_FS_MDS, topology.ROLE_FS_MDS_CLI}) {
				// check mds's mds_storage_engine config is tikv
				if dcs[0].GetRole() == topology.ROLE_FS_MDS && dcs[0].GetMdsStorageEngine() == "tikv" {
					dingocli.WriteOutln("cluster name : %s", dingocli.ClusterName())
					dingocli.WriteOutln("cluster kind : %s", dcs[0].GetKind())
					dingocli.WriteOutln("mds     addr : %s", getClusterMdsAddr(dcs))
					dingocli.WriteOutln("tikv    addr : %s", strings.TrimPrefix(dcs[0].GetMdsStorageUrl(), "list://"))
				} else {
					dingocli.WriteOutln("cluster name     : %s", dingocli.ClusterName())
					dingocli.WriteOutln("cluster kind     : %s", dcs[0].GetKind())
					dingocli.WriteOutln("mds     addr     : %s", getClusterMdsAddr(dcs))
					dingocli.WriteOutln("coordinator addr : %s", dcs[0].GetDingoStoreCoordinatorAddr())
				}
			} else {
				dingocli.WriteOutln("cluster name     : %s", dingocli.ClusterName())
				dingocli.WriteOutln("cluster kind     : %s", dcs[0].GetKind())
				dingocli.WriteOutln("mds     addr     : %s", getClusterMdsAddr(dcs))
				dingocli.WriteOutln("coordinator addr : %s", getClusterCoorAddr(dcs))
			}
		} else {
			dingocli.WriteOutln("cluster name      : %s", dingocli.ClusterName())
			dingocli.WriteOutln("cluster kind      : %s", dcs[0].GetKind())
			dingocli.WriteOutln("cluster mds addr  : %s", getClusterMdsAddr(dcs))
			dingocli.WriteOutln("cluster mds leader: %s", getClusterMdsLeader(statuses))
		}
	case topology.KIND_DINGOSTORE:
		dingocli.WriteOutln("cluster name             : %s", dingocli.ClusterName())
		dingocli.WriteOutln("cluster kind             : %s", dcs[0].GetKind())
		dingocli.WriteOutln("cooridinator server addr : %s", getClusterCoorServerAddr(dcs))
		dingocli.WriteOutln("cooridinator raft   addr : %s", getClusterCoorRaftAddr(dcs))
	case topology.KIND_DINGODB:
		dingocli.WriteOutln("cluster name             : %s", dingocli.ClusterName())
		dingocli.WriteOutln("cluster kind             : %s", dcs[0].GetKind())
		dingocli.WriteOutln("coordinator addr         : %s", getClusterCoorServerAddr(dcs))
		dingocli.WriteOutln("coordinator raft   addr  : %s", getClusterCoorRaftAddr(dcs))
	}

	dingocli.WriteOutln("")
	dingocli.WriteOut("%s", output)
	return width
}

func genStatusPlaybook(dingocli *cli.DingoCli,
	dcs []*topology.DeployConfig,
	options statusOptions) (*playbook.Playbook, error) {
	dcs = dingocli.FilterDeployConfig(dcs, topology.FilterOption{
		Id:   options.id,
		Role: options.role,
		Host: options.host,
	})

	// skip ROLE_TMP dc
	for i := 0; i < len(dcs); i++ {
		if dcs[i].GetRole() == topology.ROLE_FS_MDS_CLI {
			dcs = append(dcs[:i], dcs[i+1:]...)
			i-- // adjust index after removal
		}
	}

	if len(dcs) == 0 {
		return nil, errno.ERR_NO_SERVICES_MATCHED
	}

	steps := GET_STATUS_PLAYBOOK_STEPS
	pb := playbook.NewPlaybook(dingocli)
	for _, step := range steps {
		pb.AddStep(&playbook.PlaybookStep{
			Type:    step,
			Configs: dcs,
			ExecOptions: playbook.ExecOptions{
				//Concurrency:   10,
				SilentSubBar:  true,
				SilentMainBar: step == playbook.INIT_SERVIE_STATUS,
				SkipError:     true,
			},
		})
	}
	return pb, nil
}

func runStatus(dingocli *cli.DingoCli, options statusOptions) error {
	// 1) parse cluster topology
	dcs, err := dingocli.ParseTopology()
	if err != nil {
		return err
	}

	// 2) generate get status playbook
	pb, err := genStatusPlaybook(dingocli, dcs, options)
	if err != nil {
		return err
	}

	// 3) run playground
	err = pb.Run()

	// 4) display service status
	width := displayStatus(dingocli, dcs, options)
	if options.withCluster != "" {

		dingocli.WriteOutln("\n%s\n", strings.Repeat("-", width))
		storage := dingocli.Storage()
		attachCluster, err := storage.GetClusterByName(options.withCluster)
		if err != nil || attachCluster.Id <= 0 {
			dingocli.WriteOutln("Not Found cluster: %s ", options.withCluster)
		} else {
			err = dingocli.SwitchCluster(attachCluster)
			if err != nil {
				dingocli.WriteOutln("Switch cluster: %s failed ", options.withCluster)
			} else {
				dcs, err := dingocli.ParseTopology()
				if err == nil {
					pb, err := genStatusPlaybook(dingocli, dcs, options)
					if err == nil {
						pb.Run()
						displayStatus(dingocli, dcs, options)
					}
				}
			}
		}
	}
	return err
}
