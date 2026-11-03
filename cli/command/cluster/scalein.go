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
	"github.com/dingodb/dingocli/cli/cli"
	comm "github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/configure/topology"
	"github.com/dingodb/dingocli/internal/errno"
	tui "github.com/dingodb/dingocli/internal/tui/common"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	SCALE_IN_EXAMPLE = `Examples:
  $ dingo cluster scale-in /path/to/new-topology.yaml             # Remove the mds services deleted in new topology
  $ dingo cluster scale-in /path/to/new-topology.yaml --with-log  # Also remove log directories of removed services`
)

type scaleInOptions struct {
	filename string
	withLog  bool
	force    bool
}

func NewScaleInCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options scaleInOptions

	cmd := &cobra.Command{
		Use:     "scale-in TOPOLOGY [OPTIONS]",
		Short:   "Scale in cluster by removing mds services deleted in new topology",
		Args:    cliutil.ExactArgs(1),
		Example: SCALE_IN_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.filename = args[0]
			return runScaleIn(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.BoolVar(&options.withLog, "with-log", false, "Also remove log directories of removed services")
	flags.BoolVarP(&options.force, "force", "f", false, "Never prompt")

	return cmd
}

// getScaleInServices compares the committed topology with the new one and
// returns the deploy configs which only exist in the committed topology.
func getScaleInServices(dingocli *cli.DingoCli, newData string) ([]*topology.DeployConfig, error) {
	diffs, err := dingocli.DiffTopology(dingocli.ClusterTopologyData(), newData)
	if err != nil {
		return nil, err
	}

	removed := []*topology.DeployConfig{}
	for _, diff := range diffs {
		dc := diff.DeployConfig
		switch diff.DiffType {
		case topology.DIFF_ADD:
			return nil, errno.ERR_ADD_SERVICE_WHILE_SCALE_IN_CLUSTER_IS_DENIED.
				F("add service: %s.host[%s]", dc.GetRole(), dc.GetHost())
		case topology.DIFF_DELETE:
			if dc.GetRole() != topology.ROLE_FS_MDS {
				return nil, errno.ERR_UNSUPPORT_ROLE_FOR_SCALE_IN_CLUSTER.
					F("delete service: %s.host[%s]", dc.GetRole(), dc.GetHost())
			}
			removed = append(removed, dc)
		case topology.DIFF_CHANGE:
			dingocli.WriteOutln(color.YellowString(
				"Warning: service %s.host[%s] changed, it will not be touched (use upgrade to apply)",
				dc.GetRole(), dc.GetHost()))
		}
	}
	if len(removed) == 0 {
		return nil, errno.ERR_NO_SERVICES_FOR_SCALE_IN_CLUSTER
	}
	return removed, nil
}

func runScaleIn(dingocli *cli.DingoCli, options scaleInOptions) error {
	// 1) read new topology and find out removed services
	if _, err := dingocli.ParseTopology(); err != nil {
		return err
	}
	data, err := readTopology(options.filename)
	if err != nil {
		return err
	}
	removed, err := getScaleInServices(dingocli, data)
	if err != nil {
		return err
	}

	// 2) display and confirm by user
	dingocli.WriteOutln("Cluster Name   : %s", dingocli.ClusterName())
	dingocli.WriteOutln("Scale in       : %s", serviceStats(dingocli, removed))
	for _, dc := range removed {
		dingocli.WriteOutln("  - host=%s  role=%s  id=%s",
			dc.GetHost(), dc.GetRole(), dingocli.GetServiceId(dc.GetId()))
	}
	dingocli.WriteOutln("")
	if options.force {
		dingocli.WriteOutln(tui.PromptForceOpetation("scale in cluster"))
	} else if pass := tui.ConfirmYes(tui.PromptScaleIn()); !pass {
		dingocli.WriteOut(tui.PromptCancelOpetation("scale in cluster"))
		return errno.ERR_CANCEL_OPERATION
	}

	// 3) stop and remove the container of removed services only
	only := []string{comm.CLEAN_ITEM_CONTAINER}
	if options.withLog {
		only = append(only, comm.CLEAN_ITEM_LOG)
	}
	pb, err := genCleanPlaybook(dingocli, removed, cleanOptions{
		id: "*", role: "*", host: "*", only: only,
	})
	if err != nil {
		return err
	}
	if err = pb.Run(); err != nil {
		return err
	}

	// 4) commit the new topology
	if err = dingocli.Storage().SetClusterTopology(dingocli.ClusterId(), data); err != nil {
		return errno.ERR_UPDATE_CLUSTER_TOPOLOGY_FAILED.E(err)
	}

	dingocli.WriteOutln("")
	dingocli.WriteOutln("%s", color.GreenString("Cluster '%s' scaled in %d services successfully ^_^.",
		dingocli.ClusterName(), len(removed)))
	return nil
}
