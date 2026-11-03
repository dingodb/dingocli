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
	"github.com/dingodb/dingocli/internal/configure/topology"
	"github.com/dingodb/dingocli/internal/errno"
	tui "github.com/dingodb/dingocli/internal/tui/common"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	SCALE_OUT_EXAMPLE = `Examples:
  $ dingo cluster scale-out /path/to/new-topology.yaml  # Deploy the services added in new topology`
)

type scaleOutOptions struct {
	filename      string
	useLocalImage bool
}

func NewScaleOutCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options scaleOutOptions

	cmd := &cobra.Command{
		Use:     "scale-out TOPOLOGY [OPTIONS]",
		Short:   "Scale out cluster by deploying services added in new topology",
		Args:    cliutil.ExactArgs(1),
		Example: SCALE_OUT_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.filename = args[0]
			return runScaleOut(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.BoolVar(&options.useLocalImage, "local", false, "Use local image")

	return cmd
}

// getScaleOutServices compares the committed topology with the new one and
// returns the deploy configs which only exist in the new topology.
func getScaleOutServices(dingocli *cli.DingoCli, newData string) ([]*topology.DeployConfig, error) {
	diffs, err := dingocli.DiffTopology(dingocli.ClusterTopologyData(), newData)
	if err != nil {
		return nil, err
	}

	addedIds := map[string]bool{}
	for _, diff := range diffs {
		dc := diff.DeployConfig
		switch diff.DiffType {
		case topology.DIFF_DELETE:
			return nil, errno.ERR_DELETE_SERVICE_WHILE_SCALE_OUT_CLUSTER_IS_DENIED.
				F("delete service: %s.host[%s]", dc.GetRole(), dc.GetHost())
		case topology.DIFF_ADD:
			addedIds[dc.GetId()] = true
		case topology.DIFF_CHANGE:
			dingocli.WriteOutln(color.YellowString(
				"Warning: service %s.host[%s] changed, it will not be touched (use upgrade to apply)",
				dc.GetRole(), dc.GetHost()))
		}
	}

	dcs, err := dingocli.ParseTopologyData(newData)
	if err != nil {
		return nil, err
	}
	// keep the order of new topology
	added := []*topology.DeployConfig{}
	for _, dc := range dcs {
		if addedIds[dc.GetId()] {
			added = append(added, dc)
		}
	}
	if len(added) == 0 {
		return nil, errno.ERR_NO_SERVICES_FOR_SCALE_OUT_CLUSTER
	}
	if err = assignScaleOutInstanceIds(dingocli, added); err != nil {
		return nil, err
	}
	return added, nil
}

// assignScaleOutInstanceIds gives the added services instance ids following the
// largest id already used by the same role in the committed topology. The
// default id is derived from host/replica sequence and replica count, so it
// shifts once replicas are increased and would collide with running services.
func assignScaleOutInstanceIds(dingocli *cli.DingoCli, added []*topology.DeployConfig) error {
	oldDcs, err := dingocli.ParseTopologyData(dingocli.ClusterTopologyData())
	if err != nil {
		return err
	}
	maxIds := map[string]int{}
	for _, dc := range oldDcs {
		if id := dc.GetDingoInstanceId(); id > maxIds[dc.GetRole()] {
			maxIds[dc.GetRole()] = id
		}
	}
	for _, dc := range added {
		role := dc.GetRole()
		if _, ok := maxIds[role]; !ok {
			continue // no running service of this role, keep default ids
		}
		maxIds[role]++
		dc.SetDingoInstanceId(maxIds[role])
	}
	return nil
}

// precheckBeforeScaleOut checks the environment of the added services only.
// The topology check is skipped: the topology was validated when it was added
// or committed, and its whole-cluster rules (e.g. at least 3 services per
// role) do not apply to a subset of services.
func precheckBeforeScaleOut(dingocli *cli.DingoCli, dcs []*topology.DeployConfig) error {
	pb, err := genPrecheckPlaybook(dingocli, dcs, precheckOptions{
		skip: []string{CHECK_ITEM_TOPOLOGY},
	})
	if err != nil {
		return err
	}
	if err = pb.Run(); err != nil {
		return err
	}

	dingocli.WriteOutln("")
	dingocli.WriteOutln("%s", color.GreenString("Congratulations!!! all precheck passed :)"))
	dingocli.WriteOutln("")
	return nil
}

func runScaleOut(dingocli *cli.DingoCli, options scaleOutOptions) error {
	// 1) read new topology and find out added services
	if _, err := dingocli.ParseTopology(); err != nil {
		return err
	}
	data, err := readTopology(options.filename)
	if err != nil {
		return err
	}
	added, err := getScaleOutServices(dingocli, data)
	if err != nil {
		return err
	}

	// 2) display and confirm by user
	dingocli.WriteOutln("Cluster Name   : %s", dingocli.ClusterName())
	dingocli.WriteOutln("Scale out      : %s", serviceStats(dingocli, added))
	for _, dc := range added {
		dingocli.WriteOutln("  + host=%s  role=%s  id=%s",
			dc.GetHost(), dc.GetRole(), dingocli.GetServiceId(dc.GetId()))
	}
	dingocli.WriteOutln("")
	if pass := tui.ConfirmYes(tui.PromptScaleOut()); !pass {
		dingocli.WriteOut(tui.PromptCancelOpetation("scale out cluster"))
		return errno.ERR_CANCEL_OPERATION
	}

	// 3) precheck and deploy the added services only
	deployOpts := deployOptions{useLocalImage: options.useLocalImage}
	if err = precheckBeforeScaleOut(dingocli, added); err != nil {
		return err
	}
	pb, err := genDeployPlaybook(dingocli, added, deployOpts)
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
	dingocli.WriteOutln("%s", color.GreenString("Cluster '%s' scaled out %d services successfully ^_^.",
		dingocli.ClusterName(), len(added)))
	return nil
}
