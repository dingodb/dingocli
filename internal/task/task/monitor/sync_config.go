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

package monitor

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dingodb/dingocli/cli/cli"
	comm "github.com/dingodb/dingocli/internal/common"
	"github.com/dingodb/dingocli/internal/configure"
	"github.com/dingodb/dingocli/internal/task/context"
	"github.com/dingodb/dingocli/internal/task/scripts"
	"github.com/dingodb/dingocli/internal/task/step"
	"github.com/dingodb/dingocli/internal/task/task"
	"github.com/dingodb/dingocli/internal/task/task/common"
	tui "github.com/dingodb/dingocli/internal/tui/common"
	"github.com/dingodb/dingocli/pkg/variable"
)

const (
	MONITOR_CONF_PATH              = "monitor"
	PROMETHEUS_CONTAINER_CONF_PATH = "/etc/prometheus"
	GRAFANA_CONTAINER_PATH         = "/etc/grafana/grafana.ini"
	DASHBOARD_CONTAINER_PATH       = "/etc/grafana/provisioning/dashboards"
	GRAFANA_DATA_SOURCE_PATH       = "/etc/grafana/provisioning/datasources/all.yml"
	DINGO_TOOL_ROOT_DIR            = "/root/.dingo"
	DINGO_TOOL_SRC_PATH            = "/dingofs/conf/dingo.yaml"
	DINGO_TOOL_DEST_PATH           = "/root/.dingo/dingo.yaml"
	ORIGIN_MONITOR_PATH            = "/dingofs/monitor"
)

func syncPrometheusUid(cfg *configure.MonitorConfig, dingocli cli.DingoCli) step.LambdaType {
	return func(ctx *context.Context) error {
		var prometheusInfo string
		// fetch prometheus info retry 10 times
		for i := 0; i < 10; i++ {
			curlStep := &step.Curl{
				Url:         fmt.Sprintf("-u admin:admin http://localhost:%d/api/datasources", cfg.GetListenPort()),
				Insecure:    true,
				Out:         &prometheusInfo,
				Silent:      true,
				ExecOptions: dingocli.ExecOptions(),
			}
			curlStep.Execute(ctx)
			if len(prometheusInfo) > 0 {
				// try parse prometheus info
				var arr []map[string]interface{}
				if err := json.Unmarshal([]byte(prometheusInfo), &arr); err == nil && len(arr) > 0 {
					if v, ok := arr[0]["uid"].(string); ok {
						// *prometheusUid = v
						sedStep := &step.Command{
							Command:     combineSedCMD(cfg, v),
							ExecOptions: dingocli.ExecOptions(),
						}
						sedStep.Execute(ctx)
						return nil
					}
					return nil
				}
			}
			time.Sleep(3 * time.Second)
		}
		return fmt.Errorf("failed to fetch prometheus info")
	}
}

func combineSedCMD(cfg *configure.MonitorConfig, prometheusUid string) string {
	return fmt.Sprintf(`sed -i 's/${PROMETHEUS_UID}/%s/g' %s/dashboards/server_metric_zh.json`, prometheusUid, cfg.GetProvisionDir())
}

func MutateTool(vars *variable.Variables, delimiter string) step.Mutate {
	return func(in, key, value string) (out string, err error) {
		if len(key) == 0 {
			out = in
			return
		}

		muteKey := strings.TrimSpace(key)

		if muteKey == comm.DINGOCLI_KEY_MDS_ADDR {
			// special handle for mdsaddr config
			value, err = vars.Get(comm.KEY_ENV_MDS_ADDR)
		} else {
			// replace variable
			value, err = vars.Rendering(value)
			if err != nil {
				return
			}
		}

		out = fmt.Sprintf("%s%s%s", key, delimiter, value)
		return
	}
}

func getNodeExporterAddrs(hosts []string, port int) string {
	endpoint := []string{}
	for _, item := range hosts {
		endpoint = append(endpoint, fmt.Sprintf("'%s:%d'", item, port))
	}
	return fmt.Sprintf("[%s]", strings.Join(endpoint, ","))
}

func NewSyncConfigTask(dingocli *cli.DingoCli, cfg *configure.MonitorConfig) (*task.Task, error) {
	serviceId := dingocli.GetServiceId(cfg.GetId())
	containerId, err := dingocli.GetContainerId(serviceId)
	if IsSkip(cfg, []string{ROLE_MONITOR_CONF, ROLE_NODE_EXPORTER}) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	role, host := cfg.GetRole(), cfg.GetHost()
	hc, err := dingocli.GetHost(host)
	if err != nil {
		return nil, err
	}

	// new task
	subname := fmt.Sprintf("host=%s role=%s containerId=%s",
		cfg.GetHost(), cfg.GetRole(), tui.TrimContainerId(containerId))
	t := task.NewTask("Sync Config", subname, hc.GetSSHConfig())
	// add step to task
	var out string
	t.AddStep(&step.ListContainers{ // gurantee container exist
		ShowAll:     true,
		Format:      `"{{.ID}}"`,
		Filter:      fmt.Sprintf("id=%s", containerId),
		Out:         &out,
		ExecOptions: dingocli.ExecOptions(),
	})
	t.AddStep(&step.Lambda{
		Lambda: common.CheckContainerExist(cfg.GetHost(), cfg.GetRole(), containerId, &out),
	})

	// confServiceId = dingocli.GetServiceId(fmt.Sprintf("%s_%s", ROLE_MONITOR_SYNC, cfg.GetHost()))
	// confContainerId, err := dingocli.GetContainerId(serviceId)

	switch role {
	case ROLE_PROMETHEUS:
		// replace prometheus/prometheus.yml port info
		sedCMD := fmt.Sprintf(`sed -i 's/localhost:[0-9]*/localhost:%d/g' %s/prometheus.yml`, cfg.GetListenPort(), cfg.GetConfDir())
		t.AddStep(&step.Command{
			Command:     sedCMD,
			Out:         &out,
			ExecOptions: dingocli.ExecOptions(),
		})

		// replace node exporter addrs
		nodeHosts := cfg.GetNodeIps()
		nodePort := cfg.GetNodeListenPort()
		nodeExporterAddrs := getNodeExporterAddrs(nodeHosts, nodePort)

		// install sync_prometheus.sh
		t.AddStep(&step.InstallFile{
			HostDestPath: fmt.Sprintf("%s/sync_prometheus.sh", cfg.GetConfDir()),
			Content:      &scripts.SYNC_PROMETHEUS,
			ExecOptions:  dingocli.ExecOptions(),
		})

		t.AddStep(&step.Command{
			Command:     fmt.Sprintf("bash %s/sync_prometheus.sh %s/prometheus.yml %s", cfg.GetConfDir(), cfg.GetConfDir(), nodeExporterAddrs),
			Out:         &out,
			ExecOptions: dingocli.ExecOptions(),
		})

	case ROLE_GRAFANA:

		// replace grafana/provisioning/datasources/all.yml port info
		sedPortCMD := fmt.Sprintf(`sed -i 's/localhost:[0-9]*/localhost:%d/g' %s/datasources/all.yml`, cfg.GetPrometheusListenPort(), cfg.GetProvisionDir())
		t.AddStep(&step.Command{
			Command:     sedPortCMD,
			Out:         &out,
			ExecOptions: dingocli.ExecOptions(),
		})

	case ROLE_MONITOR_SYNC:

		confID := cfg.GetServiceConfig()[configure.KEY_ORIGIN_CONFIG_ID].(string)
		confServiceId := dingocli.GetServiceId(confID)
		confContainerId, err := dingocli.GetContainerId(confServiceId)
		if err != nil {
			return nil, err
		}

		// init /root/.dingo dir in container
		t.AddStep(&step.CreateAndUploadDir{
			HostDirName:       ".dingo",
			ContainerDestId:   &containerId,
			ContainerDestPath: DINGO_TOOL_ROOT_DIR,
			ExecOptions:       dingocli.ExecOptions(),
		})

		t.AddStep(&step.TrySyncFile{ // sync dingocli config
			ContainerSrcId:    &confContainerId,
			ContainerSrcPath:  DINGO_TOOL_SRC_PATH,
			ContainerDestId:   &containerId,
			ContainerDestPath: DINGO_TOOL_DEST_PATH,
			KVFieldSplit:      common.CONFIG_DELIMITER_COLON,
			Mutate:            MutateTool(cfg.GetVariables(), common.CONFIG_DELIMITER_COLON),
			ExecOptions:       dingocli.ExecOptions(),
		})

		hostMonitorDir := cfg.GetDataDir()
		t.AddStep(&step.Step2CopyFilesFromContainer{ // copy monitor directory
			ContainerId:   confContainerId,
			Files:         &[]string{ORIGIN_MONITOR_PATH},
			HostDestDir:   hostMonitorDir,
			ExcludeParent: true,
			ExecOptions:   dingocli.ExecOptions(),
		})

		t.AddStep(&step.InstallFile{ // install start_monitor_sync script
			HostDestPath: hostMonitorDir + "/start_monitor_sync.sh",
			Content:      &scripts.START_MONITOR_SYNC,
			ExecOptions:  dingocli.ExecOptions(),
		})

		t.AddStep(&step.Command{
			Command:     fmt.Sprintf("chmod +x %s/start_monitor_sync.sh", hostMonitorDir),
			Out:         &out,
			ExecOptions: dingocli.ExecOptions(),
		})
	}
	return t, nil
}

func NewSyncGrafanaDashboardTask(dingocli *cli.DingoCli, cfg *configure.MonitorConfig) (*task.Task, error) {
	serviceId := dingocli.GetServiceId(cfg.GetId())
	containerId, err := dingocli.GetContainerId(serviceId)
	if IsSkip(cfg, []string{ROLE_MONITOR_CONF, ROLE_NODE_EXPORTER}) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	_, host := cfg.GetRole(), cfg.GetHost()
	hc, err := dingocli.GetHost(host)
	if err != nil {
		return nil, err
	}

	// new task
	subname := fmt.Sprintf("host=%s role=%s containerId=%s",
		cfg.GetHost(), cfg.GetRole(), tui.TrimContainerId(containerId))
	t := task.NewTask("Sync Grafana Dashboard", subname, hc.GetSSHConfig())
	// add step to task
	var out string
	t.AddStep(&step.ListContainers{ // gurantee container exist
		ShowAll:     true,
		Format:      `"{{.ID}}"`,
		Filter:      fmt.Sprintf("id=%s", containerId),
		Out:         &out,
		ExecOptions: dingocli.ExecOptions(),
	})
	t.AddStep(&step.Lambda{
		Lambda: common.CheckContainerExist(cfg.GetHost(), cfg.GetRole(), containerId, &out),
	})

	t.AddStep(&step.InstallFile{ // install server_metric_zh.json
		HostDestPath: fmt.Sprintf("%s/dashboards/%s", cfg.GetProvisionDir(), "server_metric_zh.json"),
		Content:      &scripts.GRAFANA_SERVER_METRIC,
		ExecOptions:  dingocli.ExecOptions(),
	})

	// wait for grafana service started
	t.AddStep(&step.Lambda{
		//Lambda: wait(30),
		Lambda: syncPrometheusUid(cfg, *dingocli),
	})

	return t, nil
}
