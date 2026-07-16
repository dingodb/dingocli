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
	"fmt"
	"strings"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/configure"
	"github.com/dingodb/dingocli/internal/task/step"
	"github.com/dingodb/dingocli/internal/task/task"
	"github.com/dingodb/dingocli/internal/task/task/common"
)

func getEntrypoint(cfg *configure.MonitorConfig) string {
	role := cfg.GetRole()
	if role == ROLE_MONITOR_SYNC {
		return "/dingofs/monitor/start_monitor_sync.sh"
	}
	return ""
}

func getArguments(cfg *configure.MonitorConfig) string {
	role := cfg.GetRole()
	var argsMap map[string]interface{}
	switch role {
	case ROLE_NODE_EXPORTER:
		argsMap = map[string]interface{}{
			"path.rootfs":        "/host",
			"collector.cpu.info": nil,
			"web.listen-address": fmt.Sprintf(":%d", cfg.GetListenPort()),
		}
	case ROLE_PROMETHEUS:
		argsMap = map[string]interface{}{
			"config.file":                 "/etc/prometheus/prometheus.yml",
			"storage.tsdb.path":           "/prometheus",
			"storage.tsdb.retention.time": cfg.GetPrometheusRetentionTime(),
			"storage.tsdb.retention.size": cfg.GetPrometheusRetentionSize(),
			"web.console.libraries":       "/usr/share/prometheus/console_libraries",
			"web.console.templates":       "/usr/share/prometheus/consoles",
			"web.listen-address":          fmt.Sprintf(":%d", cfg.GetListenPort()),
		}
	}
	args := []string{}
	for k, v := range argsMap {
		var item string
		if v != nil {
			item = fmt.Sprintf("--%s=%v", k, v)
		} else {
			item = fmt.Sprintf("--%s", k)
		}
		args = append(args, item)
	}
	return strings.Join(args, " ")
}

func getMountVolumes(cfg *configure.MonitorConfig) []step.Volume {
	role := cfg.GetRole()
	volumes := []step.Volume{}
	switch role {
	case ROLE_NODE_EXPORTER:
		volumes = append(volumes, step.Volume{
			HostPath:      "/",
			ContainerPath: "/host:ro,rslave",
		},
			step.Volume{
				HostPath:      "/run/udev/data",
				ContainerPath: "/run/udev/data",
			},
			step.Volume{
				HostPath:      "/run/dbus/system_bus_socket",
				ContainerPath: "/var/run/dbus/system_bus_socket:ro",
			})
	case ROLE_PROMETHEUS:
		volumes = append(volumes, step.Volume{
			HostPath:      cfg.GetDataDir(),
			ContainerPath: "/prometheus",
		})
		volumes = append(volumes, step.Volume{
			HostPath:      cfg.GetConfDir(),
			ContainerPath: "/etc/prometheus",
		})
	case ROLE_GRAFANA:
		volumes = append(volumes, step.Volume{
			HostPath:      cfg.GetDataDir(),
			ContainerPath: "/var/lib/grafana",
		})
		volumes = append(volumes, step.Volume{
			HostPath:      cfg.GetConfDir(),
			ContainerPath: "/etc/grafana",
		})
		volumes = append(volumes, step.Volume{
			HostPath:      cfg.GetProvisionDir(),
			ContainerPath: "/etc/grafana/provisioning",
		})
	case ROLE_MONITOR_SYNC:
		volumes = append(volumes, step.Volume{
			HostPath:      cfg.GetDataDir(),
			ContainerPath: "/dingofs/monitor",
		})
	}
	return volumes
}

func getEnvironments(cfg *configure.MonitorConfig) []string {
	role := cfg.GetRole()
	if role == ROLE_GRAFANA {
		return []string{
			// "GF_INSTALL_PLUGINS=grafana-piechart-panel",
			fmt.Sprintf("GF_SECURITY_ADMIN_USER=%s", cfg.GetGrafanaUser()),
			fmt.Sprintf("GF_SECURITY_ADMIN_PASSWORD=%s", cfg.GetGrafanaPassword()),
			fmt.Sprintf("GF_SERVER_HTTP_PORT=%d", cfg.GetListenPort()),
		}
	}
	return []string{}
}

func NewCreateContainerTask(dingocli *cli.DingoCli, cfg *configure.MonitorConfig) (*task.Task, error) {
	host := cfg.GetHost()
	hc, err := dingocli.GetHost(host)
	if err != nil {
		return nil, err
	}

	// new task
	subname := fmt.Sprintf("host=%s role=%s", host, cfg.GetRole())
	t := task.NewTask("Create Container", subname, hc.GetSSHConfig())

	// add step to task
	var oldContainerId, containerId string
	clusterId := dingocli.ClusterId()
	mcId := cfg.GetId()
	serviceId := dingocli.GetServiceId(mcId)
	kind := cfg.GetKind()
	role := cfg.GetRole()
	hostname := fmt.Sprintf("%s-%s-%s", kind, role, serviceId)
	options := dingocli.ExecOptions()
	options.ExecWithSudo = false

	t.AddStep(&common.Step2GetService{ // if service exist, break task
		ServiceId:   serviceId,
		ContainerId: &oldContainerId,
		Storage:     dingocli.Storage(),
	})
	paths := []string{cfg.GetDataDir()}
	switch role {
	case ROLE_GRAFANA:
		paths = append(paths, cfg.GetConfDir(), cfg.GetProvisionDir()+"/datasources")
	case ROLE_PROMETHEUS:
		paths = append(paths, cfg.GetConfDir())
	}
	t.AddStep(&step.CreateDirectory{
		Paths:       paths,
		ExecOptions: options,
	})
	t.AddStep(&step.CreateContainer{
		Image:       cfg.GetImage(),
		Entrypoint:  getEntrypoint(cfg),
		Command:     getArguments(cfg),
		AddHost:     []string{fmt.Sprintf("%s:127.0.0.1", hostname)},
		Envs:        getEnvironments(cfg),
		Hostname:    hostname,
		Init:        dingocli.Engine() == common.ENGINE_DOCKER,
		Name:        hostname,
		Privileged:  true,
		User:        "0:0",
		Pid:         "host",
		Restart:     common.POLICY_NEVER_RESTART,
		Ulimits:     []string{"core=-1"},
		Volumes:     getMountVolumes(cfg),
		Out:         &containerId,
		ExecOptions: dingocli.ExecOptions(),
	})
	t.AddStep(&step.Lambda{
		Lambda: common.TrimContainerId(&containerId),
	})
	t.AddStep(&common.Step2InsertService{
		ClusterId:      clusterId,
		ServiceId:      serviceId,
		ContainerId:    &containerId,
		OldContainerId: &oldContainerId,
		Storage:        dingocli.Storage(),
	})
	return t, nil
}
