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

package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	comm "github.com/dingodb/dingocli/internal/common"
	configure "github.com/dingodb/dingocli/internal/configure/dingocli"
	"github.com/dingodb/dingocli/internal/configure/hosts"
	"github.com/dingodb/dingocli/internal/configure/topology"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/storage"
	tools "github.com/dingodb/dingocli/internal/tools/upgrade"
	tui "github.com/dingodb/dingocli/internal/tui/common"
	"github.com/dingodb/dingocli/internal/utils"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	log "github.com/dingodb/dingocli/pkg/log/glg"
	"github.com/dingodb/dingocli/pkg/logger"
	"github.com/dingodb/dingocli/pkg/module"
)

type DingoCli struct {
	// project layout
	rootDir   string
	binDir    string
	dataDir   string
	pluginDir string
	logDir    string
	tempDir   string
	logpath   string
	config    *configure.DingoCliConfig

	// data pipeline
	in         io.Reader
	out        io.Writer
	err        io.Writer
	storage    *storage.Storage
	memStorage *utils.SafeMap

	// properties (hosts/cluster)
	hosts               string // hosts
	clusterId           int    // current cluster id
	clusterUUId         string // current cluster uuid
	clusterName         string // current cluster name
	clusterTopologyData string // cluster topology
	clusterPoolData     string // cluster pool
	monitor             storage.Monitor

	dingoLogger *logger.DingoLogger
}

/*
 * $HOME/.dingocli
 *   - dingocli.cfg
 *   - /bin/dingocli
 *   - /data/dingocli.db
 *   - /plugins/{shell,file,polarfs}
 *   - /logs/2006-01-02_15-04-05.log
 *   - /temp/
 */
func NewDingoCli() (*DingoCli, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, errno.ERR_GET_USER_HOME_DIR_FAILED.E(err)
	}

	rootDir := fmt.Sprintf("%s/.dingo", home)
	dingocli := &DingoCli{
		rootDir:   rootDir,
		binDir:    path.Join(rootDir, "bin"),
		dataDir:   path.Join(rootDir, "data"),
		pluginDir: path.Join(rootDir, "plugins"),
		logDir:    path.Join(rootDir, "logs"),
		tempDir:   path.Join(rootDir, "temp"),
	}

	err = dingocli.init()
	if err != nil {
		return nil, err
	}

	return dingocli, nil
}

func (dingocli *DingoCli) init() error {
	// (1) Create directory
	dirs := []string{
		dingocli.rootDir,
		dingocli.binDir,
		dingocli.dataDir,
		dingocli.pluginDir,
		dingocli.logDir,
		dingocli.tempDir,
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return errno.ERR_CREATE_DINGOADM_SUBDIRECTORY_FAILED.E(err)
		}
	}

	// (2) Parse dingocli.cfg
	confpath := fmt.Sprintf("%s/dingocli.cfg", dingocli.rootDir)
	config, err := configure.ParseDingoCliConfig(confpath)
	if err != nil {
		return err
	}
	configure.ReplaceGlobals(config)

	// (3) Init logger
	now := time.Now().Format("2006-01-02_15-04-05")
	logpath := fmt.Sprintf("%s/dingocli-%s.log", dingocli.logDir, now)
	if err := log.Init(config.GetLogLevel(), logpath); err != nil {
		return errno.ERR_INIT_LOGGER_FAILED.E(err)
	} else {
		log.Info("Init logger success",
			log.Field("LogPath", logpath),
			log.Field("LogLevel", config.GetLogLevel()))
	}

	// (4) Init error code
	errno.Init(logpath)

	// (5) New storage: create table in sqlite/rqlite
	dbUrl := config.GetDBUrl()
	s, err := storage.NewStorage(dbUrl)
	if err != nil {
		log.Error("Init SQLite database failed",
			log.Field("Error", err))
		return errno.ERR_INIT_SQL_DATABASE_FAILED.E(err)
	}

	// (6) Get hosts
	var hosts storage.Hosts
	hostses, err := s.GetHostses()
	if err != nil {
		log.Error("Get hosts failed",
			log.Field("Error", err))
		return errno.ERR_GET_HOSTS_FAILED.E(err)
	} else if len(hostses) == 1 {
		hosts = hostses[0]
	}

	// (7) Get current cluster
	var cluster storage.Cluster
	// check current active cluster config in env or not

	if activatedClusterName := getActivatedClusterFromEnv(); activatedClusterName != "" {
		cluster, err = s.GetClusterByName(activatedClusterName)
		if err != nil {
			log.Error("Get cluster by name failed",
				log.Field("Error", err))
		}
	} else {
		cluster, err = s.GetCurrentCluster()
		if err != nil {
			log.Error("Get current cluster failed",
				log.Field("Error", err))
			return errno.ERR_GET_CURRENT_CLUSTER_FAILED.E(err)
		} else {
			log.Info("Get current cluster success",
				log.Field("ClusterId", cluster.Id),
				log.Field("ClusterName", cluster.Name))
		}
	}

	// (8) Get monitor configure
	monitor, err := s.GetMonitor(cluster.Id)
	if err != nil {
		log.Error("Get monitor failed", log.Field("Error", err))
		return errno.ERR_GET_MONITOR_FAILED.E(err)
	}

	dingocli.logpath = logpath
	dingocli.config = config
	dingocli.in = os.Stdin
	dingocli.out = os.Stdout
	dingocli.err = os.Stderr
	dingocli.storage = s
	dingocli.memStorage = utils.NewSafeMap()
	dingocli.hosts = hosts.Data
	dingocli.clusterId = cluster.Id
	dingocli.clusterUUId = cluster.UUId
	dingocli.clusterName = cluster.Name
	dingocli.clusterTopologyData = cluster.Topology
	dingocli.clusterPoolData = cluster.Pool
	dingocli.monitor = monitor
	dingocli.dingoLogger = logger.InitGlobalLogger(logger.WithLogFile(fmt.Sprintf("%s/dingo.log", dingocli.logDir)))

	return nil
}

func getActivatedClusterFromEnv() string {
	// Check original case first
	if activatedClusterName, exists := os.LookupEnv(comm.KEY_ENV_ACTIVATE_CLUSTER); exists && len(activatedClusterName) > 0 {
		return activatedClusterName
	}

	// Check lowercase version as fallback
	if activatedClusterName, exists := os.LookupEnv(strings.ToLower(comm.KEY_ENV_ACTIVATE_CLUSTER)); exists && len(activatedClusterName) > 0 {
		return activatedClusterName
	}

	return ""
}

func (dingocli *DingoCli) Upgrade() (bool, error) {
	if dingocli.config.GetAutoUpgrade() == false {
		return false, nil
	}

	versions, err := dingocli.Storage().GetVersions()
	if err != nil || len(versions) == 0 {
		return false, nil
	}

	// (1) skip upgrade if the pending version is stale
	latestVersion := versions[0].Version
	err, yes := tools.IsLatest(Version, strings.TrimPrefix(latestVersion, "v"))
	if err != nil || yes {
		return false, nil
	}

	// (2) skip upgrade if user has confirmed
	day := time.Now().Format("2006-01-02")
	lastConfirm := versions[0].LastConfirm
	if day == lastConfirm {
		return false, nil
	}

	dingocli.Storage().SetVersion(latestVersion, day)
	pass := tui.ConfirmYes(tui.PromptAutoUpgrade(latestVersion))
	if !pass {
		return false, errno.ERR_CANCEL_OPERATION
	}

	err = tools.Upgrade(latestVersion)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (dingocli *DingoCli) RootDir() string                   { return dingocli.rootDir }
func (dingocli *DingoCli) DataDir() string                   { return dingocli.dataDir }
func (dingocli *DingoCli) PluginDir() string                 { return dingocli.pluginDir }
func (dingocli *DingoCli) LogDir() string                    { return dingocli.logDir }
func (dingocli *DingoCli) TempDir() string                   { return dingocli.tempDir }
func (dingocli *DingoCli) LogPath() string                   { return dingocli.logpath }
func (dingocli *DingoCli) Config() *configure.DingoCliConfig { return dingocli.config }
func (dingocli *DingoCli) SudoAlias() string                 { return dingocli.config.GetSudoAlias() }
func (dingocli *DingoCli) SSHTimeout() int                   { return dingocli.config.GetSSHTimeout() }
func (dingocli *DingoCli) Engine() string                    { return dingocli.config.GetEngine() }
func (dingocli *DingoCli) In() io.Reader                     { return dingocli.in }
func (dingocli *DingoCli) Out() io.Writer                    { return dingocli.out }
func (dingocli *DingoCli) Err() io.Writer                    { return dingocli.err }
func (dingocli *DingoCli) Storage() *storage.Storage         { return dingocli.storage }
func (dingocli *DingoCli) MemStorage() *utils.SafeMap        { return dingocli.memStorage }
func (dingocli *DingoCli) Hosts() string                     { return dingocli.hosts }
func (dingocli *DingoCli) ClusterId() int                    { return dingocli.clusterId }
func (dingocli *DingoCli) ClusterUUId() string               { return dingocli.clusterUUId }
func (dingocli *DingoCli) ClusterName() string               { return dingocli.clusterName }
func (dingocli *DingoCli) ClusterTopologyData() string       { return dingocli.clusterTopologyData }
func (dingocli *DingoCli) ClusterPoolData() string           { return dingocli.clusterPoolData }
func (dingocli *DingoCli) Monitor() storage.Monitor          { return dingocli.monitor }

func (dingocli *DingoCli) GetHost(host string) (*hosts.HostConfig, error) {
	if len(dingocli.Hosts()) == 0 {
		return nil, errno.ERR_HOST_NOT_FOUND.
			F("host: %s", host)
	}
	hcs, err := hosts.ParseHosts(dingocli.Hosts())
	if err != nil {
		return nil, err
	}

	for _, hc := range hcs {
		if hc.GetHost() == host {
			return hc, nil
		}
	}
	return nil, errno.ERR_HOST_NOT_FOUND.
		F("host: %s", host)
}

func (dingocli *DingoCli) ParseTopologyData(data string) ([]*topology.DeployConfig, error) {
	ctx := topology.NewContext()
	hcs, err := hosts.ParseHosts(dingocli.Hosts())
	if err != nil {
		return nil, err
	}
	for _, hc := range hcs {
		ctx.Add(hc.GetHost(), hc.GetHostname())
	}

	dcs, err := topology.ParseTopology(data, ctx)
	if err != nil {
		return nil, err
	} else if len(dcs) == 0 {
		return nil, errno.ERR_NO_SERVICES_IN_TOPOLOGY
	}
	return dcs, err
}

func (dingocli *DingoCli) ParseTopology() ([]*topology.DeployConfig, error) {
	if dingocli.ClusterId() == -1 {
		return nil, errno.ERR_NO_CLUSTER_SPECIFIED
	}
	return dingocli.ParseTopologyData(dingocli.ClusterTopologyData())
}

func (dingocli *DingoCli) FilterDeployConfig(deployConfigs []*topology.DeployConfig,
	options topology.FilterOption) []*topology.DeployConfig {
	dcs := []*topology.DeployConfig{}
	for _, dc := range deployConfigs {
		dcId := dc.GetId()
		role := dc.GetRole()
		host := dc.GetHost()
		serviceId := dingocli.GetServiceId(dcId)
		if (options.Id == "*" || options.Id == serviceId) &&
			(options.Role == "*" || options.Role == role) &&
			(options.Host == "*" || options.Host == host) {
			dcs = append(dcs, dc)
		}
	}

	return dcs
}

func (dingocli *DingoCli) FilterDeployConfigByGateway(deployConfigs []*topology.DeployConfig,
	options topology.FilterOption) *topology.DeployConfig {
	for _, dc := range deployConfigs {
		host := dc.GetHost()
		if options.Host == host {
			return dc
		}
	}

	return nil
}

func (dingocli *DingoCli) FilterDeployConfigByRole(dcs []*topology.DeployConfig,
	role string) []*topology.DeployConfig {
	options := topology.FilterOption{Id: "*", Role: role, Host: "*"}
	return dingocli.FilterDeployConfig(dcs, options)
}

func (dingocli *DingoCli) GetServiceId(dcId string) string {
	serviceId := fmt.Sprintf("%s_%s", dingocli.ClusterUUId(), dcId)
	return utils.MD5Sum(serviceId)[:12]
}

func (dingocli *DingoCli) GetContainerId(serviceId string) (string, error) {
	containerId, err := dingocli.Storage().GetContainerId(serviceId)
	if err != nil {
		return "", errno.ERR_GET_SERVICE_CONTAINER_ID_FAILED
	} else if len(containerId) == 0 {
		// return "", errno.ERR_SERVICE_CONTAINER_ID_NOT_FOUND
		return comm.CLEANED_CONTAINER_ID, nil
	}
	return containerId, nil
}

// FIXME
func (dingocli *DingoCli) IsSkip(dc *topology.DeployConfig) bool {
	serviceId := dingocli.GetServiceId(dc.GetId())
	containerId, err := dingocli.Storage().GetContainerId(serviceId)
	return err == nil && len(containerId) == 0 && dc.GetRole() == topology.ROLE_SNAPSHOTCLONE
}

func (dingocli *DingoCli) GetFilesystemId(host, mountPoint string) string {
	filesystemId := fmt.Sprintf("dingofs_filesystem_%s_%s", host, mountPoint)
	return utils.MD5Sum(filesystemId)[:12]
}

func (dingocli *DingoCli) ExecOptions() module.ExecOptions {
	return module.ExecOptions{
		ExecWithSudo:   true,
		ExecInLocal:    false,
		ExecSudoAlias:  dingocli.config.GetSudoAlias(),
		ExecTimeoutSec: dingocli.config.GetTimeout(),
		ExecWithEngine: dingocli.config.GetEngine(),
	}
}

func (dingocli *DingoCli) CheckId(id string) error {
	services, err := dingocli.Storage().GetServices(dingocli.ClusterId())
	if err != nil {
		return err
	}
	for _, service := range services {
		if service.Id == id {
			return nil
		}
	}
	return errno.ERR_ID_NOT_FOUND.F("id: %s", id)
}

func (dingocli *DingoCli) CheckRole(role string) error {
	dcs, err := dingocli.ParseTopology()
	if err != nil {
		return err
	}

	kind := dcs[0].GetKind()
	roles := topology.DINGOFS_ROLES
	switch kind {
	case topology.KIND_DINGODB:
		roles = topology.DINGODB_ROLES
	case topology.KIND_DINGOSTORE:
		roles = topology.DINGOSTORE_ROLES
	}
	supported := utils.Slice2Map(roles)
	if !supported[role] {
		switch kind {
		case topology.KIND_DINGOFS:
			return errno.ERR_UNSUPPORT_DINGOFS_ROLE.
				F("role: %s", role)
		case topology.KIND_DINGODB:
			return errno.ERR_UNSUPPORT_DINGODB_ROLE.
				F("role: %s", role)
		case topology.KIND_DINGOSTORE:
			return errno.ERR_UNSUPPORT_DINGOSTORE_ROLE.
				F("role: %s", role)
		}
	}
	return nil
}

func (dingocli *DingoCli) CheckHost(host string) error {
	_, err := dingocli.GetHost(host)
	return err
}

// writer for cobra command error
func (dingocli *DingoCli) Write(p []byte) (int, error) {
	// trim prefix which generate by cobra
	p = p[len(cliutil.PREFIX_COBRA_COMMAND_ERROR):]
	return dingocli.WriteOut(string(p))
}

func (dingocli *DingoCli) WriteOut(format string, a ...interface{}) (int, error) {
	output := fmt.Sprintf(format, a...)
	return dingocli.out.Write([]byte(output))
}

func (dingocli *DingoCli) WriteOutln(format string, a ...interface{}) (int, error) {
	output := fmt.Sprintf(format, a...) + "\n"
	return dingocli.out.Write([]byte(output))
}

func (dingocli *DingoCli) IsSameRole(dcs []*topology.DeployConfig) bool {
	role := dcs[0].GetRole()
	for _, dc := range dcs {
		if dc.GetRole() != role {
			return false
		}
	}
	return true
}

func (dingocli *DingoCli) DiffTopology(data1, data2 string) ([]topology.TopologyDiff, error) {
	ctx := topology.NewContext()
	hcs, err := hosts.ParseHosts(dingocli.Hosts())
	if err != nil {
		return nil, err
	}
	for _, hc := range hcs {
		ctx.Add(hc.GetHost(), hc.GetHostname())
	}

	if len(data1) == 0 {
		return nil, errno.ERR_EMPTY_CLUSTER_TOPOLOGY
	}

	dcs, err := topology.ParseTopology(data1, ctx)
	if err != nil {
		return nil, err // err is error code
	}
	if len(dcs) == 0 {
		return nil, errno.ERR_NO_SERVICES_IN_TOPOLOGY
	}
	return topology.DiffTopology(data1, data2, ctx)
}

func (dingocli *DingoCli) PreAudit(now time.Time, args []string) int64 {
	if len(args) == 0 {
		return -1
	} else if args[0] == "audit" || args[0] == "__complete" {
		return -1
	}

	cwd, _ := os.Getwd()
	command := fmt.Sprintf("dingocli %s", strings.Join(args, " "))
	id, err := dingocli.Storage().InsertAuditLog(
		now, cwd, command, comm.AUDIT_STATUS_ABORT)
	if err != nil {
		log.Error("Insert audit log failed",
			log.Field("Error", err))
	}

	return id
}

func (dingocli *DingoCli) PostAudit(id int64, ec error) {
	if id < 0 {
		return
	}

	auditLogs, err := dingocli.Storage().GetAuditLog(id)
	if err != nil {
		log.Error("Get audit log failed",
			log.Field("Error", err))
		return
	} else if len(auditLogs) != 1 {
		return
	}

	auditLog := auditLogs[0]
	status := auditLog.Status
	errorCode := 0
	if ec == nil {
		status = comm.AUDIT_STATUS_SUCCESS
	} else if errors.Is(ec, errno.ERR_CANCEL_OPERATION) {
		status = comm.AUDIT_STATUS_CANCEL
	} else {
		status = comm.AUDIT_STATUS_FAIL
		if v, ok := ec.(*errno.ErrorCode); ok {
			errorCode = v.GetCode()
		}
	}

	err = dingocli.Storage().SetAuditLogStatus(id, status, errorCode)
	if err != nil {
		log.Error("Set audit log status failed",
			log.Field("Error", err))
	}
}

func (dingocli *DingoCli) SwitchCluster(cluster storage.Cluster) error {

	dingocli.memStorage = utils.NewSafeMap()
	dingocli.clusterId = cluster.Id
	dingocli.clusterUUId = cluster.UUId
	dingocli.clusterName = cluster.Name
	dingocli.clusterTopologyData = cluster.Topology
	dingocli.clusterPoolData = cluster.Pool

	return nil
}

// extract all deploy configs's role and deduplicate same role
func (dingocli *DingoCli) GetRoles(dcs []*topology.DeployConfig) []string {
	roles := []string{}
	roleMap := make(map[string]bool)

	for _, dc := range dcs {
		role := dc.GetRole()
		if _, ok := roleMap[role]; !ok {
			roleMap[role] = true
			roles = append(roles, role)
		}
	}

	return roles
}
