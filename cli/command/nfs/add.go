/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

package nfs

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/dingodb/dingocli/pkg/logger"
	"github.com/dingodb/dingocli/pkg/module"

	"github.com/spf13/cobra"
)

const (
	NFS_ADD_EXAMPLE = `Examples:
   $ dingo nfs add /mnt/dingofs/export --conf "*(Access_Type=RW,Protocols=3:4,Squash=no_root_squash)"`
)

const (
	NFS_EXPORT_TEMPLATE = `EXPORT
{
	Export_Id = {{.ExportID}};
	Path = {{.Path}};
	Pseudo = {{.Pseudo}};

	CLIENT {
		Clients = {{.Client}};
		Protocols = {{.Protocols}};
		Access_Type = {{.Access}};
		Squash = {{.Squash}};
		Sectype = {{.Sectype}};
	}

	FSAL {
		Name = {{.FSALName}};
	}
}`
)

type ExportConfig struct {
	ExportID  string
	Path      string
	Pseudo    string
	Protocols string
	Access    string
	Squash    string
	Sectype   string
	Client    string
	FSALName  string
}

var (
	DefaultExportConfig *ExportConfig = &ExportConfig{
		Access:    "RW",
		Protocols: "3,4",
		Squash:    "no_root_squash",
		Sectype:   "sys",
		FSALName:  "VFS",
	}
)

type addOptions struct {
	shell       *module.Shell
	execOptions module.ExecOptions
	exportPath  string
	exportConf  string
}

func NewNFSAddCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options addOptions

	cmd := &cobra.Command{
		Use:     "add PATH [OPTIONS]",
		Short:   "add nfs-ganesha export",
		Args:    utils.ExactArgs(1),
		Example: NFS_ADD_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.exportPath = args[0]

			var err error
			options.exportConf, err = cmd.Flags().GetString("conf")
			if err != nil {
				return err
			}

			return runAdd(cmd, dingocli, &options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().StringP("conf", "c", "", "Export config attribute")

	return cmd
}

func runAdd(cmd *cobra.Command, dingocli *cli.DingoCli, options *addOptions) error {

	options.shell = module.NewShell(nil)
	options.execOptions = module.ExecOptions{ExecWithSudo: true, ExecInLocal: true, ExecTimeoutSec: 10}

	//step 1: create directory for store export conf if not exists
	err := GenerateExportStoragePath(options)
	if err != nil {
		return err
	}

	// step 2: check path if exported
	isExported := utils.CheckPathIsExported(options.shell, options.execOptions, options.exportPath, utils.NFS_EXPORT_STORE_PATH)
	if isExported {
		return fmt.Errorf("path %s is already exported in %s", options.exportPath, utils.NFS_EXPORT_STORE_PATH)
	}

	//step 3: save export config file to NFS_EXPORT_STORE_PATH
	err = SaveExportConfigFile(options)
	if err != nil {
		return err
	}

	//step 4: get current nfs-ganesha pid
	ganeshaPid, err := utils.GetGaneshaPID(options.shell, options.execOptions)
	if err != nil {
		return err
	}

	// step 5: send SIGHUP signal to nfs-ganesha
	err = utils.NotifyGaneshaReLoadConfig(options.shell, options.execOptions, ganeshaPid)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully add export %s\n", options.exportPath)

	return nil
}

func GenerateExportStoragePath(options *addOptions) error {
	options.shell.ClearOption().AddOption("-d")
	options.shell.Test(utils.NFS_EXPORT_STORE_PATH)
	_, execErr := options.shell.Execute(options.execOptions)
	if execErr != nil {
		options.shell.ClearOption()
		options.shell.Mkdir(utils.NFS_EXPORT_STORE_PATH)
		_, execErr = options.shell.Execute(options.execOptions)
		if execErr != nil {
			return fmt.Errorf("create nfs export directory %s failed, err: %v", utils.NFS_EXPORT_STORE_PATH, execErr)
		}
		logger.Infof("create nfs export directory %s ok", utils.NFS_EXPORT_STORE_PATH)
	} else {
		logger.Infof("nfs export directory %s already exists, ignore create", utils.NFS_EXPORT_STORE_PATH)
	}

	return nil
}

// input format :
// "*(Access_Type=RW,Protocols=3:4,Squash=no_root_squash)
// "192.168.1.1/24(Access_Type=RW,Protocols=3:4,Squash=no_root_squash)
func parseExportConfig(input string) (*ExportConfig, error) {
	exportConfig := DefaultExportConfig

	if input == "" {
		return exportConfig, nil
	}

	parts := strings.SplitN(input, "(", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid nfs config format: %s", input)
	}

	clientPart := strings.TrimSpace(parts[0])
	exportConfig.Client = clientPart

	// remove ")"
	configPart := strings.TrimSuffix(parts[1], ")")

	// pare key/value
	pairs := strings.Split(configPart, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		switch key {
		case "Access_Type":
			exportConfig.Access = value
		case "Protocols":
			//convert "3:4" to "3,4"
			exportConfig.Protocols = strings.ReplaceAll(value, ":", ",")
		case "Squash":
			exportConfig.Squash = value
		}
	}

	return exportConfig, nil
}

func GenerateExportConfigFile(exportID string, exportPath string, exportCfg string) (string, error) {

	nfsConfig, err := parseExportConfig(exportCfg)
	if err != nil {
		return "", err
	}

	nfsConfig.ExportID = exportID
	nfsConfig.Path = exportPath
	nfsConfig.Pseudo = exportPath

	tmpl := template.Must(template.New("export").Parse(NFS_EXPORT_TEMPLATE))
	buffer := bytes.NewBufferString("")
	err = tmpl.Execute(buffer, nfsConfig)
	if err != nil {
		return "", fmt.Errorf("generate nfs export template failed, err: %v", err)
	}
	logger.Infof("export conf:\n%s\n", buffer.String())

	fileManager := module.NewFileManager(nil)
	return fileManager.InstallTmpFile(buffer.String())
}

func SaveExportConfigFile(options *addOptions) error {
	inodeId, err := utils.GetInodeId(options.shell, options.execOptions, options.exportPath)
	if err != nil {
		return err
	}

	// generate export_id
	exportId, err := utils.GenetateExportId(options.shell, options.execOptions, utils.NFS_EXPORT_STORE_PATH)
	if err != nil {
		return err
	}
	tmpFileName, err := GenerateExportConfigFile(fmt.Sprintf("%d", exportId), options.exportPath, options.exportConf)
	if err != nil {
		return err
	}
	newFileName := utils.GenerateFileName(inodeId, options.exportPath)

	return SaveToLocal(options, tmpFileName, newFileName)

}

func SaveToLocal(options *addOptions, src string, dest string) error {
	options.shell.ClearOption()
	options.shell.Rename(src, dest)
	_, err := options.shell.Execute(options.execOptions)
	if err != nil {
		return fmt.Errorf("rename file %s to %s failed, err: %v", src, dest, err)
	}
	logger.Infof("rename file %s to %s ok", src, dest)

	return nil
}
