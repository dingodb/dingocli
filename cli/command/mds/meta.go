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

package mds

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/dingodb/dingocli/cli/cli"
	compmgr "github.com/dingodb/dingocli/internal/component"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	MDS_META_EXAMPLE = `Examples:
   $ dingo mds meta --cmd=backup  --type=meta --coor_addr=file://./coor_list --output_type=file --out=meta_backup
   $ dingo mds meta --cmd=restore --type=meta --coor_addr=file://./coor_list --input_type=file  --in=meta_backup`
)

var (
	DINGOFS_META_BINARY = fmt.Sprintf("%s/.dingofs/bin/dingo-mds-client", utils.GetHomeDir())
)

type metaOptions struct {
	metaBinary string
	cmdArgs    []string
	daemonize  bool
}

func NewMdsMetaCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options metaOptions
	cmd := &cobra.Command{
		Use:                "meta [OPTIONS]",
		Short:              "manage meta data",
		Args:               utils.RequiresMinArgs(0),
		DisableFlagParsing: true,
		Example:            MDS_META_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.cmdArgs = args

			componentManager, err := compmgr.NewComponentManager()
			if err != nil {
				return err
			}
			component, err := componentManager.GetActiveComponent(compmgr.DINGO_MDS_CLIENT)
			if err != nil {
				fmt.Printf("%s: %v\n", color.BlueString("[WARNING]"), err)
				component, err = componentManager.InstallComponent(compmgr.DINGO_MDS_CLIENT, compmgr.MAIN_VERSION)
				if err != nil {
					return fmt.Errorf("failed to install dingo-mds binary: %v", err)
				}
			}
			options.metaBinary = filepath.Join(component.Path, component.Name)

			if !utils.IsFileExists(options.metaBinary) {
				return fmt.Errorf("%s not found, run dingo component install dingo-mds-client:[VERSION] to install.", options.metaBinary)
			}

			if err := utils.AddExecutePermission(options.metaBinary); err != nil {
				return fmt.Errorf("failed to add execute permission for %s,error: %v", options.metaBinary, err)
			}

			// check flags
			for _, arg := range args {
				if arg == "--help" || arg == "-h" {
					return runMetaCommandHelp(cmd, options.metaBinary)
				}
			}

			fmt.Println(color.CyanString("use %s:%s(%s)\n", component.Name, component.Version, options.metaBinary))

			return runMeta(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	return cmd
}

func runMeta(cmd *cobra.Command, dingocli *cli.DingoCli, options metaOptions) error {
	var oscmd *exec.Cmd
	var name string

	name = options.metaBinary
	cmdarg := options.cmdArgs

	oscmd = exec.Command(name, cmdarg...)

	oscmd.Stdout = os.Stdout
	oscmd.Stderr = os.Stderr

	if err := oscmd.Run(); err != nil {
		return err
	}

	return nil
}

func runMetaCommandHelp(cmd *cobra.Command, command string) error {
	// print dingo usage
	fmt.Printf("Usage: dingo %s %s\n", cmd.Parent().Use, cmd.Use)
	fmt.Println("")
	fmt.Println(cmd.Short)
	fmt.Println("")

	// print  dingo-mds-client options
	fmt.Println("Options:")

	helpArgs := []string{"--help"}
	oscmd := exec.Command(command, helpArgs...)
	output, err := oscmd.CombinedOutput()
	if err != nil && len(output) == 0 {
		return err
	}

	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			fmt.Printf("  %s\n", trimmed)
		}
	}

	// print dingocli example
	fmt.Println("")
	fmt.Println(cmd.Example)

	return nil
}
