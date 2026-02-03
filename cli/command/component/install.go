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

package component

import (
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/component"
	compmgr "github.com/dingodb/dingocli/internal/component"
	"github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"

	"github.com/spf13/cobra"
)

const (
	COMPONENT_INSTALL_EXAMPLE = `Examples:
   # install latest stable version
   $ dingo component install dingo-client

   # install specify version
   $ dingo component install dingo-client:v3.0.5

   # install main, not stable version
   $ dingo component install dingo-client:main

   # install multiple components at once
   $ dingo component install dingo-client:main dingo-cache dingo-mds:v3.0.5`
)

type installOptions struct {
	components []string
}

func NewInstallCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options installOptions

	cmd := &cobra.Command{
		Use:     "install <component1>[:version] [component2...N] [OPTIONS]",
		Short:   "install component(s)",
		Args:    utils.RequiresMinArgs(1),
		Example: COMPONENT_INSTALL_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.components = args

			return runInstall(cmd, dingocli, &options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	return cmd
}

func runInstall(cmd *cobra.Command, dingocli *cli.DingoCli, options *installOptions) error {
	componentManager, err := compmgr.NewComponentManager()
	if err != nil {
		return err
	}

	var installed []string
	var errors []error

	for _, comp := range options.components {
		name, version := component.ParseComponentVersion(comp)
		if comp, err := componentManager.InstallComponent(name, utils.Ternary(version == "", component.LASTEST_VERSION, version)); err != nil {
			errors = append(errors, err)
			fmt.Println(color.GreenString("%s", err.Error()))
		} else {
			installed = append(installed, fmt.Sprintf("%s:%s", comp.Name, comp.Version))
		}
	}

	if len(errors) == 0 {
		fmt.Printf("Successfully install components %s ^_^!\n", installed)
	}

	return nil
}
