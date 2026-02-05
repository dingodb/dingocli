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
	"errors"
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/component"
	"github.com/dingodb/dingocli/internal/utils"

	"github.com/spf13/cobra"
)

const (
	COMPONENT_UPDATE_EXAMPLE = `Examples:
   # update dingo-client to latest stable version
   $ dingo component update dingo-client

   # update dingo-client:v3.0.5 to latest build
   $ dingo component update dingo-client:v3.0.5

   # update all installed components to latest build
   $ dingo component update --all
   `
)

type updateOptions struct {
	components []string
	all        bool
}

func NewUpdateCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options updateOptions

	cmd := &cobra.Command{
		Use:     "update <component1>[:version] [component2...N] [OPTIONS]",
		Short:   "update component(s)",
		Args:    utils.RequiresMinArgs(0),
		Example: COMPONENT_UPDATE_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {
			options.components = args

			return runUpdate(cmd, dingocli, &options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().BoolVar(&options.all, "all", false, "Update all installed component to latest build")

	return cmd
}

func runUpdate(cmd *cobra.Command, dingocli *cli.DingoCli, options *updateOptions) error {
	componentManager, err := component.NewComponentManager()
	if err != nil {
		return err
	}

	updateFunc := func(name, version string) error {
		comp, err := componentManager.UpdateComponent(name, version)
		if err != nil {
			switch {
			case errors.Is(err, component.ErrAlreadyLatest):
				return fmt.Errorf("%s:%s already with latest build: %s, commit: %s", name, comp.Version, comp.Release, comp.Commit)
			case errors.Is(err, component.ErrAlreadyExist):
				return fmt.Errorf("%s:%s already installed", name, comp.Version)
			default:
				return fmt.Errorf("update component %s:%s failed: %w", name, version, err)
			}
		}

		return nil
	}

	var errors []error
	if options.all {
		installed, err := componentManager.LoadInstalledComponents()
		if err != nil {
			return err
		}
		if len(installed) == 0 {
			return fmt.Errorf("no component installed")
		}

		for _, comp := range installed {
			if err := updateFunc(comp.Name, comp.Version); err != nil {
				errors = append(errors, err)
				fmt.Println(err.Error())
			}
		}
	} else {
		for _, compinfo := range options.components {
			name, version := component.ParseComponentVersion(compinfo)

			targetVersion := utils.Ternary(version == "", component.LASTEST_VERSION, version)
			if err := updateFunc(name, targetVersion); err != nil {
				errors = append(errors, err)
				fmt.Println(err.Error())
			}
		}
	}

	if len(errors) == 0 {
		fmt.Println("Updated successfully ^_^!")
	}

	return nil
}
