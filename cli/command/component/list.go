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
	"os"
	"text/tabwriter"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/component"
	"github.com/dingodb/dingocli/internal/utils"

	"github.com/spf13/cobra"
)

const (
	COMPONENT_LIST_EXAMPLE = `Examples:
   # list all components
   $ dingo component list

   # list all installed components
   $ dingo component list --installed
   `
)

type listOptions struct {
	verbose   bool
	installed bool
}

func NewListCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options listOptions

	cmd := &cobra.Command{
		Use:     "list [OPTIONS]",
		Short:   "list all available and installed components",
		Args:    utils.ExactArgs(0),
		Example: COMPONENT_LIST_EXAMPLE,
		RunE: func(cmd *cobra.Command, args []string) error {

			return runList(cmd, dingocli, options)
		},
		SilenceUsage:          false,
		DisableFlagsInUseLine: true,
	}

	utils.SetFlagErrorFunc(cmd)

	cmd.Flags().BoolVarP(&options.verbose, "verbose", "v", false, "Show more component info")
	cmd.Flags().BoolVar(&options.installed, "installed", false, "List all installed components")

	return cmd
}

func runList(cmd *cobra.Command, dingocli *cli.DingoCli, options listOptions) error {
	componentManager, err := component.NewComponentManager()
	if err != nil {
		return err
	}

	components, err := componentManager.ListComponents()
	if err != nil {
		return err
	}

	if len(components) == 0 {
		fmt.Println("No available components.")
		return nil
	}

	return FormatOutput(components, options)
}

func FormatOutput(components []*component.Component, options listOptions) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if options.verbose {
		fmt.Fprintln(w, "Name\tVersion\tInstalled\tRelease\tCommit\tActive\tPath")
		fmt.Fprintln(w, "----\t-------\t---------\t-------\t------\t------\t----")
	} else {
		fmt.Fprintln(w, "Name\tVersion\tInstalled\tCommit\tActive")
		fmt.Fprintln(w, "----\t-------\t---------\t------\t------")
	}

	for _, comp := range components {
		if options.installed && !comp.IsInstalled {
			continue
		}

		installText := utils.Ternary(comp.IsInstalled, fmt.Sprintf("Yes%s", utils.Ternary(comp.Updatable, "(U)", "")), "")
		activeText := utils.Ternary(comp.IsInstalled && comp.IsActive, "Yes", "")

		if options.verbose {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", comp.Name, comp.Version, installText, comp.Release, comp.Commit, activeText, comp.Path)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", comp.Name, comp.Version, installText, comp.Commit, activeText)
		}
	}

	return w.Flush()
}
