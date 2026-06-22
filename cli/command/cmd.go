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

package command

import (
	"fmt"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/cli/command/cache"
	"github.com/dingodb/dingocli/cli/command/cluster"
	"github.com/dingodb/dingocli/cli/command/component"
	"github.com/dingodb/dingocli/cli/command/config"
	"github.com/dingodb/dingocli/cli/command/fs"
	"github.com/dingodb/dingocli/cli/command/hosts"
	"github.com/dingodb/dingocli/cli/command/mds"
	"github.com/dingodb/dingocli/cli/command/monitor"
	"github.com/dingodb/dingocli/cli/command/nfs"
	"github.com/dingodb/dingocli/internal/errno"
	tools "github.com/dingodb/dingocli/internal/tools/upgrade"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/spf13/cobra"
)

var dingoExample = `Examples:
  $ dingo fs mount mds://ip:port/myfs /mnt # Mount myfs to local path /mnt
  $ dingo fs umount /mnt                   # Unmount filesystem from local path /mnt
  $ dingo cache start --id=<UUID>          # Start dingo-cache
  $ dingo mds start --conf ./mds.conf  	   # Start mds with specified config file
  $ dingo cluster add c1                   # Add a cluster named 'c1'
  $ dingo cluster deploy                   # Deploy current cluster
  $ dingo cluster stop                     # Stop current cluster service
  $ dingo cluster clean                    # Clean current cluster
  $ dingo enter 6ff561598c6f               # Enter specified service container
  $ dingo -u                               # Upgrade dingo itself to the latest version`

type rootOptions struct {
	debug   bool
	upgrade bool
}

func addSubCommands(cmd *cobra.Command, dingocli *cli.DingoCli) {
	cmd.AddGroup(
		&cobra.Group{
			ID:    "ADMIN",
			Title: "Admin Commands:",
		},
		&cobra.Group{
			ID:    "DEPLOY",
			Title: "Deploy Commands:",
		},
		&cobra.Group{
			ID:    "UTILS",
			Title: "Utils Commands:",
		},
	)

	cmd.AddCommand(
		cluster.NewClusterCommand(dingocli),     // dingocli cluster ...
		config.NewConfigCommand(dingocli),       // dingocli config ...
		hosts.NewHostsCommand(dingocli),         // dingocli hosts ...
		monitor.NewMonitorCommand(dingocli),     // dingocli monitor ...
		cache.NewCacheCommand(dingocli),         // dingocli cache ...
		nfs.NewNFSCommand(dingocli),             // dingocli export...
		mds.NewMDSCommand(dingocli),             // dingocli mds ...
		fs.NewFSCommand(dingocli),               // dingocli fs ...
		component.NewComponentCommand(dingocli), // dingocli component ...

		NewAuditCommand(dingocli),      // dingocli audit
		NewCompletionCommand(dingocli), // dingocli completion
		NewEnterCommand(dingocli),      // dingocli enter
		NewExecCommand(dingocli),       // dingocli exec
		// commonly used shorthands
		NewSSHCommand(dingocli),      // dingocli ssh
		NewPlaybookCommand(dingocli), // dingocli playbook
	)
}

func setupRootCommand(cmd *cobra.Command, dingocli *cli.DingoCli) {
	cmd.SetVersionTemplate(`{{with .Name}}{{printf "%s " .}}{{end}}{{printf "Version %s" .Version}}
Copyright 2026 dingofs Org.
Licensed under the Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
`)
	cliutil.SetFlagErrorFunc(cmd)
	cliutil.SetHelpTemplate(cmd)
	cliutil.SetUsageTemplate(cmd)
	cliutil.SetErr(cmd, dingocli)
}

func NewDingoCliCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options rootOptions

	cmd := &cobra.Command{
		Use:   "dingo [OPTIONS] COMMAND [ARGS...]",
		Short: "Deploy and manage dingoFS cluster",
		// Version: fmt.Sprintf("dingocli v%s, build %s", cli.Version, cli.CommitId),
		Version: fmt.Sprintf("%s (commit: %s) \nBuild Date: %s", cli.Version, cli.CommitId, cli.BuildTime),
		Example: dingoExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.debug {
				return errno.List()
			} else if options.upgrade {
				return tools.Upgrade2Latest(cli.CommitId)
			} else if len(args) == 0 {
				return cliutil.ShowHelp(dingocli.Err())(cmd, args)
			}

			return fmt.Errorf("dingo: '%s' is not a dingo command.\n"+
				"See 'dingo --help'", args[0])
		},
		SilenceUsage:          true, // silence usage when an error occurs
		DisableFlagsInUseLine: true,
	}

	cmd.Flags().BoolP("version", "v", false, "Print version information and quit")
	cmd.PersistentFlags().BoolP("help", "h", false, "Print usage")
	cmd.Flags().BoolVarP(&options.debug, "debug", "d", false, "Print debug information")
	cmd.Flags().BoolVarP(&options.upgrade, "upgrade", "u", false, "Upgrade dingo itself to the latest version")

	addSubCommands(cmd, dingocli)
	setupRootCommand(cmd, dingocli)

	return cmd
}
