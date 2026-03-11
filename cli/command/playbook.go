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

// NOTE: playbook under beta version
import (
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/configure/hosts"
	hostconfig "github.com/dingodb/dingocli/internal/configure/hosts"
	"github.com/dingodb/dingocli/internal/tools"
	"github.com/dingodb/dingocli/internal/utils"
	cliutil "github.com/dingodb/dingocli/internal/utils"
	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var (
	retC chan result
	wg   sync.WaitGroup
)

type result struct {
	index int
	host  string
	out   string
	err   error
}

type playbookOptions struct {
	filepath  string
	args      []string
	labels    []string
	scpMode   bool
	scpTarget string
}

func checkPlaybookOptions(dingocli *cli.DingoCli, options playbookOptions) error {
	// TODO: added error code
	if !utils.PathExist(options.filepath) {
		return fmt.Errorf("%s: no such file", options.filepath)
	}
	return nil
}

func NewPlaybookCommand(dingocli *cli.DingoCli) *cobra.Command {
	var options playbookOptions

	cmd := &cobra.Command{
		Use:     "playbook PLAYBOOK [ARGS...] [OPTIONS]",
		Short:   "Execute playbook or copy files to remote hosts",
		GroupID: "UTILS",
		Example: `Examples:
  # Execute playbook.sh on remote hosts
  $ dingo playbook playbook.sh

  # Copy local file to remote hosts
  $ dingo playbook --scp --target /tmp/remote.conf local.conf`,
		Args: cliutil.RequiresMinArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			options.filepath = args[0]

			// SCP mode: copy file to remote hosts
			if options.scpMode {
				if options.scpTarget == "" {
					return fmt.Errorf("--scp flag requires a target path on remote host")
				}

				return checkPlaybookOptions(dingocli, options)
			}

			// Script execution mode
			if len(args) == 1 {
				// generate any.sh script to /tmp/any.sh
				anyScript := path.Join("/tmp", "any.sh")
				if !utils.PathExist(anyScript) {
					if err := utils.WriteFile(anyScript, "#!/usr/bin/env bash\n\n\"$@\"\n", 0644); err != nil {
						return fmt.Errorf("write any.sh failed: %w", err)
					}
				}
				args = append([]string{anyScript}, args...) // prepend any.sh to args
			}
			options.filepath = args[0]
			options.args = args[1:]
			return checkPlaybookOptions(dingocli, options)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.scpMode {
				return runScpMode(dingocli, options)
			}
			return runPlaybook(dingocli, options)
		},
		DisableFlagsInUseLine: true,
	}

	flags := cmd.Flags()
	flags.StringSliceVarP(&options.labels, "labels", "l", []string{}, "Specify the host labels")
	flags.BoolVar(&options.scpMode, "scp", false, "Enable SCP mode to copy files to remote hosts")
	flags.StringVar(&options.scpTarget, "target", "", "Target path on remote host (used with --scp)")

	return cmd
}

func executeScp(dingocli *cli.DingoCli, options playbookOptions, idx int, hc *hosts.HostConfig) {
	defer func() { wg.Done() }()
	name := hc.GetHost()

	err := tools.Scp(dingocli, name, options.filepath, options.scpTarget)
	if err != nil {
		retC <- result{index: idx, host: name, err: err}
		return
	}

	out := fmt.Sprintf("file copied: %s -> %s\n", options.filepath, options.scpTarget)
	retC <- result{index: idx, host: name, out: out, err: nil}
}

func execute(dingocli *cli.DingoCli, options playbookOptions, idx int, hc *hosts.HostConfig) {
	defer func() { wg.Done() }()
	name := hc.GetHost()
	target := path.Join("/tmp", utils.RandString(8))
	err := tools.Scp(dingocli, name, options.filepath, target)
	if err != nil {
		retC <- result{host: name, err: err}
		return
	}

	defer func() {
		command := fmt.Sprintf("rm -rf %s", target)
		tools.ExecuteRemoteCommand(dingocli, name, command)
	}()

	command := strings.Join([]string{
		strings.Join(hc.GetEnvs(), " "),
		"bash",
		target,
		strings.Join(options.args, " "),
	}, " ")
	out, err := tools.ExecuteRemoteCommand(dingocli, name, command)
	retC <- result{index: idx, host: name, out: out, err: err}
}

func output(dingocli *cli.DingoCli, ret *result) {
	dingocli.WriteOutln("")
	out, err := ret.out, ret.err
	dingocli.WriteOutln("%s [%s]", color.YellowString(ret.host),
		utils.Choose(err == nil, color.GreenString("SUCCESS"), color.RedString("FAIL")))
	dingocli.WriteOutln("---")
	if err != nil {
		dingocli.Out().Write([]byte(out))
		dingocli.WriteOutln(err.Error())
	} else if len(out) > 0 {
		dingocli.Out().Write([]byte(out))
	}
}

func receiver(dingocli *cli.DingoCli, total int) {
	dingocli.WriteOutln("TOTAL: %d hosts", total)
	current := 0
	rets := map[int]result{}
	for ret := range retC {
		rets[ret.index] = ret
		for {
			if v, ok := rets[current]; ok {
				output(dingocli, &v)
				current++
			} else {
				break
			}
		}
	}
}

func runScpMode(dingocli *cli.DingoCli, options playbookOptions) error {
	var hcs []*hostconfig.HostConfig
	var err error
	hosts := dingocli.Hosts()
	if len(hosts) > 0 {
		hcs, err = hostconfig.Filter(hosts, options.labels)
		if err != nil {
			return err
		}
	}

	if len(hcs) == 0 {
		return fmt.Errorf("no hosts configured. Use 'dingo hosts add' to add hosts first")
	}

	dingocli.WriteOutln("SCP Mode: Copying %s to %s on %d host(s)...",
		color.CyanString(options.filepath),
		color.CyanString(options.scpTarget),
		len(hcs))

	retC = make(chan result)
	wg.Add(len(hcs))
	go receiver(dingocli, len(hcs))
	for i, hc := range hcs {
		go executeScp(dingocli, options, i, hc)
	}
	wg.Wait()
	close(retC)
	return nil
}

func runPlaybook(dingocli *cli.DingoCli, options playbookOptions) error {
	var hcs []*hostconfig.HostConfig
	var err error
	hosts := dingocli.Hosts()
	if len(hosts) > 0 {
		hcs, err = hostconfig.Filter(hosts, options.labels) // filter hosts
		if err != nil {
			return err
		}
	}

	retC = make(chan result)
	wg.Add(len(hcs))
	go receiver(dingocli, len(hcs))
	for i, hc := range hcs {
		go execute(dingocli, options, i, hc)
	}
	wg.Wait()
	close(retC)
	return nil
}
