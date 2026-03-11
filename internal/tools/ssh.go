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

package tools

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"text/template"

	"github.com/dingodb/dingocli/cli/cli"
	"github.com/dingodb/dingocli/internal/errno"
	"github.com/dingodb/dingocli/internal/utils"
)

const (
	TEMPLATE_SCP                             = `scp -P {{.port}} {{or .options ""}} {{.source}} {{.user}}@{{.host}}:{{.target}}`
	TEMPLATE_SSH_COMMAND                     = `ssh {{.user}}@{{.host}} -p {{.port}} {{or .options ""}} {{or .become ""}} {{.command}}`
	TEMPLATE_SSH_ATTACH                      = `ssh -tt {{.user}}@{{.host}} -p {{.port}} {{or .options ""}} {{or .become ""}} {{.command}}`
	TEMPLATE_COMMAND_EXEC_CONTAINER          = `{{.sudo}} {{.engine}} exec -it {{.container_id}} /bin/bash -c "cd {{.home_dir}}; /bin/bash"`
	TEMPLATE_LOCAL_EXEC_CONTAINER            = `{{.engine}} exec -it {{.container_id}} /bin/bash` // FIXME: merge it
	TEMPLATE_COMMAND_EXEC_CONTAINER_NOATTACH = `{{.sudo}} {{.engine}} exec -t {{.container_id}} /bin/bash -c "{{.command}}"`
)

func prepareOptions(dingocli *cli.DingoCli, host string, become bool, extra map[string]interface{}) (map[string]interface{}, error) {
	hc, err := dingocli.GetHost(host)
	if err != nil {
		return nil, err
	}

	config := hc.GetSSHConfig()

	opts := []string{
		"-o StrictHostKeyChecking=no",
		//"-o UserKnownHostsFile=/dev/null",
	}
	if !config.ForwardAgent {
		opts = append(opts, fmt.Sprintf("-i %s", config.PrivateKeyPath))
	}

	// Start with extra options first (so custom options like -r are preserved)
	options := map[string]interface{}{
		"user":    config.User,
		"host":    config.Host,
		"port":    config.Port,
		"options": strings.Join(opts, " "),
	}

	for k, v := range extra {
		options[k] = v
	}

	if len(config.BecomeUser) > 0 && become {
		options["become"] = fmt.Sprintf("%s %s %s",
			config.BecomeMethod, config.BecomeFlags, config.BecomeUser)
	}

	return options, nil
}

func newCommand(dingocli *cli.DingoCli, text string, options map[string]interface{}) (*exec.Cmd, error) {
	tmpl := template.Must(template.New(utils.MD5Sum(text)).Parse(text))
	buffer := bytes.NewBufferString("")
	if err := tmpl.Execute(buffer, options); err != nil {
		return nil, errno.ERR_BUILD_TEMPLATE_FAILED.E(err)
	}
	command := buffer.String()
	items := strings.Split(command, " ")
	return exec.Command(items[0], items[1:]...), nil
}

func runCommand(dingocli *cli.DingoCli, text string, options map[string]interface{}) error {
	cmd, err := newCommand(dingocli, text, options)
	if err != nil {
		return err
	}
	cmd.Stdout = dingocli.Out()
	cmd.Stderr = dingocli.Err()
	cmd.Stdin = dingocli.In()
	return cmd.Run()
}

func runCommandOutput(dingocli *cli.DingoCli, text string, options map[string]interface{}) (string, error) {
	cmd, err := newCommand(dingocli, text, options)
	if err != nil {
		return "", err
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func ssh(dingocli *cli.DingoCli, options map[string]interface{}) error {
	err := runCommand(dingocli, TEMPLATE_SSH_ATTACH, options)
	if err != nil && !strings.HasPrefix(err.Error(), "exit status") {
		return errno.ERR_CONNECT_REMOTE_HOST_WITH_INTERACT_BY_SSH_FAILED.E(err)
	}
	return nil
}

func scp(dingocli *cli.DingoCli, options map[string]interface{}) error {
	// TODO: added error code
	_, err := runCommandOutput(dingocli, TEMPLATE_SCP, options)
	return err
}

func execute(dingocli *cli.DingoCli, options map[string]interface{}) (string, error) {
	return runCommandOutput(dingocli, TEMPLATE_SSH_COMMAND, options)
}

func AttachRemoteHost(dingocli *cli.DingoCli, host string, become bool) error {
	options, err := prepareOptions(dingocli, host, become,
		map[string]interface{}{"command": "/bin/bash"})
	if err != nil {
		return err
	}
	return ssh(dingocli, options)
}

func AttachRemoteContainer(dingocli *cli.DingoCli, host, containerId, home string) error {
	data := map[string]interface{}{
		"sudo":         dingocli.Config().GetSudoAlias(),
		"engine":       dingocli.Config().GetEngine(),
		"container_id": containerId,
		"home_dir":     home,
	}
	tmpl := template.Must(template.New("command").Parse(TEMPLATE_COMMAND_EXEC_CONTAINER))
	buffer := bytes.NewBufferString("")
	if err := tmpl.Execute(buffer, data); err != nil {
		return errno.ERR_BUILD_TEMPLATE_FAILED.E(err)
	}
	command := buffer.String()

	options, err := prepareOptions(dingocli, host, true,
		map[string]interface{}{"command": command})
	if err != nil {
		return err
	}
	return ssh(dingocli, options)
}

func AttachLocalContainer(dingocli *cli.DingoCli, containerId string) error {
	data := map[string]interface{}{
		"container_id": containerId,
		"engine":       dingocli.Config().GetEngine(),
	}
	tmpl := template.Must(template.New("command").Parse(TEMPLATE_LOCAL_EXEC_CONTAINER))
	buffer := bytes.NewBufferString("")
	if err := tmpl.Execute(buffer, data); err != nil {
		return errno.ERR_BUILD_TEMPLATE_FAILED.E(err)
	}
	command := buffer.String()
	return runCommand(dingocli, command, map[string]interface{}{})
}

func ExecCmdInRemoteContainer(dingocli *cli.DingoCli, host, containerId, cmd string) error {
	data := map[string]interface{}{
		"sudo":         dingocli.Config().GetSudoAlias(),
		"engine":       dingocli.Config().GetEngine(),
		"container_id": containerId,
		"command":      cmd,
	}
	tmpl := template.Must(template.New("command").Parse(TEMPLATE_COMMAND_EXEC_CONTAINER_NOATTACH))
	buffer := bytes.NewBufferString("")
	if err := tmpl.Execute(buffer, data); err != nil {
		return errno.ERR_BUILD_TEMPLATE_FAILED.E(err)
	}
	command := buffer.String()

	options, err := prepareOptions(dingocli, host, true,
		map[string]interface{}{"command": command})
	if err != nil {
		return err
	}
	return ssh(dingocli, options)
}

func Scp(dingocli *cli.DingoCli, host, source, target string) error {
	optionsMap := map[string]interface{}{
		"source": source,
		"target": target,
	}

	// Check if source is a directory and add recursive flag
	if utils.IsDir(source) {
		optionsMap["options"] = "-r"
	}

	options, err := prepareOptions(dingocli, host, false, optionsMap)
	if err != nil {
		return err
	}
	return scp(dingocli, options)
}

func ExecuteRemoteCommand(dingocli *cli.DingoCli, host, command string) (string, error) {
	options, err := prepareOptions(dingocli, host, true,
		map[string]interface{}{"command": command})
	if err != nil {
		return "", err
	}
	return execute(dingocli, options)
}
