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

package utils

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

func RunCommand(command string, args []string) error {
	oscmd := exec.Command(command, args...)
	output, err := oscmd.CombinedOutput()
	if err != nil && len(output) == 0 {
		return err
	}

	fmt.Print(string(output))

	return nil
}

func RunCommandHelp(cmd *cobra.Command, command string) error {
	// print  usage
	fmt.Printf("Usage: dingo %s %s\n", cmd.Parent().Use, cmd.Use)
	fmt.Println("")
	fmt.Println(cmd.Short)
	fmt.Println("")

	// print options
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

	// print example
	fmt.Println("")
	fmt.Println(cmd.Example)

	return nil
}
