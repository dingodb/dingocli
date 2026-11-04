//go:build !unix

package inventory

import "os/exec"

func configureProcess(command *exec.Cmd) {}
