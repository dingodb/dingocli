package cache

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRunStartPropagatesDaemonFailure(t *testing.T) {
	err := runStart(nil, nil, startOptions{
		cacheBinary: "/bin/sh", cmdArgs: []string{"-c", "exit 7"}, daemonize: true,
	})
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 7 {
		t.Fatalf("expected daemon exit code 7, got %v", err)
	}
}

func TestRunStartWaitsForDaemonReadiness(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ready")
	err := runStart(nil, nil, startOptions{
		cacheBinary: "/bin/sh",
		cmdArgs:     []string{"-c", `sleep 3; touch "$1"`, "daemon", marker},
		daemonize:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("returned before daemon was ready: %v", err)
	}
}
