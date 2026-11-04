package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dingodb/dingocli/internal/inventory"
	cliutil "github.com/dingodb/dingocli/internal/utils"
)

func TestWritePreservesRawDiagnosticsAndConsumesCobraPrefix(t *testing.T) {
	for _, test := range []struct{ input, output string }{
		{"", ""},
		{"x", "x"},
		{"警告：节点地址相同，仍按 storage 使用\n", "警告：节点地址相同，仍按 storage 使用\n"},
		{"progress 100%: %s %d %%\n", "progress 100%: %s %d %%\n"},
		{cliutil.PREFIX_COBRA_COMMAND_ERROR + "failure 100%\n", "failure 100%\n"},
		{cliutil.PREFIX_COBRA_COMMAND_ERROR, ""},
		{"prefix " + cliutil.PREFIX_COBRA_COMMAND_ERROR, "prefix " + cliutil.PREFIX_COBRA_COMMAND_ERROR},
	} {
		t.Run(test.input, func(t *testing.T) {
			var output bytes.Buffer
			client := &DingoCli{out: &output}
			n, err := client.Write([]byte(test.input))
			if err != nil || n != len(test.input) || output.String() != test.output {
				t.Fatalf("Write(%q) = (%d, %v), output=%q", test.input, n, err, output.String())
			}
		})
	}
}

type shortDiagnosticWriter struct{ err error }

func (w shortDiagnosticWriter) Write(p []byte) (int, error) { return 1, w.err }

func TestWriteReportsRealDestinationFailures(t *testing.T) {
	failure := errors.New("destination failed")
	for _, prefix := range []string{"", cliutil.PREFIX_COBRA_COMMAND_ERROR} {
		for _, destinationError := range []error{nil, failure} {
			client := &DingoCli{out: shortDiagnosticWriter{err: destinationError}}
			n, err := client.Write([]byte(prefix + "message"))
			want := destinationError
			if want == nil {
				want = io.ErrShortWrite
			}
			if n != len(prefix)+1 || !errors.Is(err, want) {
				t.Fatalf("n=%d err=%v, want n=%d err=%v", n, err, len(prefix)+1, want)
			}
		}
	}
}

func TestInventoryWarningsThroughCLIWriterDoNotFailSuccessfulScript(t *testing.T) {
	warning := "警告：node-1 的存储网地址与 k8s 相同，进度 100%：%s\n"
	file := filepath.Join(t.TempDir(), "hosts.py")
	body := "#!/bin/sh\nprintf '%s' '" + warning + "' >&2\n" +
		"printf '%s' '{\"cache_servers\":{\"hosts\":[\"127.0.0.1\"]}}'\n"
	if err := os.WriteFile(file, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	client := &DingoCli{out: &output}
	data, err := inventory.Load(context.Background(), file, inventory.Options{Timeout: 5 * time.Second}, client)
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != warning || !strings.Contains(data, "127.0.0.1") {
		t.Fatalf("diagnostics=%q inventory=%s", output.String(), data)
	}
	// A real nonzero inventory exit must still fail, even with valid stdout.
	if err := os.WriteFile(file, []byte(body+"exit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := inventory.Load(context.Background(), file, inventory.Options{Timeout: 5 * time.Second}, client); err == nil || !strings.Contains(err.Error(), "exit status 7") {
		t.Fatalf("expected the script's exit status, got %v", err)
	}
}
