package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func script(t *testing.T, body string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "inventory script")
	if err := os.WriteFile(file, []byte("#!/bin/sh\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	return file
}
func emitted(t *testing.T, input string, opts Options) []map[string]interface{} {
	t.Helper()
	data, err := convert(context.Background(), []byte(input), opts)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Hosts []map[string]interface{} `json:"hosts"`
	}
	if err := json.Unmarshal([]byte(data), &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Hosts
}

func TestStandardInventoryDeduplicatesAndInheritsGroups(t *testing.T) {
	rows := emitted(t, `{"_meta":{"hostvars":{"gpu":{"ansible_host":"10.0.0.1","ansible_user":"host-user"},"cpu":{"ansible_host":"10.0.0.2"},"unlisted":{"ansible_host":"10.0.0.3"}}},"all":{"children":["clients"],"vars":{"ansible_user":"all-user"}},"clients":{"hosts":["gpu","cpu"],"children":["cache"],"vars":{"ansible_port":2222,"ansible_user":"group-user"}},"cache":{"hosts":["gpu"]}}`, Options{})
	if len(rows) != 2 || rows[0]["host"] != "cpu" || rows[1]["host"] != "gpu" {
		t.Fatalf("rows=%v", rows)
	}
	if rows[1]["user"] != "host-user" || rows[0]["user"] != "group-user" || rows[1]["ssh_port"] != float64(2222) {
		t.Fatal(rows)
	}
	labels := rows[1]["labels"].([]interface{})
	if len(labels) != 4 {
		t.Fatal(labels)
	}
}
func TestAuthOverridesAndAliases(t *testing.T) {
	input := `{"_meta":{"hostvars":{"h":{"ansible_ssh_host":"10.0.0.1","ansible_ssh_user":"{{ old }}","ansible_ssh_port":"2222","ansible_ssh_private_key_file":"{{ old_key }}"}}},"g":["h"]}`
	rows := emitted(t, input, Options{User: "root", Port: 22, PrivateKey: "/new"})
	if rows[0]["user"] != "root" || rows[0]["ssh_port"] != float64(22) || rows[0]["private_key_file"] != "/new" {
		t.Fatal(rows)
	}
}
func TestInvalidInventoriesFail(t *testing.T) {
	for _, input := range []string{
		`{}`, `[]`, `null`, `{"a":null}`, `{"g":{"hosts":[]}}`,
		`{"g":{"hosts":"wrong"}}`, `{"a":{"children":["missing"]}}`,
		`{"a":{"children":["b"]},"b":{"children":["a"]}}`,
		`{"g":{"hosts":["127.0.0.1"]}} {}`, "log before JSON\n{}",
		`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_port":0}}}`,
		`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_port":2.5}}}`,
		`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_connection":"local"}}}`,
		`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_host":"{{ address }}"}}}`,
		`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_become":true}}}`,
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := convert(context.Background(), []byte(input), Options{}); err == nil {
				t.Fatal("accepted invalid inventory")
			}
		})
	}
}
func TestLoadRunsOncePreservesSymlinkAndEnvironment(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	t.Setenv("INVENTORY_TEST_CALLS", calls)
	original := script(t, `test "$1" = --list || exit 9
printf 'called\n' >> "$INVENTORY_TEST_CALLS"
echo "$0" >&2
printf '{"g":{"hosts":["127.0.0.1"]}}'
`)
	link := filepath.Join(dir, "region inventory.py")
	if err := os.Symlink(original, link); err != nil {
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	if _, err := Load(context.Background(), link, Options{Timeout: time.Second}, &diagnostics); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diagnostics.String(), link) {
		t.Fatal(diagnostics.String())
	}
	content, err := os.ReadFile(calls)
	if err != nil || string(content) != "called\n" {
		t.Fatalf("%q %v", content, err)
	}
}
func TestLoadRefreshesAndFailsOnNonzeroEvenWithJSON(t *testing.T) {
	file := script(t, `echo '{"g":{"hosts":["127.0.0.1"]}}'`)
	first, err := Load(context.Background(), file, Options{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("#!/bin/sh\necho '{\"g\":{\"hosts\":[\"127.0.0.2\"]}}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	second, err := Load(context.Background(), file, Options{Timeout: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || strings.Contains(second, "127.0.0.1") {
		t.Fatal(second)
	}
	file = script(t, `echo '{"g":{"hosts":["127.0.0.1"]}}'; exit 7`)
	if _, err := Load(context.Background(), file, Options{Timeout: time.Second}, nil); err == nil {
		t.Fatal("accepted failed script")
	}
}
func TestTimeoutAndOutputLimit(t *testing.T) {
	file := script(t, "sleep 20\n")
	start := time.Now()
	if _, err := Load(context.Background(), file, Options{Timeout: 50 * time.Millisecond}, nil); err == nil {
		t.Fatal("missing timeout")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("waited for child pipes")
	}
	var output boundedOutput
	if _, err := output.Write(make([]byte, maxOutput+1)); err == nil {
		t.Fatal("unbounded output")
	}
}
func TestNoSecretsInUnsupportedVariableError(t *testing.T) {
	_, err := convert(context.Background(), []byte(`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_password":"secret-value"}}}`), Options{})
	if err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatal(err)
	}
}

func TestGroupPriorityAndPrivateKeyPathValidation(t *testing.T) {
	rows := emitted(t, `{"a":{"hosts":["127.0.0.1"],"vars":{"ansible_group_priority":10,"ansible_user":"priority"}},"z":{"hosts":["127.0.0.1"],"vars":{"ansible_user":"alphabetical"}}}`, Options{})
	if rows[0]["user"] != "priority" {
		t.Fatal(rows)
	}
	if _, err := convert(context.Background(), []byte(`{"g":{"hosts":["127.0.0.1"],"vars":{"ansible_private_key_file":"relative-key"}}}`), Options{}); err == nil {
		t.Fatal("accepted relative key")
	}
}

func TestNonExecutableScriptFails(t *testing.T) {
	file := script(t, "exit 0\n")
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(context.Background(), file, Options{Timeout: time.Second}, nil); err == nil {
		t.Fatal("accepted non-executable inventory")
	}
}
