package command

import (
	"bytes"
	"encoding/json"
	"github.com/dingodb/dingocli/cli/cli"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func inventoryFixture(t *testing.T) (*cli.DingoCli, string, string) {
	t.Helper()
	dir := t.TempDir()
	key := filepath.Join(dir, "key")
	script := filepath.Join(dir, "hosts.py")
	calls := filepath.Join(dir, "calls")
	if err := os.WriteFile(key, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	stored, _ := json.Marshal(map[string]interface{}{"global": map[string]interface{}{"private_key_file": key}, "hosts": []interface{}{map[string]interface{}{"host": "stored", "hostname": "127.0.0.1", "labels": []string{"stored"}}}})
	client, err := new(cli.DingoCli).WithHosts(string(stored))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]interface{}{"_meta": map[string]interface{}{"hostvars": map[string]interface{}{"fresh": map[string]interface{}{"ansible_host": "127.0.0.2", "ansible_ssh_private_key_file": key}}}, "cache_servers": map[string]interface{}{"hosts": []string{"fresh"}}})
	body := "#!/bin/sh\ntest \"$1\" = --list || exit 9\necho called >> '" + calls + "'\ncat <<'JSON'\n" + string(data) + "\nJSON\n"
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	return client, script, calls
}
func TestPlaybookDynamicPreview(t *testing.T) {
	client, script, calls := inventoryFixture(t)
	cmd := NewPlaybookCommand(client)
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"-i", script, "--inventory-user", "root", "-l", "cache_servers", "--list-hosts"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "fresh\t127.0.0.2\troot") || strings.Contains(output.String(), "stored") {
		t.Fatal(output.String())
	}
	content, _ := os.ReadFile(calls)
	if string(content) != "called\n" {
		t.Fatalf("calls %q", content)
	}
	if !strings.Contains(client.Hosts(), "stored") {
		t.Fatal("changed stored inventory")
	}
}
func TestInventoryAppliesToEveryRemoteMode(t *testing.T) {
	for _, args := range [][]string{
		{"--tune", "vm.swappiness", "0"},
		{"--scp", "--target", "/tmp/unused", "FILE"},
		{"FILE"},
		{"config", "check", "dingo-cache.service"},
		{"config", "get", "dingo-cache.service", "log_level"},
		{"config", "set", "dingo-cache.service", "log_level", "INFO"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			client, script, calls := inventoryFixture(t)
			file := filepath.Join(t.TempDir(), "playbook.sh")
			os.WriteFile(file, []byte("#!/bin/sh\nexit 0\n"), 0700)
			var commandArgs []string
			for _, arg := range args {
				if arg == "FILE" {
					arg = file
				}
				commandArgs = append(commandArgs, arg)
			}
			commandArgs = append(commandArgs, "-i", script, "-l", "stored")
			cmd := NewPlaybookCommand(client)
			var output bytes.Buffer
			cmd.SetErr(&output)
			cmd.SetOut(&output)
			cmd.SetArgs(commandArgs)
			// 'stored' exists only in the database snapshot. Selection must stop before SSH.
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "no hosts") {
				t.Fatalf("error=%v output=%s", err, output.String())
			}
			content, _ := os.ReadFile(calls)
			if string(content) != "called\n" {
				t.Fatalf("calls %q", content)
			}
		})
	}
}
func TestInventoryFailureNeverFallsBackToDatabase(t *testing.T) {
	client, script, _ := inventoryFixture(t)
	os.WriteFile(script, []byte("#!/bin/sh\nexit 7\n"), 0700)
	cmd := NewPlaybookCommand(client)
	cmd.SetArgs([]string{"config", "check", "cache.service", "-i", script, "-l", "stored"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "inventory script failed") {
		t.Fatal(err)
	}
}
func TestPreviewWithoutInventoryUsesStoredHosts(t *testing.T) {
	client, _, _ := inventoryFixture(t)
	cmd := NewPlaybookCommand(client)
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"--list-hosts"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "stored") {
		t.Fatal(output.String())
	}
}

func TestDynamicInventoryExactHostAndRegexPreview(t *testing.T) {
	for _, selectors := range [][]string{{"-l", "fresh"}, {"-l", "re:^fre[a-z]{1,3}$"}, {"-l", "cache_servers", "-l", "&re:^fresh$"}} {
		t.Run(strings.Join(selectors, " "), func(t *testing.T) {
			client, script, _ := inventoryFixture(t)
			cmd := NewPlaybookCommand(client)
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			args := append([]string{"-i", script, "--list-hosts"}, selectors...)
			cmd.SetArgs(args)
			if err := cmd.Execute(); err != nil {
				t.Fatal(err, output.String())
			}
			if !strings.Contains(output.String(), "fresh\t") || strings.Contains(output.String(), "stored\t") {
				t.Fatal(output.String())
			}
		})
	}
}

func TestInvalidRegexStopsBeforeInventoryExecution(t *testing.T) {
	client, script, calls := inventoryFixture(t)
	cmd := NewPlaybookCommand(client)
	cmd.SetArgs([]string{"-i", script, "config", "set", "cache.service", "log_level", "INFO", "-l", "re:["})
	if err := cmd.Execute(); err == nil {
		t.Fatal("accepted invalid regex")
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatal("ran inventory despite invalid selector")
	}
}
