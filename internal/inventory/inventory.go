// Package inventory loads executable Ansible --list inventories for one command.
package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Options explicitly override inventory SSH variables when nonempty.
type Options struct {
	User       string
	Port       int
	PrivateKey string
	Timeout    time.Duration
}

const maxOutput = 32 << 20

type boundedOutput struct{ bytes.Buffer }

func (b *boundedOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > maxOutput {
		return 0, fmt.Errorf("inventory output exceeds %d bytes", maxOutput)
	}
	return b.Buffer.Write(p)
}

// Load executes the supplied path directly, preserving symlinks and caller env/cwd.
// The result is a JSON document, also valid YAML for the native hosts parser.
func Load(ctx context.Context, script string, options Options, diagnostics io.Writer) (string, error) {
	if options.Timeout <= 0 {
		return "", fmt.Errorf("inventory timeout must be positive")
	}
	if options.Port < 0 || options.Port > 65535 {
		return "", fmt.Errorf("invalid inventory SSH port")
	}
	filename, err := filepath.Abs(script)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(filename)
	if err != nil {
		return "", fmt.Errorf("inventory script: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return "", fmt.Errorf("inventory script must be an executable file: %s", filename)
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	command := exec.CommandContext(ctx, filename, "--list")
	command.WaitDelay = time.Second
	configureProcess(command)
	var output boundedOutput
	command.Stdout = &output
	command.Stderr = diagnostics
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("inventory script: %w", ctx.Err())
		}
		return "", fmt.Errorf("inventory script failed: %w", err)
	}
	return convert(ctx, output.Bytes(), options)
}

type group struct {
	Hosts    []string               `json:"hosts"`
	Children []string               `json:"children"`
	Vars     map[string]interface{} `json:"vars"`
}

type metadata struct {
	Hostvars map[string]map[string]interface{} `json:"hostvars"`
}

func decode(data []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(interface{})); err != io.EOF {
		return fmt.Errorf("expected one JSON document")
	}
	return nil
}

func convert(ctx context.Context, data []byte, options Options) (string, error) {
	var raw map[string]json.RawMessage
	if err := decode(data, &raw); err != nil || raw == nil {
		return "", fmt.Errorf("inventory must emit one Ansible JSON object on stdout")
	}
	groups := map[string]group{}
	meta := metadata{}
	for name, value := range raw {
		if name == "_meta" {
			if err := decode(value, &meta); err != nil {
				return "", fmt.Errorf("invalid _meta: %w", err)
			}
			continue
		}
		if name == "" {
			return "", fmt.Errorf("empty inventory group name")
		}
		var g group
		if len(bytes.TrimSpace(value)) > 0 && bytes.TrimSpace(value)[0] == '[' {
			if err := decode(value, &g.Hosts); err != nil {
				return "", fmt.Errorf("invalid group %q: %w", name, err)
			}
		} else {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return "", fmt.Errorf("null inventory group %q", name)
			}
			if err := decode(value, &g); err != nil {
				return "", fmt.Errorf("invalid group %q: %w", name, err)
			}
		}
		if value, exists := g.Vars["ansible_group_priority"]; exists {
			if _, err := strconv.Atoi(fmt.Sprint(value)); err != nil {
				return "", fmt.Errorf("group %q has invalid ansible_group_priority", name)
			}
		}
		groups[name] = g
	}
	// Expand a DAG in topological order rather than revisiting shared subtrees.
	memberships := map[string]map[string]bool{}
	depth := map[string]int{}
	indegree := map[string]int{}
	members := map[string]map[string]bool{}
	for name, g := range groups {
		members[name] = map[string]bool{}
		for _, host := range g.Hosts {
			if strings.TrimSpace(host) == "" {
				return "", fmt.Errorf("empty host in group %q", name)
			}
			members[name][host] = true
		}
		for _, child := range g.Children {
			if _, exists := groups[child]; !exists {
				return "", fmt.Errorf("missing child group %q", child)
			}
			indegree[child]++
		}
	}
	queue := []string{}
	for name := range groups {
		if indegree[name] == 0 {
			queue = append(queue, name)
		}
	}
	order := []string{}
	for len(queue) > 0 {
		sort.Strings(queue)
		name := queue[0]
		queue = queue[1:]
		order = append(order, name)
		for _, child := range groups[name].Children {
			if depth[child] < depth[name]+1 {
				depth[child] = depth[name] + 1
			}
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if len(order) != len(groups) {
		return "", fmt.Errorf("inventory group children contain a cycle")
	}
	for i := len(order) - 1; i >= 0; i-- {
		name := order[i]
		for _, child := range groups[name].Children {
			for host := range members[child] {
				members[name][host] = true
			}
		}
		for host := range members[name] {
			if memberships[host] == nil {
				memberships[host] = map[string]bool{"all": true}
			}
			memberships[host][name] = true
		}
	}
	if len(memberships) == 0 {
		return "", fmt.Errorf("dynamic inventory selected no hosts")
	}
	names := make([]string, 0, len(memberships))
	for name := range memberships {
		names = append(names, name)
	}
	sort.Strings(names)
	hosts := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		labels := make([]string, 0, len(memberships[name]))
		for label := range memberships[name] {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		ordered := append([]string{}, labels...)
		sort.Slice(ordered, func(i, j int) bool {
			a, b := ordered[i], ordered[j]
			if a == "all" || b == "all" {
				return a == "all" && b != "all"
			}
			if depth[a] != depth[b] {
				return depth[a] < depth[b]
			}
			pa, pb := fmt.Sprint(groups[a].Vars["ansible_group_priority"]), fmt.Sprint(groups[b].Vars["ansible_group_priority"])
			ia, ib := 1, 1
			if pa != "<nil>" {
				ia, _ = strconv.Atoi(pa)
			}
			if pb != "<nil>" {
				ib, _ = strconv.Atoi(pb)
			}
			if ia != ib {
				return ia < ib
			}
			return a < b
		})
		vars := map[string]interface{}{}
		for _, label := range ordered {
			for key, value := range groups[label].Vars {
				vars[key] = value
			}
		}
		for key, value := range meta.Hostvars[name] {
			vars[key] = value
		}
		host, err := hostConfig(ctx, name, vars, options)
		if err != nil {
			return "", fmt.Errorf("inventory host %q: %w", name, err)
		}
		// Preserve native dingo's ability to select a host by its own name.
		if !memberships[name][name] {
			labels = append(labels, name)
			sort.Strings(labels)
		}
		host["labels"] = labels
		hosts = append(hosts, host)
	}
	encoded, err := json.Marshal(map[string]interface{}{"hosts": hosts})
	return string(encoded), err
}

func text(vars map[string]interface{}, names ...string) (string, error) {
	for _, name := range names {
		value, found := vars[name]
		if !found {
			continue
		}
		s, ok := value.(string)
		if !ok || strings.Contains(s, "{{") || strings.Contains(s, "{%") {
			return "", fmt.Errorf("%s must be a resolved string", name)
		}
		return s, nil
	}
	return "", nil
}

func hostConfig(ctx context.Context, name string, vars map[string]interface{}, options Options) (map[string]interface{}, error) {
	if options.User != "" {
		vars["ansible_user"] = options.User
	}
	if options.PrivateKey != "" {
		vars["ansible_private_key_file"] = options.PrivateKey
	}
	for _, key := range []string{"ansible_password", "ansible_ssh_pass", "ansible_ssh_common_args", "ansible_ssh_extra_args", "ansible_become_password"} {
		if value, found := vars[key]; found && value != nil && value != "" {
			return nil, fmt.Errorf("%s is not supported by the dingo SSH transport", key)
		}
	}
	if value, found := vars["ansible_become"]; found {
		switch strings.ToLower(fmt.Sprint(value)) {
		case "false", "no", "0":
		default:
			return nil, fmt.Errorf("ansible_become is not supported; choose the SSH user explicitly")
		}
	}
	connection, err := text(vars, "ansible_connection")
	if err != nil {
		return nil, err
	}
	if connection != "" && connection != "ssh" && connection != "smart" && connection != "paramiko" {
		return nil, fmt.Errorf("only SSH inventories are supported")
	}
	target, err := text(vars, "ansible_host", "ansible_ssh_host")
	if err != nil {
		return nil, err
	}
	if target == "" {
		target = name
	}
	address := target
	if net.ParseIP(target) == nil {
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, target)
		if err != nil || len(addresses) == 0 {
			return nil, fmt.Errorf("cannot resolve SSH address for host")
		}
		address = addresses[0].IP.String()
		for _, candidate := range addresses {
			if candidate.IP.To4() != nil {
				address = candidate.IP.String()
				break
			}
		}
	}
	host := map[string]interface{}{"host": name, "hostname": address, "ssh_hostname": target}
	user, err := text(vars, "ansible_user", "ansible_ssh_user")
	if err != nil {
		return nil, err
	}
	if user != "" {
		host["user"] = user
	}
	port := options.Port
	if port == 0 {
		for _, key := range []string{"ansible_port", "ansible_ssh_port"} {
			value, found := vars[key]
			if !found {
				continue
			}
			switch value.(type) {
			case string, json.Number:
			default:
				return nil, fmt.Errorf("%s must be an integer", key)
			}
			port, err = strconv.Atoi(fmt.Sprint(value))
			if err != nil || port < 1 || port > 65535 {
				return nil, fmt.Errorf("invalid SSH port")
			}
			break
		}
	}
	if port != 0 {
		host["ssh_port"] = port
	}
	key, err := text(vars, "ansible_private_key_file", "ansible_ssh_private_key_file")
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(key, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		key = filepath.Join(home, key[2:])
	}
	if key != "" {
		if !filepath.IsAbs(key) {
			return nil, fmt.Errorf("private key path must be absolute or start with ~/")
		}
		host["private_key_file"] = key
	}
	return host, nil
}
