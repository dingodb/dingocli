DingoCli
---

DingoCli is a tool for deploy and manage DingoFS cluster, it's easy to use and with rich features.

Documentation
---

* [简体中文][docs]
 
Installation
---

```shell
bash -c "$(curl -fsSL https://raw.githubusercontent.com/dingodb/dingocli/master/scripts/install_dingo.sh)"
```

Usage
---

Run `dingocli -h` for more informations.

Contributing
---

Contributions of code, tests, documentation and bugs are welcome,
you can see [CONTRIBUTING][contributing] for details.

License
---

DingoCli is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.

[docs]: https://github.com/dingodb/dingocli/wiki
[contributing]: https://github.com/dingodb/dingocli/wiki/others#参与-dingocli-的开发
### Dynamic inventory for playbook commands

Use an executable Ansible inventory script to discover hosts for the current
`playbook` invocation. Dingo calls the supplied path with `--list`, inherits the
caller's environment and working directory, and keeps symlinks intact so scripts
can find region-specific files beside the invoked link.

```bash
# Preview the selected hosts. Choose the control node's actual private key.
dingo playbook -i /path/to/region/hosts.py \
  --inventory-user root --inventory-private-key ~/.ssh/id_rsa \
  -l cache_servers --list-hosts

# Inspect runtime configuration using a fresh inventory snapshot.
dingo playbook -i /path/to/region/hosts.py \
  --inventory-user root --inventory-private-key ~/.ssh/id_rsa \
  config check dingo-cache-v5.2.service -l cache_servers

# Copy a file to dynamically selected client nodes.
dingo playbook -i /path/to/region/hosts.py --inventory-user root \
  -l client_servers --scp --target /tmp/check.sh ./check.sh

# Execute a local script on the selected nodes.
dingo playbook -i /path/to/region/hosts.py --inventory-user root \
  -l client_servers ./check.sh
```

The same `-i` option applies to `config get`, `config set`, `--tune`, SCP and
script execution. A local script supplied without extra arguments is uploaded
and executed. A single non-file argument retains the existing remote-command
shorthand.

The loader reads Ansible `--list` JSON with group `hosts`, `children`, `vars`
and `_meta.hostvars`. Group membership becomes dingo labels, including ancestor
groups and each host's own name. Hosts appearing in multiple groups are selected
once. Group variables merge in depth, `ansible_group_priority`, and alphabetical
order, followed by `_meta.hostvars`. Use fully resolved values in this output.
Dingo does not evaluate Jinja, load adjacent Ansible group_vars/host_vars files,
or call the script's per-host `--host` endpoint.

| Inventory variable | Dingo connection field |
| --- | --- |
| `ansible_host` / `ansible_ssh_host` | SSH address, resolved to an IP for the native host record |
| `ansible_user` / `ansible_ssh_user` | SSH user |
| `ansible_port` / `ansible_ssh_port` | SSH port |
| `ansible_private_key_file` / `ansible_ssh_private_key_file` | Private key path, absolute or starting with `~/` |

`--inventory-user`, `--inventory-port` and `--inventory-private-key` override
inventory values. Missing connection fields use normal dingo defaults, including
the current local user, port 22 and `~/.ssh/id_rsa`. Password authentication,
custom Ansible SSH argument strings, non-SSH connection plugins and enabled
Ansible become are rejected because this path uses dingo's SSH transport.

Scripts must be executable and emit exactly one JSON object on stdout.
Diagnostics belong on stderr and are forwarded to the caller. Nonzero exit,
invalid JSON, invalid group graphs, empty inventory and empty label selection
stop the operation. `--inventory-timeout` defaults to `1m`, covers discovery and
address resolution, and must be positive. Stdout is limited to 32 MiB. Unix
cancellation also terminates the inventory process group.

Each invocation uses one fresh snapshot for filtering and all SSH/SCP lookups.
The snapshot is held in an invocation-local CLI view. Saved host registrations
remain available to later commands using the database. Normal CLI configuration,
database initialization and auditing still apply. Omitting `-i` uses the stored
host list as before. No `hosts commit` is needed for this workflow.

For `hosts-inspect.py`, existing variables such as `DINGOFS_ANSIBLE_NET` and
`DINGOFS_INCLUDE_CORDONED` can be set on the dingo command:

```bash
DINGOFS_ANSIBLE_NET=k8s DINGOFS_INCLUDE_CORDONED=false \
  dingo playbook -i /path/to/region/hosts.py --inventory-user root \
  -l cache_servers --list-hosts
```
