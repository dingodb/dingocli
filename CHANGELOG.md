# Changelog

All notable changes to this project will be documented in this file.

## [Rlease]

## [v5.1.0] - 2026-03-12

### Added

- Dingoadm deployment tool is now integrated into dingo
- Support dingofs core components (cache, mds, client) management, including download, installation, upgrade, startup, and version-specific startup
- Support Prometheus and Grafana deployment for dingofs monitoring
- Support command grouping
- Support auto-discovery of mountpoint IP address for Prometheus
- Add vfs read/write buffer usage statistics to the stats command
- Support nfs-ganesha export management

### Changed

- Refactor warmup feature
