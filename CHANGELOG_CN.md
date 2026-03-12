# 更新日志

本项目的所有重大变更都将记录在此文件中。

## [发布]

## [v5.1.0] - 2026-03-12

### 新增功能

- 部署工具dingoadm集成到dingo
- 支持cache、mds、client管理，包括下载，安装，升级和启动，支持指定组件版本启动
- 支持监控组件Prometheus、Grafana的部署
- 支持命令分组
- 支持为Prometheus自动获取挂载点的IP地址
- stats命令增加vfs读写缓冲区的使用情况
- 支持nfs-ganesha export的管理

### 功能优化

- warmup功能重构
