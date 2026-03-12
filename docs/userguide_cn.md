# Dingo 工具使用指南

DingoFS 集群管理工具

- [dingo tool 使用](#dingo-tool-使用)
  - [如何使用 dingo 工具](#how-to-use-dingo-tool)
    - [配置](#configure)
    - [简介](#introduction)
  - [命令](#command)
    - [fs](#fs)
      - [fs mount](#fs-mount)
      - [fs umount](#fs-umount)
      - [fs create](#fs-create)
      - [fs delete](#fs-delete)
      - [fs list](#fs-list)
      - [fs mountpoint](#fs-mountpoint)
      - [fs query](#fs-query)
      - [fs usage](#fs-usage)
      - [fs stats](#fs-stats)
      - [fs quota](#fs-quota)
        - [fs quota set](#fs-quota-set)
        - [fs quota get](#fs-quota-get)
        - [fs quota check](#fs-quota-check)
    - [component](#component)
      - [component list](#component-list)
      - [component install](#component-install)
      - [component update](#component-update)
      - [component uninstall](#component-uninstall)
      - [component use](#component-use)
    - [mds](#mds)
      - [mds status](#mds-status)
      - [mds start](#mds-start)
      - [mds meta](#mds-meta)
    - [cache](#cache)
      - [cache start](#cache-start)
      - [cache group](#cache-group)
        - [cache group list](#cache-group-list)
      - [cache member](#cache-member)
        - [cache member set](#cache-member-set)     
        - [cache member list](#cache-member-list)
        - [cache member unlock](#cache-member-unlock)
        - [cache member leave](#cache-member-leave)
        - [cache member delete](#cache-member-delete)     
    - [warmup](#warmup)
      - [warmup add](#warmup-add)
      - [warmup query](#warmup-query)
    - [quota](#quota)
      - [quota set](#quota-set)
      - [quota get](#quota-get)
      - [quota list](#quota-list)
      - [quota delete](#quota-delete)
      - [quota check](#quota-check)
       
## 如何使用 dingo 工具

### 配置

设置配置文件

dingo.yaml 文件对于部署 dingofs 集群不是必需的，仅用于管理 dingofs 集群。
```bash
wget https://raw.githubusercontent.com/dingodb/dingocli/main/dingo.yaml
```
请根据需要修改 dingo.yaml 文件中 dingofs 下的 `mdsaddr`

配置文件优先级
环境变量(CONF=/opt/dingo.yaml) > 默认值 (~/.dingo/dingo.yaml)
```bash
mv dingo.yaml ~/.dingo/dingo.yaml
或者
export CONF=/opt/dingo.yaml
```

### 简介

工具使用方法如下

```bash
dingo COMMAND [options]
```

当您不确定如何使用某个命令时，--help 可以提供使用示例：

```bash
dingo COMMAND --help
```

例如：

dingo status mds --help
```bash
使用:  dingo mds status [OPTIONS]

show mds cluster status

Options:
  -c, --conf string              Specify configuration file (default "$HOME/.dingo/dingo.yaml")
      --format string            output format (json|plain) (default "plain")
  -h, --help                     Print 使用
      --mdsaddr string           Specify mds address (default "127.0.0.1:7400")
      --rpcretrydelay duration   RPC retry delay (default 200ms)
      --rpcretrytimes uint32     RPC retry times (default 5)
      --rpctimeout duration      RPC timeout (default 30s)
      --verbose                  Show more debug info

Examples:
   $ dingo mds status

```

## 命令

### fs

#### fs mount

挂载文件系统

使用:

```shell
dingo fs mount METAURL MOUNTPOINT [OPTIONS]
```

输出:

```shell
$ dingo fs mount mds://10.220.69.6:8400/dingofs1 /mnt

dingofs1 is ready at /mnt

current configuration:
  config               []
  log                  [/home/yansp/.dingofs/log INFO 0(verbose)]
  meta                 [mds://10.220.69.6:8400/dingofs1]
  storage              [s3://10.220.32.13:8001/dingofs-bucket]
  cache                [/home/yansp/.dingofs/cache 102400MB 10%(ratio)]
  monitor              [10.220.69.6:10000]
```

#### fs umount

卸载文件系统

使用:

```shell
dingo fs umount MOUNTPOINT [OPTIONS]
```

输出:

```shell
$ dingo fs umount /mnt

Successfully unmounted /mnt
```

#### fs create

在集群中创建文件系统

使用:

```shell
# 存储在 s3
$ dingo create fs dingofs1 --storagetype s3 --s3.ak AK --s3.sk SK --s3.endpoint http://localhost:9000 --s3.bucketname dingofs-bucket

# 存储在 rados
$ dingo create fs dingofs1 --storagetype rados --rados.username admin --rados.key AQDg3Y2h --rados.mon 10.220.32.1:3300,10.220.32.2:3300,10.220.32.3:3300 --rados.poolname pool1 --rados.clustername ceph
```

输出:

```shell
$ dingo fs create dingofs1 
Successfully create filesystem dingofs1, uuid: d58cca2b-08d7-4aac-91b6-69b21d1a1de1
```

#### fs delete

从集群中删除文件系统 

使用:

```shell
dingo fs delete FSNAME [OPTIONS]
```

输出:

```shell
$ dingo fs delete dingofs1
WARNING:Are you sure to delete fs dingofs1?
please input [dingofs1] to confirm: dingofs1
Successfully delete filesystem dingofs1
```

#### fs list

列出所有文件系统信息 

使用:

```shell
dingo fs list [OPTIONS]
```

输出:

```shell
$ dingo fs list
+-------+-----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| FSID  |  FSNAME   | STATUS  | BLOCKSIZE | CHUNKSIZE | MDSNUM |  STORAGETYPE  |               STORAGE               | MOUNTNUM |                 UUID                 |
+-------+-----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| 10000 | yanspfs01 | NORMAL  | 4194304   | 67108864  | 3      | S3(HASH 1024) | http://10.220.32.13:8001/yansp-test | 1        | a88a67e8-d550-4564-a551-27f21520ffd2 |
+-------+-----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| 10002 | dingofs1  | DELETED | 4194304   | 67108864  | 3      | S3(HASH 1024) | http://10.220.32.13:8001/yansp-test | 0        | d58cca2b-08d7-4aac-91b6-69b21d1a1de1 |
+-------+-----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
```

#### fs mountpoint

列出集群中所有挂载点

使用:

```shell
dingo fs mountpoint [OPTIONS]
```

输出:

```shell
$ dingo fs mountpoint
+-------+-----------+--------------------------------------+------------------------------+-------+
| FSID  |  FSNAME   |               CLIENTID               |       MOUNTPOINT             |  CTO  |
+-------+-----------+--------------------------------------+------------------------------+-------+
| 10000 | dingofs1 | 7d16a4a9-b231-4394-8a5e-fe61bf6f66ac | dingofs-6:10000:/mnt/dingofs  | false |
+-------+-----------+--------------------------------------+------------------------------+-------+
```

#### fs query

查询单个文件系统信息

使用:

```shell
dingo fs query [OPTIONS]
```

输出:

```shell
$ dingo fs query --fsname dingofs1
+-------+----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| FSID  |  FSNAME  | STATUS  | BLOCKSIZE | CHUNKSIZE | MDSNUM |  STORAGETYPE  |               STORAGE               | MOUNTNUM |                 UUID                 |
+-------+----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| 10002 | dingofs1 | DELETED | 4194304   | 67108864  | 3      | S3(HASH 1024) | http://10.220.32.13:8001/yansp-test | 0        | d58cca2b-08d7-4aac-91b6-69b21d1a1de1 |
+-------+----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
```

#### fs 使用

获取文件系统使用情况

使用:

```shell
dingo fs usage [OPTIONS]
```

输出:

```shell
$ dingo fs usage --humanize
+-------+-----------+---------+-------+
| FSID  |  FSNAME   |  USED   | IUSED |
+-------+-----------+---------+-------+
| 10000 | yanspfs01 | 3.9 GiB | 1,746 |
+-------+-----------+---------+-------+
```

#### fs stats

显示 dingofs 挂载点的实时性能统计

使用:

```shell
dingo fs stats MOUNTPOINT [OPTIONS]

# 普通模式
dingo fs stats /mnt/dingofs
			
# fuse 指标
dingo fs stats /mnt/dingofs --schema f

# s3 指标
dingo fs stats /mnt/dingofs --schema o

# 更多指标
dingo fs stats /mnt/dingofs --verbose

# 显示 3 次
dingo fs stats /mnt/dingofs --count 3

# 每 4 秒显示一次
dingo fs stats /mnt/dingofs --interval 4s

```
输出:

```shell
dingo fs stats /mnt/dingofs

------使用------ ----------fuse--------- ----blockcache--- ---object-- ------remotecache------
 cpu   mem   used| ops   lat   read write| load stage cache| get   put | load stage cache  hit 
 525% 4691M 2688K|   0     0     0     0 |   0     0     0 |   0     0 |   0     0     0   0.0%
 526% 4691M 1664K|1433  5.52   177M   95M|   0     0     0 |   0    96M| 453M    0    95M 99.4%
 527% 4691M 1152K|1418  5.71   157M   75M|   0     0     0 |   0    76M| 405M    0    76M 99.6%
 527% 4692M   64K|1531  5.24   189M   86M|   0     0     0 |   0    87M| 428M    0    86M 99.8%
 535% 4692M   64K|1415  5.55   180M   93M|   0     0     0 |   0    93M| 424M    0    93M 99.5%
 535% 4693M 1536K|1404  5.62   172M   96M|   0     0     0 |   0    95M| 396M    0    95M 99.5%
 537% 4692M 1152K|1420  5.55   171M   83M|   0     0     0 |   0    83M| 381M    0    84M 99.6%
 537% 4692M    0 |1303  5.92   170M   90M|   0     0     0 |   0    92M| 390M    0    90M 99.4%
 529% 4692M 2752K|1159  6.87   160M   81M|   0     0     0 |   0    79M| 391M    0    79M 99.5%
 528% 4692M 1600K|1372  5.87   166M   83M|   0     0     0 |   0    84M| 383M    0    86M 99.5%
 530% 4692M 3584K|1428  5.63   168M   79M|   0     0     0 |   0    77M| 435M    0    78M 99.4%
 528% 4692M    0 |1161  6.85   159M   71M|   0     0     0 |   0    74M| 363M    0    72M 99.3%
 500% 4692M    0 | 500  17.9    74M   37M|   0     0     0 |   0    37M| 167M    0    37M 99.6%
 490% 4692M 1664K|1113  7.35   146M   82M|   0     0     0 |   0    80M| 360M    0    80M 99.1%
 488% 4692M  640K|1431  5.53   167M   86M|   0     0     0 |   0    87M| 440M    0    87M 99.3%
 488% 4692M 1088K|1413  5.49   198M   92M|   0     0     0 |   0    92M| 441M    0    92M 99.6%
```

#### fs quota

##### fs quota set

设置文件系统配额

使用:

```shell
dingo fs quota set [OPTIONS]
```

输出:

```shell
$ dingo fs quota set  --fsname dingofs1 --capacity 10 --inodes 1000000
Successfully config fs quota, capacity: 10 GiB, inodes: 1,000,000
```

##### fs quota get

获取文件系统配额

使用:

```shell
dingo fs quota get [OPTIONS]
```

输出:

```shell
$ dingo fs quota get --fsname dingofs1 
+-------+-----------+----------+---------+------+-----------+-------+-------+
| FSID  |  FSNAME   | CAPACITY |  USED   | USE% |  INODES   | IUSED | IUSE% |
+-------+-----------+----------+---------+------+-----------+-------+-------+
| 10000 | dingofs1 | 10 GiB   | 3.9 GiB | 39   | 1,000,000 | 2,255 | 0     |
+-------+-----------+----------+---------+------+-----------+-------+-------+
```

##### fs quota check

检查文件系统配额

使用:

```shell
dingo fs quota check [OPTIONS]
```

输出:

```shell
$ dingo fs quota check --fsname dingofs1 
+-------+-----------+----------------+---------------+---------------+-----------+-------+-----------+---------+
| FSID  |  FSNAME   |    CAPACITY    |     USED      |   REALUSED    |  INODES   | IUSED | REALIUSED | STATUS  |
+-------+-----------+----------------+---------------+---------------+-----------+-------+-----------+---------+
| 10000 | dingofs1  | 10,737,418,240 | 4,198,684,323 | 4,198,684,323 | 1,000,000 | 2,255 | 2,255     | success |
+-------+-----------+----------------+---------------+---------------+-----------+-------+-----------+---------+
```

### component

component 命令用于管理 dingofs 核心组件（dingo-client、dingo-cache、dingo-mds、dingo-mds-client），支持下载、安装、升级、启动以及指定版本启动等功能。

支持的组件列表：
- dingo-client
- dingo-cache
- dingo-mds
- dingo-mds-client

#### component list

列出所有可用组件和已安装组件

使用:

```shell
dingo component list [OPTIONS]

# 显示详细输出
dingo component list -v

# 仅显示已安装的组件
dingo component list --installed
```

输出:

```shell
$ dingo component list
Name              Version      Installed    Commit      Active
----              -------      ----------   ------      ------
dingo-client      v3.0.0      Yes          abc123      Yes
dingo-client      v3.0.5      Yes(U)       def456
dingo-cache       v3.0.0      Yes          abc123      Yes
dingo-mds         v3.0.0      Yes          abc123      Yes
dingo-mds-client  v3.0.0      Yes          abc123      Yes

$ dingo component list --installed
Name              Version      Installed    Commit      Active
----              -------      ----------   ------      ------
dingo-client      v3.0.0      Yes          abc123      Yes
dingo-cache       v3.0.0      Yes          abc123      Yes
dingo-mds         v3.0.0      Yes          abc123      Yes
dingo-mds-client  v3.0.0      Yes          abc123      Yes
```

> 注：(U) 表示有可用的更新版本

#### component install

安装组件

使用:

```shell
dingo component install <component1>[:version] [component2...N] [OPTIONS]
```

Examples:

```shell
# 安装最新稳定版
$ dingo component install dingo-client

# 安装指定版本
$ dingo component install dingo-client:v3.0.5

# 安装 main 分支（非稳定版）
$ dingo component install dingo-client:main

# 同时安装多个组件
$ dingo component install dingo-client:main dingo-cache dingo-mds:v3.0.5
```

输出:

```shell
$ dingo component install dingo-client
Download dingo-client from https://www.dingodb.com/dingofs/dingo-client/dingo-client-v3.0.0.tar.gz
Successfully install components [dingo-client:v3.0.0] ^_^!
```

#### component update

更新已安装的组件

使用:

```shell
dingo component update <component1>[:version] [component2...N] [OPTIONS]
```

Options:
- `--all`: 更新所有已安装的组件

Examples:

```shell
# 更新 dingo-client 到最新稳定版
$ dingo component update dingo-client

# 更新 dingo-client:v3.0.5 到最新构建版本
$ dingo component update dingo-client:v3.0.5

# 更新所有已安装组件
$ dingo component update --all
```

输出:

```shell
$ dingo component update dingo-client
Download dingo-client from https://www.dingodb.com/dingofs/dingo-client/dingo-client-v3.0.5.tar.gz
Updated successfully ^_^!
```

#### component uninstall

卸载组件

使用:

```shell
dingo component uninstall <component1>:<version> [OPTIONS]
```

Options:
- `--all`: 卸载指定组件的所有版本
- `--force`: 强制卸载，即使组件正在使用

Examples:

```shell
# 卸载指定版本
$ dingo component uninstall dingo-client:v1.2.0

# 卸载指定组件的所有版本
$ dingo component uninstall dingo-client --all

# 强制卸载正在使用的组件
$ dingo component uninstall dingo-client:v1.2.0 --force
```

输出:

```shell
$ dingo component uninstall dingo-client:v1.2.0
Successfully removed component: dingo-client:v1.2.0

$ dingo component uninstall dingo-client --all
Successfully removed components: 
  dingo-client:v1.0.0 
  dingo-client:v1.2.0 
```

#### component use

设置默认版本

使用:

```shell
dingo component use <component1>:[version] [OPTIONS]
```

Examples:

```shell
# 使用指定版本作为默认版本
$ dingo component use dingo-client:v1.2.0

# 使用最新版本作为默认版本
$ dingo component use dingo-client
```

输出:

```shell
$ dingo component use dingo-client:v1.2.0
Successfully use dingo-client:v1.2.0 as default version
```

### mds

#### mds status

获取 mds 状态

使用:

```shell
dingo mds status
```

输出:

```shell
+------+------------------+--------+-------------------------+-------------+
|  ID  |       ADDR       | STATE  |    LAST ONLINE TIME     | ONLINESTATE |
+------+------------------+--------+-------------------------+-------------+
| 1001 | 10.220.69.6:8400 | NORMAL | 2026-01-19 15:37:50.585 | online      |
+------+------------------+--------+-------------------------+-------------+
| 1002 | 10.220.69.6:8401 | NORMAL | 2026-01-19 15:37:50.574 | online      |
+------+------------------+--------+-------------------------+-------------+
| 1003 | 10.220.69.6:8402 | NORMAL | 2026-01-19 15:37:50.708 | online      |
+------+------------------+--------+-------------------------+-------------+
```

#### mds start

启动 mds

使用:

```shell
dingo mds start --conf=./mds.conf
```

输出:

```shell
$ dingo mds start --conf=./mds.conf 
current configuration:
  id                   [1001]
  config               [./mds.conf]
  log                  [/home/yansp/.dingofs/log INFO 0(verbose)]
  storage              [dummy]

mds is listening on 0.0.0.0:7777
```

#### mds meta

备份和恢复元数据

使用:

```shell
dingo mds meta --cmd=backup  --type=meta --coor_addr=file://./coor_list --output_type=file --out=meta_backup
```

输出:

```shell
$ dingo mds meta --cmd=backup  --type=meta --coor_addr=file://./coor_list --output_type=file --out=meta_backup --fs_id=10000
### use cluster id: 0
backup meta table done.
summary total_count(9) lock_count(2) auto_increment_id_count(0) mds_heartbeat_count(3) client_heartbeat_count(1) cache_member_heartbeat_count(0) fs_count(1) fs_quota_count(1) fs_oplog_count(1).
```

### cache

#### cache start

启动缓存节点

使用:

```shell
dingo cache start [OPTIONS]
```

输出:

```shell
$ dingo cache start --id=85a4b352-4097-4868-9cd6-9ec5e53db1b6 --conf ./cache.conf
current configuration:
  id                   [85a4b352-4097-4868-9cd6-9ec5e53db1b6]
  config               [./cache.conf]
  log                  [/home/yansp/.dingofs/log INFO 0(verbose)]
  mds                  [10.220.69.6:8400]
  cache                [disk /home/yansp/.dingofs/cache 102400MB 10%(ratio)]

dingo-cache is listening on 10.220.69.6:8888
```

#### cache group

##### cache group list

列出所有远程缓存组名称

使用:

```shell
dingo cache group list [OPTIONS]
```

输出:

```shell
$ dingo cache group list 
+--------+
| GROUP  |
+--------+
| group1 |
+--------+
```

#### cache member

##### cache member set

设置缓存成员权重

使用:

```shell
dingo cache member set --memberid MEMBERID --ip IP --port PORT --weight WEIGHT [OPTIONS]]
```

输出:

```shell
$ dingo cache member set --memberid 85a4b352-4097-4868-9cd6-9ec5e53db1b6 --ip 10.220.69.6 --port 8888 --weight 70
Successfully reweight cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6 to 70
```

##### cache member list

列出所有缓存成员

使用:

```shell
dingo cache member list [OPTIONS]
```

输出:

```shell
$ dingo cache member list 
+--------------------------------------+-------------+------+--------+--------+-------------------------+-------------------------+--------+--------+
|               MEMBERID               |     IP      | PORT | WEIGHT | LOCKED |       CREATE TIME       |    LAST ONLINE TIME     | STATE  | GROUP  |
+--------------------------------------+-------------+------+--------+--------+-------------------------+-------------------------+--------+--------+
| 85a4b352-4097-4868-9cd6-9ec5e53db1b6 | 10.220.69.6 | 8888 | 100    | true   | 2026-01-19 15:48:46.000 | 2026-01-19 16:07:29.179 | online | group1 |
+--------------------------------------+-------------+------+--------+--------+-------------------------+-------------------------+--------+--------+
```

##### cache member leave

将缓存成员从组中移除

使用:

```shell
dingo cache member leave [OPTIONS]
```

输出:

```shell
$ dingo cache member leave --group group1  --memberid 85a4b352-4097-4868-9cd6-9ec5e53db1b6 --ip 10.220.69.6 --port 8888 
Successfully leave cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6
```

##### cache member unlock

解除缓存成员与 IP 和端口的绑定

使用:

```shell
dingo cache member unlock [OPTIONS]
```

输出:

```shell
$ dingo cache member unlock  --memberid 85a4b352-4097-4868-9cd6-9ec5e53db1b6 --ip 10.220.69.6 --port 8888 
Successfully unlock cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6
```

##### cache member delete

删除缓存成员

使用:

```shell
dingo cache member delete MEMBERID [OPTIONS]
```

输出:

```shell
$ dingo cache member delete 85a4b352-4097-4868-9cd6-9ec5e53db1b6
WARNING:Are you sure to delete cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6?
please input [85a4b352-4097-4868-9cd6-9ec5e53db1b6] to confirm: 85a4b352-4097-4868-9cd6-9ec5e53db1b6
Successfully delete cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6
```

### warmup

#### warmup add

预热文件（目录），或提供包含要预热的文件（目录）列表的文件。

使用:

```shell
dingo warmup add /mnt/dingofs/warmup
dingo warmup add --filelist /mnt/dingofs/warmup.list
```

#### warmup query

查询预热进度

使用:

```shell
dingo warmup query /mnt/dingofs/warmup
```

### config
#### config fs

为 dingofs 配置文件系统配额

使用:

```shell
dingo config fs --fsid 1 --capacity 100
dingo config fs --fsname dingofs --capacity 10 --inodes 1000000000
```
#### config get

获取 dingofs 文件系统配额

使用:

```shell
dingo config get --fsid 1
dingo config get --fsname dingofs
```
输出:

```shell
+------+---------+----------+------+------+---------------+-------+-------+
| FSID | FSNAME  | CAPACITY | USED | USE% |    INODES     | IUSED | IUSE% |
+------+---------+----------+------+------+---------------+-------+-------+
| 2    | dingofs | 10 GiB   | 0 B  | 0    | 1,000,000,000 | 0     | 0     |
+------+---------+----------+------+------+---------------+-------+-------+
```

#### config check

检查文件系统配额

使用:

```shell
dingo config check --fsid 1
dingo config check --fsname dingofs
```
输出:

```shell
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
| FSID |  FSNAME  |    CAPACITY     |     USED      |   REALUSED    |  INODES   | IUSED | REALIUSED | STATUS  |
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
| 1    | dingofs  | 107,374,182,400 | 1,083,981,835 | 1,083,981,835 | unlimited | 9     | 9         | success |
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
```

### quota
#### quota set

设置目录配额

使用:

```shell
使用:  dingo quota set [OPTIONS]
```

输出:

```shell
$ dingo quota set --fsname dingofs1  --path /dir01  --capacity 10 --inodes 100000
Successfully set directory[/dir01] quota, capacity: 10 GiB, inodes: 100,000
```
#### quota get

获取目录配额

使用:

```shell
dingo quota get [OPTIONS]
```
输出:

```shell
$ dingo quota get --fsname dingofs1  --path /dir01
+-------------+--------+----------+------+------+---------+-------+-------+
|   INODEID   |  PATH  | CAPACITY | USED | USE% | INODES  | IUSED | IUSE% |
+-------------+--------+----------+------+------+---------+-------+-------+
| 20000005055 | /dir01 | 10 GiB   | 0 B  | 0    | 100,000 | 1     | 0     |
+-------------+--------+----------+------+------+---------+-------+-------+
```
#### quota list

列出文件系统所有目录配额

使用:

```shell
dingo quota list --fsname dingofs1
```

输出:

```shell
$ dingo quota list --fsname dingofs1
+-------------+--------+----------+------+------+---------+-------+-------+
|   INODEID   |  PATH  | CAPACITY | USED | USE% | INODES  | IUSED | IUSE% |
+-------------+--------+----------+------+------+---------+-------+-------+
| 20000005055 | /dir01 | 10 GiB   | 0 B  | 0    | 100,000 | 1     | 0     |
+-------------+--------+----------+------+------+---------+-------+-------+
```

#### quota delete

删除目录配额

使用:

```shell
dingo quota delete [OPTIONS]
```

输出:

```shell
$ dingo quota delete --fsname dingofs1 --path /dir01
Successfully delete directory[/dir01] quota
```

#### quota check

验证目录配额的一致性

使用:

```shell
dingo quota check [OPTIONS]
```

输出:

```shell
$ dingo quota check --fsname dingofs1 --path /dir01
+-------------+--------+----------------+------+----------+---------+-------+-----------+---------+
|   INODEID   |  NAME  |    CAPACITY    | USED | REALUSED | INODES  | IUSED | REALIUSED | STATUS  |
+-------------+--------+----------------+------+----------+---------+-------+-----------+---------+
| 20000005055 | /dir01 | 10,737,418,240 | 0    | 0        | 100,000 | 1     | 1         | success |
+-------------+--------+----------------+------+----------+---------+-------+-----------+---------+
```
