# dingo tool usage

A tool for DingoFS

- [dingo tool usage](#dingo-tool-usage)
  - [How to use dingo tool](#how-to-use-dingo-tool)
    - [Configure](#configure)
    - [Introduction](#introduction)
  - [Command](#command)
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
      
## How to use dingo tool

### Configure

set configure file

The dingo.yaml file is not necessary for deploy dingofs cluster, it is only used for managing dingofs cluster.
```bash
wget https://raw.githubusercontent.com/dingodb/dingocli/main/dingo.yaml
```
Please modify the `mdsaddr` under `dingofs` in the dingo.yaml file as required

configure file priority
environment variables(CONF=/opt/dingo.yaml) > default (~/.dingo/dingo.yaml)
```bash
mv dingo.yaml ~/.dingo/dingo.yaml
or
export CONF=/opt/dingo.yaml
```

### Introduction

Here's how to use the tool

```bash
dingo COMMAND [options]
```

When you are not sure how to use a command, --help can give you an example of use:

```bash
dingo COMMAND --help
```

For example:

dingo status mds --help
```bash
Usage:  dingo mds status [OPTIONS]

show mds cluster status

Options:
  -c, --conf string              Specify configuration file (default "$HOME/.dingo/dingo.yaml")
      --format string            output format (json|plain) (default "plain")
  -h, --help                     Print usage
      --mdsaddr string           Specify mds address (default "127.0.0.1:7400")
      --rpcretrydelay duration   RPC retry delay (default 200ms)
      --rpcretrytimes uint32     RPC retry times (default 5)
      --rpctimeout duration      RPC timeout (default 30s)
      --verbose                  Show more debug info

Examples:
   $ dingo mds status

```

## Command

### fs

#### fs mount

mount filesystem

Usage:

```shell
dingo fs mount METAURL MOUNTPOINT [OPTIONS]
```

Output:

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

umount filesystem

Usage:

```shell
dingo fs umount MOUNTPOINT [OPTIONS]
```

Output:

```shell
$ dingo fs umount /mnt

Successfully unmounted /mnt
```

#### fs create

create fs in cluster

Usage:

```shell
# store in s3
$ dingo create fs dingofs1 --storagetype s3 --s3.ak AK --s3.sk SK --s3.endpoint http://localhost:9000 --s3.bucketname dingofs-bucket

# store in rados
$ dingo create fs dingofs1 --storagetype rados --rados.username admin --rados.key AQDg3Y2h --rados.mon 10.220.32.1:3300,10.220.32.2:3300,10.220.32.3:3300 --rados.poolname pool1 --rados.clustername ceph
```

Output:

```shell
$ dingo fs create dingofs1 
Successfully create filesystem dingofs1, uuid: d58cca2b-08d7-4aac-91b6-69b21d1a1de1
```

#### fs delete

delete fs from cluster 

Usage:

```shell
dingo fs delete FSNAME [OPTIONS]
```

Output:

```shell
$ dingo fs delete dingofs1
WARNING:Are you sure to delete fs dingofs1?
please input [dingofs1] to confirm: dingofs1
Successfully delete filesystem dingofs1
```

#### fs list

list all fs info 

Usage:

```shell
dingo fs list [OPTIONS]
```

Output:

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

list all mountpoints in the cluster

Usage:

```shell
dingo fs mountpoint [OPTIONS]
```

Output:

```shell
$ dingo fs mountpoint
+-------+-----------+--------------------------------------+------------------------------+-------+
| FSID  |  FSNAME   |               CLIENTID               |       MOUNTPOINT             |  CTO  |
+-------+-----------+--------------------------------------+------------------------------+-------+
| 10000 | dingofs1 | 7d16a4a9-b231-4394-8a5e-fe61bf6f66ac | dingofs-6:10000:/mnt/dingofs  | false |
+-------+-----------+--------------------------------------+------------------------------+-------+
```

#### fs query

query one fs info

Usage:

```shell
dingo fs query [OPTIONS]
```

Output:

```shell
$ dingo fs query --fsname dingofs1
+-------+----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| FSID  |  FSNAME  | STATUS  | BLOCKSIZE | CHUNKSIZE | MDSNUM |  STORAGETYPE  |               STORAGE               | MOUNTNUM |                 UUID                 |
+-------+----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
| 10002 | dingofs1 | DELETED | 4194304   | 67108864  | 3      | S3(HASH 1024) | http://10.220.32.13:8001/yansp-test | 0        | d58cca2b-08d7-4aac-91b6-69b21d1a1de1 |
+-------+----------+---------+-----------+-----------+--------+---------------+-------------------------------------+----------+--------------------------------------+
```

#### fs usage

get the filesystem usage

Usage:

```shell
dingo fs usage [OPTIONS]
```

Output:

```shell
$ dingo fs usage --humanize
+-------+-----------+---------+-------+
| FSID  |  FSNAME   |  USED   | IUSED |
+-------+-----------+---------+-------+
| 10000 | yanspfs01 | 3.9 GiB | 1,746 |
+-------+-----------+---------+-------+
```

#### fs stats

show real time performance statistics of dingofs mountpoint

Usage:

```shell
dingo fs stats MOUNTPOINT [OPTIONS]

# normal
dingo fs stats /mnt/dingofs
			
# fuse metrics
dingo fs stats /mnt/dingofs --schema f

# s3 metrics
dingo fs stats /mnt/dingofs --schema o

# More metrics
dingo fs stats /mnt/dingofs --verbose

# Show 3 times
dingo fs stats /mnt/dingofs --count 3

# Show every 4 seconds
dingo fs stats /mnt/dingofs --interval 4s

```
Output:

```shell
dingo fs stats /mnt/dingofs

------usage------ ----------fuse--------- ----blockcache--- ---object-- ------remotecache------
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

set fs quota

Usage:

```shell
dingo fs quota set [OPTIONS]
```

Output:

```shell
$ dingo fs quota set  --fsname dingofs1 --capacity 10 --inodes 1000000
Successfully config fs quota, capacity: 10 GiB, inodes: 1,000,000
```

##### fs quota set

set fs quota

Usage:

```shell
dingo fs quota set [OPTIONS]
```

Output:

```shell
$ dingo fs quota set  --fsname dingofs1 --capacity 10 --inodes 1000000
Successfully config fs quota, capacity: 10 GiB, inodes: 1,000,000
```

##### fs quota get

get fs quota

Usage:

```shell
dingo fs quota get [OPTIONS]
```

Output:

```shell
$ dingo fs quota get --fsname dingofs1 
+-------+-----------+----------+---------+------+-----------+-------+-------+
| FSID  |  FSNAME   | CAPACITY |  USED   | USE% |  INODES   | IUSED | IUSE% |
+-------+-----------+----------+---------+------+-----------+-------+-------+
| 10000 | dingofs1 | 10 GiB   | 3.9 GiB | 39   | 1,000,000 | 2,255 | 0     |
+-------+-----------+----------+---------+------+-----------+-------+-------+
```

##### fs quota check

check fs quota

Usage:

```shell
dingo fs quota check [OPTIONS]
```

Output:

```shell
$ dingo fs quota check --fsname dingofs1 
+-------+-----------+----------------+---------------+---------------+-----------+-------+-----------+---------+
| FSID  |  FSNAME   |    CAPACITY    |     USED      |   REALUSED    |  INODES   | IUSED | REALIUSED | STATUS  |
+-------+-----------+----------------+---------------+---------------+-----------+-------+-----------+---------+
| 10000 | dingofs1  | 10,737,418,240 | 4,198,684,323 | 4,198,684,323 | 1,000,000 | 2,255 | 2,255     | success |
+-------+-----------+----------------+---------------+---------------+-----------+-------+-----------+---------+
```

### mds

#### mds status

get status of mds

Usage:

```shell
dingo mds status
```

Output:

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

start mds

Usage:

```shell
dingo mds start --conf=./mds.conf
```

Output:

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

backup and restore meta data

Usage:

```shell
dingo mds meta --cmd=backup  --type=meta --coor_addr=file://./coor_list --output_type=file --out=meta_backup
```

Output:

```shell
$ dingo mds meta --cmd=backup  --type=meta --coor_addr=file://./coor_list --output_type=file --out=meta_backup --fs_id=10000
### use cluster id: 0
backup meta table done.
summary total_count(9) lock_count(2) auto_increment_id_count(0) mds_heartbeat_count(3) client_heartbeat_count(1) cache_member_heartbeat_count(0) fs_count(1) fs_quota_count(1) fs_oplog_count(1).
```

### cache

#### cache start

start cache node

Usage:

```shell
dingo cache start [OPTIONS]
```

Output:

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

list all remote cache group name

Usage:

```shell
dingo cache group list [OPTIONS]
```

Output:

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

set cache member weight

Usage:

```shell
dingo cache member set --memberid MEMBERID --ip IP --port PORT --weight WEIGHT [OPTIONS]]
```

Output:

```shell
$ dingo cache member set --memberid 85a4b352-4097-4868-9cd6-9ec5e53db1b6 --ip 10.220.69.6 --port 8888 --weight 70
Successfully reweight cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6 to 70
```

##### cache member list

list all cache members

Usage:

```shell
dingo cache member list [OPTIONS]
```

Output:

```shell
$ dingo cache member list 
+--------------------------------------+-------------+------+--------+--------+-------------------------+-------------------------+--------+--------+
|               MEMBERID               |     IP      | PORT | WEIGHT | LOCKED |       CREATE TIME       |    LAST ONLINE TIME     | STATE  | GROUP  |
+--------------------------------------+-------------+------+--------+--------+-------------------------+-------------------------+--------+--------+
| 85a4b352-4097-4868-9cd6-9ec5e53db1b6 | 10.220.69.6 | 8888 | 100    | true   | 2026-01-19 15:48:46.000 | 2026-01-19 16:07:29.179 | online | group1 |
+--------------------------------------+-------------+------+--------+--------+-------------------------+-------------------------+--------+--------+
```

##### cache member leave

leave cache member from group

Usage:

```shell
dingo cache member leave [OPTIONS]
```

Output:

```shell
$ dingo cache member leave --group group1  --memberid 85a4b352-4097-4868-9cd6-9ec5e53db1b6 --ip 10.220.69.6 --port 8888 
Successfully leave cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6
```

##### cache member unlock

unbind the cache memberid with IP and Port

Usage:

```shell
dingo cache member unlock [OPTIONS]
```

Output:

```shell
$ dingo cache member unlock  --memberid 85a4b352-4097-4868-9cd6-9ec5e53db1b6 --ip 10.220.69.6 --port 8888 
Successfully unlock cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6
```

##### cache member delete

delete cache member

Usage:

```shell
dingo cache member delete MEMBERID [OPTIONS]
```

Output:

```shell
$ dingo cache member delete 85a4b352-4097-4868-9cd6-9ec5e53db1b6
WARNING:Are you sure to delete cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6?
please input [85a4b352-4097-4868-9cd6-9ec5e53db1b6] to confirm: 85a4b352-4097-4868-9cd6-9ec5e53db1b6
Successfully delete cachemember 85a4b352-4097-4868-9cd6-9ec5e53db1b6
```

### warmup

#### warmup add

warmup a file(directory), or given a list file contains a list of files(directories) that you want to warmup.

Usage:

```shell
dingo warmup add /mnt/dingofs/warmup
dingo warmup add --filelist /mnt/dingofs/warmup.list
```

#### warmup query

query the warmup progress

Usage:

```shell
dingo warmup query /mnt/dingofs/warmup
```

### config
#### config fs

config fs quota for dingofs

Usage:

```shell
dingo config fs --fsid 1 --capacity 100
dingo config fs --fsname dingofs --capacity 10 --inodes 1000000000
```
#### config get

get fs quota for dingofs

Usage:

```shell
dingo config get --fsid 1
dingo config get --fsname dingofs
```
Output:

```shell
+------+---------+----------+------+------+---------------+-------+-------+
| FSID | FSNAME  | CAPACITY | USED | USE% |    INODES     | IUSED | IUSE% |
+------+---------+----------+------+------+---------------+-------+-------+
| 2    | dingofs | 10 GiB   | 0 B  | 0    | 1,000,000,000 | 0     | 0     |
+------+---------+----------+------+------+---------------+-------+-------+
```

#### config check

check quota of fs

Usage:

```shell
dingo config check --fsid 1
dingo config check --fsname dingofs
```
Output:

```shell
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
| FSID |  FSNAME  |    CAPACITY     |     USED      |   REALUSED    |  INODES   | IUSED | REALIUSED | STATUS  |
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
| 1    | dingofs  | 107,374,182,400 | 1,083,981,835 | 1,083,981,835 | unlimited | 9     | 9         | success |
+------+----------+-----------------+---------------+---------------+-----------+-------+-----------+---------+
```

### quota
#### quota set

set quota to directory

Usage:

```shell
Usage:  dingo quota set [OPTIONS]
```

Output:

```shell
$ dingo quota set --fsname dingofs1  --path /dir01  --capacity 10 --inodes 100000
Successfully set directory[/dir01] quota, capacity: 10 GiB, inodes: 100,000
```
#### quota get

get directory quota

Usage:

```shell
dingo quota get [OPTIONS]
```
Output:

```shell
$ dingo quota get --fsname dingofs1  --path /dir01
+-------------+--------+----------+------+------+---------+-------+-------+
|   INODEID   |  PATH  | CAPACITY | USED | USE% | INODES  | IUSED | IUSE% |
+-------------+--------+----------+------+------+---------+-------+-------+
| 20000005055 | /dir01 | 10 GiB   | 0 B  | 0    | 100,000 | 1     | 0     |
+-------------+--------+----------+------+------+---------+-------+-------+
```
#### quota list

list fs all directory quota

Usage:

```shell
dingo quota list --fsname dingofs1
```

Output:

```shell
$ dingo quota list --fsname dingofs1
+-------------+--------+----------+------+------+---------+-------+-------+
|   INODEID   |  PATH  | CAPACITY | USED | USE% | INODES  | IUSED | IUSE% |
+-------------+--------+----------+------+------+---------+-------+-------+
| 20000005055 | /dir01 | 10 GiB   | 0 B  | 0    | 100,000 | 1     | 0     |
+-------------+--------+----------+------+------+---------+-------+-------+
```

#### quota delete

delete quota of a directory

Usage:

```shell
dingo quota delete [OPTIONS]
```

Output:

```shell
$ dingo quota delete --fsname dingofs1 --path /dir01
Successfully delete directory[/dir01] quota
```

#### quota check

verify the consistency of directory quota

Usage:

```shell
dingo quota check [OPTIONS]
```

Output:

```shell
$ dingo quota check --fsname dingofs1 --path /dir01
+-------------+--------+----------------+------+----------+---------+-------+-----------+---------+
|   INODEID   |  NAME  |    CAPACITY    | USED | REALUSED | INODES  | IUSED | REALIUSED | STATUS  |
+-------------+--------+----------------+------+----------+---------+-------+-----------+---------+
| 20000005055 | /dir01 | 10,737,418,240 | 0    | 0        | 100,000 | 1     | 1         | success |
+-------------+--------+----------------+------+----------+---------+-------+-----------+---------+
```