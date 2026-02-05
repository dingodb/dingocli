/*
 * 	Copyright (c) 2025 dingodb.com Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package utils

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	IP_PORT_REGEX = "((\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5]):([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))|(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])"
)

// format
const (
	FORMAT_JSON  = "json"
	FORMAT_PLAIN = "plain"
	FORMAT_NOOUT = "noout"
)

const (
	RPCTIMEOUT                  = "rpctimeout"
	VIPER_GLOBALE_RPCTIMEOUT    = "global.rpctimeout"
	DEFAULT_RPCTIMEOUT          = 30000 * time.Millisecond
	RPCRETRYTIMES               = "rpcretrytimes"
	VIPER_GLOBALE_RPCRETRYTIMES = "global.rpcretrytimes"
	DEFAULT_RPCRETRYTIMES       = uint32(5)
	RPCRETRYDElAY               = "rpcretrydelay"
	VIPER_GLOBALE_RPCRETRYDELAY = "global.rpcretrydelay"
	DEFAULT_RPCRETRYDELAY       = 200 * time.Millisecond
	VERBOSE                     = "verbose"
	VIPER_GLOBALE_VERBOSE       = "global.verbose"
	DEFAULT_VERBOSE             = false
	FORMAT                      = "format"

	// dingofs
	DINGOFS_MDSADDR         = "mdsaddr"
	VIPER_DINGOFS_MDSADDR   = "dingofs.mdsaddr"
	DEFAULT_DINGOFS_MDSADDR = "127.0.0.1:7400"
	DINGOFS_FSID            = "fsid"
	VIPER_DINGOFS_FSID      = "dingofs.fsid"
	DEFAULT_DINGOFS_FSID    = uint32(0)

	DINGOFS_FSNAME              = "fsname"
	VIPER_DINGOFS_FSNAME        = "dingofs.fsname"
	DINGOFS_NOCONFIRM           = "noconfirm"
	VIPER_DINGOFS_NOCONFIRM     = "dingofs.noconfirm"
	DINGOFS_BLOCKSIZE           = "blocksize"
	VIPER_DINGOFS_BLOCKSIZE     = "dingofs.blocksize"
	DINGOFS_DEFAULT_BLOCKSIZE   = "4 MiB"
	DINGOFS_CHUNKSIZE           = "chunksize"
	VIPER_DINGOFS_CHUNKSIZE     = "dingofs.chunksize"
	DINGOFS_DEFAULT_CHUNKSIZE   = "64 MiB"
	DINGOFS_STORAGETYPE         = "storagetype"
	VIPER_DINGOFS_STORAGETYPE   = "dingofs.storagetype"
	DINGOFS_DEFAULT_STORAGETYPE = "s3"
	DINGOFS_DEFAULT_DETAIL      = false

	// mds numbers
	DINGOFS_MDS_NUM         = "mdsnum"
	VIPER_DINGOFS_MDS_NUM   = "dingofs.mdsnum"
	DINGOFS_DEFAULT_MDS_NUM = uint32(0)

	DINGOFS_THREADS                = "threads"
	VIPER_DINGOFS_THREADS          = "dingofs.threads"
	DINGOFS_DEFAULT_THREADS        = uint32(8)
	DINGOFS_QUOTA_CAPACITY         = "capacity"
	DINGOFS_QUOTA_INODES           = "inodes"
	DINGOFS_PARTITION_TYPE         = "partitiontype"
	VIPER_DINGOFS_PARTITION_TYPE   = "dingofs.partitiontype"
	DINGOFS_DEFAULT_PARTITION_TYPE = "hash"
	DINGOFS_HUMANIZE               = "humanize"
	VIPER_DINGOFS_HUMANIZE         = "dingofs.humanize"
	DINGOFS_DEFAULT_HUMANIZE       = false

	// S3
	DINGOFS_S3_AK                 = "s3.ak"
	VIPER_DINGOFS_S3_AK           = "dingofs.s3.ak"
	DINGOFS_DEFAULT_S3_AK         = ""
	DINGOFS_S3_SK                 = "s3.sk"
	VIPER_DINGOFS_S3_SK           = "dingofs.s3.sk"
	DINGOFS_DEFAULT_S3_SK         = ""
	DINGOFS_S3_ENDPOINT           = "s3.endpoint"
	VIPER_DINGOFS_S3_ENDPOINT     = "dingofs.s3.endpoint"
	DINGOFS_DEFAULT_ENDPOINT      = ""
	DINGOFS_S3_BUCKETNAME         = "s3.bucketname"
	VIPER_DINGOFS_S3_BUCKETNAME   = "dingofs.s3.bucketname"
	DINGOFS_DEFAULT_S3_BUCKETNAME = ""

	// rados
	DINGOFS_RADOS_USERNAME            = "rados.username"
	VIPER_DINGOFS_RADOS_USERNAME      = "dingofs.rados.username"
	DINGOFS_DEFAULT_RADOS_USERNAME    = ""
	DINGOFS_RADOS_KEY                 = "rados.key"
	VIPER_DINGOFS_RADOS_KEY           = "dingofs.rados.key"
	DINGOFS_DEFAULT_RADOS_KEY         = ""
	DINGOFS_RADOS_MON                 = "rados.mon"
	VIPER_DINGOFS_RADOS_MON           = "dingofs.rados.mon"
	DINGOFS_DEFAULT_RADOS_MON         = ""
	DINGOFS_RADOS_POOLNAME            = "rados.poolname"
	VIPER_DINGOFS_RADOS_POOLNAME      = "dingofs.rados.poolname"
	DINGOFS_DEFAULT_RADOS_POOLNAME    = ""
	DINGOFS_RADOS_CLUSTERNAME         = "rados.clustername"
	VIPER_DINGOFS_RADOS_CLUSTERNAME   = "dingofs.rados.clustername"
	DINGOFS_DEFAULT_RADOS_CLUSTERNAME = "ceph"

	// subpath uid,gid
	DINGOFS_SUBPATH_UID         = "uid"
	VIPER_DINGOFS_SUBPATH_UID   = "dingofs.subpath.uid"
	DINGOFS_DEFAULT_SUBPATH_UID = uint32(0)
	DINGOFS_SUBPATH_GID         = "gid"
	VIPER_DINGOFS_SUBPATH_GID   = "dingofs.subpath.gid"
	DINGOFS_DEFAULT_SUBPATH_GID = uint32(0)

	// cache group
	DINGOFS_CACHE_GROUP            = "group"
	VIPER_DINGOFS_CACHE_GROUP      = "dingofs.cachegroup.group"
	DINGOFS_DEFAULT_CACHE_GROUP    = ""
	DINGOFS_CACHE_MEMBERID         = "memberid"
	VIPER_DINGOFS_CACHE_MEMBERID   = "dingofs.cachegroup.memberid"
	DINGOFS_DEFAULT_CACHE_MEMBERID = ""
	DINGOFS_CACHE_WEIGHT           = "weight"
	VIPER_DINGOFS_CACHE_WEIGHT     = "dingofs.cachegroup.weight"
	DINGOFS_DEFAULT_CACHE_WEIGHT   = uint32(0)
	DINGOFS_CACHE_IP               = "ip"
	VIPER_DINGOFS_CACHE_IP         = "dingofs.cachegroup.ip"
	DINGOFS_DEFAULT_CACHE_IP       = ""
	DINGOFS_CACHE_PORT             = "port"
	VIPER_DINGOFS_CACHE_PORT       = "dingofs.cachegroup.port"
	DINGOFS_DEFAULT_CACHE_PORT     = uint32(0)
)

var (
	FLAG2VIPER = map[string]string{
		RPCTIMEOUT:             VIPER_GLOBALE_RPCTIMEOUT,
		RPCRETRYTIMES:          VIPER_GLOBALE_RPCRETRYTIMES,
		RPCRETRYDElAY:          VIPER_GLOBALE_RPCRETRYDELAY,
		VERBOSE:                VIPER_GLOBALE_VERBOSE,
		DINGOFS_MDSADDR:        VIPER_DINGOFS_MDSADDR,
		DINGOFS_FSID:           VIPER_DINGOFS_FSID,
		DINGOFS_FSNAME:         VIPER_DINGOFS_FSNAME,
		DINGOFS_NOCONFIRM:      VIPER_DINGOFS_NOCONFIRM,
		DINGOFS_BLOCKSIZE:      VIPER_DINGOFS_BLOCKSIZE,
		DINGOFS_CHUNKSIZE:      VIPER_DINGOFS_CHUNKSIZE,
		DINGOFS_STORAGETYPE:    VIPER_DINGOFS_STORAGETYPE,
		DINGOFS_THREADS:        VIPER_DINGOFS_THREADS,
		DINGOFS_PARTITION_TYPE: VIPER_DINGOFS_PARTITION_TYPE,
		DINGOFS_HUMANIZE:       VIPER_DINGOFS_HUMANIZE,

		// S3
		DINGOFS_S3_AK:         VIPER_DINGOFS_S3_AK,
		DINGOFS_S3_SK:         VIPER_DINGOFS_S3_SK,
		DINGOFS_S3_ENDPOINT:   VIPER_DINGOFS_S3_ENDPOINT,
		DINGOFS_S3_BUCKETNAME: VIPER_DINGOFS_S3_BUCKETNAME,

		// rados
		DINGOFS_RADOS_USERNAME:    VIPER_DINGOFS_RADOS_USERNAME,
		DINGOFS_RADOS_KEY:         VIPER_DINGOFS_RADOS_KEY,
		DINGOFS_RADOS_MON:         VIPER_DINGOFS_RADOS_MON,
		DINGOFS_RADOS_POOLNAME:    VIPER_DINGOFS_RADOS_POOLNAME,
		DINGOFS_RADOS_CLUSTERNAME: VIPER_DINGOFS_RADOS_CLUSTERNAME,

		//subpath
		DINGOFS_SUBPATH_UID: VIPER_DINGOFS_SUBPATH_UID,
		DINGOFS_SUBPATH_GID: VIPER_DINGOFS_SUBPATH_GID,

		// cache group
		DINGOFS_CACHE_GROUP:    VIPER_DINGOFS_CACHE_GROUP,
		DINGOFS_CACHE_MEMBERID: VIPER_DINGOFS_CACHE_MEMBERID,
		DINGOFS_CACHE_WEIGHT:   VIPER_DINGOFS_CACHE_WEIGHT,
		DINGOFS_CACHE_IP:       VIPER_DINGOFS_CACHE_IP,
		DINGOFS_CACHE_PORT:     VIPER_DINGOFS_CACHE_PORT,

		// mds numbers
		DINGOFS_MDS_NUM: VIPER_DINGOFS_MDS_NUM,
	}
	FLAG2DEFAULT = map[string]interface{}{
		// rpc
		RPCTIMEOUT:    DEFAULT_RPCTIMEOUT,
		RPCRETRYTIMES: DEFAULT_RPCRETRYTIMES,
		RPCRETRYDElAY: DEFAULT_RPCRETRYDELAY,
		VERBOSE:       DEFAULT_VERBOSE,

		DINGOFS_FSID:           DEFAULT_DINGOFS_FSID,
		DINGOFS_MDSADDR:        DEFAULT_DINGOFS_MDSADDR,
		DINGOFS_THREADS:        DINGOFS_DEFAULT_THREADS,
		DINGOFS_BLOCKSIZE:      DINGOFS_DEFAULT_BLOCKSIZE,
		DINGOFS_CHUNKSIZE:      DINGOFS_DEFAULT_CHUNKSIZE,
		DINGOFS_PARTITION_TYPE: DINGOFS_DEFAULT_PARTITION_TYPE,
		DINGOFS_HUMANIZE:       DINGOFS_DEFAULT_HUMANIZE,

		// S3
		DINGOFS_S3_AK:         DINGOFS_DEFAULT_S3_AK,
		DINGOFS_S3_SK:         DINGOFS_DEFAULT_S3_SK,
		DINGOFS_S3_ENDPOINT:   DINGOFS_DEFAULT_ENDPOINT,
		DINGOFS_S3_BUCKETNAME: DINGOFS_DEFAULT_S3_BUCKETNAME,

		//rados
		DINGOFS_RADOS_USERNAME:    DINGOFS_DEFAULT_RADOS_USERNAME,
		DINGOFS_RADOS_KEY:         DINGOFS_DEFAULT_RADOS_KEY,
		DINGOFS_RADOS_MON:         DINGOFS_DEFAULT_RADOS_MON,
		DINGOFS_RADOS_POOLNAME:    DINGOFS_DEFAULT_RADOS_POOLNAME,
		DINGOFS_RADOS_CLUSTERNAME: DINGOFS_DEFAULT_RADOS_CLUSTERNAME,

		//subpath
		DINGOFS_SUBPATH_UID: DINGOFS_DEFAULT_SUBPATH_UID,
		DINGOFS_SUBPATH_GID: DINGOFS_DEFAULT_SUBPATH_GID,

		// cache group
		DINGOFS_CACHE_GROUP:    DINGOFS_DEFAULT_CACHE_GROUP,
		DINGOFS_CACHE_MEMBERID: DINGOFS_DEFAULT_CACHE_MEMBERID,
		DINGOFS_CACHE_WEIGHT:   DINGOFS_DEFAULT_CACHE_WEIGHT,
		DINGOFS_CACHE_IP:       DINGOFS_DEFAULT_CACHE_IP,
		DINGOFS_CACHE_PORT:     DINGOFS_DEFAULT_CACHE_PORT,

		// mds numbers
		DINGOFS_MDS_NUM: DINGOFS_DEFAULT_MDS_NUM,
	}
)

func AddStringFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = ""
	}
	cmd.Flags().String(name, defaultValue.(string), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddStringRequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().String(name, "", usage+color.RedString("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetStringFlag(cmd *cobra.Command, flagName string) string {
	var value string
	if cmd.Flag(flagName).Changed {
		value = cmd.Flag(flagName).Value.String()
	} else {
		value = viper.GetString(FLAG2VIPER[flagName])
	}
	return value
}

func AddBoolFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = false
	}
	cmd.Flags().Bool(name, defaultValue.(bool), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetBoolFlag(cmd *cobra.Command, flagName string) bool {
	var value bool
	flag := cmd.Flag(flagName)
	if flag == nil {
		return false
	}
	if flag.Changed {
		value, _ = cmd.Flags().GetBool(flagName)
	} else {
		value = viper.GetBool(FLAG2VIPER[flagName])
	}
	return value
}

func AddUint64Flag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Uint64(name, defaultValue.(uint64), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetUint64Flag(cmd *cobra.Command, flagName string) uint64 {
	value, err := cmd.Flags().GetUint64(flagName)
	if err != nil {
		return 0
	}
	return value
}

func AddUint32Flag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Uint32(name, defaultValue.(uint32), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func AddUint32RequiredFlag(cmd *cobra.Command, name string, usage string) {
	cmd.Flags().Uint32(name, uint32(0), usage+color.RedString("[required]"))
	cmd.MarkFlagRequired(name)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetUint32Flag(cmd *cobra.Command, flagName string) uint32 {
	var value uint32
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetUint32(flagName)
	} else {
		value = viper.GetUint32(FLAG2VIPER[flagName])
	}
	return value
}

func AddDurationFlag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = 0
	}
	cmd.Flags().Duration(name, defaultValue.(time.Duration), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetDurationFlag(cmd *cobra.Command, flagName string) time.Duration {
	var value time.Duration
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetDuration(flagName)
	} else {
		value = viper.GetDuration(FLAG2VIPER[flagName])
	}
	return value
}

func AddInt32Flag(cmd *cobra.Command, name string, usage string) {
	defaultValue := FLAG2DEFAULT[name]
	if defaultValue == nil {
		defaultValue = int32(0)
	}
	cmd.Flags().Int32(name, defaultValue.(int32), usage)
	err := viper.BindPFlag(FLAG2VIPER[name], cmd.Flags().Lookup(name))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetInt32Flag(cmd *cobra.Command, flagName string) int32 {
	var value int32
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetInt32(flagName)
	} else {
		value = viper.GetInt32(FLAG2VIPER[flagName])
	}
	return value
}

func GetStringSliceFlag(cmd *cobra.Command, flagName string) []string {
	var value []string
	if cmd.Flag(flagName).Changed {
		value, _ = cmd.Flags().GetStringSlice(flagName)
	} else {
		value = viper.GetStringSlice(FLAG2VIPER[flagName])
	}
	return value
}

func AddConfigFileFlag(cmd *cobra.Command) {
	cmd.Flags().StringP("conf", "c", "$HOME/.dingo/dingo.yaml", "Specify configuration file")
}

func AddFormatFlag(cmd *cobra.Command) {
	cmd.Flags().StringP(FORMAT, "", FORMAT_PLAIN, "output format (json|plain)")
	err := viper.BindPFlag(FORMAT, cmd.Flags().Lookup(FORMAT))
	if err != nil {
		cobra.CheckErr(err)
	}
}

func GetConfigFile(cmd *cobra.Command) string {
	var value string
	if cmd.Flag("conf").Changed {
		value = cmd.Flag("conf").Value.String()
	} else {
		// using $HOME/.dingo/dingo.yaml as default configuration file path
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		value = fmt.Sprintf("%s/.dingo/dingo.yaml", home)
	}

	return value
}

func ReadCommandConfig(cmd *cobra.Command) {
	// configure file priority
	// command line (--conf dingo.yaml) > environment variables(CONF=/opt/dingo.yaml) > default (~/.dingo/dingo.yaml)
	var value string
	if cmd.Flag("conf").Changed {
		value = cmd.Flag("conf").Value.String()
	} else {
		value = os.Getenv("CONF") //check environment variable
	}

	if value != "" {
		viper.SetConfigFile(value)
	} else { // use default
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		viper.AddConfigPath(home + "/.dingo")
		viper.SetConfigType("yaml")
		viper.SetConfigName("dingo")
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Printf("config file name: %v", viper.ConfigFileUsed())
			cobra.CheckErr(err)
		}
	}
}

func isIpAddrValid(addr string) bool {
	matched, err := regexp.MatchString(IP_PORT_REGEX, addr)
	if err != nil || !matched {
		return false
	}

	return true
}

// get mdsaddr slice
func GetMDSAddrSlice(cmd *cobra.Command) ([]string, error) {
	addrsStr := GetStringFlag(cmd, DINGOFS_MDSADDR)

	addrslice := strings.Split(addrsStr, ",")
	for _, addr := range addrslice {
		if !isIpAddrValid(addr) {
			return nil, fmt.Errorf("invalid address: %s", addr)
		}
	}

	return addrslice, nil
}

// check fsid and fsname
func GetFsInfoFlagValue(cmd *cobra.Command) (uint32, string, error) {
	var fsId uint32
	var fsName string
	if !cmd.Flag(DINGOFS_FSNAME).Changed && !cmd.Flag(DINGOFS_FSID).Changed {
		return 0, "", fmt.Errorf("fsname or fsid is required")
	}
	if cmd.Flag(DINGOFS_FSID).Changed {
		fsId = GetUint32Flag(cmd, DINGOFS_FSID)
	} else {
		fsName = GetStringFlag(cmd, DINGOFS_FSNAME)
	}
	if fsId == 0 && len(fsName) == 0 {
		return 0, "", fmt.Errorf("fsname or fsid is invalid")
	}

	return fsId, fsName, nil
}
