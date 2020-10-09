/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/6
   Description :
-------------------------------------------------
*/

package zconn

import (
	"fmt"

	"github.com/spf13/viper"
)

var defaultManager = NewManager()

// 添加配置, 同一个conn类型中重复的conn名会被替换掉
func AddConfig(conntype ConnType, config interface{}, conn_name ...string) {
	defaultManager.AddConfig(conntype, config, conn_name...)
}

// 移除, 移除之前会关闭连接
func Remove(conntype ConnType, conn_name ...string) {
	defaultManager.Remove(conntype, conn_name...)
}

// 连接所有
func ConnectAll() error {
	return defaultManager.ConnectAll()
}

// 关闭所有连接
func CloseAll() {
	defaultManager.CloseAll()
}

// 获取Conn
func GetConn(conntype ConnType, conn_name ...string) (conn *Conn, ok bool) {
	return defaultManager.GetConn(conntype, conn_name...)
}

// 添加配置文件, 支持 json, ini, toml, yaml等
func AddFile(file string, filetype ...string) error {
	return defaultManager.AddFile(file, filetype...)
}

// 添加viper树
func AddViperTree(tree *viper.Viper) error {
	return defaultManager.AddViperTree(tree)
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func panicOnErrf(err error, format string, msg ...interface{}) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", fmt.Sprintf(format, msg...), err))
	}
}
