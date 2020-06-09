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
)

// 连接器
type IConnector interface {
    // 创建一个空的配置结构
    NewEmptyConfig() interface{}
    // 根据配置结构进行连接, 返回一个连接实例
    //
    // 注意, conf 一定是带指针的
    Connect(config interface{}) (instance interface{}, err error)
    // 关闭连接实例
    Close(instance interface{}) error
}

type ConnType string

const (
    // 在这里定义连接器类型
    Xorm ConnType = "xorm"
    Gorm          = "gorm"
    Es7           = "es7"
)

var connectorRegistry map[ConnType]IConnector

func init() {
    connectorRegistry = make(map[ConnType]IConnector)

    // 在这里注册连接器
    RegistryConnector(Xorm, new(xormConnector))
    RegistryConnector(Gorm, new(gormConnector))
    RegistryConnector(Es7, new(es7Connector))
}

// 注册自定义连接器
func RegistryConnector(conntype ConnType, connector IConnector) {
    connectorRegistry[conntype] = connector
}

func mustGetConnector(conntype ConnType) IConnector {
    if connector, ok := connectorRegistry[conntype]; ok {
        return connector
    }
    panic(fmt.Errorf("未注册的conn类型<%v>", conntype))
}
