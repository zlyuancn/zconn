/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/9
   Description :
-------------------------------------------------
*/

package zconn

import (
    "fmt"

    "github.com/seefan/gossdb"
    ssdbconf "github.com/seefan/gossdb/conf"
)

type ssdbConnector struct{}

var _ IConnector = (*ssdbConnector)(nil)

type SsdbConfig struct {
    Host             string
    Port             int
    Password         string
    GetClientTimeout int  // 获取客户端超时(毫秒)
    MinPoolSize      int  // 最小连接池数
    MaxPoolSize      int  // 最大连接池个数
    RetryEnabled     bool // 是否启用重试，设置为true时，如果请求失败会再重试一次
}

func (*ssdbConnector) NewEmptyConfig() interface{} {
    return new(SsdbConfig)
}

func (*ssdbConnector) Connect(config interface{}) (instance interface{}, err error) {
    conf := config.(*SsdbConfig)
    pool, err := gossdb.NewPool(&ssdbconf.Config{
        Host:             conf.Host,
        Port:             conf.Port,
        Password:         conf.Password,
        GetClientTimeout: conf.GetClientTimeout / 1e3,
        MinPoolSize:      conf.MinPoolSize,
        MaxPoolSize:      conf.MaxPoolSize,
        RetryEnabled:     conf.RetryEnabled,
    })
    if err != nil {
        return nil, fmt.Errorf("连接失败: %s", err)
    }

    return pool, nil
}

func (*ssdbConnector) Close(instance interface{}) error {
    c := instance.(*gossdb.Connectors)
    c.Close()
    return nil
}

func AddSsdb(config interface{}, conn_name ...string) {
    AddConfig(Ssdb, config, conn_name...)
}

func GetSsdb(conn_name ...string) (*gossdb.Connectors, error) {
    c, ok := GetConn(Ssdb, conn_name...)
    if !ok {
        return nil, ErrNoConn
    }

    if !c.IsConnect() {
        return nil, ErrConnNotConnected
    }

    return c.Instance().(*gossdb.Connectors), nil
}

func MustSsdb(conn_name ...string) *gossdb.Connectors {
    c, err := GetSsdb(conn_name...)
    panicOnErr(err)
    return c
}
