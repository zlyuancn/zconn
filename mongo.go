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
    "time"

    "github.com/zlyuancn/zmongo"
)

type mongoConnector struct{}

var _ IConnector = (*mongoConnector)(nil)

type MongoConfig struct {
    Address       []string // 连接地址, 如: 127.0.0.1:27017
    DBName        string   // 库名
    UserName      string   // 用户名
    Password      string   // 密码
    PoolSize      uint64   // 连接池的数量
    DialTimeout   int64    // 连接超时(毫秒
    DoTimeout     int64    // 操作超时(毫秒
    SocketTimeout int64    // Socket超时
    Ping          bool     // 开始连接时是否ping确认连接情况
}

func (*mongoConnector) NewEmptyConfig() interface{} {
    return new(MongoConfig)
}

func (*mongoConnector) Connect(config interface{}) (instance interface{}, err error) {
    conf := config.(*MongoConfig)
    c, err := zmongo.New(&zmongo.Config{
        Address:       conf.Address,
        DBName:        conf.DBName,
        UserName:      conf.UserName,
        Password:      conf.Password,
        PoolSize:      conf.PoolSize,
        DialTimeout:   time.Duration(conf.DialTimeout * 1e6),
        DoTimeout:     time.Duration(conf.DoTimeout * 1e6),
        SocketTimeout: time.Duration(conf.SocketTimeout * 1e6),
    })
    if err != nil {
        return nil, fmt.Errorf("连接失败: %s", err)
    }

    if conf.Ping {
        if err = c.Ping(nil); err != nil {
            return nil, fmt.Errorf("ping失败: %s", err)
        }
    }

    return c, nil
}

func (*mongoConnector) Close(instance interface{}) error {
    c := instance.(*zmongo.Client)
    return c.Close()
}

func AddMongo(config interface{}, conn_name ...string) {
    AddConfig(Mongo, config, conn_name...)
}

func GetMongo(conn_name ...string) (*zmongo.Client, error) {
    c, ok := GetConn(Mongo, conn_name...)
    if !ok {
        return nil, ErrNoConn
    }

    if !c.IsConnect() {
        return nil, ErrConnNotConnected
    }

    return c.Instance().(*zmongo.Client), nil
}

func MustMongo(conn_name ...string) *zmongo.Client {
    c, err := GetMongo(conn_name...)
    panicOnErr(err)
    return c
}
