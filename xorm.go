/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/7
   Description :
-------------------------------------------------
*/

package zconn

import (
    "time"

    _ "github.com/denisenkom/go-mssqldb"
    _ "github.com/go-sql-driver/mysql"
    _ "github.com/lib/pq"
    _ "github.com/mattn/go-sqlite3"

    "xorm.io/xorm"
)

type XormConfig struct {
    Driver          string // 驱动
    Source          string // 连接源
    MaxIdleConns    int    // 最大空闲连接数
    MaxOpenConns    int    // 最大连接池个数
    ConnMaxLifetime int    // 最大续航时间(毫秒, 0表示无限
    Ping            bool   // 开始连接时是否ping确认连接情况
}

var _ IConnector = (*xormConnector)(nil)

type xormConnector struct{}

func (x *xormConnector) NewEmptyConfig() interface{} {
    return new(XormConfig)
}

func (x *xormConnector) Connect(config interface{}) (instance interface{}, err error) {
    conf := config.(*XormConfig)
    engine, err := xorm.NewEngine(conf.Driver, conf.Source)
    if err != nil {
        return nil, err
    }

    engine.SetMaxIdleConns(conf.MaxIdleConns)
    engine.SetMaxOpenConns(conf.MaxOpenConns)
    engine.SetConnMaxLifetime(time.Duration(conf.ConnMaxLifetime) * time.Millisecond)
    if conf.Ping {
        err = engine.Ping()
        if err != nil {
            return nil, err
        }
    }
    return engine, nil
}

func (x *xormConnector) Close(instance interface{}) error {
    engine := instance.(*xorm.Engine)
    return engine.Close()
}

func AddXorm(config interface{}, conn_name ...string) {
    AddConfig(Xorm, config, conn_name...)
}

func GetXorm(conn_name ...string) (*xorm.Engine, error) {
    c, ok := GetConn(Xorm, conn_name...)
    if !ok {
        return nil, ErrNoConn
    }

    if !c.IsConnect() {
        return nil, ErrConnNotConnected
    }

    return c.Instance().(*xorm.Engine), nil
}

func MustXorm(conn_name ...string) *xorm.Engine {
    c, err := GetXorm(conn_name...)
    panicOnErr(err)
    return c
}
