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

    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/mssql"
    _ "github.com/jinzhu/gorm/dialects/postgres"
    _ "github.com/jinzhu/gorm/dialects/sqlite"
)

type GormConfig struct {
    Driver          string // 驱动
    Source          string // 连接源
    MaxIdleConns    int    // 最大空闲连接数
    MaxOpenConns    int    // 最大连接池个数
    ConnMaxLifetime int    // 最大续航时间(毫秒, 0表示无限
    Ping            bool   // 开始连接时是否ping确认连接情况
}

var _ IConnector = (*gormConnector)(nil)

type gormConnector struct{}

func (*gormConnector) NewEmptyConfig() interface{} {
    return new(GormConfig)
}

func (*gormConnector) Connect(config interface{}) (instance interface{}, err error) {
    conf := config.(*GormConfig)
    c, err := gorm.Open(conf.Driver, conf.Source)
    if err != nil {
        return nil, err
    }

    db := c.DB()
    db.SetMaxIdleConns(conf.MaxIdleConns)
    db.SetMaxOpenConns(conf.MaxOpenConns)
    db.SetConnMaxLifetime(time.Duration(conf.ConnMaxLifetime) * time.Millisecond)

    if conf.Ping {
        if err = db.Ping(); err != nil {
            return nil, err
        }
    }

    return c, nil
}

func (*gormConnector) Close(instance interface{}) error {
    c := instance.(*gorm.DB)
    return c.Close()
}

func AddGorm(config interface{}, conn_name ...string) {
    AddConfig(Gorm, config, conn_name...)
}

func GetGorm(conn_name ...string) (*gorm.DB, error) {
    c, ok := GetConn(Gorm, conn_name...)
    if !ok {
        return nil, ErrNoConn
    }

    if !c.IsConnect() {
        return nil, ErrConnNotConnected
    }

    return c.Instance().(*gorm.DB), nil
}

func MustGorm(conn_name ...string) *gorm.DB {
    c, err := GetGorm(conn_name...)
    panicOnErr(err)
    return c
}
