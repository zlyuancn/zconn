/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/9
   Description :
-------------------------------------------------
*/

package zconn

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

type etcd3Connector struct{}

var _ IConnector = (*etcd3Connector)(nil)

type Etcd3Config struct {
	Address     []string
	UserName    string // 用户名
	Password    string // 密码
	DialTimeout int64  // 连接超时(毫秒
	Ping        bool   // 开始连接时是否ping确认连接情况
}

func (*etcd3Connector) NewEmptyConfig() interface{} {
	return new(Etcd3Config)
}

func (*etcd3Connector) Connect(config interface{}) (interface{}, error) {
	conf := config.(*Etcd3Config)
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   conf.Address,
		Username:    conf.UserName,
		Password:    conf.Password,
		DialTimeout: time.Duration(conf.DialTimeout * 1e6),
	})
	if err != nil {
		return nil, fmt.Errorf("连接失败: %s", err)
	}

	if conf.Ping {
		if _, err = c.Get(context.Background(), "/"); err != nil {
			return nil, fmt.Errorf("ping失败: %s", err)
		}
	}

	return c, nil
}

func (*etcd3Connector) Close(instance interface{}) error {
	c := instance.(*clientv3.Client)
	return c.Close()
}

func AddEtcd3(config interface{}, conn_name ...string) {
	AddConfig(Etcd3, config, conn_name...)
}

func GetEtcd3(conn_name ...string) (*clientv3.Client, error) {
	c, ok := GetConn(Etcd3, conn_name...)
	if !ok {
		return nil, ErrNoConn
	}

	if !c.IsConnect() {
		return nil, ErrConnNotConnected
	}

	return c.Instance().(*clientv3.Client), nil
}

func MustEtcd3(conn_name ...string) *clientv3.Client {
	c, err := GetEtcd3(conn_name...)
	panicOnErr(err)
	return c
}
