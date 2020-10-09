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

	"gopkg.in/olivere/elastic.v6"
)

type es6Connector struct{}

var _ IConnector = (*es6Connector)(nil)

type Es6Config struct {
	Address       []string // 地址
	UserName      string   // 用户名
	Password      string   // 密码
	DialTimeout   int64    // 连接超时(毫秒
	Sniff         bool     // 嗅探器
	Healthcheck   *bool    // 心跳检查(默认true
	Retry         int      // 重试次数
	RetryInterval int      // 重试间隔(毫秒)
	GZip          bool     // 启用gzip压缩
}

func (*es6Connector) NewEmptyConfig() interface{} {
	return new(Es6Config)
}

func (*es6Connector) Connect(config interface{}) (interface{}, error) {
	conf := config.(*Es6Config)
	if conf.Healthcheck == nil {
		check := true
		conf.Healthcheck = &check
	}

	opts := []elastic.ClientOptionFunc{
		elastic.SetURL(conf.Address...),
		elastic.SetSniff(conf.Sniff),
		elastic.SetHealthcheck(*conf.Healthcheck),
		elastic.SetGzip(conf.GZip),
	}
	if conf.UserName != "" || conf.Password != "" {
		opts = append(opts, elastic.SetBasicAuth(conf.UserName, conf.Password))
	}
	if conf.Retry > 0 {
		ticks := make([]int, conf.Retry)
		for i := 0; i < conf.Retry; i++ {
			ticks[i] = conf.RetryInterval
		}
		opts = append(opts, elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewSimpleBackoff(ticks...))))
	}

	ctx := context.Background()
	if conf.DialTimeout > 0 {
		ctx, _ = context.WithTimeout(ctx, time.Duration(conf.DialTimeout*1e6))
	}

	c, err := elastic.DialContext(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %s", err)
	}

	return c, nil
}
func (*es6Connector) Close(instance interface{}) error {
	c := instance.(*elastic.Client)
	c.Stop()
	return nil
}

func AddEs6(config interface{}, conn_name ...string) {
	AddConfig(Es6, config, conn_name...)
}

func GetES6(conn_name ...string) (*elastic.Client, error) {
	c, ok := GetConn(Es6, conn_name...)
	if !ok {
		return nil, ErrNoConn
	}

	if !c.IsConnect() {
		return nil, ErrConnNotConnected
	}

	return c.Instance().(*elastic.Client), nil
}

func MustEs6(conn_name ...string) *elastic.Client {
	c, err := GetES6(conn_name...)
	panicOnErr(err)
	return c
}
