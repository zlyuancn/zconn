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
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisConnector struct{}

var _ IConnector = (*redisConnector)(nil)

type RedisConfig struct {
	Address      []string // [host1:port1, host2:port2]
	Password     string
	DB           int
	IsCluster    bool
	PoolSize     int
	ReadTimeout  int64 // 超时(毫秒
	WriteTimeout int64 // 超时(毫秒
	DialTimeout  int64 // 超时(毫秒
	Ping         bool  // 开始连接时是否ping确认连接情况
}

func (*redisConnector) NewEmptyConfig() interface{} {
	return new(RedisConfig)
}

func (*redisConnector) Connect(config interface{}) (instance interface{}, err error) {
	conf := config.(*RedisConfig)
	if len(conf.Address) == 0 {
		return nil, errors.New("请检查redis配置的address")
	}

	var c redis.UniversalClient
	if conf.IsCluster {
		c = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        conf.Address,
			Password:     conf.Password,
			PoolSize:     conf.PoolSize,
			ReadTimeout:  time.Duration(conf.ReadTimeout * 1e6),
			WriteTimeout: time.Duration(conf.WriteTimeout * 1e6),
			DialTimeout:  time.Duration(conf.DialTimeout * 1e6),
		})
	} else {
		c = redis.NewClient(&redis.Options{
			Addr:         conf.Address[0],
			Password:     conf.Password,
			DB:           conf.DB,
			PoolSize:     conf.PoolSize,
			ReadTimeout:  time.Duration(conf.ReadTimeout * 1e6),
			WriteTimeout: time.Duration(conf.WriteTimeout * 1e6),
			DialTimeout:  time.Duration(conf.DialTimeout * 1e6),
		})
	}

	if conf.Ping {
		if _, err := c.Ping(context.Background()).Result(); err != nil {
			return nil, fmt.Errorf("ping失败: %s", err)
		}
	}
	return c, nil
}

func (*redisConnector) Close(instance interface{}) error {
	c := instance.(redis.UniversalClient)
	return c.Close()
}

func AddRedis(config interface{}, conn_name ...string) {
	AddConfig(Redis, config, conn_name...)
}

func GetRedis(conn_name ...string) (redis.UniversalClient, error) {
	c, ok := GetConn(Redis, conn_name...)
	if !ok {
		return nil, ErrNoConn
	}

	if !c.IsConnect() {
		return nil, ErrConnNotConnected
	}

	return c.Instance().(redis.UniversalClient), nil
}

func MustRedis(conn_name ...string) redis.UniversalClient {
	c, err := GetRedis(conn_name...)
	panicOnErr(err)
	return c
}
