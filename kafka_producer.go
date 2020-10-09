/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/9
   Description :
-------------------------------------------------
*/

package zconn

import (
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

type kafkaProducerConnector struct{}

var _ IConnector = (*kafkaProducerConnector)(nil)

type KafkaProducerConfig struct {
	Address []string
	Async   bool // 是否异步
}

func (*kafkaProducerConnector) NewEmptyConfig() interface{} {
	return new(KafkaProducerConfig)
}

func (*kafkaProducerConnector) Connect(config interface{}) (instance interface{}, err error) {
	conf := config.(KafkaProducerConfig)
	kconf := sarama.NewConfig()
	kconf.Producer.Return.Successes = true // producer把消息发给kafka之后不会等待结果返回
	kconf.Producer.Return.Errors = true    // 如果启用了该选项，未交付的消息将在Errors通道上返回，包括error(默认启用)。

	if conf.Async {
		producer, err := sarama.NewAsyncProducer(conf.Address, kconf)
		if err != nil {
			return nil, fmt.Errorf("连接失败: %s", err)
		}
		return producer, nil
	}

	producer, err := sarama.NewSyncProducer(conf.Address, kconf)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %s", err)
	}
	return producer, nil
}

func (*kafkaProducerConnector) Close(instance interface{}) error {
	if c, ok := instance.(sarama.SyncProducer); ok {
		return c.Close()
	}
	if c, ok := instance.(sarama.AsyncProducer); ok {
		return c.Close()
	}
	panic(errors.New("非sarama.SyncProducer或sarama.AsyncProducer结构"))
}

func AddKafkaProducer(config interface{}, conn_name ...string) {
	AddConfig(KafkaProducer, config, conn_name...)
}

func GetKafkaProducer(conn_name ...string) (sarama.SyncProducer, error) {
	c, ok := GetConn(KafkaProducer, conn_name...)
	if !ok {
		return nil, ErrNoConn
	}

	if !c.IsConnect() {
		return nil, ErrConnNotConnected
	}

	if p, ok := c.Instance().(sarama.SyncProducer); ok {
		return p, nil
	}

	panic(fmt.Errorf("非sarama.SyncProducer结构: %T", c.Instance()))
}

func MustKafkaProducer(conn_name ...string) sarama.SyncProducer {
	c, err := GetKafkaProducer(conn_name...)
	panicOnErr(err)
	return c
}

func GetKafkaAsyncProducer(conn_name ...string) (sarama.AsyncProducer, error) {
	c, ok := GetConn(KafkaProducer, conn_name...)
	if !ok {
		return nil, ErrNoConn
	}

	if !c.IsConnect() {
		return nil, ErrConnNotConnected
	}

	if p, ok := c.Instance().(sarama.AsyncProducer); ok {
		return p, nil
	}

	panic(fmt.Errorf("非sarama.AsyncProducer结构: %T", c.Instance()))
}

func MustKafkaAsyncProducer(conn_name ...string) sarama.AsyncProducer {
	c, err := GetKafkaAsyncProducer(conn_name...)
	panicOnErr(err)
	return c
}
