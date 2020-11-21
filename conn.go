/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/6
   Description :
-------------------------------------------------
*/

package zconn

import (
	"sync"
)

// conn
type Conn struct {
	connType    ConnType
	instance    interface{} // 实例
	config      interface{} // 配置
	isConnected bool        // 状态

	mx sync.RWMutex
}

func newConn(conntype ConnType, configPtr interface{}) *Conn {
	return &Conn{
		connType:    conntype,
		instance:    nil,
		config:      configPtr,
		isConnected: false,
	}
}

// 类型
func (m *Conn) Type() ConnType {
	return m.connType
}

// 实例
func (m *Conn) Instance() interface{} {
	m.mx.RLock()
	defer m.mx.RUnlock()

	if m.isConnected {
		return m.instance
	}
	return nil
}

// 配置
func (m *Conn) Config() interface{} {
	return m.config
}

// 是否已连接
func (m *Conn) IsConnect() bool {
	m.mx.RLock()
	defer m.mx.RUnlock()
	return m.isConnected
}

// 连接
func (m *Conn) Connect() error {
	if m.IsConnect() {
		return nil
	}

	m.mx.Lock()
	defer m.mx.Unlock()

	if m.isConnected {
		return nil
	}

	instance, err := mustGetConnector(m.connType).Connect(m.config)
	if err != nil {
		return err
	}

	m.instance = instance
	m.isConnected = true
	return nil
}

// 关闭连接
func (m *Conn) Close() error {
	if !m.IsConnect() {
		return nil
	}

	m.mx.Lock()
	defer m.mx.Unlock()

	if !m.isConnected {
		return nil
	}

	m.isConnected = false
	m.instance = nil

	return mustGetConnector(m.connType).Close(m.instance)
}
