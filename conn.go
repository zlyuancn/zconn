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
	connType ConnType
	config   interface{} // 配置

	mx sync.RWMutex
	wg *connWaitGroup
}

type connWaitGroup struct {
	instance interface{}
	e        error
	wg       sync.WaitGroup
}

func newConn(conntype ConnType, configPtr interface{}) *Conn {
	return &Conn{
		connType: conntype,
		config:   configPtr,
	}
}

// 类型
func (m *Conn) Type() ConnType {
	return m.connType
}

// 实例
func (m *Conn) Instance() interface{} {
	m.mx.RLock()
	wg := m.wg
	m.mx.RUnlock()

	if wg == nil {
		return nil
	}

	wg.wg.Wait()
	return wg.instance
}

// 配置
func (m *Conn) Config() interface{} {
	return m.config
}

// 是否已连接
func (m *Conn) IsConnect() bool {
	m.mx.RLock()
	wg := m.wg
	m.mx.RUnlock()

	if wg == nil {
		return false
	}

	wg.wg.Wait()
	return wg.e == nil
}

// 连接
func (m *Conn) Connect() error {
	m.mx.RLock()
	wg := m.wg
	m.mx.RUnlock()

	if wg != nil {
		wg.wg.Wait()
		return wg.e
	}

	m.mx.Lock()
	wg = m.wg
	if wg != nil {
		m.mx.Unlock()
		wg.wg.Wait()
		return wg.e
	}

	wg = new(connWaitGroup)
	wg.wg.Add(1)
	defer wg.wg.Done()
	m.wg = wg
	m.mx.Unlock()

	instance, err := mustGetConnector(m.connType).Connect(m.config)
	if err != nil {
		wg.e = err
		m.mx.Lock()
		m.wg = nil
		m.mx.Unlock()
		return err
	}

	wg.instance = instance
	return nil
}

// 关闭连接
func (m *Conn) Close() error {
	m.mx.RLock()
	wg := m.wg
	m.mx.RUnlock()

	if wg == nil {
		return nil
	}
	wg.wg.Wait()
	if wg.e != nil { // 没有连上
		return nil
	}

	m.mx.Lock()
	wg = m.wg
	if wg == nil {
		m.mx.Unlock()
		return nil
	}
	m.wg = nil
	m.mx.Unlock()

	wg.wg.Wait()
	if wg.e != nil { // 没有连上
		return nil
	}

	return mustGetConnector(m.connType).Close(wg.instance)
}
