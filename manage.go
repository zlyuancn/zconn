/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/6
   Description :
-------------------------------------------------
*/

package zconn

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	ErrNoConn           = errors.New("没有这个conn")
	ErrConnNotConnected = errors.New("conn未连接")
)

type Conns map[string]*Conn

const DefaultConnName = "default"

type Manager struct {
	storage map[ConnType]Conns
	mx      sync.RWMutex
	opts    *Options
}

// 创建一个管理器
func NewManager(opts ...Option) *Manager {
	manager := &Manager{
		storage: make(map[ConnType]Conns),
		opts:    newOptions(),
	}

	for _, o := range opts {
		o(manager.opts)
	}

	return manager
}

// 添加配置, 同一个conn类型中重复的conn名会被替换掉
func (m *Manager) AddConfig(conntype ConnType, config interface{}, conn_name ...string) {
	name := makeConnName(conn_name...)

	var oldConn *Conn

	m.mx.Lock()

	conns, ok := m.storage[conntype]
	if !ok {
		conns = make(Conns)
		m.storage[conntype] = conns
	}

	oldConn = conns[name]

	// 设置新的配置
	conns[name] = newConn(conntype, makeConfigPtr(config))

	m.mx.Unlock()

	// 关闭之前的连接
	if oldConn != nil {
		_ = oldConn.Close()
	}
}

// 移除, 移除后会自动关闭连接
func (m *Manager) Remove(conntype ConnType, conn_name ...string) {
	name := makeConnName(conn_name...)

	var oldConns []*Conn

	m.mx.Lock()

	if conns, ok := m.storage[conntype]; ok {
		if conn, ok := conns[name]; ok {
			oldConns = append(oldConns, conn)
			delete(conns, name)
		}
	}

	m.mx.Unlock()

	// 关闭之前的连接
	for _, conn := range oldConns {
		_ = conn.Close()
	}
}

// 连接所有
func (m *Manager) ConnectAll() error {
	m.mx.Lock()
	defer m.mx.Unlock()
	for _, conns := range m.storage {
		for conn_name, conn := range conns {
			if err := conn.Connect(); err != nil {
				return fmt.Errorf("[%s.%s], %s", conn.Type(), conn_name, err)
			}
		}
	}
	return nil
}

// 关闭所有连接
func (m *Manager) CloseAll() {
	m.mx.Lock()
	defer m.mx.Unlock()
	for _, conns := range m.storage {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}
	m.storage = make(map[ConnType]Conns)
}

// 获取Conn
func (m *Manager) GetConn(conntype ConnType, conn_name ...string) (conn *Conn, ok bool) {
	m.mx.RLock()
	var conns Conns
	if conns, ok = m.storage[conntype]; ok {
		conn, ok = conns[makeConnName(conn_name...)]
	}
	m.mx.RUnlock()

	if ok && m.opts.GetAutoConnect {
		_ = conn.Connect()
	}
	return
}

func makeConnName(conn_name ...string) string {
	if len(conn_name) > 0 {
		return strings.ToLower(conn_name[0])
	}
	return DefaultConnName
}

func makeConfigPtr(config interface{}) interface{} {
	config_value := reflect.ValueOf(config)
	if config_value.Kind() == reflect.Ptr {
		return config
	}

	new_v := reflect.New(config_value.Type())
	new_v.Elem().Set(config_value)
	return new_v.Interface()
}
