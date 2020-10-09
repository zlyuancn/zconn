/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/6
   Description :
-------------------------------------------------
*/

package zconn

// conn
type Conn struct {
	conntype   ConnType
	instance   interface{} // 实例
	config     interface{} // 配置
	is_connect bool        // 是否已连接
}

// 类型
func (m *Conn) Type() ConnType {
	return m.conntype
}

// 实例
func (m *Conn) Instance() interface{} {
	return m.instance
}

// 配置
func (m *Conn) Config() interface{} {
	return m.config
}

// 是否已连接
func (m *Conn) IsConnect() bool {
	return m.is_connect
}

// 连接
func (m *Conn) Connect() error {
	if m.is_connect {
		return nil
	}

	instance, err := mustGetConnector(m.conntype).Connect(m.config)
	if err != nil {
		return err
	}

	m.instance = instance
	m.is_connect = true
	return nil
}

// 关闭连接
func (m *Conn) Close() error {
	if !m.is_connect {
		return nil
	}

	m.is_connect = false
	return mustGetConnector(m.conntype).Close(m.instance)
}
