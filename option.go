/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/10/9
   Description :
-------------------------------------------------
*/

package zconn

type Options struct {
	GetAutoConnect bool
}

type Option func(o *Options)

// 获取时自动连接
func WithGetAutoConnect(auto ...bool) Option {
	return func(o *Options) {
		o.GetAutoConnect = len(auto) > 0 && auto[0]
	}
}
