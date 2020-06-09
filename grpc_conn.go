/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/9
   Description :
-------------------------------------------------
*/

package zconn

import (
    grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
    "github.com/opentracing/opentracing-go"
    "google.golang.org/grpc"
)

type grpcConnConnector struct{}

var _ IConnector = (*grpcConnConnector)(nil)

type GrpcConnConfig struct {
    TracerEnable bool   // 链路追踪
    Addr         string // 连接地址
    Insecure     bool   // 不安全的
}

func (*grpcConnConnector) NewEmptyConfig() interface{} {
    return new(GrpcConnConfig)
}

func (*grpcConnConnector) Connect(config interface{}) (instance interface{}, err error) {
    conf := config.(*GrpcConnConfig)

    var opts []grpc.DialOption
    if conf.Insecure {
        opts = append(opts, grpc.WithInsecure())
    }
    if conf.TracerEnable {
        tracer := opentracing.GlobalTracer()
        wrap_tracer := grpc_opentracing.WithTracer(tracer)
        opts = append(opts, grpc.WithUnaryInterceptor(grpc_opentracing.UnaryClientInterceptor(wrap_tracer)))
    }

    conn, err := grpc.Dial(conf.Addr, opts...)
    return conn, err
}

func (*grpcConnConnector) Close(instance interface{}) error {
    c := instance.(*grpc.ClientConn)
    return c.Close()
}

func AddGrpcConn(config interface{}, conn_name ...string) {
    AddConfig(GrpcConn, config, conn_name...)
}

func GetGrpcConn(conn_name ...string) (*grpc.ClientConn, error) {
    c, ok := GetConn(GrpcConn, conn_name...)
    if !ok {
        return nil, ErrNoConn
    }

    if !c.IsConnect() {
        return nil, ErrConnNotConnected
    }

    return c.Instance().(*grpc.ClientConn), nil
}

func MustGrpcConn(conn_name ...string) *grpc.ClientConn {
    c, err := GetGrpcConn(conn_name...)
    panicOnErr(err)
    return c
}
