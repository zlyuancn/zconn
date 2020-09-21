/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/6/6
   Description :
-------------------------------------------------
*/

package zconn

import (
    "fmt"

    "github.com/mitchellh/mapstructure"
    "github.com/spf13/viper"
)

const ConfigShardName = "zconn"

type viperConfig map[ConnType]map[string]interface{}

// 添加配置文件, 支持 json, ini, toml, yaml等
//
// toml示例:
//   [zconn.es7.default]
//   address=['http://127.0.0.1:9200']
//   username='your_user'
//   password='your_pwd'
//
func (m *Manager) AddFile(file string, filetype ...string) error {
    v := viper.New()
    v.SetConfigFile(file)
    if len(filetype) > 0 {
        v.SetConfigType(filetype[0])
    }
    if err := v.ReadInConfig(); err != nil {
        return err
    }
    return m.AddViperTree(v)
}

// 添加viper树
func (m *Manager) AddViperTree(tree *viper.Viper) error {
    vconf := make(viperConfig)
    err := tree.UnmarshalKey(ConfigShardName, &vconf)
    if err != nil {
        return fmt.Errorf("tree解析失败: %s", err.Error())
    }

    for conn_type, conns := range vconf {
        for name, raw_conf := range conns {
            conf := mustGetConnector(conn_type).NewEmptyConfig()
            conf_ptr := makeConfigPtr(conf)
            err = decodeRawConfig(raw_conf, conf_ptr)
            if err != nil {
                return fmt.Errorf("配置解析失败: %s.%s: %s", conn_type, name, err.Error())
            }

            m.AddConfig(conn_type, conf_ptr, name)
        }
    }

    return nil
}

func decodeRawConfig(raw interface{}, config interface{}) error {
    c := &mapstructure.DecoderConfig{
        Result:           config,
        WeaklyTypedInput: true,
        DecodeHook: mapstructure.ComposeDecodeHookFunc(
            mapstructure.StringToTimeDurationHookFunc(),
            mapstructure.StringToSliceHookFunc(","),
        ),
    }
    decoder, err := mapstructure.NewDecoder(c)
    if err != nil {
        return err
    }
    return decoder.Decode(raw)
}
