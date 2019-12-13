package service

import (
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
)

var _ver = "v1.11.5"

func Init(brokers []string, opts []model.Option) error {
	dis := model.MakeDispatcher(opts)
	core.InitLogger(dis.LogLevel, core.ProjectName)
	core.InitConfig(brokers, dis.ToCoreConfig())
	core.InitSaramaConfig(dis.ToSaramaConfig())
	core.Logger.Infof("Dispatcher %v started with brokers: %v, defaultGroupID: %v, kafkaConfig: %+v", _ver, core.Config.Brokers, core.Config.DefaultGroupID, core.Config.KafkaConfig)
	return nil
}
