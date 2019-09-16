package service

import (
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
)

func Init(brokers []string, opts []model.Option) error {
	dis := model.MakeDispatcher(opts)
	core.InitLogger(dis.LogLevel, core.ProjectName)
	core.InitConfig(brokers, dis.ToCoreConfig())
	core.InitSaramaConfig(dis.ToSaramaConfig())
	core.Logger.Infof("Dispatcher started with brokers: %v, groupID: %v, kafka: %+v", core.Config.Brokers, core.Config.DefaultGroupID, core.Config.KafkaConfig)
	return nil
}
