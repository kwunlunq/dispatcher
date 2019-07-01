package core

import (
	"log"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/v2/model"
)

const ProjName = "dispatcher"

func Init(brokers []string, groupID string, opts ...model.Option) error {
	dis := model.MakeDispatcher(opts)

	initConfig(brokers, groupID, dis)
	initSaramaConfig(dis)

	initLogger(dis.LogLevel, ProjName)

	Logger.Infof("Dispatcher started with brokers: %v, group-id: %v", Config.Brokers, Config.GroupID)
	Config.isInitialized = true
	return nil
}

func IsInitialized() bool {
	if !Config.isInitialized {
		log.Println("dispatcher hasn't initialized!")
	}
	return Config.isInitialized
}
