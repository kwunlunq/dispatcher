package glob

import (
	"github.com/Shopify/sarama"
)

var (
	Config       config
	SaramaConfig sarama.Config
)

const ProjName = "dispatcher"

func init() {
	loadConf()
	initLogger(Config.LogLevel)
	Logger.Infof(" Kafka brokers: %v", Config.Brokers)
}
