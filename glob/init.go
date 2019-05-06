package glob

import (
	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

var (
	Config       config
	SaramaConfig sarama.Config
)

const ProjName = "dispatcher"

func init() {
	loadConf()
	tracer.Infof(ProjName, " Kafka brokers: %v", Config.Brokers)
}
