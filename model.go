package dispatcher

import (
	"context"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"time"
)

type KafkaConfig core.KafkaConfig

// Producer
type Message struct {
	TaskID               string // 任務ID, 訊息編號
	Topic                string
	Key                  string
	Value                []byte
	Partition            int32
	Offset               int64
	ConsumerErrorStr     string // Consumer執行callback時發生error後回傳的錯誤
	ProducerSentTime     time.Time
	ConsumerReceivedTime time.Time
	ConsumerFinishTime   time.Time
	ProducerReceivedTime time.Time // Producer收到由consumer發送回來的訊息的時間 (error msg, reply msg)
}

func APIMessageFromMessage(message model.Message) (apiMessage Message) {
	glob.ConvertStruct(message, &apiMessage)
	return
}

// Subscriber
type SubscriberCtrl struct {
	cancelFunc context.CancelFunc
	errors     <-chan error
}

func (c *SubscriberCtrl) Stop() {
	c.cancelFunc()
}

func (c *SubscriberCtrl) Errors() <-chan error {
	return c.errors
}
