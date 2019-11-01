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
	Value                []byte // 原始訊息
	Partition            int32  // 訊息被分送到的partition
	Offset               int64  // 訊息在partition中的序號
	ConsumerErrorStr     string // Consumer執行callback時發生的error
	ConsumerGroupID      string // 收到這筆訊息的consumer group id
	ProducerSentTime     time.Time
	ConsumerReceivedTime time.Time
	ConsumerFinishTime   time.Time
	ProducerReceivedTime time.Time
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
