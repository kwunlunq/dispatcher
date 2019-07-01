package dispatcher

import (
	"gitlab.paradise-soft.com.tw/glob/dispatcher/v2/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/v2/model"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/v2/service"
)

// Init start dispatcher with options specified.
func Init(brokers []string, groupID string, opts ...model.Option) error {
	return core.Init(brokers, groupID, opts...)
}

// Send message to subscribers listening on the same topic.
func Send(topic string, value []byte, opts ...model.Option) error {
	return service.ProducerService.Send(topic, value, opts...)
}

// Subscribe receive messages of the specified topic.
func Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) error {
	return service.ConsumerService.Subscribe(topic, callback, opts...)
}
