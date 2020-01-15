package dispatcher

import (
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/service"
	"time"
)

// Init start dispatcher with options specified.
func Init(brokers []string, opts ...model.Option) error {
	return service.Init(brokers, opts)
}

// Send message to subscribers listening on the same topic.
func Send(topic string, value []byte, opts ...model.Option) error {
	return service.ProducerService.Send(topic, value, opts...)
}

// Subscribe receive messages of the specified topic.
func Subscribe(topic string, callback model.BytesConsumerCallback, opts ...model.Option) (ctrl *SubscriberCtrl, err error) {
	c, err := service.ConsumerService.Subscribe(topic, callback, opts...)
	if err != nil {
		return
	}
	ctrl = &SubscriberCtrl{errors: c.ConsumeErrChan, cancelFunc: c.CancelConsume}
	return
}

// SubscribeWithRetry receive messages until error count meet specified number.
func SubscribeWithRetry(topic string, callback model.BytesConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (ctrl *SubscriberWithRetryCtrl) {
	sCtrl := service.ConsumerService.SubscribeWithRetry(topic, callback, failRetryLimit, getRetryDuration, opts...)
	ctrl = &SubscriberWithRetryCtrl{sCtrl: *sCtrl}
	return
}
