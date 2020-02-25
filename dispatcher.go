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

// Subscribe receive messages(dispatcher.Message) of the specified topic.
func SubscribeMessage(topic string, callback func(Message) error, opts ...model.Option) (ctrl *SubscriberCtrl, err error) {
	messageCallback := func(message model.Message) error {
		return callback(APIMessageFromMessage(message))
	}
	c, err := service.ConsumerService.SubscribeMessage(topic, messageCallback, opts...)
	if err != nil {
		return
	}
	ctrl = &SubscriberCtrl{errors: c.ConsumeErrChan, cancelFunc: c.CancelConsume}
	return
}

// SubscribeWithRetry receive messages until error count meet specified number.
func SubscribeWithRetry(topic string, callback model.BytesConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (ctrl *SubscriberWithRetryCtrl) {
	sCtrl := service.ConsumerService.SubscribeWithRetry(topic, callback, failRetryLimit, getRetryDuration, opts...)
	ctrl = &SubscriberWithRetryCtrl{sCtrl: *sCtrl, GroupID: sCtrl.GroupID}
	return
}

// SubscribeWithRetryMessage receive messages(dispatcher.Message) until error count meet specified number.
func SubscribeWithRetryMessage(topic string, callback func(Message) error, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (ctrl *SubscriberWithRetryCtrl) {
	messageCallback := func(message model.Message) error {
		return callback(APIMessageFromMessage(message))
	}
	sCtrl := service.ConsumerService.SubscribeWithRetryMessage(topic, messageCallback, failRetryLimit, getRetryDuration, opts...)
	ctrl = &SubscriberWithRetryCtrl{sCtrl: *sCtrl, GroupID: sCtrl.GroupID}
	return
}

func GetConsumeStatusByGroupID(topic, groupID string) (status ConsumeStatus, err error) {
	var s model.ConsumeStatus
	s, err = service.ConsumeStatusService.Get(topic, groupID)
	status = ConsumeStatus(s)
	return
}

func RemoveTopics(topics ...string) (err error) {
	return service.TopicService.Remove(topics...)
}

// TODO
func GetConsumeStatus(topic string) (list []ConsumeStatus) {
	return
}
