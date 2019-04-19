package dispatcher

import (
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"
)

func Send(topic string, key, value []byte, errHandler model.ProducerCustomerErrHandler) {
	service.ProducerService.Send(topic, key, value, errHandler)
}

func SubscribeGroup(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {
	service.ConsumerService.SubscribeGroup(topic, groupID, callback, asyncNum)
}

func Subscribe(topic string, callback model.ConsumerCallback, asyncNum int) {
	service.ConsumerService.Subscribe(topic, callback, asyncNum)
}
