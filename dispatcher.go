package dispatcher

import (
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"
)

func Send(topic string, key, value []byte, errHandler model.ProducerErrHandler) {
	service.ProducerService.Send(topic, key, value, errHandler)
}

func SubscribeGroup(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {
	service.ConsummerService.SubscribeGroup(topic, groupID, callback, asyncNum)
}

func Subscribe(topic string, callback model.ConsumerCallback, asyncNum int) {
	service.ConsummerService.Subscribe(topic, callback, asyncNum)
}
