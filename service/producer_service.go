package service

import (
	"encoding/json"
	"errors"
	"runtime/debug"
	"sync"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"github.com/Shopify/sarama"
)

var ProducerService = &producerService{lock: &sync.Mutex{}}

type producerService struct {
	producer sarama.AsyncProducer
	lock     *sync.Mutex
}

func (p *producerService) Send(topic string, key, value []byte, customErrHandler model.ProducerCustomerErrHandler) {
	p.send(topic, key, value)
	ConsumerService.SubscribeGroup(glob.ErrTopic(topic), glob.Config.GroupID, makeErrCallback(customErrHandler), 1)
}

func (p *producerService) send(topic string, key, value []byte) {

	TopicService.Create(topic)
	p.get()

	defer func() {
		if r := recover(); r != nil {
			glob.Logger.Errorf("Closing producer due to panic: %v", r)
			if err := p.producer.Close(); err != nil {
				glob.Logger.Errorf("Error closing producer: %v", err.Error())
			}
			p.producer = nil
		}
	}()

	p.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}

	select {
	// case p.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}:
	case msg := <-p.producer.Successes():
		glob.Logger.Debugf(" Message sent: [%v-%v-%v/%v/%v]\n", msg.Topic, msg.Partition, msg.Offset, string(key[:]), glob.TrimBytes(value))
	case err := <-p.producer.Errors():
		glob.Logger.Errorf(" Failed to produce message: %v, len: %v", err, len(value))
	}

	// glob.Logger.Debugf(" Sent: [%v/%v/%v]\n", topic, string(key[:]), glob.TrimBytes(value))
}

func (p *producerService) get() {
	if p.producer == nil {
		p.lock.Lock()
		if p.producer == nil {
			var err error
			p.producer, err = sarama.NewAsyncProducerFromClient(ClientService.Get())
			glob.Logger.Debugf(" Producer created.")
			if err != nil {
				panic(err)
			}
		}
		p.lock.Unlock()
	}
	return
}

func makeErrCallback(producerErrHandler model.ProducerCustomerErrHandler) model.ConsumerCallback {
	defer func() {
		if err := recover(); err != nil {
			glob.Logger.Errorf(" Panic on err handler: %v", string(debug.Stack()))
		}
	}()
	return func(key, value []byte) error {
		// glob.Logger.Debugf("Received err from consumer")
		var item model.ConsumerCallbackError
		err := json.Unmarshal(value, &item)
		if err != nil {
			glob.Logger.Errorf("Error parsing callbackErr: %v", err.Error())
		}
		producerErrHandler(item.Message.Key, item.Message.Value, errors.New(item.ErrStr))
		return nil
	}
}
