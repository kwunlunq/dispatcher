package service

import (
	"encoding/json"
	"errors"
	"runtime/debug"
	"sync"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
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
	ConsumerService.Subscribe(glob.ErrTopic(topic), makeErrCallback(customErrHandler), 1)
}

func (p *producerService) send(topic string, key, value []byte) {

	TopicService.Create(topic)
	p.get()

	defer func() {
		if r := recover(); r != nil {
			tracer.Errorf("dispatcher", "Closing producer due to panic: %v", r)
			if err := p.producer.Close(); err != nil {
				tracer.Errorf("dispatcher", "Error closing producer: %v", err.Error())
			}
			p.producer = nil
		}
	}()

	p.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}

	select {
	// case p.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}:
	case msg := <-p.producer.Successes():
		tracer.Errorf(glob.ProjName, " Sent [%v-%v/%v/%v]\n", msg.Topic, msg.Offset, string(key[:]), glob.TrimBytes(value))
	case err := <-p.producer.Errors():
		tracer.Errorf(glob.ProjName, " Failed to produce message: %v", err)
	}

	// tracer.Tracef(glob.ProjName, " Sent: [%v/%v/%v]\n", topic, string(key[:]), glob.TrimBytes(value))
}

func (p *producerService) get() {
	if p.producer == nil {
		p.lock.Lock()
		if p.producer == nil {
			var err error
			p.producer, err = sarama.NewAsyncProducerFromClient(ClientService.Get())
			tracer.Trace(glob.ProjName, " Producer created.")
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
			tracer.Errorf(glob.ProjName, " Panic on err handler: %v", string(debug.Stack()))
		}
	}()
	return func(key, value []byte) (err error) {
		tracer.Trace(glob.ProjName, "Received err from consumer")
		var item model.ConsumerCallbackError
		err = json.Unmarshal(value, &item)
		if err != nil {
			tracer.Errorf(glob.ProjName, "Error parsing callbackErr: %v", err.Error())
			return
		}
		producerErrHandler(item.Message.Key, item.Message.Value, errors.New(item.ErrStr))
		return
	}
}
