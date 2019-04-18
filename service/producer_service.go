package service

import (
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

func (p *producerService) Send(topic string, key, value []byte, errHandler model.ProducerErrHandler) {
	p.send(topic, key, value)
	// ConsummerService.Subscribe(topic, callback, asyncNum)
}

func (p *producerService) send(topic string, key, value []byte) {

	TopicService.Create(topic)
	p.create()

	defer func() {
		if r := recover(); r != nil {
			tracer.Errorf("dispatcher", "Closing producer due to panic: %v", r)
			if err := p.producer.Close(); err != nil {
				tracer.Errorf("dispatcher", "Error closing producer: %v", err.Error())
			}
		}
	}()

	select {
	case p.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}:
	case err := <-p.producer.Errors():
		tracer.Errorf(glob.ProjName, " Failed to produce message: %v", err)
	}

	tracer.Tracef(glob.ProjName, " Sent: [%v/%v/%v]\n", topic, string(key[:]), string(value[:]))
}

func (p *producerService) create() {
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
