package service

import (
	"encoding/json"
	"errors"
	"runtime/debug"
	"sync"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"

	"github.com/Shopify/sarama"
)

var ProducerService = &producerService{lock: &sync.Mutex{}}

type producerService struct {
	producer sarama.AsyncProducer
	lock     *sync.Mutex
}

func (p *producerService) Send(topic string, value []byte, opts ...model.Option) (err error) {
	if !core.IsInitialized() {
		return model.ErrNotInitialized
	}

	// Send message
	dis := model.MakeDispatcher(opts)
	err = p.send(topic, value, dis.ProducerEnsureOrder)
	if err != nil {
		return
	}

	// Listen error from consumer
	if dis.ProducerErrHandler != nil {
		go func() {
			err := ConsumerService.Subscribe(glob.ErrTopic(topic), makeErrCallback(dis.ProducerErrHandler))
			if err != nil {
				core.Logger.Error("Error listening on err_topic:", err.Error())
			}
		}()

	}
	return
}

func (p *producerService) send(topic string, value []byte, ensureOrder bool) (err error) {

	// Create topic
	err = TopicService.Create(topic)
	if err != nil {
		return
	}

	// Get/Create producer
	err = p.get()
	if err != nil {
		return
	}

	// Close producer at the end
	defer func() {
		if r := recover(); r != nil {
			core.Logger.Errorf("Closing producer due to panic: %v", r)
			if err = p.producer.Close(); err != nil {
				core.Logger.Errorf("Error closing producer: %v", err.Error())
			}
			p.producer = nil
		}
	}()

	// Send message
	message := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(value)}
	if ensureOrder {
		message.Key = sarama.StringEncoder(topic)
	}
	p.producer.Input() <- message

	// Receive send result
	select {
	// case p.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}:
	case <-p.producer.Successes():
		// core.Logger.Debugf("Message sent: [%v-%v-%v/%v]\n", msg.Topic, msg.Partition, msg.Offset, glob.TrimBytes(value))
	case err = <-p.producer.Errors():
		core.Logger.Errorf("Failed to produce message: %v, len: %v", err, len(value))
	}

	// core.Logger.Debugf("Sent: [%v/%v/%v]\n", topic, string(key[:]), glob.TrimBytes(value))
	return
}

func (p *producerService) get() (err error) {
	if p.producer == nil {
		p.lock.Lock()
		if p.producer == nil {
			p.producer, err = sarama.NewAsyncProducerFromClient(ClientService.Get())
			core.Logger.Debugf("Producer created.")
			if err != nil {
				//panic(err)
				core.Logger.Errorf("Error creating Producer: %s", err)
				p.lock.Unlock()
				return
			}
		}
		p.lock.Unlock()
	}
	return
}

func makeErrCallback(producerErrHandler model.ProducerCustomerErrHandler) model.ConsumerCallback {
	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("Panic on err handler: %v", string(debug.Stack()))
		}
	}()
	return func(value []byte) error {
		var item model.ConsumerCallbackError
		err := json.Unmarshal(value, &item)
		if err != nil {
			core.Logger.Errorf("Error parsing callbackErr: %v", err.Error())
		}
		producerErrHandler(item.Message.Value, errors.New(item.ErrStr))
		return nil
	}
}
