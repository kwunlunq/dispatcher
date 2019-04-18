package dispatcher

import (
	"sync"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"

	"github.com/Shopify/sarama"
)

var (
	saramaProducer sarama.AsyncProducer
	producerLock   sync.Mutex
)

func Send(topic string, key, value []byte) {

	service.TopicService.Create(topic)

	producer, err := getSaramaProducer()
	if err != nil {
		panic(err)
	}

	defer func() {
		if r := recover(); r != nil {
			tracer.Errorf("dispatcher", "Closing producer due to panic: %v", r)
			if err := producer.Close(); err != nil {
				tracer.Errorf("dispatcher", "Error closing producer: %v", err.Error())
			}
		}
	}()

	// tracer.Tracef(glob.ProjName, " Sending message [%v/%v/%v] ...\n", topic, string(key[:]), string(value[:]))

	select {
	case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(value)}:
	case err := <-producer.Errors():
		tracer.Errorf(glob.ProjName, " Failed to produce message: %v", err)
	}

	tracer.Tracef(glob.ProjName, " Sent: [%v/%v/%v]\n", topic, string(key[:]), string(value[:]))
}

func getSaramaProducer() (p sarama.AsyncProducer, err error) {
	if p == nil {
		producerLock.Lock()
		if p == nil {
			p, err = newSaramaProducer()
			saramaProducer = p
		}
		producerLock.Unlock()
	}
	return
}

func newSaramaProducer() (saramaProducer sarama.AsyncProducer, err error) {

	// On the broker side, you may want to change the following settings to get
	// stronger consistency guarantees:
	// - For your broker, set `unclean.leader.election.enable` to false
	// - For the topic, you could increase `min.insync.replicas`.
	// saramaProducer, err = sarama.NewAsyncProducer(glob.BrokerList, glob.SaramaConfig)
	saramaProducer, err = sarama.NewAsyncProducerFromClient(service.ClientService.Get())

	if err != nil {
		// log.Fatalln("Failed to start Sarama producer:", err)
		tracer.Errorf(glob.ProjName, "Failed to start Sarama producer: %v", err.Error())
	}

	// saramaProducer.Close()
	return
}
