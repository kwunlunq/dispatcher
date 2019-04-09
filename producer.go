package dispatcher

import (
	"log"
	"sync"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"

	"github.com/Shopify/sarama"
)

var (
	saramaProducer sarama.AsyncProducer
	producerLock   sync.Mutex
)

func Send(topic, key, data string) {

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

	tracer.Infof(projName, "Sending message [%v/%v/%v] ...\n", topic, key, data)

	select {
	case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(data)}:
	case err := <-producer.Errors():
		tracer.Errorf(projName, "Failed to produce message: %v", err)
	}

	tracer.Infof(projName, "Message [%v/%v/%v] sent\n", topic, key, data)
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
	saramaProducer, err = sarama.NewAsyncProducer(brokerList, saramaConfig)

	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// saramaProducer.Close()
	return
}
