package dispatcher

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	saramaProducer sarama.AsyncProducer
	producerLock   sync.Mutex
)

type ProducerMessage struct {
}

type Producer interface {
	SendMessage(data interface{})
}

type asyncProducer struct {
	sarama.AsyncProducer
}

func (this asyncProducer) SendMessage(data interface{}) {

}

type ProducerOption interface {
	apply(*producerOption)
}

type producerOption struct {
}

// func (p *asyncProducer) SendMessage(msg *ProducerMessage) {
// 	var saramaProducer sarama.SyncProducer = sarama.SyncProducer(*p)
// 	pMsg := sarama.ProducerMessage(*msg)
// 	saramaProducer.SendMessage(&pMsg)
// }

func Send(topic, key, data string) {
	producer, err := getSaramaProducer()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Printf("Sending message [%v/%v/%v] ...\n", topic, key, data)

	var enqueued, errors int
	// ProducerLoop:
	// 	for {
	select {
	case producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(data)}:
		enqueued++
	case err := <-producer.Errors():
		log.Println("Failed to produce message", err)
		errors++
		// case <-signals:
		// 	break ProducerLoop
	}
	// }

	// log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

func getSaramaProducer() (p sarama.AsyncProducer, err error) {
	if p == nil {
		producerLock.Lock()
		if p == nil {
			p, err = NewSaramaProducer()
			saramaProducer = p
		}
		producerLock.Unlock()
	}
	return
}

func NewSaramaProducer() (saramaProducer sarama.AsyncProducer, err error) {

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

func NewProducer(opts ...ProducerOption) Producer {
	producer, err := NewSaramaProducer()
	log.Fatal(err)
	return asyncProducer{producer}
}
