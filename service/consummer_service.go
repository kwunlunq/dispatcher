package service

import (
	"context"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"
)

var ConsummerService = &consummerService{}

type consummerService struct{}

func (c consummerService) SubscribeGroup(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {
	if asyncNum <= 0 {
		asyncNum = 1
	}
	go c.subscribeGroup(topic, groupID, callback, asyncNum)
}

func (c consummerService) Subscribe(topic string, callback model.ConsumerCallback, asyncNum int) {
	if asyncNum <= 0 {
		asyncNum = 1
	}
	go c.subscribe(topic, callback, asyncNum)
}

func (c consummerService) subscribeGroup(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {

	// Create topic
	TopicService.Create(topic)

	// Create client
	client := ClientService.GetNew()

	// Close client on panic
	defer func() {
		err := client.Close()
		if err != nil {
			tracer.Errorf(glob.ProjName, "Error closing client: %v", err.Error())
		}
	}()

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		panic(err)
	}

	// Close consumer group on panic
	defer func() {
		err := group.Close()
		if err != nil {
			tracer.Errorf(glob.ProjName, "Error closing group: %v", err.Error())
		}
	}()

	// Track errors
	go func() {
		for err := range group.Errors() {
			tracer.Errorf(glob.ProjName, "Consumer group err: %v", err.Error())
			// panic(err)
		}
	}()

	tracer.Infof(glob.ProjName, " Listening on topic [%v] with groupID [%v] by [%v] workers ...\n", topic, groupID, asyncNum)

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{topic}

		handler := consumerHandler{WorkerPoolService.MakeWorkerPool(callback, asyncNum, true)}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

func (c consummerService) subscribe(topic string, callback model.ConsumerCallback, asyncNum int) {

	// Create topic
	TopicService.Create(topic)

	// Create client
	client := ClientService.GetNew()

	// Close client on panic
	defer func() {
		err := client.Close()
		if err != nil {
			tracer.Errorf(glob.ProjName, "Error closing client: %v", err.Error())
		}
	}()

	// Start a new consumer group
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		panic(err)
	}

	// Close consumer group on panic
	defer func() {
		err := consumer.Close()
		if err != nil {
			tracer.Errorf(glob.ProjName, "Error closing group: %v", err.Error())
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, glob.Config.SaramaConfig.Consumer.Offsets.Initial)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			tracer.Errorf(glob.ProjName, "Err closing partitionConsumer: %v", err.Error())
		}
	}()

	tracer.Infof(glob.ProjName, " Listening on topic [%v] by [%v] workers ...\n", topic, asyncNum)

	pool := WorkerPoolService.MakeWorkerPool(callback, asyncNum, false)

	for msg := range partitionConsumer.Messages() {
		pool.AddJob(msg)
	}
}

type consumerHandler struct {
	pool WorkerPool
}

func (consumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	tracer.Trace(glob.ProjName, " Consumer is ready.")

	// Receive processed messages
	go h.markMessage(sess)

	// Process messages
	h.claimMessage(claim)

	tracer.Trace(glob.ProjName, " Finished consuming claim.")
	return nil
}

func (h consumerHandler) claimMessage(claim sarama.ConsumerGroupClaim) {
	for msg := range claim.Messages() {
		h.pool.AddJob(msg)
	}
}

func (h consumerHandler) markMessage(sess sarama.ConsumerGroupSession) {
	for result := range h.pool.Results() {
		sess.MarkMessage(result, "")
	}
}
