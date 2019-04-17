package dispatcher

import (
	"context"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"

	"github.com/Shopify/sarama"
)

func Subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {
	if asyncNum <= 0 {
		asyncNum = 1
	}
	go subscribe(topic, groupID, callback, asyncNum)
}

func subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {

	// Start with a client
	// client, err := sarama.NewClient(glob.BrokerList, glob.SaramaConfig)
	// if err != nil {
	// 	panic(err)
	// }

	service.TopicService.Create(topic)

	client := service.ClientService.GetNew()
	// client := service.ClientService.Get()

	// Close client in the end
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
	// Close consumer group in the end
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

		handler := consumerHandler{service.WorkerPoolService.MakeWorkerPool(callback, asyncNum)}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

type consumerHandler struct {
	pool service.WorkerPool
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
