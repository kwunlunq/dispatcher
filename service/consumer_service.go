package service

import (
	"context"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"
)

var ConsumerService = &consumerService{[]string{}}

type consumerService struct {
	subscribedTopics []string
}

func (c *consumerService) Subscribe(topic string, callback model.ConsumerCallback, asyncNum int) {
	c.SubscribeGroup(topic, "", callback, asyncNum)
}

func (c *consumerService) SubscribeGroup(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {
	if asyncNum <= 0 {
		asyncNum = 1
	}
	go c.subscribe(topic, groupID, callback, asyncNum)
}

func (c *consumerService) isExisted(t string) bool {
	for _, topic := range c.subscribedTopics {
		if topic == t {
			return true
		}
	}
	return false
}

func (c *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int) {

	if c.isExisted(topic) {
		return
	}

	if groupID == "" {
		groupID = glob.Config.GroupID
	}

	c.addTopic(topic)

	// Create topic
	TopicService.Create(topic)

	tracer.Tracef(glob.ProjName, "Subscribing to %v", topic)

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

func (c *consumerService) addTopic(topic string) {
	c.subscribedTopics = append(c.subscribedTopics, topic)
}

/*
func (c *consumerService) subscribe(topic string, callback model.ConsumerCallback, asyncNum int) {

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
*/

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
		// time.Sleep(time.Second)
		// if result.Offset%2 == 0 {
		// 	tracer.Tracef("TESTING", " Message skipped [%v-%v/%v]", result.Offset, string(result.Key[:]), glob.TrimBytes(result.Value))
		// 	continue
		// }
		sess.MarkMessage(result, "")
		// tracer.Tracef("TESTING", " Message marked [%v-%v/%v]", result.Offset, string(result.Key[:]), glob.TrimBytes(result.Value))
	}
}
