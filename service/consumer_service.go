package service

import (
	"context"
	"strings"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
)

var ConsumerService = &consumerService{[]string{}}

type consumerService struct {
	subscribedTopics []string
}

func (c *consumerService) Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) error {
	if !core.IsInitialized() {
		return model.ErrNotInitialized
	}

	dis := model.MakeDispatcher(opts)
	asyncNum := 1
	if dis.ConsumerAsyncNum > 1 {
		asyncNum = dis.ConsumerAsyncNum
	}

	offsetOldest := true
	if dis.ConsumerOmitOldMsg {
		offsetOldest = false
	}

	groupID := core.Config.GroupID
	if strings.TrimSpace(dis.ConsumerGroupID) != "" {
		groupID = dis.ConsumerGroupID
	}

	go c.subscribe(topic, groupID, callback, asyncNum, offsetOldest)

	return nil
}

func (c *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int, offsetOldest bool) {

	if c.isTopicExisted(topic) {
		return
	}
	c.addSubTopics(topic)

	if groupID == "" {
		groupID = core.Config.GroupID
	}

	consumer := c.newConsumer(topic, offsetOldest, groupID)

	// Close consumer group on panic
	defer func() {
		err := consumer.Close()
		if err != nil {
			core.Logger.Errorf("Error closing consumer: %v", err.Error())
		}
	}()

	// Track errors
	go func() {
		for err := range consumer.Errors() {
			core.Logger.Errorf("Consumer consumer err: %v", err.Error())
			// panic(err)
		}
	}()

	core.Logger.Infof("Listening on topic [%v] with groupID [%v] by [%v] workers ...", topic, groupID, asyncNum)

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{topic}

		handler := consumerHandler{WorkerPoolService.MakeWorkerPool(callback, asyncNum, true)}

		err := consumer.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

func (c *consumerService) newConsumer(topic string, offsetOldest bool, groupID string) sarama.ConsumerGroup {
	TopicService.Create(topic)
	time.Sleep(100 * time.Millisecond)

	sconf := core.SaramaConfig
	if offsetOldest {
		sconf.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		sconf.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	group, err := sarama.NewConsumerGroup(core.Config.Brokers, groupID, &sconf)
	core.Logger.Debugf("Consumer created.")
	if err != nil {
		panic(err)
	}

	return group
}

func (c *consumerService) addSubTopics(topic string) {
	c.subscribedTopics = append(c.subscribedTopics, topic)
}

func (c *consumerService) isTopicExisted(t string) bool {
	for _, topic := range c.subscribedTopics {
		if topic == t {
			return true
		}
	}
	return false
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

	core.Logger.Debugf("Consumer claim [init/high offset: %v/%v, topic: %v, partition: %v]", claim.InitialOffset(), claim.HighWaterMarkOffset(), claim.Topic(), claim.Partition())

	// Receive processed messages
	go h.markMessage(sess)

	// Process messages
	h.claimMessage(claim)

	core.Logger.Debugf("Finished consuming claim.")
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
